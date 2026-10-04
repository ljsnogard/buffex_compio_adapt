//! 出向适配：把**任意** compio 设备接上 `buffex` 环，变成
//! [`TrBuffWrite`](abs_buff::TrBuffWrite) / [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite)。
//!
//! # 形态：环半部 + 内部 spawn 的后台泵
//!
//! [`BuffWrite`] 自己持有一份环容器、一个写半部（调用方用它写入）和**一个后台泵任务**：
//!
//! * 调用方经 `TrBuffWrite` / `TrBuffTryWrite` **直接往环里写段**——实现是把请求原样
//!   委托给 [`RingWriter`]，future 用 ring 自己的 [`RingWriteAsync`]，取消与 park 语义
//!   全部由 ring 提供；
//! * 「环 → 设备」的搬运完全在后台：泵循环「借读段（环空则 park）→ 写设备 → 提交」；
//! * **段 drop 只提交进环，不等于已经写进设备**：调用方收尾要用
//!   [`BuffWrite::flush_async`]（等环被泵取空）或 [`BuffWrite::close_async`]
//!   （请求泵排空 + `shutdown` 并等它退出）。
//!
//! # 关键不变式：段要握到设备写完再 drop
//!
//! 泵从借出读段到设备写完之间**不 drop 段**。ring 只在该段 drop 时按已写出量
//! `advance_read`，于是：
//!
//! ```text
//! ring.data_size() == 0  ⟺  调用方写进环的字节已经全部被设备收下
//! ```
//!
//! [`BuffWrite::flush_async`] 正是建立在这条判据上：它用一个下限等于**满容量**的
//! `Demand` 借写段——只有 `free_size == capacity`（即 `data_size == 0`）时才借得到。
//!
//! # 收尾与错误
//!
//! * `close_async` 先 `tx_.close()`（置 `PRODUCER_CLOSED` 并唤醒 park 中的泵），泵把剩余
//!   数据交付、`shutdown` 设备后退出，调用方 await 它的 `JoinHandle`；
//! * 设备出错时泵记错误并退出，其守卫把消费端置为「已关闭」，于是调用方的
//!   `write_async` / `flush_async` 拿到 `ProducerError::Closing`（详情经
//!   [`BuffWrite::take_error`] 取回），不会在没人消费的环上永久 park。

use core::{borrow::Borrow, marker::PhantomData};

use std::rc::Rc;

use compio::{io::AsyncWrite, runtime::JoinHandle};

use abs_buff::{
    Demand,
    buffer::TrBuffSegmRef,
    error::WriteErrTag,
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrMayCancel;
use anylr::{SomeOf, some_of::SomeLR};
use buffex::{
    ring::{ProducerError, Ring, RingReader, RingSegmMut, RingWriteAsync, RingWriter},
    x_deps::abs_buff,
};

use crate::{
    alloc_::{DefaultAllocConfig, TrAllocConfig},
    device_::WriteAsOutputOwned,
    shared_::{
        CloseOnDrop_, Ctl_, RingBufOf, SharedRingOf, TryNewError, alloc_ring_, in_runtime_,
        split_ring_,
    },
};

/// 出向适配器的写半部类型（容器与缓冲本体由配置 `C` 决定）。
pub type WriterOf<C, S> = RingWriter<S, RingBufOf<C>, u8>;

/// 出向适配器的读半部类型（由后台泵持有）。
pub type ReaderOf<C, S> = RingReader<S, RingBufOf<C>, u8>;

/// 把**任意** compio [`AsyncWrite`] 设备接上 `buffex` 环的写端适配器。
///
/// 实现 [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite) 与
/// [`TrBuffWrite`](abs_buff::TrBuffWrite)（都是对环半部的委托）；环内数据由**构造时
/// spawn 的后台泵**自动搬给设备。
///
/// # 构造前提
///
/// 与 [`BuffRead`](crate::BuffRead) 相同：必须在正在运行的 compio runtime 内构造
/// （否则返回 [`TryNewError::NotInRuntime`]），设备被移入后台泵（`W: 'static`）。
///
/// # 类型参数
///
/// * `W`——设备类型（已移入后台泵）；
/// * `C`——分配器配置（默认 [`DefaultAllocConfig`]）；
/// * `S`——环的共享容器（默认 `mm_ptr::Shared`）。
///
/// # 收尾与 `drop`
///
/// * [`flush_async`](Self::flush_async) 保证「此刻已提交进环的数据都被设备收下」；
/// * [`close_async`](Self::close_async) 在其之上 `shutdown` 设备并等后台泵退出；
/// * **`drop` 适配器会取消后台泵**：环里还没送出的数据随之丢失，设备也不会被 `shutdown`。
///   需要「送达」语义时必须显式收尾。
///
/// # Examples
///
/// ```no_run
/// #![feature(allocator_ext)]
/// use buffex_compio_adapt::{BuffWrite, DefaultAllocConfig};
/// use compio::net::UnixStream;
///
/// # async fn demo(dev: UnixStream) {
/// let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
///     dev,
///     4096,
///     DefaultAllocConfig,
/// )
/// .expect("在 compio runtime 内且容量合法");
/// // 写入后记得收尾（把环内数据真正写进设备）：
/// tx.close_async().await.expect("写方向应正常关闭");
/// # }
/// ```
pub struct BuffWrite<W, C = DefaultAllocConfig, S = SharedRingOf<C>>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 环的共享容器（查容量 / 状态；泵与写半部各持一份克隆）。
    ring_: S,
    /// 调用方这一侧的写半部。
    tx_: RingWriter<S, RingBufOf<C>, u8>,
    /// 设备错误暂存（泵写、本类型读）。
    ctl_: Rc<Ctl_>,
    /// 后台泵任务；`Drop` 即取消。
    pump_: Option<JoinHandle<()>>,
    /// 设备类型参数（设备本身已移入后台泵）。
    _dev_: PhantomData<fn() -> W>,
}

// 同 `BuffRead`：适配器自身不依赖固定地址（环由 `Shared` 分配，地址稳定）。
impl<W, C, S> Unpin for BuffWrite<W, C, S>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
}

// 同 `BuffRead`：`B = Owned<[MaybeUninit<u8>], _>` 从不满足 `B: Debug`，故手写。
impl<W, C, S> core::fmt::Debug for BuffWrite<W, C, S>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BuffWrite")
            .field("ctl_", &self.ctl_)
            .field("pump_finished", &self.pump_.as_ref().map(|p| p.is_finished()))
            .finish_non_exhaustive()
    }
}

impl<W, C> BuffWrite<W, C, SharedRingOf<C>>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
{
    /// 用「设备 + 环容量 + 分配器配置」构造写端适配器，并**立即启动后台泵**。
    ///
    /// # Errors
    ///
    /// * [`TryNewError::NotInRuntime`]——当前线程没有运行中的 compio runtime；
    /// * [`TryNewError::InvalidCapacity`]——`cap` 不在 ring 允许的容量区间内。
    pub fn try_new(dev: W, cap: usize, cfg: C) -> Result<Self, TryNewError> {
        if !in_runtime_() {
            return Result::Err(TryNewError::NotInRuntime);
        }
        let ring = alloc_ring_(cap, &cfg)?;
        let (tx, rx) = split_ring_(ring.clone());
        let ctl = Rc::new(Ctl_::new_());
        // 同 `BuffRead`：显式泛型实参，绕开投影类型下的 E0284。
        let pump = compio::runtime::spawn(write_pump_::<W, C, SharedRingOf<C>>(
            dev,
            rx,
            ring.clone(),
            ctl.clone(),
        ));
        Result::Ok(BuffWrite {
            ring_: ring,
            tx_: tx,
            ctl_: ctl,
            pump_: Option::Some(pump),
            _dev_: PhantomData,
        })
    }
}

impl<W, C, S> BuffWrite<W, C, S>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 取回最近一次设备的写错误及其标签（取走即清空）。
    pub fn take_error(&mut self) -> Option<(std::io::Error, WriteErrTag)> {
        self.ctl_.take_write_err_()
    }

    /// 环容量。
    pub fn capacity(&self) -> usize {
        self.ring_.borrow().capacity()
    }

    /// 环里还有多少已提交、尚未被设备收下的字节。
    pub fn data_size(&self) -> usize {
        self.ring_.borrow().data_size()
    }

    /// 后台泵是否已退出（设备出错 / 收尾完成 / 被取消）。
    pub fn is_pump_finished(&self) -> bool {
        match self.pump_ {
            Option::Some(ref pump) => pump.is_finished(),
            Option::None => true,
        }
    }

    /// 把环里已提交的数据尽量搬给设备（异步，可取消）。
    ///
    /// 返回 `Ok(())` 表示**环已被取空**（等价于「已提交数据都被设备收下了」）；
    /// 设备出错 / 消费端关闭时返回 `ProducerError::Closing`（详情经
    /// [`take_error`](Self::take_error) 取回）。
    ///
    /// 调用方在「不再写新数据、但要求已提交数据确实送达设备」时 await 它。
    pub fn flush_async(&mut self) -> BuffWriteFlushAsync<'_, '_, W, C, S> {
        BuffWriteFlushAsync::new(self)
    }

    /// 冲刷剩余数据、`shutdown` 设备（关闭写方向），并等后台泵退出。
    ///
    /// 之后 [`try_write`](abs_buff::TrBuffTryWrite::try_write) 一律返回
    /// `ProducerError::Closing`。
    ///
    /// # Errors
    ///
    /// 设备写 / `shutdown` 出错、或泵任务自身失败时返回 `ProducerError::Closing`
    /// （详情经 [`take_error`](Self::take_error) 取回）。
    pub fn close_async(&mut self) -> BuffWriteCloseAsync<'_, '_, W, C, S> {
        BuffWriteCloseAsync::new(self)
    }
}

impl<W, C, S> abs_buff::TrBuffTryWrite<u8> for BuffWrite<W, C, S>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    type SegmMut<'f>
        = RingSegmMut<'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    type Err = ProducerError<usize>;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        // 委托给环：向设备搬运的进度由后台泵负责，这里只管借段。
        self.tx_.try_write(demand)
    }
}

impl<W, C, S> abs_buff::TrBuffWrite<u8> for BuffWrite<W, C, S>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 直接复用 ring 生成的 future（见 `BuffRead::ReadAsync` 的说明）。
    type WriteAsync<'f>
        = RingWriteAsync<'f, 'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    #[inline]
    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        self.tx_.write_async(demand)
    }
}

/// 「环 → 设备」的后台泵：环空则 park 等数据，设备出错则记状态并退出。
///
/// 正常退出的条件是**生产端已关闭**（`close_async` 置 `PRODUCER_CLOSED`）：此时 ring 的
/// `read_async` 会在环被取空之后给出 `Closing`，泵据此进入收尾。
///
/// 守卫**独占持有**泵的消费端半部 `rx`（`close` 收 `&mut self`，关闭权归半部的所有者），
/// 循环里经由 [`DerefMut`](core::ops::DerefMut) 使用它。
async fn write_pump_<W, C, S>(
    dev: W,
    rx: RingReader<S, RingBufOf<C>, u8>,
    ring: S,
    ctl: Rc<Ctl_>,
) where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + Clone + 'static,
{
    // 守卫独占泵的消费端半部：退出时置 CONSUMER_CLOSED 并唤醒生产者，调用方随即拿到 Closing。
    let mut close_guard = CloseOnDrop_::new_(rx);
    let stage_cap = ring.borrow().capacity();
    let mut output = WriteAsOutputOwned::with_capacity(dev, stage_cap);

    loop {
        // 借读段：环空时 park，由生产者的 advance_write 唤醒。
        let demand = Demand::at_least(1);
        let some = close_guard.read_async(&demand).await;
        let Some(mut segm) = some.pick_left() else {
            // 生产端已关闭且环已取空（`close_async` 的正常收尾路径）。
            break;
        };
        // 注意：段要握到设备写完为止——`move_items_into_output_async` 只推进段内偏移，
        // 真正的 `advance_read` 发生在 `drop(segm)`，`flush_async` 的判据依赖这一点。
        let moved = segm
            .move_items_into_output_async(&mut output, &Demand::at_least(1))
            .await;
        drop(segm);
        match moved.into_inner() {
            SomeLR::Left(_) => {}
            SomeLR::Right(err) => {
                ctl.set_write_err_(err);
                break;
            }
            SomeLR::Both(_n, err) => {
                ctl.set_write_err_(err);
                break;
            }
        }
    }

    // 只有「生产端关闭」这一正常收尾才 shutdown；设备出错时不再打扰它。
    if !ctl.has_write_err_() {
        let mut dev = output.into_inner();
        let _ = dev.shutdown().await;
    }
}

/// `flush_async`：等到环被泵取空（`free == capacity`）为止。
///
/// 借段的下限取**满容量**：`try_write` 只有在 `free_size >= capacity`（即
/// `data_size == 0`）时才会成功，而按 §模块文档的不变式，那一刻「已提交数据都已被设备
/// 收下」。等待本身复用 ring 的生产者等待槽（由 `advance_read` 唤醒），不需要任何自造
/// 的通知原语。
// 生命周期 `'f` 在函数体里只出现一次，但它是 `gen_may_cancel_future` 生成类型的
// 泛型元数的一部分，不能省略（省略会改变生成 future 的类型形状）。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(BuffWriteFlush, pub, new(pub(crate)))]
async fn buff_write_flush_async_<'f, W, C, S, K>(
    me: &'f mut BuffWrite<W, C, S>,
    cancel: K,
) -> Result<(), ProducerError<usize>>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
    K: abs_cancel::TrCancellationToken,
{
    if me.ctl_.has_write_err_() {
        return Result::Err(ProducerError::Closing);
    }
    let cap = me.ring_.borrow().capacity();
    let demand = Demand::at_least(cap);
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ProducerError::Cancelled);
        }
        // 先试一次同步借段：环已空就直接成功。
        match me.tx_.try_write(&demand).into_inner() {
            SomeLR::Left(segm) => {
                drop(segm); // 提交 0 字节：只是把「借到的那一刻」当作冲刷完成
                return Result::Ok(());
            }
            // 环里还有数据 / 泵正握着段：等泵提交消费量后唤醒。
            SomeLR::Right(ProducerError::Stuffed(_)) => {}
            SomeLR::Right(err) => return Result::Err(err),
            SomeLR::Both(_, err) => return Result::Err(err),
        }
        let some = me
            .tx_
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        match some.into_inner() {
            SomeLR::Left(segm) => {
                drop(segm);
                return Result::Ok(());
            }
            SomeLR::Right(ProducerError::Stuffed(_)) => continue,
            SomeLR::Right(err) => return Result::Err(err),
            SomeLR::Both(_, err) => return Result::Err(err),
        }
    }
}

/// `close_async`：关闭生产端 → 等泵排空剩余数据、`shutdown` 设备并退出。
///
/// 幂等：泵的 `JoinHandle` 只会被取走一次，第二次调用直接看错误槽返回。
// 同 `buff_write_flush_async_`：`'f` 不能省略。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(BuffWriteClose, pub, new(pub(crate)))]
async fn buff_write_close_async_<'f, W, C, S, K>(
    me: &'f mut BuffWrite<W, C, S>,
    _cancel: K,
) -> Result<(), ProducerError<usize>>
where
    W: AsyncWrite + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
    K: abs_cancel::TrCancellationToken,
{
    // ① 不再有新数据：置 PRODUCER_CLOSED 并唤醒 park 中的泵；此后 try_write 一律 Closing。
    me.tx_.close();
    // ② 等泵把环里剩余数据交给设备、shutdown 之后退出。
    //    `JoinHandle` 完成后再 poll 会 panic，因此只能用 `Option::take` 取一次。
    if let Option::Some(pump) = me.pump_.take()
        && let Result::Err(join_err) = pump.await
    {
        // 泵 panic / 被取消：记进错误槽，调用方经 `take_error` 能看到原因。
        me.ctl_.set_pump_err_(join_err);
    }
    if me.ctl_.has_write_err_() {
        return Result::Err(ProducerError::Closing);
    }
    Result::Ok(())
}
