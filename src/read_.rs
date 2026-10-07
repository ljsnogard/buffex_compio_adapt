//! 入向适配：把**任意** compio 设备接上 `buffex` 环，变成
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffTryRead`](abs_buff::TrBuffTryRead)。
//!
//! # 形态：环半部 + 内部 spawn 的后台泵
//!
//! [`BuffRead`] 自己持有一份环容器、一个读半部（调用方用它消费）和**一个后台泵任务**：
//!
//! * 调用方经 `TrBuffRead` / `TrBuffTryRead` **直接消费环里的段**——这两个 trait 的实现
//!   是把请求原样委托给 [`RingReader`]，连 future 类型都用 ring 自己的
//!   [`RingReadAsync`]，因此取消（`may_cancel_with`）、park / 唤醒语义全部由 ring 提供，
//!   本模块一行都不重复实现；
//! * 「设备 → 环」的搬运完全在后台：泵循环「借写段（环满则 park）→ 读设备 → 提交」；
//! * 设备 EOF / 出错时泵退出，其 [`CloseOnDrop_`](crate::shared_::CloseOnDrop_) 守卫把
//!   生产端置为「已关闭」，于是消费者取空环之后拿到 [`ConsumerError::Closing`]，
//!   而不是永久 park。
//!
//! 与 `buffex_tokio_adapt` 的差别只有驱动模型：那里多线程 `spawn` 要求 `Send`，而环的
//! park future 无法被证明 `Send`，所以改成「按需驱动」；compio 的执行体是 thread-local
//! 的、`spawn` **不要求 `Send`**，于是可以走更省事的后台泵。
//!
//! # 错误与 EOF
//!
//! 设备结束 / 出错的详情记在 [`Ctl_`](crate::shared_::Ctl_) 里（泵写、调用方取），
//! 经 [`BuffRead::take_error`] 取回；「设备正常结束」（`ReadErrTag::Closing`）会被记下来
//! 但**不算**设备错误，调用方拿到的仍然只是 `Closing`。
//!
//! # 零拷贝程度
//!
//! compio 的设备接口要求 `'static` 的 owned 缓冲，因此「设备 ↔ 环」之间必然过一次中转
//! 缓冲（见 [`crate::device_`]）；本 crate 用 owned 形态的适配让这次中转**稳态零分配**。
//! 读侧另有**一次**拷贝：为了让「设备给多少就交付多少」成为可能，泵先单独读一次设备、
//! 再把这一批拷进环段（`read_pump_` 的模块文档说明了为什么不能像 tokio / smol 那样
//! 「只 poll 一次搬移」——compio 的设备读是提交型操作，丢弃未完成的 future 会连带丢掉
//! 已完成的读结果）。

use core::{borrow::Borrow, marker::PhantomData, mem::MaybeUninit};
use std::rc::Rc;

use compio::{io::AsyncRead, runtime::JoinHandle};

use abs_buff::{
    Demand,
    buffer::TrBuffSegmMut,
    error::ReadErrTag,
    io::TrInput,
    x_deps::anylr::{self, SomeOf},
};
use buffex::{
    ring::{ConsumerError, Ring, RingReadAsync, RingReader, RingSegmRef, RingWriter},
    x_deps::abs_buff,
};

use crate::{
    alloc_::{DefaultAllocConfig, TrAllocConfig},
    device_::ReadAsInputOwned,
    shared_::{
        CloseOnDrop_, Ctl_, RingBufOf, SharedRingOf, TryNewError, alloc_ring_, in_runtime_,
        split_ring_,
    },
};

/// 入向适配器的读半部类型（容器与缓冲本体由配置 `C` 决定）。
pub type ReaderOf<C, S> = RingReader<S, RingBufOf<C>, u8>;

/// 入向适配器的写半部类型（由后台泵持有）。
pub type WriterOf<C, S> = RingWriter<S, RingBufOf<C>, u8>;

/// 把**任意** compio [`AsyncRead`] 设备接上 `buffex` 环的读端适配器。
///
/// 实现 [`TrBuffTryRead`](abs_buff::TrBuffTryRead) 与
/// [`TrBuffRead`](abs_buff::TrBuffRead)（都是对环半部的委托）；设备数据由**构造时
/// spawn 的后台泵**自动搬进环。
///
/// # 构造前提
///
/// * 必须在**正在运行的 compio runtime 内**构造（后台泵要 `spawn`）：否则返回
///   [`TryNewError::NotInRuntime`]，不会 panic；
/// * 设备会被**移入**后台泵任务，因此 `R: 'static`；适配器本身用
///   `PhantomData<fn() -> R>` 保留设备类型参数，便于与 `buffex_tokio_adapt` 保持同形。
///
/// # 类型参数
///
/// * `R`——设备类型（已移入后台泵）；
/// * `C`——分配器配置（默认 [`DefaultAllocConfig`]：各分配点都用 `CoreAlloc`）；
/// * `S`——环的共享容器（默认 `mm_ptr::Shared`，由配置选分配器）。
///
/// # Examples
///
/// ```no_run
/// #![feature(allocator_ext)]
/// use buffex_compio_adapt::{BuffRead, DefaultAllocConfig};
/// use compio::net::UnixStream;
///
/// # async fn demo(dev: UnixStream) {
/// let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
///     dev,
///     4096,
///     DefaultAllocConfig,
/// )
/// .expect("在 compio runtime 内且容量合法");
/// // 此后按 TrBuffRead / TrBuffTryRead 消费；设备错误经 take_error 取回。
/// let _ = rx.take_error();
/// # }
/// ```
pub struct BuffRead<R, C = DefaultAllocConfig, S = SharedRingOf<C>>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 环的共享容器（查容量 / 状态；泵与读半部各持一份克隆）。
    ring_: S,
    /// 调用方这一侧的读半部。
    rx_: RingReader<S, RingBufOf<C>, u8>,
    /// 设备错误暂存（泵写、本类型读）。
    ctl_: Rc<Ctl_>,
    /// 后台泵任务；`Drop` 即取消（`JoinHandle::drop` ⇒ `task.cancel(true)`）。
    pump_: Option<JoinHandle<()>>,
    /// 设备类型参数（设备本身已移入后台泵）。
    _dev_: PhantomData<fn() -> R>,
}

// 适配器自身不依赖固定地址：`Ring` 虽含 `PhantomPinned`（因此 `RingReader` 是 `!Unpin`），
// 但它由 `Shared` 分配、地址稳定，代码里从不把 `Ring` 按值搬出堆指针。
impl<R, C, S> Unpin for BuffRead<R, C, S>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{}

// 环的两半只有在缓冲本体 `B: Debug` 时才 `Debug`，而 `B` 是
// `Owned<[MaybeUninit<u8>], _>`：`MaybeUninit` 从不是 `Debug`。这里只打印与设备 /
// 错误有关的部分，跳过需要 `B: Debug` 的环内数据。
impl<R, C, S> core::fmt::Debug for BuffRead<R, C, S>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BuffRead")
            .field("ctl_", &self.ctl_)
            .field("pump_finished", &self.pump_.as_ref().map(|p| p.is_finished()))
            .finish_non_exhaustive()
    }
}

impl<R, C> BuffRead<R, C, SharedRingOf<C>>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
{
    /// 用「设备 + 环容量 + 分配器配置」构造读端适配器，并**立即启动后台泵**。
    ///
    /// # Errors
    ///
    /// * [`TryNewError::NotInRuntime`]——当前线程没有运行中的 compio runtime；
    /// * [`TryNewError::InvalidCapacity`]——`cap` 不在 ring 允许的容量区间内。
    pub fn try_new(dev: R, cap: usize, cfg: C) -> Result<Self, TryNewError> {
        if !in_runtime_() {
            return Result::Err(TryNewError::NotInRuntime);
        }
        let ring = alloc_ring_(cap, &cfg)?;
        let (tx, rx) = split_ring_(ring.clone());
        let ctl = Rc::new(Ctl_::new_());
        // 显式给出泛型实参：`S` 要从 `RingWriter<SharedRingOf<C>, …>` 这样的投影类型里
        // 反推时，rustc 无法证明 opaque future 的 `'static`（E0284），写全了就没事。
        let pump = compio::runtime::spawn(read_pump_::<R, C, SharedRingOf<C>>(
            dev,
            tx,
            ring.clone(),
            ctl.clone(),
        ));
        Result::Ok(BuffRead {
            ring_: ring,
            rx_: rx,
            ctl_: ctl,
            pump_: Option::Some(pump),
            _dev_: PhantomData,
        })
    }
}

impl<R, C, S> BuffRead<R, C, S>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 取回最近一次设备的读错误及其标签（取走即清空）。
    ///
    /// 设备正常结束（EOF）也会被记下，标签是 [`ReadErrTag::Closing`]；调用方据此区分
    /// 「正常结束」与真实 IO 错误。
    pub fn take_error(&mut self) -> Option<(std::io::Error, ReadErrTag)> {
        self.ctl_.take_read_err_()
    }

    /// 环容量。
    pub fn capacity(&self) -> usize {
        self.ring_.borrow().capacity()
    }

    /// 当前可读数据量（不借段，仅查状态）。
    pub fn data_size(&self) -> usize {
        self.ring_.borrow().data_size()
    }

    /// 设备是否已经正常结束（读到过 EOF）。
    pub fn is_read_ended(&self) -> bool {
        self.ctl_.read_ended_()
    }

    /// 后台泵是否已退出（设备结束 / 出错 / 被取消）。
    pub fn is_pump_finished(&self) -> bool {
        match self.pump_ {
            Option::Some(ref pump) => pump.is_finished(),
            Option::None => true,
        }
    }
}

impl<R, C, S> abs_buff::TrBuffTryRead<u8> for BuffRead<R, C, S>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    type SegmRef<'f> = RingSegmRef<'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    type Err = ConsumerError<usize>;

    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        // 委托给环：设备那一侧的进度由后台泵负责，这里只管借段。
        self.rx_.try_read(demand)
    }
}

impl<R, C, S> abs_buff::TrBuffRead<u8> for BuffRead<R, C, S>
where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + 'static,
{
    /// 直接复用 ring 生成的 future（本身就是 `gen_may_cancel_future` 的产物，
    /// `may_cancel_with` 指可用），因此这里不需要再造一层可取消包装。
    type ReadAsync<'f>
        = RingReadAsync<'f, 'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        self.rx_.read_async(demand)
    }
}

/// 「设备 → 环」的后台泵：环满则 park 等消费，设备结束 / 出错则记状态并退出。
///
/// 退出路径由 [`CloseOnDrop_`] 守卫兜底（正常返回、被取消、panic 三种情形都会关闭
/// 生产端），因此调用方的 park 一定能醒。守卫**独占持有**泵的生产端半部 `tx`
/// （`close` 收 `&mut self`，关闭权归半部的所有者），循环里经由 [`DerefMut`] 使用它。
///
/// # 「有数据就交付」：为什么每次只读一次设备
///
/// 上游 `move_items_from_input_async` 的语义是**填满**段：设备给出 25 字节后转为空闲时
/// 它只是 `Pending`——数据被扣在段里、一个字节也不提交。对「请求 / 应答」式上层
/// （握手帧、乒乓消息）来说这等于永久静止：对端在等我们应答，而我们还在等它把段填满。
/// 上层协议栈只会把本适配器当 `TrBuffRead` 用，没有「喂满一段」的义务。
///
/// 因此这里把「读设备」与「写进环」拆成两步：**先**单独做一次设备读（compio 的
/// `read` 就是一次读系统调用，读到多少算多少，不 park 等填满），**再**把读到的那一批
/// 拷进环段、逐段提交。代价是 compio 侧本就存在的中转拷贝之外多一次拷贝——换来的是
/// 「上层提交完就能被看到」，而不是「等一段填满才被看到」。
///
/// （对 tokio / smol 侧可以用「只 poll 一次搬移」保住零拷贝；compio 的设备读是**提交
/// 型**操作、丢弃未完成的 future 即取消它，已完成的读结果会跟着丢，所以这里不用那一招。）
async fn read_pump_<R, C, S>(
    dev: R,
    tx: RingWriter<S, RingBufOf<C>, u8>,
    ring: S,
    ctl: Rc<Ctl_>,
) where
    R: AsyncRead + 'static,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>> + Clone + 'static,
{
    // 守卫独占泵的生产端半部：退出时置 PRODUCER_CLOSED 并唤醒消费者。
    let mut close_guard = CloseOnDrop_::new_(tx);
    // 中转缓冲按环容量预置一次，之后一直复用（compio 要求 owned 缓冲，这是那一次拷贝）。
    let stage_cap = ring.borrow().capacity();
    let mut input = ReadAsInputOwned::with_capacity(dev, stage_cap);
    // 一次设备读的落点；容量按环容量预置一次，之后复用。
    let mut staged: Vec<MaybeUninit<u8>> =
        (0..stage_cap).map(|_| MaybeUninit::uninit()).collect();
    let demand = Demand::at_least(1);

    loop {
        // ① 读一次设备：读到多少算多少（`Ok(0)` 会被设备适配层翻成 `Closing`）。
        let read = input.read_async(&mut staged[..]).await;
        let n = match read.into_inner() {
            anylr::some_of::SomeLR::Left(n) => n,
            anylr::some_of::SomeLR::Right(err) => {
                ctl.set_read_err_(err);
                break;
            }
            anylr::some_of::SomeLR::Both(n, err) => {
                ctl.set_read_err_(err);
                if n == 0 {
                    break;
                }
                n
            }
        };
        if n == 0 {
            // 设备适配层不允许「读到 0 且无错误」；真出现就收工，免得空转。
            break;
        }
        // ② 把这一批拷进环：逐段借、逐段提交（环满则 park 等消费者腾空间）。
        let mut off = 0usize;
        while off < n {
            let some = close_guard.write_async(&demand).await;
            let Some(mut segm) = some.pick_left() else {
                // 消费端已关闭（适配器被 drop）：泵没有继续搬运的意义。
                return;
            };
            let moved = segm.move_items_from_as_buff(&staged[off..n]);
            // 段在这里 drop：按已写入量 advance_write，唤醒等待的读端。
            drop(segm);
            if moved == 0 {
                break;
            }
            off += moved;
        }
    }
}
