//! 两个适配器共用的装配件与容器助手。
//!
//! * [`RingBufOf`] / [`SharedRingOf`]——把 [`TrAllocConfig`](crate::TrAllocConfig)
//!   的分配点摊成具体类型；
//! * [`alloc_ring_`]——按容量与配置里的分配器造一个元素为 `u8` 的环；
//! * [`split_ring_`]——把 `Shared<Ring<…>>` 拆成一对**可长期持有**的半部；
//! * [`Ctl_`]——设备错误暂存（泵写、适配器读）；
//! * [`CloseOnDrop_`]——泵的「退出即关闭自己那一端」守卫（独占持有泵手里的半部）；
//! * [`TrCloseHalf_`]——半部的「关闭本端」能力，[`CloseOnDrop_`] 据此在 `Drop` 里收尾；
//! * [`TryNewError`]——构造期的两类错误（容量越界 / 不在 compio runtime 内）。

use core::{
    borrow::{Borrow, BorrowMut},
    cell::{Cell, RefCell},
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};
use std::io;

use abs_buff::error::{ReadErrTag, TaggedError, TrErrorWrapper, WriteErrTag};
use buffex::{
    ring::{Ring, RingReader, RingWriter},
    x_deps::abs_buff,
};
use mm_ptr::{Owned, Shared};

use crate::alloc_::TrAllocConfig;

/// 某个配置下的环缓冲本体类型：`mm_ptr::Owned<[MaybeUninit<u8>], C::RingBodyAlloc>`。
///
/// 元素固定 `u8`；本体用 `mm_ptr::Owned`（而不是 `Box`）是为了不依赖 `std` 的智能指针，
/// 它由 `Owned::new_uninit_slice` 在 `allocator_ext` 上分配，容量校验交给 `Ring::try_new`。
pub type RingBufOf<C> = Owned<[MaybeUninit<u8>], <C as TrAllocConfig>::RingBodyAlloc>;

/// 某个配置下的共享容器类型：`mm_ptr::Shared<Ring<RingBufOf<C>, u8>, C::RingSharedAlloc>`。
///
/// `buffex` 自己不依赖 `alloc`，共享容器由调用方决定；这里给 `mm_ptr::Shared`
/// （非 `std` 的引用计数指针），分配器同样由配置选。
pub type SharedRingOf<C> =
    Shared<Ring<RingBufOf<C>, u8>, <C as TrAllocConfig>::RingSharedAlloc>;

/// 构造两个适配器时的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryNewError {
    /// 容量不在 ring 允许的区间内（见 `Ring::check_buffer_size`）。
    InvalidCapacity(usize),
    /// 当前线程没有正在运行的 compio runtime：后台泵需要 `spawn`。
    NotInRuntime,
}

impl core::fmt::Display for TryNewError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            TryNewError::InvalidCapacity(cap) => {
                write!(f, "容量 {cap} 不在 ring 允许的区间内")
            }
            TryNewError::NotInRuntime => {
                write!(f, "不在 compio runtime 内：后台泵需要 spawn，请在 Runtime 里构造")
            }
        }
    }
}

impl core::error::Error for TryNewError {}

/// 适配器的设备错误暂存。
///
/// compio 的 `Runtime` 是 thread-local 的，适配器与其后台泵始终在同一个线程、同一个
/// 执行体上，因此这里用 `Rc<RefCell<…>>` 就够，**不需要原子**：泵写、适配器取走。
///
/// 读侧与写侧共用一个结构（两侧的适配器是各自独立的实例，不会互相串台）。
#[derive(Debug, Default)]
pub(crate) struct Ctl_ {
    /// 设备读错误（含 `Closing` 这一类「正常结束」的标签），经 `take_error` 取走即清空。
    read_err_: RefCell<Option<(io::Error, ReadErrTag)>>,
    /// 设备写错误，经 `take_error` 取走即清空。
    write_err_: RefCell<Option<(io::Error, WriteErrTag)>>,
    /// 设备是否已经正常结束（读到 EOF）。它**不是**错误，因此单独记一个标志。
    read_ended_: Cell<bool>,
}

impl Ctl_ {
    /// 建立空的错误暂存。
    pub(crate) fn new_() -> Self {
        Ctl_::default()
    }

    /// 记录一次设备读结束 / 出错。
    ///
    /// `ReadErrTag::Closing` 表示**设备正常结束（EOF）**：只置结束标志，不进错误槽，
    /// 免得调用方把正常收尾当成设备故障。其余标签按设备错误记录（后到的覆盖先到的）。
    pub(crate) fn set_read_err_(&self, err: TaggedError<io::Error, ReadErrTag>) {
        if err.tag() == ReadErrTag::Closing {
            self.read_ended_.set(true);
            return;
        }
        *self.read_err_.borrow_mut() = Some(take_read_err_(err));
    }

    /// 设备是否已经正常结束（读到过 EOF）。
    pub(crate) fn read_ended_(&self) -> bool {
        self.read_ended_.get()
    }

    /// 取走设备读错误（取走即清空）。
    pub(crate) fn take_read_err_(&self) -> Option<(io::Error, ReadErrTag)> {
        self.read_err_.borrow_mut().take()
    }

    /// 记录一次设备写错误。
    pub(crate) fn set_write_err_(&self, err: TaggedError<io::Error, WriteErrTag>) {
        *self.write_err_.borrow_mut() = Some(take_write_err_(err));
    }

    /// 取走设备写错误（取走即清空）。
    pub(crate) fn take_write_err_(&self) -> Option<(io::Error, WriteErrTag)> {
        self.write_err_.borrow_mut().take()
    }

    /// 是否已经记录过设备写错误（`close_async` 据此把结果映射成 `Closing`）。
    pub(crate) fn has_write_err_(&self) -> bool {
        self.write_err_.borrow().is_some()
    }

    /// 记录**泵任务自身**的失败（panic / 被取消），与设备错误共用同一个槽位。
    ///
    /// 这样调用方无论是「设备坏了」还是「泵炸了」，都能通过 `take_error` 看到原因，
    /// 而返回值统一是 `Closing`。
    pub(crate) fn set_pump_err_(&self, err: compio::runtime::JoinError) {
        *self.write_err_.borrow_mut() = Some((io::Error::from(err), WriteErrTag::Unknown));
    }
}

/// 从设备带标签错误里取出 `std::io::Error` 与标签（`propagated_err` 由 `abs_buff` 提供）。
fn take_read_err_(err: TaggedError<io::Error, ReadErrTag>) -> (io::Error, ReadErrTag) {
    let tag = err.tag();
    match TrErrorWrapper::propagated_err(err) {
        Result::Ok(e) => (e, tag),
        // 理论上设备错误都带 `Propagated` 标签；取不到内层错误就把整个标签错误
        // 包成 `io::Error`，保证信息不丢。
        Result::Err(other) => (io::Error::other(other), tag),
    }
}

/// 见 [`take_read_err_`]。
fn take_write_err_(err: TaggedError<io::Error, WriteErrTag>) -> (io::Error, WriteErrTag) {
    let tag = err.tag();
    match TrErrorWrapper::propagated_err(err) {
        Result::Ok(e) => (e, tag),
        Result::Err(other) => (io::Error::other(other), tag),
    }
}

/// 环半部的「关闭本端」能力：由持有半部的一方实现，供 [`CloseOnDrop_`] 调用。
///
/// `buffex` 把关闭权收归到 `&mut self` 的持有者（`RingWriter::close` /
/// `RingReader::close` 都收 `&mut self`），因此「谁能关闭」等价于「谁独占拥有半部」。
/// 本 trait 只是把两个半部各自的 `close` 收敛成一个名字，好让守卫对二者一视同仁。
pub(crate) trait TrCloseHalf_ {
    /// 关闭本半部代表的那一端。
    fn close_half_(&mut self);
}

impl<S, B> TrCloseHalf_ for RingWriter<S, B, u8>
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>,
{
    fn close_half_(&mut self) {
        self.close()
    }
}

impl<S, B> TrCloseHalf_ for RingReader<S, B, u8>
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>,
{
    fn close_half_(&mut self) {
        self.close()
    }
}

/// 后台泵的守卫：**无论正常返回、被取消还是 panic**，退出时都把自己那一端置为「已关闭」。
///
/// 为什么需要它：泵是唯一的搬运方，一旦它退出（设备结束 / 出错 / 适配器被 drop），
/// 对端就再也不会有任何进度。没有这条守卫，正在 park 的调用方会永久挂起；
/// 有了它，调用方一定会被唤醒并拿到 `Closing`。另外 `buffex` 的
/// [`RingWriter`] / [`RingReader`] **自身没有 `Drop`**，因此本守卫是泵退出时唯一的
/// 关闭者，不是可有可无的保险。
///
/// # 为什么是「拥有半部」而不是「再克隆一份共享容器」
///
/// `buffex` 的 `close` 收 `&mut self`：关闭权归**独占持有半部**的一方。共享容器
/// （`mm_ptr::Shared` 等）只实现 `Borrow`，给出的是 `&Ring`，拿不到 `&mut Ring`；
/// 就算给它补上 `BorrowMut`，守卫活着时泵的半部与适配器手里的对端半部也仍然存在，
/// 借出 `&mut Ring` 就是对同一条环同时存在 `&mut` 与其它活引用——正是关闭权收紧
/// 想要排除的别名场景。
///
/// 于是这里让守卫**直接拥有**泵那一份半部：半部本来就由泵独占，`Drop` 拿到
/// `&mut self` 后合法地转交 `&mut` 给半部，关闭权与所有权自然对齐。泵对半部的
/// 使用经 [`Deref`] / [`DerefMut`] 转发，写法与直接持有时完全一致。
pub(crate) struct CloseOnDrop_<H: TrCloseHalf_> {
    /// 被守卫独占的半部。
    half_: H,
}

impl<H: TrCloseHalf_> CloseOnDrop_<H> {
    /// 用泵自己那份半部建立守卫；退出时关闭的就是这一端。
    pub(crate) fn new_(half: H) -> Self {
        CloseOnDrop_ { half_: half }
    }
}

impl<H: TrCloseHalf_> Deref for CloseOnDrop_<H> {
    type Target = H;

    fn deref(&self) -> &H {
        &self.half_
    }
}

impl<H: TrCloseHalf_> DerefMut for CloseOnDrop_<H> {
    fn deref_mut(&mut self) -> &mut H {
        &mut self.half_
    }
}

impl<H: TrCloseHalf_> Drop for CloseOnDrop_<H> {
    fn drop(&mut self) {
        // 关闭权归本守卫独占的半部；这里只是把 `&mut` 转交给它。
        self.half_.close_half_()
    }
}

/// 当前线程是否在 compio runtime 内（后台泵要用 `spawn`）。
///
/// 用 `try_with_current` 而不是 `try_current`：前者不做 runtime 的引用计数克隆，
/// 只回答「有没有」。
pub(crate) fn in_runtime_() -> bool {
    compio::runtime::Runtime::try_with_current(|_| ()).is_ok()
}

/// 按容量与配置造一个元素为 `u8` 的环，并装进配置指定的共享容器。
///
/// 缓冲本体取 `C::RingBodyAlloc`、共享容器取 `C::RingSharedAlloc`（两者可以是不同的
/// 分配器）。
///
/// # Errors
///
/// 容量被 `Ring::try_new` 拒绝时返回 [`TryNewError::InvalidCapacity`]。
pub(crate) fn alloc_ring_<C>(
    cap: usize,
    cfg: &C,
) -> Result<SharedRingOf<C>, TryNewError>
where
    C: TrAllocConfig,
{
    let buff = Owned::<[MaybeUninit<u8>], C::RingBodyAlloc>::new_uninit_slice(
        cap,
        cfg.ring_body_alloc(),
    );
    match Ring::try_new(buff) {
        Result::Ok(ring) => Result::Ok(Shared::new(ring, cfg.ring_shared_alloc())),
        Result::Err(cap) => Result::Err(TryNewError::InvalidCapacity(cap)),
    }
}

/// 从 `Shared` 容器拆出的读 / 写半部（容器具体类型保留）。
pub(crate) type HalfPairOf<B, A> = (
    RingWriter<Shared<Ring<B, u8>, A>, B, u8>,
    RingReader<Shared<Ring<B, u8>, A>, B, u8>,
);

/// 把环容器拆成一对半部（`unsafe` 入口的安全封装）。
///
/// `Ring` 的两个半部共享同一块 `UnsafeCell` 缓冲，**SPSC（单生产者 / 单消费者）**
/// 是安全前提。这里由本 crate 保证排他性：
///
/// * 容器是本函数调用方**刚刚创建**的 `Shared<Ring<…>>`（[`alloc_ring_`] 的产物），
///   不存在第二个句柄；
/// * 拆分后两个半部只在本 crate 的同一个适配器内核里成对存活，不会被再次传入。
pub(crate) fn split_ring_<B, A>(ring: Shared<Ring<B, u8>, A>) -> HalfPairOf<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]>,
    A: core::alloc::AllocatorClone,
{
    // SAFETY: 见本函数文档——容器刚创建且独占，拆分只发生一次。
    unsafe { Ring::split_unchecked(ring) }
}
