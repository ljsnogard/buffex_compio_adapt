//! 把**任意** compio [`AsyncRead`](compio::io::AsyncRead) / [`AsyncWrite`](compio::io::AsyncWrite)
//! 设备接上 `buffex` 环，变成 [`TrBuffRead`](abs_buff::TrBuffRead) /
//! [`TrBuffWrite`](abs_buff::TrBuffWrite)。
//!
//! # 它提供什么
//!
//! | 类型 | 方向 | 调用方拿到的 trait |
//! | --- | --- | --- |
//! | [`BuffRead`] | 设备 → 环 | `TrBuffRead` / `TrBuffTryRead` |
//! | [`BuffWrite`] | 环 → 设备 | `TrBuffWrite` / `TrBuffTryWrite` |
//! | [`ReadAsInput`] / [`ReadAsInputOwned`] | 设备 → `TrInput` | `abs_buff` 的设备级抽象 |
//! | [`WriteAsOutput`] / [`WriteAsOutputOwned`] | `TrOutput` → 设备 | 同上 |
//!
//! 两个环适配器各自持有一个 `buffex` 环（`mm_ptr::Shared<Ring<…>>`，经
//! `Ring::split_unchecked` 拆成读写两半）与**一个后台泵任务**：调用方对着环按段读写，
//! 泵负责设备与环之间的搬运。收发双工要的是**两个**适配器（各一个环），因为 ring 的安全
//! 前提是 SPSC。
//!
//! # 驱动模型：构造即 spawn 的后台泵
//!
//! compio 的 `Runtime` 是 **thread-local** 的（`Rc<Executor>` + `Rc<RefCell<Proactor>>`），
//! `compio::runtime::spawn` 只要求 `F: Future + 'static`、**不要求 `Send`**。于是：
//!
//! * 泵可以拥有「设备 + 环半部 + 控制块」进入执行体，调用方的 `try_read` / `try_write`
//!   **不需要**像 `buffex_tokio_adapt` 那样顺手推一轮搬运；
//! * 调用方看到的 `TrBuffRead` / `TrBuffWrite` **完全由 ring 的功能承担**：适配器把请求
//!   原样委托给 `RingReader` / `RingWriter`，连 future 类型都复用 ring 自己的
//!   `RingReadAsync` / `RingWriteAsync`（它们本身就是 `gen_may_cancel_future` 的产物），
//!   因此 `may_cancel_with`、park / 唤醒语义一律由 ring 提供，本 crate 一行都不重复实现。
//!
//! 与 `buffex_tokio_adapt` 的这个差别是**运行时模型**决定的：多线程 `tokio::spawn` 要求
//! `Send`，而环的异步入口 future 无法被证明 `Send`（`TrPark::ParkAsync` 是 GAT，GAT
//! 投影不传递 auto trait），那边只能改成「按需驱动」。
//!
//! # 构造前提（重要）
//!
//! [`BuffRead::try_new`] / [`BuffWrite::try_new`] 必须在**正在运行的 compio runtime 内**
//! 调用（它们要 `spawn` 后台泵）。不在 runtime 内时返回 [`TryNewError::NotInRuntime`]，
//! 不会 panic。适配器与其泵也**不能跨线程 / 跨 runtime 移动**（类型本身 `!Send`）。
//!
//! # 分配点由 [`TrAllocConfig`] 决定
//!
//! 实现里有两个**互相独立**的分配点，各有一个关联类型，由调用方逐点选分配器：
//!
//! | 关联类型 | 用在哪 | 类型形态 |
//! | --- | --- | --- |
//! | `C::RingBodyAlloc` | 环的缓冲本体 | [`RingBufOf<C>`] |
//! | `C::RingSharedAlloc` | 装环的容器 | [`SharedRingOf<C>`] |
//!
//! 默认实现是 [`DefaultAllocConfig`]（两点全 `CoreAlloc`）。两个关联类型都要求
//! `core::alloc::AllocatorClone` 且 `'static`（后者是因为泵 future 要 `'static`）。
//!
//! # 零拷贝程度（与 tokio 版的差异）
//!
//! 段级仍然零拷贝：环的写段直接交给 `move_items_from_input_async`、读段直接交给
//! `move_items_into_output_async`。但 compio 的设备接口把 buffer **按值**传入且要求
//! `'static` 的拥有型缓冲（借来的环段交不进去），因此「设备 ↔ 环」之间**必然过一次
//! 中转缓冲**。本 crate 用 [`ReadAsInputOwned`] / [`WriteAsOutputOwned`] 让这次中转
//! **稳态零分配**（缓冲随泵复用）。
//!
//! # 用法骨架
//!
//! ```no_run
//! #![feature(allocator_api)]
//! use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
//! use buffex_compio_adapt::{
//!     BuffRead, BuffWrite, DefaultAllocConfig,
//!     x_deps::abs_buff,
//! };
//! use compio::net::UnixStream;
//!
//! # async fn demo(dev_r: UnixStream, dev_w: UnixStream) -> std::io::Result<()> {
//! // 入向：设备 → 环 → 调用方。第三个参数是分配器配置（这里用默认）。
//! let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
//!     dev_r,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("在 compio runtime 内且容量合法");
//! let demand = Demand::at_least(1);
//! let some = rx.read_async(&demand).await;
//! // ……消费借到的段（`move_items_to_buff` / `move_items_into_output_async`）……
//! drop(some);
//!
//! // 出向：调用方 → 环 → 设备。
//! let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
//!     dev_w,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("在 compio runtime 内且容量合法");
//! // ……借段写入后收尾，保证环内数据确实送达设备……
//! tx.close_async().await.expect("写方向应正常关闭");
//! # Ok(())
//! # }
//! ```
//!
//! # 消费与收尾语义
//!
//! * `iter_slices()` 是**窥视**：给出 `&[u8]`，既不推进段内已消费量，drop 段时也不提交；
//! * `move_items_to_buff` / `move_items_into_output_async` **搬出多少就消费多少**；
//! * 出向还有一层：**段 drop 只是提交进环**，不等于已经写进设备；收尾必须 await
//!   [`BuffWrite::flush_async`]（等环被泵取空）或 [`BuffWrite::close_async`]
//!   （排空 + `shutdown`）。
//!
//! # 构建前提
//!
//! `buffex` / `abs_buff` 使用 nightly feature（`impl_trait_in_assoc_type` 等），因此本
//! crate 也需要 nightly 工具链。

#![feature(allocator_api)]
#![feature(impl_trait_in_assoc_type)]

mod alloc_;
mod device_;
mod read_;
mod shared_;
mod write_;

pub use alloc_::{DefaultAllocConfig, TrAllocConfig};
pub use device_::{
    CompioInputReadAsync, CompioInputReadFuture, CompioInputReadOwnedAsync,
    CompioInputReadOwnedFuture, CompioOutputWriteAsync, CompioOutputWriteFuture,
    CompioOutputWriteOwnedAsync, CompioOutputWriteOwnedFuture, ReadAsInput,
    ReadAsInputOwned, WriteAsOutput, WriteAsOutputOwned,
};
pub use read_::{BuffRead, ReaderOf as ReadReaderOf, WriterOf as ReadWriterOf};
pub use shared_::{RingBufOf, SharedRingOf, TryNewError};
pub use write_::{
    BuffWrite, BuffWriteCloseAsync, BuffWriteCloseFuture, BuffWriteFlushAsync,
    BuffWriteFlushFuture, ReaderOf as WriteReaderOf, WriterOf as WriteWriterOf,
};

/// 依赖重导出，便于调用方在不额外声明依赖的情况下对齐版本。
pub mod x_deps {
    pub use buffex;
    pub use buffex::x_deps::abs_buff;
    pub use compio;
    pub use mm_ptr;
}
