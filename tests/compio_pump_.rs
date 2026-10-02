//! 集成测试：`buffex_compio_adapt` 的两个适配器在真实 compio 运行时下的验收。
//!
//! 覆盖：入向（设备 → 环 → 调用方）、出向（调用方 → 环 → 设备）、背压、设备 EOF 合成
//! `Closing`、设备错误取回、短读、`flush` / `close` 的收尾保证、适配器 drop 之后后台泵
//! 确实停止、以及分配点配置逐点生效。
//!
//! 设备一律是本文件里的**内存设备**（`SliceDevice` / `CollectDevice` / …），它们只实现
//! compio 的 `AsyncRead` / `AsyncWrite`，因此用例是确定性的：短读、EOF、错误的时机完全
//! 由测试控制，不依赖真实 socket 的时序。

#![feature(allocator_api)]

use core::{future::pending, mem::MaybeUninit};

use std::{
    alloc::{Allocator, Global},
    io,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    error::ReadErrTag,
    x_deps::abs_cancel::CancelledToken,
};
use buffex::{
    ring::{ConsumerError, Ring, ProducerError},
    x_deps::abs_buff,
};
use buffex_compio_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, RingBufOf, TrAllocConfig, TryNewError,
};
use compio::{
    buf::{BufResult, IoBuf, IoBufMut},
    io::{AsyncRead, AsyncWrite},
};

// ---------------------------------------------------------------------------
// 内存设备
// ---------------------------------------------------------------------------

/// 只读设备：从内嵌字节序列里给出数据，每次最多给 `max_` 个字节（0 表示不限）。
///
/// 数据耗尽后 `read` 返回 `Ok(0)` —— compio 约定下的 EOF。
struct SliceDevice {
    data_: Vec<u8>,
    off_: usize,
    max_: usize,
}

impl SliceDevice {
    fn new_(data: Vec<u8>, max: usize) -> Self {
        SliceDevice {
            data_: data,
            off_: 0,
            max_: max,
        }
    }
}

impl AsyncRead for SliceDevice {
    async fn read<B: IoBufMut>(&mut self, mut buf: B) -> BufResult<usize, B> {
        if self.off_ >= self.data_.len() {
            return BufResult(Result::Ok(0usize), buf); // EOF
        }
        let dst = IoBufMut::as_uninit(&mut buf);
        let mut n = self.data_.len() - self.off_;
        if self.max_ > 0 {
            n = n.min(self.max_);
        }
        let n = n.min(dst.len());
        for (d, s) in dst[..n]
            .iter_mut()
            .zip(self.data_[self.off_..self.off_ + n].iter())
        {
            d.write(*s);
        }
        self.off_ += n;
        // SAFETY: 上面已把前 `n` 个字节初始化。
        unsafe { buf.set_len(n) };
        BufResult(Result::Ok(n), buf)
    }
}

/// 只读设备：永远返回错误的设备。
struct FailReadDevice;

impl AsyncRead for FailReadDevice {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
        BufResult(
            Result::Err(io::Error::new(io::ErrorKind::BrokenPipe, "设备坏了")),
            buf,
        )
    }
}

/// 只读设备：永远挂起（用于验证「取消令牌已就绪时不进入 park」与「drop 适配器即取消泵」）。
///
/// 它持有一个 `Rc` 探针，测试据此判断后台泵是否被真正丢弃。
struct PendingDevice {
    /// 只用于让测试观察「泵是否已被丢弃」（按 `Rc` 强计数判断，不直接读字段）。
    #[allow(dead_code)]
    probe_: Rc<()>,
}

impl AsyncRead for PendingDevice {
    async fn read<B: IoBufMut>(&mut self, _buf: B) -> BufResult<usize, B> {
        pending::<()>().await;
        unreachable!("PendingDevice 永远不会返回数据")
    }
}

/// 只写设备：永远挂起（用于「满环上的写等待被取消」与「drop 适配器即取消泵」）。
struct PendingWriteDevice {
    #[allow(dead_code)]
    probe_: Rc<()>,
}

impl AsyncWrite for PendingWriteDevice {
    async fn write<T: IoBuf>(&mut self, _buf: T) -> BufResult<usize, T> {
        pending::<()>().await;
        unreachable!("PendingWriteDevice 永远不会接受数据")
    }

    async fn flush(&mut self) -> io::Result<()> {
        pending::<()>().await;
        unreachable!("PendingWriteDevice 永远不会 flush 成功")
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Result::Ok(())
    }
}

/// 只写设备：把收到的字节累计起来，可选「每次最多收 `max_` 个字节」与「直接报错」。
struct CollectDevice {
    got_: Vec<u8>,
    max_: usize,
    fail_: bool,
    closed_: bool,
}

impl CollectDevice {
    fn new_(max: usize) -> Self {
        CollectDevice {
            got_: Vec::new(),
            max_: max,
            fail_: false,
            closed_: false,
        }
    }

    fn failing_() -> Self {
        CollectDevice {
            got_: Vec::new(),
            max_: 0,
            fail_: true,
            closed_: false,
        }
    }
}

impl AsyncWrite for CollectDevice {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        if self.fail_ {
            return BufResult(
                Result::Err(io::Error::new(io::ErrorKind::BrokenPipe, "写设备坏了")),
                buf,
            );
        }
        let mut n = buf.buf_len();
        if self.max_ > 0 {
            n = n.min(self.max_);
        }
        self.got_.extend_from_slice(&buf.as_init()[..n]);
        BufResult(Result::Ok(n), buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
        Result::Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.closed_ = true;
        Result::Ok(())
    }
}

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

/// 建立一对由内核 loopback 连通、且已注册到当前 compio 运行时的 socket。
///
/// compio 0.19 的 `UnixStream` 没有 `pair()`；这里用 `std` 的 socket 对再经
/// `UnixStream::from_std` 转换。必须在运行时上下文内调用（`from_std` 会查询当前运行时）。
fn make_pair_() -> (compio::net::UnixStream, compio::net::UnixStream) {
    let (a, b) = std::os::unix::net::UnixStream::pair()
        .expect("应当能建立 std UNIX socket 对");
    (
        compio::net::UnixStream::from_std(a).expect("a 应能转为 compio UnixStream"),
        compio::net::UnixStream::from_std(b).expect("b 应能转为 compio UnixStream"),
    )
}

/// 确定性负载：第 `i` 个字节为 `(i % 251) as u8`。
fn payload_(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 从任意实现了 `TrBuffRead` 的读端搬出字节，直到读到 `Closing`。
///
/// 返回 `(搬出的字节, 是否以 Closing 结束)`。只用 `at_least(1)`：环在「写端未关闭且
/// 可读量不足下限」时不会交出数据，按「还差多少」当下限会让调用方在短读场景下等不到段。
async fn drain_read_<T>(rx: &mut T) -> (Vec<u8>, bool)
where
    T: TrBuffRead<u8> + Unpin,
    for<'s> T::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut got = Vec::new();
    let mut staging: Vec<MaybeUninit<u8>> = Vec::new();
    loop {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = match some.pick_left() {
            Option::Some(segm) => segm,
            Option::None => break,
        };
        let now = segm.least_count();
        if staging.len() < now {
            staging.resize(now, MaybeUninit::uninit());
        }
        let moved = segm.move_items_to_buff(&mut staging[..now]);
        // SAFETY: `move_items_to_buff` 已初始化前 `moved` 个元素，且 `MaybeUninit<u8>`
        // 与 `u8` 同布局、`u8` 无无效位模式。
        let bytes =
            unsafe { core::slice::from_raw_parts(staging.as_ptr().cast::<u8>(), moved) };
        got.extend_from_slice(bytes);
        drop(segm); // 提交：唤醒等待的生产端
    }
    (got, true)
}

/// 从任意实现了 `TrBuffRead` 的读端搬出恰好 `want` 个字节（不足则返回已拿到的部分）。
async fn read_exact_or_less_<T>(rx: &mut T, want: usize) -> Vec<u8>
where
    T: TrBuffRead<u8> + Unpin,
    for<'s> T::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut got = Vec::with_capacity(want);
    let mut staging: Vec<MaybeUninit<u8>> = Vec::new();
    while got.len() < want {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = match some.pick_left() {
            Option::Some(segm) => segm,
            Option::None => break,
        };
        let now = segm.least_count().min(want - got.len());
        if staging.len() < now {
            staging.resize(now, MaybeUninit::uninit());
        }
        let moved = segm.move_items_to_buff(&mut staging[..now]);
        // SAFETY: 同 `drain_read_`。
        let bytes =
            unsafe { core::slice::from_raw_parts(staging.as_ptr().cast::<u8>(), moved) };
        got.extend_from_slice(bytes);
        drop(segm); // 提交：唤醒等待的生产端
    }
    got
}

/// 把 `payload` 写进任意实现了 `TrBuffWrite` 的写端，段 drop 即提交进环。
///
/// 返回实际写入的字节数（环被关闭时提前结束，不 panic）。
async fn fill_write_<T>(tx: &mut T, payload: &[u8]) -> usize
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = match some.pick_left() {
            Option::Some(segm) => segm,
            Option::None => break, // 已关闭：剩下的写不进去了
        };
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        assert!(moved > 0, "借出的写段不应为空");
        off += moved;
        drop(segm); // 提交：进入环，等待泵搬给设备
    }
    off
}

// ---------------------------------------------------------------------------
// 入向（设备 → 环）
// ---------------------------------------------------------------------------

/// 入向：设备 → 环 → 调用方。
/// - 测试目标：`BuffRead` 的后台泵把设备数据搬进环并借给调用方；环满（泵 park）与环空
///   （调用方 park）两条路径都要走通。
/// - 测试手段：`SliceDevice` 提供 32 KiB、每次最多给 1024 字节；环容量 1024，因此泵必然
///   反复遇到「环满」；调用方用 `drain_read_` 一直搬。
/// - 判定标准：搬出的 32 KiB 与负载逐字节一致。
#[compio::test]
async fn buff_read_delivers_device_bytes_() {
    const TOTAL: usize = 32 * 1024;
    let payload = payload_(TOTAL);
    let dev = SliceDevice::new_(payload.clone(), 1024);

    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(dev, 1024, DefaultAllocConfig).expect("容量合法");
    let (got, _) = drain_read_(&mut rx).await;

    assert_eq!(got, payload, "入向搬出的字节应与设备数据一致");
    assert!(rx.take_error().is_none(), "正常结束不应留下设备错误");
}

/// 入向：设备 EOF 被合成为 `ConsumerError::Closing`，且不丢已搬入的字节。
/// - 测试目标：泵遇到 EOF 后退出并关闭生产端，调用方读到环空时拿到 `Closing`。
/// - 测试手段：`SliceDevice` 只给 200 字节（每次最多 200），环容量 64；调用方一直搬。
/// - 判定标准：搬出的 200 字节与负载一致；结束原因是 `Closing`；`take_error()` 为空
///   （正常 EOF 不算设备错误）、`is_read_ended()` 为真。
#[compio::test]
async fn buff_read_synthesises_closing_on_device_eof_() {
    const TOTAL: usize = 200;
    let payload = payload_(TOTAL);
    let dev = SliceDevice::new_(payload.clone(), 200);

    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig).expect("容量合法");
    let (got, closing) = drain_read_(&mut rx).await;

    assert!(closing, "设备结束后应报 Closing");
    assert_eq!(got, payload, "EOF 前已搬入的字节不应丢失");
    assert!(rx.take_error().is_none(), "正常 EOF 不应记成设备错误");
    assert!(rx.is_read_ended(), "应记录「设备已正常结束」");
}

/// 入向：设备 IO 错误被暂存并经 `take_error` 取回。
/// - 测试目标：泵把设备错误记下来、关闭生产端；调用方拿到 `Closing` 后能取回真实原因。
/// - 测试手段：`FailReadDevice`（每次 `read` 都报 `BrokenPipe`）构造 `BuffRead`，
///   空环上读一次，再 `take_error`。
/// - 判定标准：读返回 `Closing`；`take_error` 给出 `BrokenPipe`。
#[compio::test]
async fn buff_read_records_device_error_() {
    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
        FailReadDevice,
        64,
        DefaultAllocConfig,
    )
    .expect("容量合法");

    let demand = Demand::at_least(1);
    let some = rx.read_async(&demand).await;
    assert!(
        matches!(some.pick_right(), Option::Some(ConsumerError::Closing)),
        "设备出错且环为空时应报 Closing"
    );

    let (err, tag) = rx.take_error().expect("设备错误应被取回");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(tag, ReadErrTag::Propagated);
    assert!(!rx.is_read_ended(), "设备错误不是正常结束");
}

/// 入向：设备短读（每次只给几个字节）不丢字节、不乱序。
/// - 测试目标：泵在「一次只给 7 个字节」的设备上持续搬运，并在耗尽后以 `Closing` 收尾。
/// - 测试手段：`SliceDevice`（每次最多 7 字节）给 300 字节，环容量 64；调用方一直搬。
/// - 判定标准：搬出的 300 字节与负载逐字节一致；结束原因是 `Closing`；无设备错误。
#[compio::test]
async fn buff_read_survives_short_reads_() {
    const TOTAL: usize = 300;
    let payload = payload_(TOTAL);
    let dev = SliceDevice::new_(payload.clone(), 7);

    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig).expect("容量合法");
    let (got, closing) = drain_read_(&mut rx).await;

    assert!(closing, "设备耗尽后应报 Closing");
    assert_eq!(got, payload, "短读路径不应丢字节或乱序");
    assert!(rx.take_error().is_none(), "正常 EOF 不应记成设备错误");
}

/// 入向：借用设备那一侧也遵守「读到末尾以 `Closing` 表达 EOF」的契约。
/// - 测试目标：`SliceDevice` 耗尽之后，环里的数据取空即报 `Closing`（而不是无限等待）。
/// - 测试手段：设备只给 3 个字节，环容量 8；先取走 3 个，再读一次。
/// - 判定标准：第一次取回 3 个字节；第二次返回 `ConsumerError::Closing`。
#[compio::test]
async fn buff_read_closing_after_short_device_() {
    let payload = payload_(3);
    let dev = SliceDevice::new_(payload.clone(), 0);
    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(dev, 8, DefaultAllocConfig).expect("容量合法");

    let got = read_exact_or_less_(&mut rx, 3).await;
    assert_eq!(got, payload, "应取回设备给出的 3 个字节");

    // 再读：设备已 EOF、环已空 ⇒ Closing。
    let demand = Demand::at_least(1);
    let some = rx.read_async(&demand).await;
    assert!(
        matches!(some.pick_right(), Option::Some(ConsumerError::Closing)),
        "环取空后应报 Closing"
    );
}

// ---------------------------------------------------------------------------
// 出向（环 → 设备）
// ---------------------------------------------------------------------------

/// 出向：调用方 → 环 → 设备。
/// - 测试目标：`BuffWrite` 的后台泵把环内数据交给设备，背压下调用方能持续借段。
/// - 测试手段：环容量 512 上写入 32 KiB（必然先填满环、再由泵边冲边写），设备是可短写的
///   `CollectDevice`（每次最多收 300 字节），写完 `flush_async` 收尾。
/// - 判定标准：设备收到的字节与负载逐字节一致；`flush_async` 正常返回；无写错误。
#[compio::test]
async fn buff_write_delivers_bytes_to_device_() {
    const TOTAL: usize = 32 * 1024;
    let payload = payload_(TOTAL);
    let dev = CollectDevice::new_(300);

    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 512, DefaultAllocConfig).expect("容量合法");
    let written = fill_write_(&mut tx, &payload).await;
    assert_eq!(written, TOTAL, "全部字节都应写进环");
    tx.flush_async().await.expect("冲刷应正常结束");

    assert_eq!(tx.data_size(), 0, "flush 之后环应被取空");
    assert!(tx.take_error().is_none(), "不应有设备写错误");
    assert!(!tx.is_pump_finished(), "未收尾时泵应仍在运行");
}

/// 出向：`close_async` 冲净剩余数据并 `shutdown` 设备。
/// - 测试目标：段 drop 只是提交进环，`close_async` 才保证数据真正送达并关闭写方向。
/// - 测试手段：环容量 8192 上写入 4096 字节后立刻 `close_async`。
/// - 判定标准：`close_async` 正常返回；之后 `try_write` 一律 `Closing`；设备收到的字节
///   与负载一致、且其 `shutdown` 已被调用。
#[compio::test]
async fn buff_write_close_flushes_and_shuts_down_() {
    const TOTAL: usize = 4096;
    let payload = payload_(TOTAL);
    let dev = CollectDevice::new_(128);

    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 8192, DefaultAllocConfig)
        .expect("容量合法");
    let written = fill_write_(&mut tx, &payload).await;
    assert_eq!(written, TOTAL);
    tx.close_async().await.expect("关闭应正常结束");

    assert!(tx.is_pump_finished(), "close_async 返回时泵应已退出");
    let demand = Demand::at_least(1);
    assert!(
        matches!(
            tx.try_write(&demand).pick_right(),
            Option::Some(ProducerError::Closing)
        ),
        "收尾之后写入口应报 Closing"
    );
    assert!(tx.take_error().is_none(), "正常收尾不应留下写错误");
}

/// 出向：`flush_async` 单独就能保证「已提交数据都被设备收下」，且之后还能继续写。
/// - 测试目标：`flush_async` 的判据（环被取空）与「泵继续可用」。
/// - 测试手段：环容量 512 上写 8 KiB → `flush_async` → 再写 1 KiB → 再 `flush_async`。
/// - 判定标准：第一次 flush 后 `data_size == 0`；第二次 flush 后总计 9 KiB 全部提交；
///   全程无写错误、泵未退出。
#[compio::test]
async fn flush_async_delivers_all_without_closing_() {
    const FIRST: usize = 8 * 1024;
    const SECOND: usize = 1024;
    let payload = payload_(FIRST);
    let dev = CollectDevice::new_(0);

    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 512, DefaultAllocConfig).expect("容量合法");
    assert_eq!(fill_write_(&mut tx, &payload).await, FIRST);
    tx.flush_async().await.expect("第一次冲刷应正常结束");
    assert_eq!(tx.data_size(), 0, "第一次 flush 后环应为空");
    assert!(!tx.is_pump_finished(), "flush 不应让泵退出");

    assert_eq!(fill_write_(&mut tx, &payload_(SECOND)).await, SECOND);
    tx.flush_async().await.expect("第二次冲刷应正常结束");
    assert_eq!(tx.data_size(), 0, "第二次 flush 后环应为空");
    assert!(tx.take_error().is_none(), "不应有设备写错误");
}

/// 出向：设备写错误被暂存，写入口据此报 `Closing` 而不是永久 park。
/// - 测试目标：泵出错 → 守卫关闭消费端 → 调用方的写等待被唤醒并拿到 `Closing`，
///   错误详情可经 `take_error` 取回。
/// - 测试手段：`CollectDevice::failing_()`（每次写都报 `BrokenPipe`）作设备，环容量 64；
///   写入 8 字节（能装进环），随后 `flush_async` 与 `try_write` 各探一次。
/// - 判定标准：`flush_async` 返回 `Closing`；`try_write` 返回 `Closing`；
///   `take_error` 给出 `BrokenPipe`。
#[compio::test]
async fn buff_write_records_device_error_() {
    let payload = payload_(8);
    let dev = CollectDevice::failing_();
    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig).expect("容量合法");

    // 8 字节能装进容量 64 的环，因此这一步不会因为环满而卡住。
    let written = fill_write_(&mut tx, &payload).await;
    assert_eq!(written, payload.len(), "小载荷应能全部写进环");

    let flush = tx.flush_async().await;
    assert!(
        matches!(flush, Result::Err(ProducerError::Closing)),
        "设备出错后 flush 应报 Closing"
    );
    let demand = Demand::at_least(1);
    assert!(
        matches!(
            tx.try_write(&demand).pick_right(),
            Option::Some(ProducerError::Closing)
        ),
        "设备出错后写入口应报 Closing"
    );

    let (err, _tag) = tx.take_error().expect("写错误应被取回");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
}

/// 真实驱动下的整条管线：socket 半边 → `BuffWrite` → 环 → socket → `BuffRead` → 调用方。
/// - 测试目标：不只用内存设备——把两个适配器接在**真实 compio 驱动**（io_uring 或 poll）
///   的 socket 两端，验证「设备 ↔ 环」的搬运在真实完成式 IO 上成立（含背压与 TCP/Unix
///   语义的短读短写）。
/// - 测试手段：`std` 的 UNIX socket 对转成 compio `UnixStream`；发送侧放进一个
///   compio 任务（`spawn` 不要求 `Send`），写入 64 KiB 后 `close_async`；主任务用
///   `drain_read_` 一直读到 `Closing`；两端环容量都取 4096。
/// - 判定标准：读到的 64 KiB 与负载逐字节一致；发送侧任务正常结束（`JoinHandle` 无错）；
///   两端都不留设备错误。
#[compio::test]
async fn socket_roundtrip_with_real_driver_() {
    const TOTAL: usize = 64 * 1024;
    let payload = payload_(TOTAL);
    let (a, b) = make_pair_();

    let feed = payload.clone();
    let sender = compio::runtime::spawn(async move {
        let mut tx =
            BuffWrite::<_, DefaultAllocConfig>::try_new(a, 4096, DefaultAllocConfig).expect("容量合法");
        assert_eq!(fill_write_(&mut tx, &feed).await, feed.len(), "全部字节都应写进环");
        tx.close_async().await.expect("发送侧应正常收尾");
        assert!(tx.take_error().is_none(), "发送侧不应有设备写错误");
    });

    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(b, 4096, DefaultAllocConfig).expect("容量合法");
    let (got, closing) = drain_read_(&mut rx).await;
    sender.await.expect("发送任务不应失败");

    assert!(closing, "对端关闭后应报 Closing");
    assert_eq!(got, payload, "经真实驱动的往返字节应与负载一致");
    assert!(rx.take_error().is_none(), "接收侧不应有设备读错误");
}


// ---------------------------------------------------------------------------
// 生命周期、取消与配置
// ---------------------------------------------------------------------------

/// drop 适配器即取消后台泵，设备随之被释放。
/// - 测试目标：`BuffRead` 的 `JoinHandle` 在 drop 时取消泵任务，不留后台读循环、不泄漏设备。
/// - 测试手段：`PendingDevice` 持有一个 `Rc<()>` 探针（泵永远停在对它的读上）；
///   构造后确认泵仍在运行、探针被两处持有；drop 适配器后让出一次运行时，再看探针计数。
/// - 判定标准：drop 之前 `is_pump_finished()` 为假、`Rc::strong_count == 2`；
///   drop 并让出运行时之后 `Rc::strong_count == 1`（泵连同设备一起被丢弃）。
#[compio::test]
async fn dropping_adapter_cancels_pump_() {
    let probe = Rc::new(());
    let dev = PendingDevice {
        probe_: probe.clone(),
    };
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("容量合法");

    assert!(!rx.is_pump_finished(), "设备永远挂起，泵不应结束");
    assert_eq!(Rc::strong_count(&probe), 2, "探针应被测试与设备各持一份");

    drop(rx);
    // 让执行体有机会处理这次取消（compio 的定时器在这里当作「让出一次」用）。
    compio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert_eq!(
        Rc::strong_count(&probe),
        1,
        "drop 适配器之后泵任务（连同设备）应已被丢弃"
    );
}

/// 委托给 ring 的 future 自带取消能力：令牌已就绪时不进入 park。
/// - 测试目标：`BuffRead::read_async` / `BuffWrite::write_async` 返回的是 ring 的
///   future，`may_cancel_with` 立刻生效（本 crate 没有再包一层）。
/// - 测试手段：设备永远挂起（环里不会有数据）时，用 `CancelledToken` 驱动读；
///   再用一个永远挂起的写设备把环写满后，用同样的令牌驱动写。
/// - 判定标准：读返回 `ConsumerError::Cancelled`；写返回 `ProducerError::Cancelled`。
#[compio::test]
async fn delegated_futures_honour_cancellation_() {
    use abs_buff::x_deps::abs_cancel::TrMayCancel;

    let probe = Rc::new(());
    let rx_dev = PendingDevice {
        probe_: probe.clone(),
    };
    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(rx_dev, 8, DefaultAllocConfig)
        .expect("容量合法");

    let demand = Demand::at_least(1);
    let some = rx
        .read_async(&demand)
        .may_cancel_with(CancelledToken::new())
        .await;
    assert!(
        matches!(some.pick_right(), Option::Some(ConsumerError::Cancelled)),
        "读等待应被取消令牌中止"
    );

    let tx_dev = PendingWriteDevice {
        probe_: probe.clone(),
    };
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(tx_dev, 8, DefaultAllocConfig)
        .expect("容量合法");
    // 先把环写满（设备永远挂起，泵会一直攥着第一段不放）。
    assert_eq!(fill_write_(&mut tx, &payload_(8)).await, 8);
    let demand = Demand::at_least(1);
    let some = tx
        .write_async(&demand)
        .may_cancel_with(CancelledToken::new())
        .await;
    assert!(
        matches!(some.pick_right(), Option::Some(ProducerError::Cancelled)),
        "满环上的写等待应被取消令牌中止"
    );
}

/// 记账分配器：转发给 `Global`，但记录每次分配。
#[derive(Clone, Debug, Default)]
struct CountingAlloc {
    events: Arc<AtomicUsize>,
}

impl CountingAlloc {
    fn events_(&self) -> usize {
        self.events.load(Ordering::Relaxed)
    }
}

unsafe impl Allocator for CountingAlloc {
    fn allocate(
        &self,
        layout: core::alloc::Layout,
    ) -> Result<core::ptr::NonNull<[u8]>, core::alloc::AllocError> {
        self.events.fetch_add(1, Ordering::Relaxed);
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: core::ptr::NonNull<u8>, layout: core::alloc::Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}

unsafe impl core::alloc::AllocatorClone for CountingAlloc {}

/// 逐点配置的示例：**每个分配点用不同的记账分配器**。
#[derive(Clone, Debug, Default)]
struct CountingConfig {
    ring_body: CountingAlloc,
    ring_shared: CountingAlloc,
}

impl TrAllocConfig for CountingConfig {
    type RingBodyAlloc = CountingAlloc;
    type RingSharedAlloc = CountingAlloc;

    fn ring_body_alloc(&self) -> Self::RingBodyAlloc {
        self.ring_body.clone()
    }
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc {
        self.ring_shared.clone()
    }
}

/// `TrAllocConfig`：每个分配点各自生效，且搬运进入稳态后不再分配。
/// - 测试目标：环体 / 共享容器分别走配置里对应的分配器；owned 设备适配让中转缓冲随泵
///   复用，因此**第一轮搬运之后**（含泵首次预置缓冲）计数不再增长。
/// - 测试手段：两个分配点各配一个独立记账的 `CountingAlloc`，构造 `BuffRead` 与
///   `BuffWrite`；先跑完一轮入向 + 出向搬运并快照计数，再跑一轮并比较。
/// - 判定标准：构造后两个计数各 > 0；第一轮之后计数 > 构造时（泵的预置缓冲）；
///   第二轮之后两个计数都不再增长；两个计数器互不串台。
#[compio::test]
async fn allocator_config_applies_per_allocation_site_() {
    let cfg = CountingConfig::default();
    let body0 = cfg.ring_body.events_();
    let shared0 = cfg.ring_shared.events_();

    let payload = payload_(2048);
    let rx_dev = SliceDevice::new_(payload.clone(), 0);
    let tx_dev = CollectDevice::new_(0);
    let mut rx = BuffRead::<_, CountingConfig>::try_new(rx_dev, 1024, cfg.clone())
        .expect("容量合法");
    let mut tx = BuffWrite::<_, CountingConfig>::try_new(tx_dev, 1024, cfg.clone())
        .expect("容量合法");

    assert!(
        cfg.ring_body.events_() > body0,
        "环的缓冲本体应当走 C::RingBodyAlloc"
    );
    assert!(
        cfg.ring_shared.events_() > shared0,
        "装环的共享容器应当走 C::RingSharedAlloc"
    );

    // 第一轮：读完整批 + 写完整批（其中包含泵首次预置中转缓冲的开销）。
    let got = drain_read_(&mut rx).await.0;
    assert_eq!(got, payload, "第一轮入向数据应完整");
    assert_eq!(fill_write_(&mut tx, &payload).await, payload.len());
    tx.flush_async().await.expect("第一轮冲刷应正常结束");

    let body1 = cfg.ring_body.events_();
    let shared1 = cfg.ring_shared.events_();

    // 第二轮：同样的搬运量，稳态下不应再有任何分配。
    let got = drain_read_(&mut rx).await.0;
    assert!(got.is_empty(), "设备已 EOF，第二轮应读不到新数据");
    assert_eq!(fill_write_(&mut tx, &payload).await, payload.len());
    tx.flush_async().await.expect("第二轮冲刷应正常结束");

    assert_eq!(
        cfg.ring_body.events_(),
        body1,
        "稳态搬运不应再分配环体缓冲"
    );
    assert_eq!(
        cfg.ring_shared.events_(),
        shared1,
        "稳态搬运不应再分配共享容器"
    );
}

/// `TryNewError::InvalidCapacity`：容量越界时给出可读错误而不是 panic。
/// - 测试目标：构造期容量校验失败的错误路径。
/// - 测试手段：用一个越界容量（1，小于 ring 的下限 2）构造 `BuffRead`。
/// - 判定标准：返回 `Err(TryNewError::InvalidCapacity(1))`。
#[compio::test]
async fn invalid_capacity_is_reported_() {
    let err = BuffRead::<_, DefaultAllocConfig>::try_new(
        SliceDevice::new_(Vec::new(), 0),
        1,
        DefaultAllocConfig,
    )
    .expect_err("容量 1 会被 ring 拒绝");
    assert_eq!(err, TryNewError::InvalidCapacity(1));
}

/// `TryNewError::NotInRuntime`：不在 compio runtime 内构造时给出错误，而不是 panic。
/// - 测试目标：`try_new` 对「没有正在运行的 compio runtime」的处理（后台泵要 `spawn`）。
/// - 测试手段：用**普通** `#[test]`（没有 compio runtime 的线程）构造 `BuffRead`。
/// - 判定标准：返回 `Err(TryNewError::NotInRuntime)`。
#[test]
fn not_in_runtime_is_reported_() {
    let err = BuffRead::<_, DefaultAllocConfig>::try_new(
        SliceDevice::new_(Vec::new(), 0),
        64,
        DefaultAllocConfig,
    )
    .expect_err("不在 runtime 内应当报错");
    assert_eq!(err, TryNewError::NotInRuntime);

    let err = BuffWrite::<_, DefaultAllocConfig>::try_new(
        CollectDevice::new_(0),
        64,
        DefaultAllocConfig,
    )
    .expect_err("不在 runtime 内应当报错");
    assert_eq!(err, TryNewError::NotInRuntime);
}

/// 段级零拷贝：环借出的写段能直接被 `move_items_from_input_async` 填充，
/// 借出的读段能直接交给 `move_items_into_output_async`（类型层面即已验证）。
/// - 测试目标：`BuffRead` / `BuffWrite` 的段类型与 `abs_buff` 的段 trait 完全对接。
/// - 测试手段：用 `TrBuffTryRead` / `TrBuffTryWrite` 的关联类型做一次静态断言。
/// - 判定标准：编译通过（本用例本身即断言）。
#[test]
fn segment_types_are_abs_buff_segments_() {
    fn assert_read_<R, C, S>()
    where
        R: AsyncRead + 'static,
        C: TrAllocConfig,
        S: core::borrow::Borrow<Ring<RingBufOf<C>, u8>> + 'static,
    {
        fn check_<'s, X: TrBuffTryRead<u8> + 's>()
        where
            X::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
        {
        }
        check_::<'static, BuffRead<R, C, S>>();
    }

    fn assert_write_<W, C, S>()
    where
        W: AsyncWrite + 'static,
        C: TrAllocConfig,
        S: core::borrow::Borrow<Ring<RingBufOf<C>, u8>> + 'static,
    {
        fn check_<'s, X: TrBuffTryWrite<u8> + 's>()
        where
            X::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
        {
        }
        check_::<'static, BuffWrite<W, C, S>>();
    }

    assert_read_::<SliceDevice, DefaultAllocConfig, buffex_compio_adapt::SharedRingOf<DefaultAllocConfig>>();
    assert_write_::<CollectDevice, DefaultAllocConfig, buffex_compio_adapt::SharedRingOf<DefaultAllocConfig>>();
}
