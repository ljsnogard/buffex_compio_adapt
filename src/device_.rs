//! 设备级适配：把 compio 的 [`AsyncRead`] / [`AsyncWrite`] 接成 `abs_buff` 的
//! [`TrInput`] / [`TrOutput`]。
//!
//! 这一族原本在 `abs_buff_compio_adapt` 里；按方案修订已整体搬进本 crate——本 crate
//! 的搬运泵只需要这一族，独立维护第二个 crate 不再划算。
//!
//! # 两种形态
//!
//! | 类型 | 设备 | 中转缓冲 | 适用 |
//! | --- | --- | --- | --- |
//! | [`ReadAsInput`] / [`WriteAsOutput`] | 借用 `&mut D` | 每次调用现分配 | 一次性、短生命周期的调用 |
//! | [`ReadAsInputOwned`] / [`WriteAsOutputOwned`] | 拥有 `D` | 与设备同寿、复用 | 长期持有的搬运泵 |
//!
//! 为什么必须有「中转缓冲」这一层：compio 的读写接口把 buffer **按值**传入、并要求
//! `IoBufMut` / `IoBuf` 满足 `'static`（`&'static mut B` 之外只支持拥有型缓冲），而环的
//! 段是借来的 `&mut [MaybeUninit<u8>]`。因此**设备 ↔ 环之间必然过一次 owned 缓冲**；
//! owned 形态只是把那一次分配省掉（缓冲随泵复用），拷贝本身仍在。
//!
//! # EOF 的契约
//!
//! `abs_buff` 的输入搬移**不允许**「返回 0 个且无错误」（内层会
//! `assert!(cc > 0 || x.contains_right())`），而 compio 的 `read` 用 `Ok(0)` 表示 EOF。
//! 本模块的读侧实现把 `Ok(0)` 翻成带 [`ReadErrTag::Closing`] 标签的错误（`Closing` 是
//! **终止性**标签，段级搬移据此收尾而不是空转）。写侧不需要这层翻译：`Ok(0)` 在
//! `AsyncWrite` 里本来就是合法结果。

use core::mem::MaybeUninit;

use abs_buff::{
    error::{ReadErrTag, TaggedError, WriteErrTag},
    gen_may_cancel_future,
    io::{TrInput, TrOutput},
    x_deps::{abs_cancel, anylr::SomeOf},
};
use compio::{
    buf::{BufResult, IoBuf, IoBufMut, SetLen},
    io::{AsyncRead, AsyncWrite},
};

/// 借用设备的读适配：把 `&mut R`（compio 读设备）适配为 [`TrInput<u8>`]。
///
/// 每次 `read_async` 都会按本次请求长度分配一段 owned 临时缓冲（compio 要求），读到之后
/// 再拷回调用方的目标切片。长期持有（如搬运泵）请用 [`ReadAsInputOwned`]。
///
/// # Examples
///
/// ```no_run
/// use buffex_compio_adapt::ReadAsInput;
/// use compio::net::UnixStream;
///
/// # async fn demo(stream: &mut UnixStream) {
/// let mut input = ReadAsInput::new(stream);
/// let _ = &mut input;
/// # }
/// ```
pub struct ReadAsInput<'a, R>(&'a mut R)
where
    R: AsyncRead;

impl<'a, R> ReadAsInput<'a, R>
where
    R: AsyncRead,
{
    /// 借用给定的 compio 读设备。
    pub const fn new(reader: &'a mut R) -> Self {
        ReadAsInput(reader)
    }

    /// 读入口，等价于 [`TrInput::read_async`]。
    pub fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> CompioInputReadAsync<'f, 'f, R> {
        CompioInputReadAsync::new(self.0, target)
    }
}

impl<R> TrInput<u8> for ReadAsInput<'_, R>
where
    R: AsyncRead,
{
    type ReadAsync<'f>
        = CompioInputReadAsync<'f, 'f, R>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, ReadErrTag>;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        ReadAsInput::read_async(self, target)
    }
}

/// [`ReadAsInput::read_async`] 的 step 函数。
#[gen_may_cancel_future(CompioInputRead, pub, new(pub(crate)))]
async fn compio_input_read_impl_async_<'f, R, C>(
    input: &'f mut R,
    target: &'f mut [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, ReadErrTag>>
where
    R: AsyncRead,
    C: abs_cancel::TrCancellationToken,
{
    if target.is_empty() {
        // 空目标：没有可写窗口，也没读任何字节。这是「没有请求」而不是「读到 0」，
        // 因此不能当成 EOF；段级搬移的调用方不会传空目标（它只在 `take > 0` 时调用）。
        return SomeOf::new_left(0usize);
    }
    // compio 的 `read` 要求按值传入 `'static` 的 `IoBufMut`，因此先读进 owned
    // 临时缓冲，再把有效前缀写回调用方的 `MaybeUninit` 目标。
    let temp: Vec<u8> = vec![0u8; target.len()];
    let BufResult(res, temp) = input.read(temp).await;
    match res {
        // 读到 0 = EOF：`abs_buff` 要求以（终止性标签的）错误表达，否则段级搬移会 panic。
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            ReadErrTag::Closing,
        )),
        Result::Ok(n) => {
            for (dst, src) in target[..n].iter_mut().zip(temp[..n].iter()) {
                dst.write(*src);
            }
            SomeOf::new_left(n)
        }
        Result::Err(err) => SomeOf::new_right(TaggedError::new(
            err,
            ReadErrTag::Propagated,
        )),
    }
}

/// compio 设备的**复用中转缓冲**。
///
/// 为什么不能直接用 `Vec<u8>`：compio 的读操作把 `IoBufMut::as_uninit()` 的**整个长度**
/// 当作可写窗口（`sys_slice_mut` / `poll` 驱动都是这么取的），而 `Vec<u8>` 的
/// `as_uninit()` 长度等于 **capacity**。于是「想读多少」必须由容量表达，容量随每次请求
/// 变化就只能重分配——复用无从谈起。本类型改为显式区分「本次请求窗口长度 `req_`」与
/// 「已读到的长度 `len_`」，底层 `Vec<u8>` 的容量一次要够、之后一直复用。
struct Stage_ {
    /// 底层分配（`len == capacity`，全部是有效可写内存；内容本身无意义）。
    buf_: Vec<u8>,
    /// 本次请求的窗口长度：`as_uninit()` 返回它，compio 最多往里写这么多。
    req_: usize,
    /// 已经由设备写入并初始化的长度：`as_init()` 返回它。
    len_: usize,
}

impl Stage_ {
    /// 以容量 `cap` 建立复用缓冲；初始没有任何已初始化数据。
    fn with_capacity_(cap: usize) -> Self {
        Stage_ {
            buf_: vec![0u8; cap],
            req_: 0usize,
            len_: 0usize,
        }
    }

    /// 设定本次请求的窗口长度（超过底层容量时会被截断，由调用方保证不越界）。
    fn set_window_(&mut self, len: usize) {
        self.req_ = len.min(self.buf_.len());
        self.len_ = 0usize;
    }

    /// 底层容量（复用缓冲的上限）。
    fn capacity_(&self) -> usize {
        self.buf_.len()
    }

    /// 读取成功后把设备写进来的前 `len` 个字节交给调用方。
    fn filled_(&self) -> &[u8] {
        &self.buf_[..self.len_]
    }
}

impl IoBuf for Stage_ {
    fn as_init(&self) -> &[u8] {
        self.filled_()
    }
}

impl IoBufMut for Stage_ {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        let n = self.req_.min(self.buf_.len());
        let ptr = self.buf_.as_mut_ptr().cast::<MaybeUninit<u8>>();
        // SAFETY:
        // - `ptr` 指向本类型独占拥有的 `Vec<u8>` 分配，`n <= buf_.len() == capacity`，
        //   因此 `[ptr, ptr + n)` 在同一个分配内、可写；
        // - `u8` 与 `MaybeUninit<u8>` 布局相同、对齐相同（均为 1），按未初始化内存看待
        //   已初始化的 `u8` 是允许的（反过来才需要保证初始化）；
        // - 返回切片的生命周期绑定在 `&mut self` 上，期间不会有第二个可写视图。
        unsafe { core::slice::from_raw_parts_mut(ptr, n) }
    }
}

impl SetLen for Stage_ {
    unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.req_, "设备写入量不应超过请求窗口");
        self.len_ = len;
    }
}

/// 拥有设备的读适配：设备与中转缓冲都随本类型存活，**稳态下不再分配**。
///
/// 语义与 [`ReadAsInput`] 逐项一致（含 `Ok(0)` → `Closing` 的 EOF 翻译），差别只有中转
/// 缓冲由本类型持有并复用。搬运泵就该用这一形态。
///
/// # Examples
///
/// ```no_run
/// use buffex_compio_adapt::ReadAsInputOwned;
/// use compio::net::UnixStream;
///
/// # async fn demo(stream: UnixStream) {
/// let input = ReadAsInputOwned::with_capacity(stream, 4096);
/// assert!(input.stage_capacity() >= 4096);
/// # }
/// ```
pub struct ReadAsInputOwned<R>
where
    R: AsyncRead,
{
    dev_: R,
    stage_: Stage_,
}

impl<R> ReadAsInputOwned<R>
where
    R: AsyncRead,
{
    /// 用空的中转缓冲建立适配（首次使用时按需增长到请求长度）。
    pub fn new(dev: R) -> Self {
        ReadAsInputOwned {
            dev_: dev,
            stage_: Stage_::with_capacity_(0usize),
        }
    }

    /// 以容量 `cap` 预置中转缓冲：搬运泵按环容量预置一次即可。
    pub fn with_capacity(dev: R, cap: usize) -> Self {
        ReadAsInputOwned {
            dev_: dev,
            stage_: Stage_::with_capacity_(cap),
        }
    }

    /// 取回底层设备与中转缓冲的容量。
    pub fn into_inner(self) -> R {
        self.dev_
    }

    /// 当前中转缓冲容量（复用上限）。
    pub fn stage_capacity(&self) -> usize {
        self.stage_.capacity_()
    }
}

impl<R> TrInput<u8> for ReadAsInputOwned<R>
where
    R: AsyncRead,
{
    type ReadAsync<'f>
        = CompioInputReadOwnedAsync<'f, 'f, R>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, ReadErrTag>;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        CompioInputReadOwnedAsync::new(self, target)
    }
}

/// [`ReadAsInputOwned::read_async`] 的 step 函数。
#[gen_may_cancel_future(CompioInputReadOwned, pub, new(pub(crate)))]
async fn compio_input_read_owned_async_<'f, R, C>(
    input: &'f mut ReadAsInputOwned<R>,
    target: &'f mut [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, ReadErrTag>>
where
    R: AsyncRead,
    C: abs_cancel::TrCancellationToken,
{
    if target.is_empty() {
        return SomeOf::new_left(0usize);
    }
    if input.stage_.capacity_() < target.len() {
        // 只在「请求超过现有容量」时增长一次；之后一直复用。
        input.stage_ = Stage_::with_capacity_(target.len());
    }
    input.stage_.set_window_(target.len());
    // 把中转缓冲按值交给 compio（这是它的接口要求），读完再放回。
    let stage = core::mem::replace(&mut input.stage_, Stage_::with_capacity_(0usize));
    let BufResult(res, stage) = input.dev_.read(stage).await;
    input.stage_ = stage;
    match res {
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            ReadErrTag::Closing,
        )),
        Result::Ok(n) => {
            for (dst, src) in target[..n].iter_mut().zip(input.stage_.filled_().iter()) {
                dst.write(*src);
            }
            SomeOf::new_left(n)
        }
        Result::Err(err) => SomeOf::new_right(TaggedError::new(
            err,
            ReadErrTag::Propagated,
        )),
    }
}

/// 借用设备的写适配：把 `&mut W`（compio 写设备）适配为 [`TrOutput<u8>`]。
///
/// 每次 `write_async` 都会把源切片复制进一段 owned 缓冲（compio 要求）再写给设备。
/// 长期持有（如搬运泵）请用 [`WriteAsOutputOwned`]。
///
/// # Examples
///
/// ```no_run
/// use buffex_compio_adapt::WriteAsOutput;
/// use compio::net::UnixStream;
///
/// # async fn demo(stream: &mut UnixStream) {
/// let mut output = WriteAsOutput::new(stream);
/// let _ = &mut output;
/// # }
/// ```
pub struct WriteAsOutput<'a, W>(&'a mut W)
where
    W: AsyncWrite;

impl<'a, W> WriteAsOutput<'a, W>
where
    W: AsyncWrite,
{
    /// 借用给定的 compio 写设备。
    pub const fn new(writer: &'a mut W) -> Self {
        WriteAsOutput(writer)
    }

    /// 写入口，等价于 [`TrOutput::write_async`]。
    pub fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> CompioOutputWriteAsync<'f, 'f, W> {
        CompioOutputWriteAsync::new(self.0, source)
    }
}

impl<W> TrOutput<u8> for WriteAsOutput<'_, W>
where
    W: AsyncWrite,
{
    type WriteAsync<'f>
        = CompioOutputWriteAsync<'f, 'f, W>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, WriteErrTag>;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        WriteAsOutput::write_async(self, source)
    }
}

/// 把 `&[MaybeUninit<u8>]` 视作已初始化的 `&[u8]`。
///
/// # Safety
///
/// 调用方（段搬移 / 本模块的写适配）保证 `source` 的所有字节都已初始化；
/// `MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同（均为 1）。
fn as_init_bytes_(source: &[MaybeUninit<u8>]) -> &[u8] {
    // SAFETY: 见本函数文档。
    unsafe {
        core::slice::from_raw_parts(source.as_ptr() as *const u8, source.len())
    }
}

/// [`WriteAsOutput::write_async`] 的 step 函数。
#[gen_may_cancel_future(CompioOutputWrite, pub, new(pub(crate)))]
async fn compio_output_write_impl_async_<'f, W, C>(
    output: &'f mut W,
    source: &'f [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, WriteErrTag>>
where
    W: AsyncWrite,
    C: abs_cancel::TrCancellationToken,
{
    if source.is_empty() {
        return SomeOf::new_left(0usize);
    }
    // compio 的 `write` 要求按值传入 `'static` 的 `IoBuf`，因此先把 source 复制进
    // owned 缓冲。
    let owned: Vec<u8> = as_init_bytes_(source).to_vec();
    let BufResult(res, _owned) = output.write(owned).await;
    match res {
        Result::Ok(n) => SomeOf::new_left(n),
        Result::Err(err) => SomeOf::new_right(TaggedError::new(
            err,
            WriteErrTag::Propagated,
        )),
    }
}

/// 拥有设备的写适配：设备与中转缓冲都随本类型存活，**稳态下不再分配**。
///
/// 语义与 [`WriteAsOutput`] 逐项一致，差别只有中转缓冲由本类型持有并复用。
///
/// # Examples
///
/// ```no_run
/// use buffex_compio_adapt::WriteAsOutputOwned;
/// use compio::net::UnixStream;
///
/// # async fn demo(stream: UnixStream) {
/// let output = WriteAsOutputOwned::with_capacity(stream, 4096);
/// assert!(output.stage_capacity() >= 4096);
/// # }
/// ```
pub struct WriteAsOutputOwned<W>
where
    W: AsyncWrite,
{
    dev_: W,
    /// 复用中转缓冲：每次写入前把源切片拷进 `[0, len)`。
    stage_: Vec<u8>,
}

impl<W> WriteAsOutputOwned<W>
where
    W: AsyncWrite,
{
    /// 用空的中转缓冲建立适配。
    pub fn new(dev: W) -> Self {
        WriteAsOutputOwned {
            dev_: dev,
            stage_: Vec::new(),
        }
    }

    /// 以容量 `cap` 预置中转缓冲：搬运泵按环容量预置一次即可。
    pub fn with_capacity(dev: W, cap: usize) -> Self {
        WriteAsOutputOwned {
            dev_: dev,
            stage_: Vec::with_capacity(cap),
        }
    }

    /// 取回底层设备。
    pub fn into_inner(self) -> W {
        self.dev_
    }

    /// 当前中转缓冲容量（复用上限）。
    pub fn stage_capacity(&self) -> usize {
        self.stage_.capacity()
    }
}

impl<W> TrOutput<u8> for WriteAsOutputOwned<W>
where
    W: AsyncWrite,
{
    type WriteAsync<'f>
        = CompioOutputWriteOwnedAsync<'f, 'f, W>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, WriteErrTag>;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        CompioOutputWriteOwnedAsync::new(self, source)
    }
}

/// [`WriteAsOutputOwned::write_async`] 的 step 函数。
#[gen_may_cancel_future(CompioOutputWriteOwned, pub, new(pub(crate)))]
async fn compio_output_write_owned_async_<'f, W, C>(
    output: &'f mut WriteAsOutputOwned<W>,
    source: &'f [MaybeUninit<u8>],
    _token: C,
) -> SomeOf<usize, TaggedError<std::io::Error, WriteErrTag>>
where
    W: AsyncWrite,
    C: abs_cancel::TrCancellationToken,
{
    if source.is_empty() {
        return SomeOf::new_left(0usize);
    }
    // 把源切片拷进复用缓冲（capacity 不够时 `extend_from_slice` 会增长一次）。
    let bytes = as_init_bytes_(source);
    let mut stage = core::mem::take(&mut output.stage_);
    stage.clear();
    stage.extend_from_slice(bytes);
    let BufResult(res, stage) = output.dev_.write(stage).await;
    output.stage_ = stage;
    match res {
        Result::Ok(n) => SomeOf::new_left(n),
        Result::Err(err) => SomeOf::new_right(TaggedError::new(
            err,
            WriteErrTag::Propagated,
        )),
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 中转缓冲的窗口语义：`as_uninit()` 恰好是本次请求长度，`set_len` 之后
    /// `as_init()` 是已写入的前缀。
    /// - 测试目标：`Stage_` 满足 compio 对 `IoBuf` / `IoBufMut` / `SetLen` 的期望，
    ///   即「读窗口 = 本次请求长度」而不是「= 容量」。
    /// - 测试手段：容量 16 的 `Stage_` 上把窗口设为 4，检查 `as_uninit().len()`；随后
    ///   `set_len(3)`，检查 `as_init().len()`。
    /// - 判定标准：窗口长度恰为 4（而不是 16）；已初始化长度为 3。
    #[test]
    fn stage_window_follows_request_() {
        let mut stage = Stage_::with_capacity_(16);
        stage.set_window_(4);
        assert_eq!(
            IoBufMut::as_uninit(&mut stage).len(),
            4,
            "读窗口必须是本次请求长度，而不是缓冲容量"
        );
        // SAFETY: 窗口内字节由设备写入，这里手工声明前 3 个已初始化。
        unsafe { stage.set_len(3) };
        assert_eq!(IoBuf::as_init(&stage).len(), 3, "已初始化长度应为 3");
    }

    /// 请求长度超过容量时窗口被截断到容量，不会越界。
    /// - 测试目标：`Stage_` 在请求长度大于底层容量时的行为是「截断」而不是 UB。
    /// - 测试手段：容量 8 的 `Stage_` 上把窗口设为 64，检查 `as_uninit().len()`。
    /// - 判定标准：窗口长度等于容量 8。
    #[test]
    fn stage_window_is_clamped_to_capacity_() {
        let mut stage = Stage_::with_capacity_(8);
        stage.set_window_(64);
        assert_eq!(IoBufMut::as_uninit(&mut stage).len(), 8);
    }

    /// `ReadAsInputOwned` / `WriteAsOutputOwned` 的容量在预置之后保持（复用），
    /// 且在 `&[u8]` / `Vec<u8>` 这类纯内存设备上能完成一次往返。
    /// - 测试目标：owned 形态的设备适配语义与借用形态一致，且中转缓冲可复用。
    /// - 测试手段：在 compio runtime 里用 `&[u8]` 作读设备（读到末尾给 EOF）、
    ///   `Vec<u8>` 作写设备，各跑一次搬运。
    /// - 判定标准：读回 3 字节后再次读到 `Closing`；写设备收到全部字节；两次调用之后
    ///   中转缓冲容量不变（没有重新分配）。
    #[compio::test]
    async fn owned_device_adapters_roundtrip_() {
        let data: Vec<u8> = vec![7u8, 8, 9];
        let slice: &[u8] = Box::leak(data.into_boxed_slice());
        let mut input = ReadAsInputOwned::with_capacity(slice, 8);
        let cap0 = input.stage_capacity();

        let mut target = [MaybeUninit::<u8>::uninit(); 8];
        let x = input.read_async(&mut target).await;
        assert_eq!(x.pick_left(), Some(3usize), "应读到 3 字节");
        // SAFETY: 上面已确认前 3 个字节被初始化。
        let got: Vec<u8> = target[..3]
            .iter()
            .map(|m| unsafe { m.assume_init() })
            .collect();
        assert_eq!(got, vec![7u8, 8, 9]);
        assert_eq!(input.stage_capacity(), cap0, "复用缓冲容量不应变化");

        // 读到末尾：EOF 必须以带 `Closing` 标签的错误表达。
        let x = input.read_async(&mut target).await;
        let err = x.pick_right().expect("读到末尾应给出错误");
        assert_eq!(err.tag(), ReadErrTag::Closing, "EOF 应带终止性标签 Closing");

        let mut output = WriteAsOutputOwned::with_capacity(Vec::<u8>::new(), 8);
        let src = [MaybeUninit::new(1u8), MaybeUninit::new(2u8)];
        let x = output.write_async(&src).await;
        assert_eq!(x.pick_left(), Some(2usize), "应写出 2 字节");
        assert_eq!(output.into_inner(), vec![1u8, 2]);
    }
}
