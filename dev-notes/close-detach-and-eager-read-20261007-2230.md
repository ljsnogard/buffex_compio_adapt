# 收尾不再取消后台泵 + 读侧「有数据就交付」（compio 侧）

日期：2026-10-07 22:30
涉及：`buffex_compio_adapt/src/{read_,write_}.rs`、`tests/read_eager_.rs`
同构改动与完整因果链：`buffex_tokio_adapt/dev-notes/local-pump-and-eager-read-20261007-2230.md`
下游：`smux_v1_sock_demo`（`dev-notes/adaptor-driven-transport-20261007-2230.md`）

## 1. 写侧：`Drop` 从「取消泵」改成「关生产端 + detach」

compio 侧的写适配器本来就用 `compio::runtime::spawn` 挂后台泵（三个后端里唯一
「自驱动」的），所以缺的不是驱动，而是**收尾**：

* `compio::runtime::JoinHandle` 在 drop 时 `cancel(true)`——丢掉适配器等于取消后台泵，
  已提交进环、还没上网的字节随之消失；
* 而上层协议栈（`smux_v1` 的写泵）只会丢连接，不会替适配器调
  `close_async`。

修法：给 `BuffWrite` 加 `Drop`：`tx_.close()`（让泵排空后正常退出并 `shutdown` 设备）+
`pump.detach()`。本机环回里「阶段末尾丢连接」因此不再丢尾巴。

## 2. 读侧：先单独读一次设备，再拷进环

读泵原先直接用 `move_items_from_input_async`，语义是**填满段**：设备给出 25 字节后转为
空闲时它只是 `Pending`，数据被扣在段里、一个字节也不提交——请求 / 应答式上层（握手帧、
乒乓消息）因此永久静止。

tokio / smol 侧可以用「每轮只 poll 一次搬移」保住零拷贝；compio **不能**用那一招：
compio 的设备读是**提交型**操作，丢弃未完成的 future 即取消它，已完成的读结果会跟着丢。
因此这里改成：

```text
① input.read_async(&mut staged[..]).await   ← 一次设备读（一次系统调用，读到多少算多少）
② while off < n { 借环写段 → 拷 staged[off..n] → drop 提交 }   ← 环满则 park
```

代价是 compio 侧本就存在的中转拷贝之外**多一次拷贝**（设备 → compio 的 owned 中转 →
本地暂存 → 环段）；换来的是「上层提交完就能被看到」。若上游 `abs_buff` 把输入搬移改成
「有数据就交付」，这一层可以整体去掉。

## 3. 验收

| 用例 | 结果 |
| --- | --- |
| `tests/read_eager_.rs`（环容量 64 KiB、设备只给 25 字节也必须交付） | 1/1 |
| 既有 `tests/compio_pump_.rs` + 文档测试 | 全通过 |
| `buffex_sock_uds_demo/tests/read_semantics_.rs` 的两条 compio 用例 | 通过（「不交付」改成「必须交付」） |
| `smux_v1_sock_demo` 全量环回（含 compio 的两组配对） | 3/3 |
