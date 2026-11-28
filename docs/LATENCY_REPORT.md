# 延迟测试报告

> 本报告记录迁移 VK 前的 Redis 测量，数据来自同一台本地机器和同一套 QMT 实盘环境，仅用于判断量级。当前传输路径与配置见 [传输层说明](RPC_TRANSPORTS.md)。

## 测量方法与常见偏差

- **端到端范围**：客户端发起 RPC → 传输层 → QMT 策略处理 → 返回，含全部编解码与网络开销（交易时段测得，非交易/刚启动耗时明显偏高）。
- **QMT 主线程与 GIL**：读取请求在主线程（`adjust`/drain）串行处理并与行情回调共享 GIL，尖峰主要源于调度与 GIL 而非网络。后台接收线程从阻塞 I/O 返回后每次拿 GIL 约等一个 `adjust` tick（~100ms），一次往返的多条命令会叠加多拍延迟。
- **防相位锁死**：`adjust` 为 100ms 拍时，固定 `sleep(0.3)` 会锁死在同一相位（同一代码可测出 11ms 假中位数，加随机抖动 0.21–0.40s 后为真实 42ms）。drain 的真实分布为 `[0, 1 拍]` 均匀分布，中位约半拍。
- **避开重启回放窗口**：策略刚启动时 QMT 用 `handlebar` 回放历史 K 线，日志出现 `adjust cadence: ticks=51429 ... over 10s`（~5000 拍/秒），此时任何 RPC 都是毫秒级（0.3.28 报告中的 3.4ms / 195 次/秒即测于此窗口）。**必须等日志回到稳态 `ticks=100~105`（10Hz）再测**。

---

## 核心结论：`rpc_background_threads` 一律 `False`

2026-09-22（v0.3.53，`schedule_adjust_interval="100nMilliSecond"`，稳态 `ticks≈100` + 0.21–0.40s 随机抖动，n=20）实测表明：

- **模式选择比传输选型更重要**：0.3.46 及之前后台线程模式由 `adjust` 顺手 LPOP 抢答，表现是 ~100ms 与 500–900ms 混杂；关闭该 LPOP 后后台线程全部退化为 500–900ms（单次往返 4–8 条命令各付一拍）。
- **drain 模式（`rpc_background_threads: False`，0.3.51 起默认）**：所有轻读和交易查询稳定在一拍内答完（中位 ~35–75ms，单拍最多处理 20 条，并发友好），无 900ms 长尾。**旧配置显式写了 `True` 不会自动切换，须手动改为 `False` 并重启策略**，并在日志确认：

```text
[bigqmt_rpc] 已启动 queue=... background_threads=False
```

- **重读 offload（0.3.53+）**：财务数据、公式、市场令牌 `get_full_tick`、>20 代码、`period="tick"` 和日期窗口 K 线交由工作线程执行、下一拍由 `adjust` 回包（重读多付 1–2 拍，轻读和交易查询不受影响）。实测（100ms 拍连打，看 `adjust cadence` avg/max）：全市场 `get_full_tick(["SH","SZ"])`（~200ms）拍 0.100/0.25–0.33s；3 只 4 个月 1m 窗口（~500ms）拍 0.100/0.27–0.29s；`get_financial_data` 10 只 3 表（~2.1s）拍 0.100/~0.5s。`download_*` 因全程持 GIL（5 只 ~1.2s，拍 0.56–0.63/1.3–1.5s），留在 `adjust` 线程。可设 `rpc_heavy_offload: False` 关闭。

### 历史对照（2026-09-08，100 只读方法回放窗口）

> 以下为早前重启后立即测得（落于回放窗口），不代表 10Hz 稳态；仅保留用于说明线缆本身非瓶颈（各渠道最快均 0.2–0.3ms）及 MySQL 轮询量级。

| 传输 + 模式 | p50（非稳态） | p90 | 最快 | 跨机 |
|------------|-------------|-----|------|------|
| redis + 后台线程 | 3.4ms | 25.2ms | 0.3ms | ✅ |
| redis + drain | 30.7ms | 93.4ms | 0.2ms | ✅ |
| mysql | ~105ms | — | — | ✅ |

---

## FormulaServer 直连对比（只读行情）

| 路径 | 典型耗时 | 说明 |
|------|---------|------|
| **FormulaServer 直连（58600）** | **~0.07ms** | 绕过策略进程，支持非复权只读行情/参考方法 |
| Redis RPC 同方法（早期批） | ~13ms | 稳态 RPC 地板约 1 拍（~100ms），直连快 2–3 个数量级 |

---

## 下单链路延迟拆解

| 环节 | 耗时 | 说明 |
|------|------|------|
| `order_stock_async` 返回 `seq` | **<1ms** | 本地入队，不碰网络 |
| 提交到 QMT（`passorder`） | 1 拍（~100ms）+ `passorder` ~200ms | QMT 主线程串行执行 |
| 委托号回填（结算） | 通常 <1s | 停放应答 + `adjust` 轮询，有 deadline 兜底 |
| `on_order_stock_async_response` | 提交后 ~0.3–2s | 等屏障从推送学到委托号（上限 2s） |
| `on_stock_order` / `on_stock_trade` | 柜台回报后 ms 级 | pub/sub 推送，保序在 `async_response` 之后 |

> 下单无法并行化（`get_trade_detail_data` 离开主线程返空）；批量提交用 `order_stock_batch`，读密集流量用 `call_async` 叠加往返延迟。

---

## 与社区方案量级参考

2026-09-08 同终端实测 [cfquant](https://github.com/95ge/cfquant)（MIT，named pipe + 中继 hub，默认 Pipe 普通桥，后台接收线程）：

- `query_stock_positions` 实测 **92.8ms**（同期非稳态 Redis 3.8ms）；裸管道回显仅 0.012ms（cfquant 0.822ms、本项目 0.207ms），99% 耗时来自 QMT 唤醒调度而非管道本身。
- 其低延迟变体 `CFQUANT_TRADE_LOWLAT.py` 在 `init()` 中 `run_forever(sleep_seconds=0.001)` 独占主线程（策略无法再做他事），未单测；`CFQUANT.py` 走 500ms 定时器更慢。其核心价值在于 `ctypes` + `kernel32` 管道可在禁 socket、禁 pip 的券商终端运行。
