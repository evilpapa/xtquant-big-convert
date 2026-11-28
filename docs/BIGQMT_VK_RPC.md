# 大 QMT VK Queue RPC 说明

> 更新时间：2026-11-21

## 1. 目标与方法映射

在大 QMT 策略进程内运行 VK RPC 服务，默认由 `run_time("adjust", ...)` 调度读取请求队列（直连 VK 需在策略 Python 3.12+ 安装 `valkey`）。白名单涵盖系统/行情/K线/板块/日历/下载/财务/期权/公式/账户查询与 `sync_positions`；下单类方法（`submit_order` / `cancel_order`）默认关闭，需显式开启 `rpc_allow_order_methods=True`。

| MiniQMT 方法名 | RPC 内部方法 | 说明 |
|---------------|-------------|------|
| `query_stock_asset` / `query_stock_positions` / `query_stock_position` | `get_asset` / `get_positions` / `query_stock_position` | 资产、全部/单股持仓 |
| `query_stock_orders` / `query_stock_trades` | `query_orders` / `query_trades` | 委托（支持 `cancelable_only`）、成交 |
| `get_full_tick` | `get_ticks` | 支持个股代码或 `["SH","SZ"]`；默认直调 RPC，可选开 VK 快照缓存 |
| `get_instrument_detail` / `get_instrumentdetail` | `get_instrument` | 合约详情 |
| `order_stock` / `order_stock_async` | `submit_order` | 默认关闭；`order_type` 23=买 / 24=卖，`price_type`（11/5/44/43/47 等）透传 `passorder()` |
| `cancel_order_stock` / `cancel_order_stock_sysid` | `cancel_order` | 默认关闭 |

**核心实现文件**：`src/bigqmt_signal_trader/vk_rpc.py`（协议与服务）、`xtquant_compat.py`（兼容层）、`src/xtquant/`（import shim）、`src/bigqmt_signal_trader_strategy.py`（策略与 drain 调度）、`src/bigqmt_signal_trader_vk_rpc_runtime.py`（纯 RPC 入口）、`tests/bigqmt_signal_trader/test_vk_rpc.py`。

---

## 2. 部署与最小配置

将 `src/bigqmt_signal_trader/`、`src/bigqmt_signal_trader_strategy.py`、`src/bigqmt_signal_trader_vk_rpc_runtime.py` 同步到 `<QMT_PYTHON_DIR>`，放入私有配置 `bigqmt_signal_trader_qmt_config.py`（勿提交）：

```python
BIGQMT_ACCOUNT_ID = "你的资金账号"
BIGQMT_CONFIG = {
    "host": "YOUR_VK_HOST", "port": 6379, "db": 5, "username": "", "password": "...",
    "rpc_allow_order_methods": False,
    "rpc_process_in_listener": True,
    "rpc_listener_methods": ("*",),
    "rpc_background_threads": False,
    "schedule_adjust": True,
    "schedule_adjust_interval": "500nMilliSecond",
    "full_tick_cache_enabled": False,
    "full_tick_demand_ttl_seconds": 10,
    "full_tick_cache_ttl_seconds": 10,
    "full_tick_refresh_interval_seconds": 3,
    "full_tick_max_requests": 8,
}
```

QMT 入口脚本（文件头 `# coding:gbk`，不勾选「启动本地 python」）负责将 `<QMT_PYTHON_DIR>` 加入 `sys.path`、reload `vk_rpc`/`strategy`/`runtime`、加载配置、绑定原生 `passorder`/`cancel`/`get_trade_detail_data` 并导出 `init`/`handlebar`/`adjust`/`order_callback`/`deal_callback`（直接复用仓库 `src/BIGQMT_VK_DRYRUN.py` 即可）。

外部直接调用示例：

```python
import valkey
from bigqmt_signal_trader.vk_rpc import call_vk_rpc

r = valkey.Valkey(host="YOUR_VK_HOST", port=6379, db=5, username="", password="...")
print(call_vk_rpc(r, account_id="YOUR_ACCOUNT_ID", method="get_positions", params={}, timeout_seconds=3))
```

---

## 3. VK 协议与通道布局

### 3.1 RPC 请求与响应

- **请求 channel**：`bigqmt:rpc:req:{account_id}`（负载含 `schema_version`、`request_id`、`account_id`、`method`、`params`、`reply_channel`、`reply_key`、`ttl_seconds`；使用安全编码）。
- **响应**：同时写入 key `bigqmt:rpc:resp:{account_id}:{request_id}`（带 TTL 便于超时排查）并 publish 同名 channel，格式含 `ok`、`data`、`error`、`handled_at`。

### 3.2 可选：`get_full_tick` 需求驱动缓存

两端开启 `full_tick_cache_enabled=True` / `BIGQMT_FULL_TICK_CACHE_CONFIG["enabled"]=True` 后启用：
- 客户端写需求到 hash `bigqmt:full_tick:demand:{account_id}`（含 `codes`、`requested_at_ts`、`expires_at_ts`、`cache_ttl_seconds`）。
- 服务端周期刷新快照写至 `bigqmt:full_tick:cache:{account_id}:{request_id}`（默认 TTL 10s，客户端校验 `updated_at_ts`）。
- 首次无缓存默认等 `3.5s`：个股列表超时回退 live RPC，市场令牌（`SH/SZ/BJ/HK`）直接抛超时不回退。

### 3.3 异步下载任务（download jobs）

避免长耗时下载冻结策略线程，拆分为 VK 队列异步分块执行：
- 键：`bigqmt:download:queue:{account_id}`、`bigqmt:download:job:{account_id}:{job_id}`（`pending → running → done | failed`）、`bigqmt:download:current:{account_id}`（串行）。
- 客户端：`submit_download_history_data2()` → `get_download_status(job_id)` / `wait_download(job_id)`；原 `download_history_data2()` 等价于提交 + 等待（默认上限 1800s，超时服务端继续跑）。
- 服务端参数：`download_jobs_enabled`、`download_job_chunk_size`（默认 10）、`download_job_max_wall_seconds`（默认 0.5s）、`download_job_ttl_seconds`（默认 3600）。

### 3.4 实时成交/委托回调推送（exec events）

服务端将 `order_callback` / `deal_callback` 的 `m_*` 字段规范化后 xadd + publish 到 `bigqmt:order_events:{account_id}` 与 `bigqmt:trade_events:{account_id}`（开关 `exec_events_enabled`，默认 `True`）：
- **成交字段**：`stock_code`、`trade_id`、`order_sys_id`、`volume`、`price`、`amount`、`commission`、`direction`、`action`（48/23→BUY、49/24→SELL，未知为空保留 `direction`）、`traded_at`。
- **委托字段**：`status`、`order_volume`、`traded_volume`、`price`、`traded_price`、`trade_amount`（精确柜台金额，缺省为 `0.0`，不估算替代）。
- 客户端注册 `XtQuantTraderCallback` 后调用 `start()` / `subscribe(acc)` 实时接收 `on_stock_order` / `on_stock_trade`。

---

## 4. 调度、延迟与预算控制

### 4.1 两档模型与 drain 线程

- **inline 档**：`ping`、行情与 `get_asset` 可在监听线程直出，但会受 QMT C++ 主循环周期占 GIL 带来的尾延迟影响。
- **deferred 档（`LISTENER_DEFERRED_METHODS`）**：所有依赖 `get_trade_detail_data` 的交易查询（离开主线程返空）及下单/撤单，一律由 `run_time("adjust", interval)` 驱动在主线程 `drain_pending()` 执行。
- **`rpc_background_threads` 一律 `False`**：后台线程每拿一次 GIL 付一拍（VK 多步往返可达 ~400ms）；drain 模式下所有轻请求 ≤1 拍。
- **重读 offload（`rpc_heavy_offload`，默认 `True`）**：财务数据、公式、市场令牌 `get_full_tick`、超 `rpc_heavy_codes_threshold`（默认 20）代码、`period="tick"` 和日期窗口 K 线交给工作线程、下一拍由 `adjust` 回包（减少占拍，重读多付 1–2 拍；`download_*` 因全程持 GIL 留在主线程）。可通过 `probe_capabilities` 的 `thread_routing.heavy_sample` 查看路由。

| `schedule_adjust_interval` | 稳态频率 | inline 尾延迟 (p90/max) | 说明 |
|---------------------------|---------|------------------------|------|
| `500nMilliSecond` | ~2.4/s | ~490 / 510ms | 省电 |
| `200nMilliSecond` | ~5/s | ~200ms 量级 | 平衡点 |
| `100nMilliSecond` | 10/s（启动回放期除外） | ~92 / 108ms | 现默认 |

> 交易查询上限取决于柜台 `get_trade_detail_data` 自身耗时（与委托/成交行数相关），高频查持仓建议读 VK 缓存。

### 4.2 每拍时间预算、过期拒绝与结果反查

- **时间预算**：`drain_pending(budget_seconds=...)` 默认取 `max(adjust 间隔, 0.5s)`（`rpc.drain_budget_seconds=0` 可关闭），每拍至少执行 1 笔，批内每 0.25s 结算并发送已完成回复，防止 20 笔串行 `passorder` 堵住 `order_callback`。
- **过期拒绝**：请求信封带 `timeout_seconds`，轮到执行时已过期限（留 1s 余量，最多期限的 1/4）则拒绝并返回 `RequestExpired`（防止客户端超时后仍落单；撤单不拒）。
- **共用快照与反查**：单次结算按账号只查一次 `get_trade_detail_data`；客户端 `order_stock` 超时后自动调用只读 `get_request_outcome(request_id)`（收包线程直答）明确「未下可重试」或「已下及编号」。

---

## 5. 安全约束与 QMT 重启后备注恢复

- **安全约束**：默认只读（远程下单关闭）、QMT API 在主线程执行、账号不匹配拒绝请求、响应写带 TTL 的 VK key。
- **重启后委托备注恢复**：ORDER/DEAL 查询与原生回调会在 VK identity 存储记录系统委托号与备注的精确映射（7 天 TTL，独立连接池 250ms 超时、不重试）。仅在原生备注为空时按「账户 + 账户类型 + 源交易日 + 交易所 + 完整系统委托号 + 证券与方向一致」精确恢复；冲突则有效期内拒绝恢复，绝不按量价猜配。未观察到、过期或 VK 不可用时仍返回空备注（调用方按未知委托处理），不补造安装前已丢失的旧记录。
- **本地测试**：`python -B -m unittest discover -s tests\bigqmt_signal_trader`。
