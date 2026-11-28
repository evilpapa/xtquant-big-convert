# subscribe_whole_quote 真推送方案（对齐 MiniQMT）

> 状态：已按 TDD 交付（实盘联调结果见 [SUBSCRIBE_WHOLE_QUOTE_LIVE_VERIFICATION.md](SUBSCRIBE_WHOLE_QUOTE_LIVE_VERIFICATION.md)）。

## 1. 背景与目标

MiniQMT 的 `xtdata.subscribe_whole_quote(code_list, callback)` 是持续真推送：注册后行情服务按周期回调 `callback(data)`（`{code: tick_dict}`），支持市场代码（`["SH"]` / `["SZ"]` / `["SH","SZ"]` / `"BJ"` / `"HK"`）与个股组合，返回 `seq` 并通过 `unsubscribe_quote(seq)` 停止。

旧版桥实现仅为客户端发事件 + 单次 `get_full_tick` 回调、服务端未消费，未实现持续推送。`full_tick_cache` 属轮询缓存而非推送，本方案新增独立推送通路，服务端复用 `BigQmtRpcHandlers` 与 `market_data` 适配器的 `ContextInfo`。

---

## 2. 核心设计决策

| 决策点 | 结论 |
|-------|------|
| **数据通路（Q1）** | 服务端大 QMT 订阅回调 → 独立 PUB/SUB 推送通道 → 客户端回调 |
| **去重粒度（Q2）** | 规范化组合 `combo_key`（去空白、大写、排序去重），相同集合在服务端共享同一大 QMT 订阅 |
| **引用计数与心跳（Q3）** | 按 `(client_id, sub_id)` 计数；客户端默认每 3s 发 `quote_keepalive`，超过 10 个周期（30s）未续期判定消亡，组合无活跃订阅者时退订大 QMT |
| **服务端重启恢复（Q4）** | **客户端自动重放**（不落盘订阅表，避免 QMT 文件 I/O 复杂度）；重放接口幂等 |
| **大 QMT 行情源（Q5）** | 优先使用策略进程内 `ContextInfo.subscribe_whole_quote`，经可替换 `QuoteSourceAdapter` 隔离 |
| **推送编码（Q7）** | `msgpack` 优先、`json` 兜底（服务端无 `msgpack` 时自动降级，客户端对应回退解码） |

---

## 3. 总体架构

```text
client A / B (xtdata)
  ├── RPC: subscribe_whole_quote / unsubscribe_whole_quote / quote_keepalive ──► QuoteSubscriptionManager (大 QMT 进程)
  │                                                                                ├─ combo_key 去重 + (client_id, sub_id) 引用计数
  │                                                                                └─ QuoteSourceAdapter → ContextInfo.subscribe_whole_quote
  └── SUB 接收线程 (按 combo topic 订阅) ◄── QuotePusher (PUB/SUB 增量广播) ◄───────── 大 QMT 行情回调 on_push(data)
```

---

## 4. 详细设计

### 4.1 组合键规范化

```python
def combo_key(code_list):
    return ",".join(sorted({str(c).strip().upper() for c in (code_list or []) if str(c).strip()}))
```

例如 `["SH", "SZ"]` 与 `["sz", "SH"]` 归一化为 `"SH,SZ"`，共享同一个大 QMT 订阅。

### 4.2 大 QMT 适配层（`QuoteSourceAdapter`）

仅暴露 `subscribe(codes, on_push) -> handle` 与 `unsubscribe(handle) -> None`，隔离真实环境差异：

| 项 | 实测结论（官方文档 + `quant-qmt-proxy` 交叉验证） |
|---|-----------------------------------------------|
| 签名与返回值 | `ContextInfo.subscribe_whole_quote(code_list, callback=None)` 返回 `int` 订阅号，`subId < 0` 表示失败 |
| 退订 | `ContextInfo.unsubscribe_quote(subId)`（与 `subscribe_quote` 共用） |
| 回调数据与线程 | **增量推送**（仅含有变化标的，全量快照由客户端 `get_full_tick` 预拉）；运行在独立推送线程，回调内不可阻塞 |

### 4.3 服务端 `QuoteSubscriptionManager`

- **内存状态**：`{combo_key: {"codes", "qmt_sub_handle", "clients": {(client_id, sub_id): last_seen_ts}, "topic"}}` + `(client_id, sub_id) → combo_key` 反向索引。
- **三个白名单 RPC handler**：
  - `subscribe_whole_quote {client_id, sub_id, codes}`：组合不存在则建大 QMT 订阅，登记引用并返回 `{combo_key, topic, push_endpoint}`（幂等）。
  - `unsubscribe_whole_quote {client_id, sub_id}`：移除引用；若组合引用归零则退订大 QMT 并销毁组合。
  - `quote_keepalive {client_id, sub_id}`：刷新 `last_seen_ts`。
- **Reaper**：挂在服务端 `adjust` 调度循环，定期清理超过 `10 * heartbeat_interval`（默认 30s）未续期的订阅项，组合清空即退订大 QMT。

### 4.4 客户端生命周期与重启恢复

- `subscribe_whole_quote`：调用订阅 RPC、按需拉初始快照、注册本地 callback、启动 SUB 接收线程与心跳线程（默认 3s，与行情数据面解耦）。
- `unsubscribe_quote(sub_id)`：发退订 RPC、移除本地表与心跳，返回 `0`（对齐 MiniQMT）。
- **重放恢复**：检测到断连、ping 失败或订阅期间推送静默超过阈值（默认 10 心跳周期），重连并对所有本地活跃 `sub_id` 幂等重发 `subscribe_whole_quote`。

---

## 5. 配置项

- **服务端（`config["quote_push"]`）**：`enabled`（默认 `true`）、`heartbeat_timeout_seconds`（默认 `30.0`）。
- **客户端（配置 / 环境变量）**：
  - `BIGQMT_QUOTE_CLIENT_ID`：唯一客户端 ID，默认自动生成并持久化至 `~/.cache/bigqmt/quote_client_id`。
  - `BIGQMT_QUOTE_HEARTBEAT_SECONDS`：心跳周期（默认 `3.0`，须小于服务端超时）。

---

## 6. TDD 实施路径与交付记录

按依赖分阶段推进（先红→绿→全套回归）：
1. **核心状态机**：`combo_key` 与 `QuoteSubscriptionManager` 引用计数/超时回收（`test_quote_subscription_manager.py`）。
2. **服务端 RPC 接线**：三 handler 加入 `vk_rpc.py` 白名单与参数校验。
3. **推送通道与回调接线**：`QuotePushChannel` 编码/发布及 `on_push` 并发线程安全。
4. **客户端会话重写**：订阅、SUB 线程管理、心跳保活与静默重放（`test_whole_quote_client.py`）。
5. **端到端验证**：多客户端共享、退订隔离、服务端重启恢复与 30s 超时回收。

**TDD 阶段捕到的两个关键缺陷**：
1. **`_sub_index` 键冲突**：两客户端 `sub_id` 均从 1 起导致单 `sub_id` 键互相覆盖，改为 `(client_id, sub_id)` 复合键。
2. **`load_client_config` 漏读 `quote_client_id`**：补齐键读取，避免静默回退持久化文件。

> 未实施项：服务端订阅表落盘（按决策 Q4 放弃，完全由客户端重放恢复）。
