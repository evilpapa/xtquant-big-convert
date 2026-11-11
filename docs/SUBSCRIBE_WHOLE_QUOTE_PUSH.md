# subscribe_whole_quote 真推送方案（对齐 miniqmt）

> 状态：**待评审**（方案已成型，未动实现。实现严格按 TDD：先红→绿→回归）
> 分支：`feat/impl_subscribe_whole_quote`

## 1. 背景与现状诊断

### 1.1 miniqmt 语义（目标行为）

`xtdata.subscribe_whole_quote(code_list, callback)` 在 miniqmt 里是**真订阅**：

- 注册后，行情服务**持续推送**，每个行情周期触发一次 `callback(data)`，`data` 是 `{code: tick_dict}` 的全推快照。
- 返回一个 `seq`（订阅句柄）；`unsubscribe_quote(seq)` 后推送停止。
- 全市场用板块代码：`["SH"]`、`["SZ"]`、`["SH","SZ"]`（也支持 `"BJ"`、`"HK"`）。

### 1.2 当前实现（client 端假订阅，server 端空转）

- **client**（`xtquant_compat.py:781`）：`subscribe_whole_quote` 只是 `publish_event("subscribe_whole_quote", ...)` 到 redis stream（`bigqmt:quote_events:{account_id}`），然后**同步调一次 `get_full_tick` 触发一次 callback** 就返回 `seq`。之后**再无任何推送**。`callback` 不被持有，纯一次性。
- **server**：**全仓库没有任何代码消费 `quote_events` stream**，也没有任何代码调用大 QMT 的 `ContextInfo.subscribe_whole_quote` / `xtdata.subscribe_whole_quote`。事件发出去石沉大海。
- `unsubscribe_quote(seq)` 同样只发事件 + 删 redis 订阅记录，无实际效果。

**结论：miniqmt 的「注册 → 持续推送 → 回调」语义当前完全没有实现。**

### 1.3 transport 现状


### 1.4 可复用的资产

- `full_tick_cache`：server 周期 `ContextInfo.get_full_tick` 拉快照写 redis，client 读缓存。**是轮询拉取，不是推送**，但有现成的 demand/TTL 机制（本方案不采用它做数据面，仅作对比参考）。
- `BigQmtRpcHandlers`（`redis_rpc.py:333`）：server 端白名单 dispatch，`market_data` 适配器持有 `ContextInfo` —— server 端订阅管理器复用此路径访问大 QMT 行情接口。
- 参考实现：`quant-qmt-proxy` 的 `SubscriptionManager` 已验证 `xtdata.subscribe_whole_quote(["SH","SZ"], callback)` 推送模式 + 心跳超时（默认 60s）在本环境可行。

---

## 2. 已确认的决策（来自讨论）

| # | 决策点 | 结论 |
|---|--------|------|
| Q1 | 数据通路 | **方案 A：真推送**。server 端大 QMT 订阅回调 → 推送通道 → client callback。新增 server→client 推送通道。 |
| Q2 | 去重粒度 | **按组合去重**：`frozenset(code_list)` 规范化后作为订阅单元 key。不同 client 传相同集合 → 共享同一个大 QMT 订阅。 |
| Q3 | 引用计数 & keepalive | client 分配 `client_id`，周期发 keepalive（带 `client_id`+组合）；server 维护 `{组合: {client_id: last_seen}}`，**超时阈值 = 10 个心跳周期**未收到才认为该 client 消亡；组合所有 client 消亡后才真正退订大 QMT。 |
| Q4 | server 重启恢复 | **client 重放**：client 记忆自己的订阅集合，检测到断连/server 重启后自动重放订阅；server 无状态、靠 client 重放/keepalive 重建订阅。**server 订阅表落盘不做**（实现阶段决定：client 重放已覆盖恢复路径，落盘引入 QMT 环境文件 IO 复杂度，无额外收益）。 |
| Q5 | 大 QMT 行情源 | **ContextInfo 优先**：server 用策略进程内 `ContextInfo.subscribe_whole_quote`。"建/退订阅"收敛为可替换适配层，按真实环境实测微调（见 §7 风险 1）。 |
| Q7 | 推送编码 | **高效编码：msgpack 优先，json 兜底**。msgpack 作为可选依赖（`optional-dependencies`），全市场推送建议安装；未装时退化为 json。 |

---

## 3. 总体架构

```
┌─────────────┐   subscribe_whole_quote   ┌──────────────────────────────────┐
│   client A  │ ────────────────────────► │            server (大 QMT 进程)    │
│  (xtdata)   │   RPC: subscribe_whole     │  QuoteSubscriptionManager         │
└─────────────┘   _quote {client_id,       │   ├─ 组合去重 frozenset            │
                  │      codes, sub_id}     │   ├─ refcount {combo: {cid: ts}} │
┌─────────────┐                             │   └─ ContextInfo/xtdata.          │
│   client B  │ ────────────────────────►  │      subscribe_whole_quote(      │
└─────────────┘   同组合 → 共享同一订阅      │         codes, on_push)          │
                  │                          └──────────┬───────────────────────┘
   keepalive RPC  │  周期心跳 {client_id, sub_id}        │ 大 QMT 行情回调 on_push(data)
        ──────────┼──────────────────────────►          │
                  │                                     ▼
                  │                          ┌──────────────────────────┐
   行情推送        │ ◄──────────────────────  │  QuotePusher (PUB socket)│
   (PUB/SUB)      │   topic=combo, {data}    │  按组合 topic 广播         │
                  │                          └──────────────────────────┘
```

**三条逻辑通道**（对应三个职责）：


---

## 4. 详细设计

### 4.1 订阅单元 key（Q2：组合去重）

```python
def combo_key(code_list):
    """规范化组合 → 唯一 key。顺序无关、大小写统一、去空白。"""
    return ",".join(sorted({str(c).strip().upper() for c in (code_list or []) if str(c).strip()}))
```

- `["SH","SZ"]` 与 `["sz","SH"]` → 同一 key `"SH,SZ"`，共享同一个大 QMT 订阅。
- 全市场 `["SH"]`、标的组合 `["000001.SZ","600000.SH"]` 都是合法 key。

### 4.2 client 端（`BigQmtXtData.subscribe_whole_quote` 重写）


`unsubscribe_quote(sub_id)`：发 RPC `unsubscribe_whole_quote {client_id, sub_id}`，从本地表删除；该 sub_id 停止 keepalive。返回 0（对齐 miniqmt）。

### 4.3 server 端 `QuoteSubscriptionManager`（新模块）

挂在 server 进程内，持有 **ContextInfo**（Q5：优先用策略进程内 `ContextInfo.subscribe_whole_quote`；复用 `market_data` 适配器的 ContextInfo 引用）与 `QuotePusher`。

**大 QMT 订阅适配层**（Q5：ContextInfo 优先）。已调研确认大 QMT 真实签名（见下方"调研结论"），适配层对外只暴露两个方法：

```python
class QuoteSourceAdapter:           # ContextInfo 优先实现
    def subscribe(self, codes, on_push) -> handle: ...
    def unsubscribe(self, handle) -> None: ...
```

**调研结论（真实大 QMT 环境，官方文档 + 隔壁 quant-qmt-proxy 实测交叉验证）**：

| 项 | 结论 |
|---|---|
| 订阅签名 | `ContextInfo.subscribe_whole_quote(code_list, callback=None)` |
| `code_list` | 市场代码 `['SH','SZ']` 或品种代码 `['600000.SH','000001.SZ']` |
| 返回值 | `int` 订阅号 `subId`；**`< 0` 表示失败**（quant-qmt-proxy 实测） |
| 退订 | `ContextInfo.unsubscribe_quote(subId)`（与 `subscribe_quote` 共用同一退订方法） |
| **回调数据** | **增量推送**：每次回调只含**有变化**品种的最新 tick；`get_full_tick` 才是全量快照 |
| 回调线程 | 独立于 `handlebar` 的推送线程（官方建议回调内不阻塞、扔队列处理） |

`QuoteSubscriptionManager` 只跟 `QuoteSourceAdapter` 打交道，不直接碰 ContextInfo——`subscribe` 内部调 `ContextInfo.subscribe_whole_quote(codes, on_push)`、`unsubscribe` 内部调 `ContextInfo.unsubscribe_quote(handle)`；若实测仍有出入，只改这一层。

**核心状态（内存，权威）**：

```python
{
  combo_key: {
    "codes": [...],              # 原始 code_list
    "qmt_sub_handle": <大QMT订阅句柄>,   # subscribe_whole_quote 返回值
    "clients": {client_id: last_seen_ts},  # 引用计数 + 心跳
    "topic": push_topic,
  }
}
```

另维护 `sub_id → (client_id, combo_key)` 反向索引，用于按 sub_id 退订。

**三个 RPC handler**（加入 `BigQmtRpcHandlers.allowed_methods`，走现有 dispatch）：

- `subscribe_whole_quote {client_id, sub_id, codes}`：
  - `key = combo_key(codes)`。
  - 若该 combo 不存在：调 `ContextInfo.subscribe_whole_quote(codes, on_push)` 建订阅，记录 handle，`on_push` 闭包绑定 `key`；建 `topic`。
  - `clients[client_id] = now`；登记 `sub_id → (client_id, key)`。
  - 返回 `{combo_key, topic, push_endpoint}`。
- `unsubscribe_whole_quote {client_id, sub_id}`：
  - 由 `sub_id` 找到 `(client_id, key)`，`clients.pop(client_id)`，删除 `sub_id` 索引。
  - 若该 combo 的 `clients` 为空 → `ContextInfo.unsubscribe_whole_quote(handle)`（或对应退订 API），销毁 combo。
  - 返回 `{}`。
- `quote_keepalive {client_id, sub_id}`：
  - 由 `sub_id` 找 combo，`clients[client_id] = now`。返回 `{}`。

**reaper（后台周期任务，挂在 server 的 adjust/调度循环上）**：

- 每 `reap_interval` 秒扫描：对每 combo，删除 `now - last_seen > 10 * heartbeat_interval` 的 client。
- combo 的 `clients` 清空后 → 退订大 QMT、销毁 combo。

**on_push 回调**（大 QMT 行情线程触发）：


### 4.5 keepalive 与超时（Q3）

- `heartbeat_interval`（client 发心跳周期），**默认 3s**。
- 超时阈值 = `10 * heartbeat_interval = 30s`（Q3 已确认 10 个周期）。server 端某 client 超过 30s 无心跳 → 判定消亡，从所有 combo 的 `clients` 移除。
- 心跳与数据面解耦：即便行情静默（盘后用快照），心跳照发，保证引用计数准确。

### 4.6 server 重启恢复（Q4）

- **client 重放**：client 的 SUB 接收线程检测到推送通道断开/server ping 失败 → 触发重连；重连成功后，对本地订阅表里**所有活跃 sub_id 重新发 `subscribe_whole_quote`**（幂等：server 按 `(client_id, combo)` 去重，重放不会重复建大 QMT 订阅）。
- **server 落盘（不做）**：实现阶段决定 server 订阅表**不落盘**。client 重放已覆盖恢复路径（重启后 client 重放即可重建全部订阅），落盘只增加 QMT 环境文件 IO 与状态一致性复杂度，无额外收益。
- **幂等性**：`subscribe_whole_quote` handler 对相同 `(client_id, sub_id, combo)` 重复调用安全（重建 last_seen，不重复建大 QMT 订阅）。

---

## 5. 配置项

server 端（`config["quote_push"]`，见 `bigqmt_signal_trader_strategy.py`）：

| 配置键 | 默认 | 说明 |
|------|------|------|
| `enabled` | `true` | 是否启用全推推送服务 |
| `heartbeat_timeout_seconds` | `30.0` | client 无心跳超时阈值（= 10 个心跳周期 × 3s） |


client 端（`bigqmt_signal_trader_client_config.py` / 环境变量）：

| 配置 | 默认 | 说明 |
|------|------|------|
| `BIGQMT_QUOTE_CLIENT_ID` | 自动生成并持久化到 `~/.cache/bigqmt/quote_client_id` | client 唯一 id（重启稳定，用于重放识别） |
| `BIGQMT_QUOTE_HEARTBEAT_SECONDS` | `3.0` | 心跳周期（环境变量），须 < server 超时 |

---

## 6. TDD 实施计划（先红→绿→回归）

按依赖顺序分 6 个增量，每个增量都是「先写失败测试 → 实现 → 回归全套」。

**阶段 1 — 组合 key + 引用计数核心（纯逻辑，无 IO）**
- 红：`tests/bigqmt_signal_trader/test_quote_subscription_manager.py`
  - `combo_key` 顺序无关/大小写统一。
  - 两个 client 订阅同组合 → 只建一次大 QMT 订阅（mock ContextInfo），refcount=2。
  - 一个 client 退 → 不退大 QMT；全部退 → 退一次大 QMT。
  - 心跳超时：构造 `last_seen` 过期 → reaper 移除 client；combo 空 → 退订。
- 绿：`src/bigqmt_signal_trader/quote_subscription_manager.py`（`combo_key` + `QuoteSubscriptionManager`，ContextInfo 用注入的 mock）。
- 回归：全套测试。

**阶段 2 — server RPC handler 接线**
- 红：`subscribe_whole_quote` / `unsubscribe_whole_quote` / `quote_keepalive` 三个方法经 `BigQmtRpcHandlers.handle` 可达、白名单放行、参数校验、幂等重放。
- 绿：在 `redis_rpc.py` 加 handler 方法 + `allowed_methods`，注入 `QuoteSubscriptionManager`。
- 回归。


**阶段 4 — server on_push → 推送通道接线**
- 红：mock ContextInfo 触发 `on_push(data)` → `QuotePushChannel.publish` 被以正确 topic+data 调用；线程安全（并发回调不竞态）。
- 绿：`QuoteSubscriptionManager` 接 `QuotePushChannel`。
- 回归。

**阶段 5 — client 端重写（订阅 + SUB 接收 + keepalive 线程）**
- 红：`tests/bigqmt_signal_trader/test_whole_quote_client.py`
  - `subscribe_whole_quote` 发正确 RPC、注册本地 callback、启动 keepalive。
  - 收到推送帧 → 触发对应 callback。
  - `unsubscribe_quote` 发 RPC、停心跳。
  - 重放：模拟断连后 → 对所有活跃 sub_id 重发 subscribe。
- 绿：重写 `BigQmtXtData.subscribe_whole_quote` / `unsubscribe_quote`，新增 client 侧 SUB 接收与 keepalive 线程、`client_id` 管理。
- 回归。

**阶段 6 — 端到端 + 多 client 共享 + server 重启恢复**
- 红：端到端测试（in-proc fake server + 两 client）
  - 两 client 订同组合 → 各自 callback 都收到推送；server 只对大 QMT 建一次订阅。
  - 一 client 退 → 另一个仍收；全退 → server 退订大 QMT。
  - server "重启"（重建 manager + 推送通道）→ client 重放 → 恢复推送。
  - client 静默超 30s → server 清引用 → 退订。
- 绿：补齐集成胶水（server 启动时装配 manager+push channel，client 重连逻辑）。
- 回归：全套 + 现有 `test_all_apis.py` 端到端不破坏。

---

## 7. 风险与开放问题


---

## 8. 交付物清单（已全部交付）


**实现期间 TDD 抓到的两个真实 bug**：
1. **`_sub_index` 键冲突**：两 client 各自 `sub_id` 从 1 开始，server 以单 `sub_id` 为键互相覆盖 → 引用计数错乱、"全退才退订"失效。e2e 测试暴露后改为 `(client_id, sub_id)` 复合键。这是多 client 场景的核心正确性问题，单 client 测试无法覆盖。
2. **`load_client_config` 漏 `quote_client_id` 键**：导致 client_id 配置读不到、静默退回持久化文件路径。

**未实现**：server 订阅表落盘兜底（§4.6，经决策不做，靠 client 重放恢复）。
