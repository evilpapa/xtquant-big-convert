# subscribe_whole_quote 全推行情 — 真机联调验证报告

> 本文记录迁移 VK 前的实盘验证（2026-08-10 交易日，及 2026-09-02 显式期权兼容补充）。当前路径与配置见 [传输层说明](RPC_TRANSPORTS.md)，设计方案见 [SUBSCRIBE_WHOLE_QUOTE_PUSH.md](SUBSCRIBE_WHOLE_QUOTE_PUSH.md)。

## 1. 环境与拓扑

```text
本地客户端 (Python 3.10 venv, editable 安装)
  ├─ BigQmtRpcClient ──► VK (内网 db5) ──► Windows QMT 服务端 (Python 3.6, BIGQMT_VK_DRYRUN)
  └─ VkQuotePushChannel ◄── pub/sub (bigqmt:quote_push:{acct}:{topic}) ◄── QuoteSubscriptionManager
```

- 服务端全量同步 40 个文件并逐文件 MD5 校验（保留生产配置，开启 `full_tick_cache_enabled: True`）。
- 教训：初期人工比对漏看大小写导致 19 个旧文件未替换，引发 `method is not allowed`；改用程序化全量 MD5 校验后解决。

---

## 2. 实盘验证结果

### 基础链路、数据正确性与多标的规模

| 阶段 | 验证内容 | 结果与关键数据 |
|------|---------|---------------|
| **基础链路（09:00–09:15）** | RPC 存活、订阅白名单、初始快照 prime、pub/sub 推送、集合竞价推送 | 全通：`get_full_tick` 2.1s；订阅 282ms 返 `seq`；竞价 `stockStatus=12` 正常推 |
| **单标的（60s，000001.SZ）** | 连续竞价推送节奏与单调性 | 21 笔，间隔 2.21–3.11s（均值 2.96s），`>4s` 为 0，time/vol/amt 零回退 |
| **20 只活跃股（120s）** | 主板/创业板/科创板覆盖与一致性 | 每只 41–42 次，间隔 2.93–3.3s（`>4s` 为 0），零丢失、零乱序、零回退 |
| **规模测试（60s）** | 20 / 50 / 100 只标的 | 订阅耗时 1.1s / 0.8s / 2.1s；增量推送 61 / 140 / 336 次；100% 覆盖、0 错误 |

### 生命周期、并发、异常与重启恢复

| 组别 | 验证项 | 结果 |
|------|-------|------|
| **A 组：心跳回收** | 正常保活（90s 31 次）、kill 不退订超时回收、同 `client_id` 重连恢复、退订后再订 | 全部通过 |
| **B 组：多客户端并发** | 两 client 共享同组合去重（仅建 1 个服务端订阅）、单方退订隔离、全退订拆订阅、不同组合隔离、同 client 多 `sub_id`、混合组合隔离 | 全部通过（B6 经 Bug #2 修复后通过） |
| **C 组：异常与重启** | 重复订阅幂等、空列表抛 `ValueError`、非法代码容错、同 topic 多 `sub_id` 退订隔离、服务端重启恢复 | 全部通过（C4 经 Bug #3、C6 经 Bug #4 修复后通过；中断 42s 自动重放恢复） |
| **D 组：配置** | `BIGQMT_QUOTE_HEARTBEAT_SECONDS=1` 实测生效；`heartbeat_timeout_seconds` 单测覆盖 | 通过 |

### 显式期权合约兼容（2026-09-02，完整大 QMT 2.1.19.0）

| 验证项 | 修复前 | 修复后 |
|-------|-------|-------|
| `get_full_tick(["10010974.SHO"])` | `{}` | 返回实时价、累计量与五档盘口 |
| 单期权实时订阅 | `subscribe_whole_quote` 30s 0 推送 | 走 `subscribe_quote(result_type="list")`，连续 500ms 推送 |
| ETF + 期权混合组合 | 仅 ETF 推送 | `510050.SH` 与 `10010974.SHO` 均收快照与增量推送（客户端仍单订阅号） |
| 510050 202609 期权链分析回归 | 28/28 | 28/28 有效，价格源为 `tick_last` |

> 回退逻辑仅作用于 `get_full_tick` 未回答的显式 `.SHO/.SZO` 代码，普通股票/ETF/市场代码走原路径。

---

## 3. 发现并修复的 4 个 Bug（TDD 修复）

| # | 问题 | 位置 | 根因 | 修复 |
|---|------|------|------|------|
| 1 | 推送解码报 `ExtraData` | `quote_push_channel.py` | QMT 无 `msgpack` 走 JSON，客户端按 `msgpack` 硬解 | `decode_push_payload` msgpack 失败时回退 JSON |
| 2 | 订阅线程泄漏与竞态 | `whole_quote_session.py` | `_sync_subscriber_locked` 每次新起线程未停旧线程 | 对 topic 集合做 diff，不变复用、变更先停再起 |
| 3 | 同 client 多 `sub_id` 退订误拆组合 | `quote_subscription_manager.py` | 引用计数按 `client_id` 而非 `(client_id, sub_id)` | 四处（订阅/退订/心跳/回收）统一改 `(client_id, sub_id)` 粒度 |
| 4 | 服务端重启后推送永久中断 | `whole_quote_session.py` | 自动重放未接线；且重启窗口 keepalive 被队列缓冲不报错 | 心跳循环增加推送静默检测（默认 10 周期 ≈30s 无推送即自动重放） |

---

## 4. 测试基线与遗留

- **回归结果**：`266 passed, 3 skipped, 1 failed`（唯一失败为预先存在的 `MysqlTransportTest::test_round_trip`，本地 venv 未装 `dbutils`）。
- **新增单测**：`test_quote_push_channel.py`（+2）、`test_whole_quote_client.py`（+6）、`test_quote_subscription_manager.py`（+1）。
- **遗留**：`heartbeat_timeout_seconds` 仅单测覆盖（实盘改配置需重启策略）。
