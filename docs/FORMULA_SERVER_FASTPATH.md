# FormulaServer 直连快速路径（58600）

## 概述与协议

大 QMT 的 `58600` 端口是内置 C++ 行情/参考数据服务 **FormulaServer**（地址见 `<QMT>/config/formulaserver/formulaserver.ini` 的 `[server_formula] address = 0.0.0.0:58600`），对应官方客户端为 `bin.x64/Lib/site-packages/qmt_api`。

协议为 **BSON over TCP**（见 `qmt_api/net/RPCBase.py`）：

```text
| packLen(uint32 BE) | seq(uint32 BE) | cmd(uint16 BE) | tag(uint16 BE) | BSON body |
```

- `cmd = 3`（`NET_CMD_RPC`），请求体 `{"func": <名>, "params": {...}}`，响应 `{"status": 0, "params": {...}}`（非 0 时含 `ErrorID` / `ErrorMsg`）。
- `tag & 7` 标记 zlib 压缩，`tag >> 8` 的低 4 位为 `seq` 高位。
- **性能**：绕开策略进程与 GIL，实测裸往返 **p50 0.07ms**、穿过完整 `BigQmtRpcClient.call` 为 **0.145ms**（对照迁移 VK 前 Redis RPC ~13ms，快约 90 倍）。

## 能力边界与路由清单

FormulaServer **仅提供行情/参考数据**，所有账户、持仓、委托、成交、下单和五档盘口（`getAsset` / `getPositions` / `getAccountDetail` / `passorder` / `getFullTick` / `getQuote`）均返回 `ErrorID 200005 未找到该服务`，仍须走 RPC 桥。

### 已接入方法（8 个）

| 客户端方法 | FormulaServer func |
|-----------|--------------------|
| `get_last_volume` / `get_total_share` | `getLastVolume` / `getTotalShare` |
| `get_contract_multiplier` / `get_main_contract` | `getContractMultiplier` / `getMainContract` |
| `get_weight_in_index` / `get_stock_list_in_sector` | `getWeightInIndex` / `getStockListInSector` |
| `get_market_data_ex` / `get_market_data` | `getMarketData`（仅 `dividend_type="none"` 或空） |

### 刻意不走直连的方法

- **`get_trading_dates`**：FormulaServer 要求传股票代码（传市场代码如 `SH` 静默返空），与本桥按市场查询语义不符。
- **`get_divid_factors` / `get_risk_free_rate`**：参数语义不同（区间 vs 单日、`index=-1` vs `timetag`）。
- **复权 K 线**：实测 `dividendType` 传 `front` 与 `none` 结果相同（复权未生效），非 `"none"` 一律回退 RPC。
- **`get_instrument` / `get_instrument_detail`（0.3.61 起移除）**：国金 build 抽样 300 只显示 43% `TotalVolume=0`、32% `OpenDate=0`、22% 陈旧（另注：其字段名有官方拼写 `FloatVolumn`/`TotalVolumn`）。改为走 RPC（#405 起在同机存在已登录 miniQMT 且 `native_xtdata_enabled` 开启时优先走 native SDK，再回落 `ContextInfo`）。

### 待盘中验证项

`getMarketData` 支持 `fields=['quoter']`（注释称返回盘口五档/十档），但收盘后实测返空，**未盘中验证前不接入 `get_full_tick`**：

```python
c.request('getMarketData', {
    'fields': ['quoter'], 'stockCodes': ['000001.SZ'],
    'startTime': '', 'endTime': '', 'period': 'tick',
    'dividendType': 'none', 'count': -1,
})
```

## 失败回退与依赖

所有异常均自动回退 RPC 桥；内置纯 Python 无依赖 BSON 编解码，若存在 `pymongo.bson` 或 `xtquant.xtbson` 则优先使用（输出逐字节一致）。

| 情况 | 行为 |
|------|------|
| 方法不在映射表 | `supports()` 为 False，直接走 RPC |
| 参数无法翻译 | 返回 `Unroutable` 回退 RPC，不触发熔断 |
| 连接/IO 失败 | 回退 RPC 并熔断 `failure_cooldown_seconds`（默认 30s） |
| 服务端返回 `200005` | 永久标记该方法为 unimplemented，不影响其他方法 |
| Socket 断开（如 QMT 重启） | 自动重连并重试一次 |

## 配置与排查

客户端默认开启，通常无需配置（`BIGQMT_CONFIG["formula_server"]` 优先级高于 `BIGQMT_FORMULA_SERVER_CONFIG`；环境变量 `BIGQMT_FORMULA_ENABLED=0` 可关闭）：

```python
BIGQMT_FORMULA_SERVER_CONFIG = {
    "enabled": True,
    # "host": "127.0.0.1",               # 跨机需放行 58600 防火墙
    # "port": 58600,                     # 缺省先读 qmt_root ini，再退回 58600
    # "qmt_root": r"D:\QMT交易端",
    # "timeout_seconds": 3.0,
    # "methods": ["get_total_share"],    # 白名单限流
    # "failure_cooldown_seconds": 30.0,
}
```

运行时排查：

```python
client._formula_router().stats()
# {'enabled': True, 'hits': 202, 'misses': 0, 'available': True, 'unimplemented': [], 'methods': [...]}
```

正常启动日志打印 `[bigqmt_formula] 已连接 127.0.0.1:58600（...）`，熔断时打印 `[bigqmt_formula] 暂不可用，接下来 30 秒走 RPC：...`。
