# MiniQMT 无损替换兼容层

## 目标

将依赖 MiniQMT（`xtquant.xttrader` / `xtquant.xttype` / `xtquant.xtdata` / `xtquant.xtconstant`）的代码无损切到「大 QMT 策略进程 + VK RPC」，保持业务层调用习惯不变（`query_stock_*`、`order_stock*`、`cancel_order_stock_sysid`、`xtdata.get_full_tick` 等）。

---

## 两种接入方式

### 方式一：显式导入（推荐灰度，不影响真实 `xtquant`）

```python
from bigqmt_signal_trader.xtquant_compat import StockAccount, configure, xt_trader, xtdata
from bigqmt_signal_trader import xtquant_compat as xtconstant

configure()  # 原地更新已导入的 xt_trader / xtdata 对象
acc = StockAccount(xt_trader.client.account_id, "STOCK")
positions = xt_trader.query_stock_positions(acc)
ticks = xtdata.get_full_tick(["600000.SH"])
```

### 方式二：`xtquant` shim 替换（最终零改动切换）

将仓库 `src` 置于 `PYTHONPATH` 最前，旧代码 `from xtquant ...` 自动命中 `src/xtquant/` shim（同机装有真实 `xtquant` 时必须保证 `src` 排在最前）：

```powershell
$env:PYTHONPATH = "D:\gjzqqmt\xtquant_big_convert\src;$env:PYTHONPATH"
```

在私有配置 `bigqmt_signal_trader_client_config.py` 中填写连接参数（勿提交）：

```python
BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
BIGQMT_RPC_TIMEOUT_SECONDS = 6.0
BIGQMT_CONFIG = {"host": "YOUR_VK_HOST", "port": 6379, "db": 5, "username": "", "password": "******"}

BIGQMT_FULL_TICK_CACHE_CONFIG = {
    "enabled": False, "demand_ttl_seconds": 10, "cache_ttl_seconds": 10, "wait_seconds": 3.5,
}
# 保持 True 以对齐 MiniQMT 历史数据行为：先下原始数据，再由 get_local_data(..., dividend_type="front_ratio") 读前复权
BIGQMT_LOCAL_CACHE_CONFIG = {"enabled": True, "dir": None, "fallback_rpc": True, "format": "auto"}
```

---

## 推荐落地步骤与下单开关

1. 大 QMT 侧运行 `BIGQMT_VK_DRYRUN` / `bigqmt_signal_trader_vk_rpc_runtime.py`，保持 `rpc_allow_order_methods=False`。
2. 策略侧先用方式一跑只读自检（资产、持仓、单票与 `["SH", "SZ"]` 全市场快照），核对与 MiniQMT 字段一致。
3. 切入 `core/trader.py` 初始化（仍关远程下单），验证风控、账号与价型无误后，在大 QMT 私有配置开启：

```python
BIGQMT_CONFIG = {
    "host": "YOUR_VK_HOST", "port": 6379, "db": 5, "username": "", "password": "******",
    "rpc_allow_order_methods": True,  # 开启后 price_type 透传至 passorder()
}
```

4. 最终通过方式二切换 `xtquant` shim，保持旧 import 不变。

---

## 兼容方法清单

| MiniQMT 调用 | 状态 | 说明 |
|-------------|------|------|
| `XtQuantTrader(path, session_id)` / `start` / `connect` / `subscribe` / `register_callback` | 已兼容 | 本地构造 RPC 客户端，`start/connect/subscribe` 返回 `0`（`subscribe` 补齐账号） |
| `query_stock_asset(acc)` | 已兼容 | 返回对象含 `cash` / `available_cash` / `total_asset` / `market_value` |
| `query_stock_positions(acc)` / `query_stock_position(acc, code)` | 已兼容 | 含 `stock_code` / `volume` / `can_use_volume` / `avg_price` / `price`；单股无持仓返 `None` |
| `query_stock_orders(acc, cancelable_only=False)` / `query_stock_trades(acc)` | 已兼容 | 含 `order_type` / `order_status` / `order_volume` / `traded_volume` / `traded_price` / `order_sysid` |
| `order_stock*()` / `cancel_order_stock_sysid()` | 已兼容 | 需大 QMT 侧开启 `rpc_allow_order_methods=True` |
| `xtdata.get_full_tick(codes)` | 已兼容 | 默认直接 RPC，支持单票/ETF/`["SH","SZ"]`；可选开 VK 快照缓存 |
| `xtdata.get_instrument_detail` / `get_instrument_type` | 已兼容 | 映射 `get_instrumentdetail()`；不支持类型查询时按代码前缀兜底 |
| `xtdata.subscribe_quote` / `subscribe_whole_quote` / `unsubscribe_quote` | VK 兼容 | 写入 `bigqmt:quote_subscriptions:{account_id}` 并向 `bigqmt:quote_events:{account_id}` 发事件；退订直接删表并推事件，不等待 QMT 确认 |
| `xtdata.get_market_data` / `get_market_data_ex` / `get_local_data` | 已接入 RPC | 支持 DataFrame 自动序列化还原；后两者不支持时回退 `get_market_data` |
| `xtdata.get_stock_list_in_sector` | 已接入 RPC | 失败时 `"沪深A股"` 回退 `get_full_tick(["SH","SZ"])` 过滤（耗时取决于全市场快照） |
| `get_sector_list` / `get_sector_info` / `get_divid_factors` / `download_history_data*` / `get_trading_dates` / `get_holidays` / `download_holiday_data` / `get_ipo_info` / `get_etf_info` / `download_etf_info` / `get_option_list` / 历史期权 / `get_financial_data` / `download_financial_data` / `call_formula` / `subscribe_formula` / `unsubscribe_formula` / `get_formula_result` / `gen_factor_index` | 已接入 RPC | 实际可用性取决于大 QMT `ContextInfo` 是否暴露对应方法；支持 DataFrame 序列化 |
| `query_ipo_data()` / `query_new_purchase_limit()` | 占位兼容 | 早期版本返回空结果（行情侧 `get_ipo_info` 已接入 RPC），打新不可直接视为无损替换 |

---

## 自检与单测

```powershell
cd D:\gjzqqmt\xtquant_big_convert
python -B -m unittest discover -s tests\bigqmt_signal_trader
```

只读自检（显式导入）：

```python
from bigqmt_signal_trader.xtquant_compat import StockAccount, configure, xt_trader, xtdata

configure()
acc = StockAccount(xt_trader.client.account_id, "STOCK")
asset = xt_trader.query_stock_asset(acc)
positions = xt_trader.query_stock_positions(acc)
tick = xtdata.get_full_tick(["600000.SH"])
print(asset.cash, asset.total_asset, len(positions), tick["600000.SH"]["bidPrice"], len(xtdata.get_stock_list_in_sector("沪深A股")))
```

shim 验证：

```python
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from xtquant import xtdata, xtconstant

trader = XtQuantTrader("", 12345)
acc = StockAccount(trader.client.account_id, "STOCK")
assert trader.connect() == 0 and trader.subscribe(acc) == 0
print(xtconstant.STOCK_BUY, trader.query_stock_asset(acc), xtdata.get_full_tick(["600000.SH"]))
```

## 注意事项

- `subscribe_quote()` / `subscribe_whole_quote()` 若走早期 VK 事件兼容模式，仅首推一次快照，持续推送需消费 `bigqmt:quote_subscriptions:{account_id}`（真推送实现见 [SUBSCRIBE_WHOLE_QUOTE_PUSH.md](SUBSCRIBE_WHOLE_QUOTE_PUSH.md)）。
- 全市场 `get_full_tick()` payload 过大时再于两端启用 VK 快照缓存。
- 开启远程下单前，务必确认大 QMT 正在运行目标资金账号的 RPC 策略。
