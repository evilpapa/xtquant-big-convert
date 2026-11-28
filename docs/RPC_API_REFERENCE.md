# RPC API 参考

本文档列出大 QMT RPC 服务对外暴露的方法、参数、返回值与大 QMT 实现边界。权威定义见 `src/bigqmt_signal_trader/vk_rpc.py`（`READ_METHODS` / `ORDER_METHODS` / `MARKET_DATA_METHODS` / `METHOD_ALIASES`）。

## 总览

| 类别 | 方法数 | 说明 |
|------|-------|------|
| 系统 | 2 | `ping` / `get_request_outcome` |
| 行情快照与订阅 | 5 | `get_ticks` / `get_instrument` / `subscribe_whole_quote` / `unsubscribe_whole_quote` / `quote_keepalive` |
| 行情/K线/基本面（转发适配器） | 84 | 见第 3 节 |
| 账户/持仓/委托/诊断 | 6 | `get_asset` / `get_positions` / `query_stock_position` / `query_orders` / `query_trades` / `describe_trade_detail_fields` |
| 运维 | 2 | `reload_deployment` / `reload_status` |
| 官方交易扩展查询 | 13 | 订单辅助、新股、历史明细、融资融券（5）、期权持仓（2）、港股通汇率；另含备用 `query_credit_account` |
| 持仓同步 | 1 | `sync_positions` |
| 下单/撤单 | 2 | `submit_order` / `cancel_order`（默认关闭） |

另有 12 个 MiniQMT 风格别名（见第 7 节）。

---

## 1. 系统

- **`ping()`**：无参数。返回 `{"pong": True, "account_id", "account_type", "account_types", "server_time"}`，其中 `account_types` 为服务端允许该账号查询的类型列表（如港股通配置）。
- **`get_request_outcome(request_id)`**：只读、在收包线程直答（主线程忙时亦可达），服务端保留近 10 分钟下单类请求记录。返回 `{"request_id", "state", "response"}`，`state` 取值：`unknown`（未轮到或已丢弃，未下单）、`dispatching`（`passorder` 执行中）、`dispatched`（已返回、回找合同号中）、`settled`（已应答，`response` 为完整回复）、`refused`（已过客户端期限被拒，未下单）。客户端 `order_stock` 超时后自动调用此接口反查。

---

## 2. 行情快照与全推订阅

- **`get_ticks`（别名 `get_full_tick`）**：参数 `codes: list[str]`（优先）或 `code: str`，支持市场令牌 `["SH"]` / `["SZ"]` / `["BJ"]` / `["HK"]`。返回 `{code: {lastPrice, open, high, low, lastClose, volume, amount, askPrice, bidPrice, askVol, bidVol, pvolume, transactionNum, stockStatus, time, stime}}`。实现走 `ContextInfo.get_full_tick`，若完整大 QMT 遗漏显式 `.SHO/.SZO` 期权合约，则自动以 `get_market_data_ex(period="tick", count=1)` 补齐。整市场快照建议配合客户端 `full_tick_cache`。
- **`get_instrument`（别名 `get_instrument_detail` / `get_instrumentdetail`）**：参数 `code: str`，走 `ContextInfo.get_instrumentdetail(code)`，返回约 30 个合约字段。

### 全推行情订阅（对齐 MiniQMT `subscribe_whole_quote`）

以下三接口管理生命周期，行情数据通过独立 VK pub/sub 通道（msgpack 优先、json 兜底）推送。同组合（`frozenset` 规范化）共享服务端订阅组；普通代码走 `ContextInfo.subscribe_whole_quote`，显式 `.SHO/.SZO` 期权逐只走 `ContextInfo.subscribe_quote(..., result_type="list")`。详见 [SUBSCRIBE_WHOLE_QUOTE_PUSH.md](SUBSCRIBE_WHOLE_QUOTE_PUSH.md)。

| 方法 | 参数 | 返回与语义 |
|------|------|-----------|
| `subscribe_whole_quote` | `client_id`, `sub_id`, `codes` | 返回 `{"combo_key", "topic", "push_endpoint"}`；幂等，供服务端重启后客户端重放恢复 |
| `unsubscribe_whole_quote` | `client_id`, `sub_id` | 返回 `{}`；移除 `(client_id, sub_id)` 引用，组合引用归零即退订大 QMT；未知 `sub_id` 为 no-op |
| `quote_keepalive` | `client_id`, `sub_id` | 返回 `{}`；刷新 `last_seen`，默认每 3s 发一次，超 30s（10 周期）无心跳由 reaper 清理 |

---

## 3. 行情 / K 线 / 板块 / 日历 / 财务 / 期权 / 资金流 / 因子

以下 84 个方法统一转发给 `BigQmtMarketDataProvider` 同名方法（`**kwargs` 展开）；兼容层 `BigQmtXtData` 对常用接口提供同名封装，其余走 `xtdata.call_method(name, **params)`。DataFrame / Series 经 `__bigqmt_type__` 序列化，客户端自动还原。

### 3.1 品种、K 线与历史行情

| 方法 | 参数 | 返回 / 说明 |
|------|------|-----------|
| `get_instrument_type` | `code`, 可选 `variety_list` | 返回 `{"stock", "fund", "etf", "bond", "index"}` 布尔字典 |
| `get_market_data` | `field_list`, `stock_list`, `period`（`1d`/`1m`/`5m`/`tick`）, `start_time`, `end_time`, `count`, `dividend_type`（`none`/`front`/`back`）, `fill_data` | `DataFrame` |
| `get_market_data_ex` / `get_local_data` | 同上（`get_local_data` 另含可选 `data_dir`） | `dict[code -> DataFrame]` |

- **合成周期回落与 `__bigqmt_partial__`**：部分终端（如 2.0.8.0）的 `get_market_data2` 对 `1mon/1q/1hy/1y` 返空时，桥自动逐只回落 `ContextInfo.get_market_data`（仅 6 列 OHLCV，且无 `fill_data`），并在应答附加 `__bigqmt_partial__`（`reason`, `period`, `source`, `requested`/`served`/`missing`, `fill_data_dropped`, `padding_rows_dropped`），客户端写入 `DataFrame.attrs["bigqmt_partial"]` 并发去重警告。
- **诊断参数 `synth_fallback_only=True`**：仅经 `call_method` / `client.call` 传入，强制合成周期跳过主路径直测回落（同时强制绕过 FormulaServer 直连）。

### 3.2 板块与交易日历

| 方法 | 参数 | 返回 / 大 QMT 实现说明 |
|------|------|-----------------------|
| `get_stock_list_in_sector` | `sector_name`, 可选 `real_timetag=-1` | `list[str]`（`ContextInfo`）。拼错不报错、返空；A 股用「上证A股/深证A股」，基金用「沪市基金/深市基金」 |
| `get_sector_list` | 可选 `allow_fallback=False` | 原生 `xtdata` SDK 优先；大 QMT 连不上行情服务时**默认抛 `NotImplementedError`**，显式传 `allow_fallback=True` 才返回 13 个实测常用板块名 |
| `get_sector_info` | `sector_name` | `ContextInfo.get_sector_info` |
| `get_trading_dates` | `market`（如 `"SH"`）, `start_time`, `end_time`, `count` | `ContextInfo.get_trading_dates`（注：桩签名首参为 `stockcode`，A 股日历一致结果正确） |
| `get_holidays` | 无 | 原生 SDK 不可用时从 `[去年1月1日, 今天]` 工作日减去 `get_trading_dates("SH")` 反推 |
| `get_markets` | 无 | 合成固定集合 `["SH", "SZ", "BJ", "HK"]` |
| `get_market_last_trade_date` | `market` | 由 `get_trading_dates(market, count=1)` 派生 |
| `get_date_location` | `date` | 日期在交易日历中的位置 |

### 3.3 数据下载、财务、ETF、期权、IPO 与除权因子

| 方法 | 参数 | 说明 |
|------|------|------|
| `download_history_data` / `download_history_data2` | `stock_code` / `stock_list`, `period`, `start_time`, `end_time`, 可选 `incrementally` | 单合约 / 批量历史下载 |
| `download_holiday_data` / `download_etf_info` | `incrementally` / 无 | 下载假日 / ETF 信息 |
| `get_financial_data` / `download_financial_data` / `download_financial_data2` | `stock_list`, `table_list`, `start_time`, `end_time`, `report_type="report_time"` | 财务数据读写 |
| `get_raw_financial_data` | `field_list`, `stock_list`, `start_time`, `end_time`, `report_type`, `data_type`（`dict`/`frame`） | 原始财务数据 |
| `get_factor_data` / `get_his_index_data` | `field_list`, `stock_list`, `start_date`, `end_date` / `stock_code` | 因子库 / 历史指数权重 |
| `get_etf_info` / `get_ETF_list` / `get_ipo_info` | 无 / `market`, `stock_code`, `type_list` / `start_time`, `end_time` | ETF 与 IPO 资料 |
| `get_option_list` / `get_his_option_list` / `get_his_option_list_batch` | `undl_code`, `dedate`（或 `start_time`, `end_time`）, `opttype`, `isavailavle` | 期权与历史期权列表 |
| `get_divid_factors` | `stock_code`, 可选 `start_time`, `end_time` | 除权除息因子（真区间：先试 SDK/3 参，不行则由日线 `preClose` 与前 `close` 差异逐日探测）。RPC 线格式为 `{毫秒时间戳: 7元列表}`；`xtdata.get_divid_factors()` 转为对齐 MiniQMT 的 8 列 `float64` DataFrame（索引 `YYYYMMDD`，列 `time/interest/stockBonus/stockGift/allotNum/allotPrice/gugai/dr`） |

### 3.4 因子模型、龙虎榜、股东、期权定价与资金流

| 方法 | 参数 | 说明 |
|------|------|------|
| `call_formula` / `subscribe_formula` | `formula_name`, `stock_code`, `period`, `start_time`, `end_time`, `count`, `dividend_type`, `extend_param` | VBA 公式调用 / 订阅 |
| `unsubscribe_formula` / `get_formula_result` | `request_id`（另含时间/数量/超时参数） | 取消订阅 / 取公式结果 |
| `gen_factor_index` | `data_name`, `formula_name`, `vars`, `sector_list`, `start_time`, `end_time`, `period`, `dividend_type` | 生成因子 |
| `get_longhubang` / `get_turnover_rate` | `stock_list`（或 `stock_code`）, `start_time`, `end_time`, `count` | 龙虎榜 / 区间换手率（均返 DataFrame；区间换手率需先下载财务股本与日线） |
| `get_top10_share_holder` / `get_holder_num` | `stock_list`, `data_name`（`holder`/`flow_holder`）, `start_time`, `end_time`, `report_type` | 十大股东 / 股东户数 |
| `get_industry` / `get_his_st_data` | `industry_name` / `stock_code` | 行业成分股 / 历史 ST |
| `bsm_price` / `bsm_iv` / `get_option_iv` | `opt_type`（`C`/`P`）, `target_price`, `strike_price`, `option_price`/`sigma`, `risk_free`, `days`, `dividend=0` / `opt_code` | B-S-M 定价、IV 反推与期权 IV |
| `get_option_detail_data` / `get_option_detail_data_batch` | `stockcode` / `stockcodes` | 单合约 / 批量期权详情（批量返回 `{code: dict}`，单只失败返 `{}`） |
| `get_option_undl_data` / `get_option_undl` | `undl_code_ref` / `opt_code` | 标的下所有期权 / 期权标的 |
| `get_main_contract` / `get_his_contract_list` | `code_market` / `market` | 主力合约 / 历史合约 |
| `get_north_finance_change` / `get_hkt_statistics` / `get_hkt_details` | `period` / `stock_code` | 北向资金 / 港股通统计与明细 |

> **客户端本地期权分析扩展（不进 RPC 白名单）**：`xtdata.get_option_analytics(opt_code)` 与 `xtdata.get_option_chain_analytics(undl_code, dedate)` 基于合约详情与最新 `close`（或显式盘口中间价）计算 IV、Delta、Gamma、Vega（`vega`/`vega_1pct`）、Theta（年/日）、Rho（`rho`/`rho_1pct`）；无套利违约合约带 `analytics_error`，不改变原生 `get_option_iv` 语义。

### 3.5 板块管理与基础查询（含大 QMT 2.1.19.0 实测订正）

| 方法 | 参数 | 返回与实测说明 |
|------|------|---------------|
| `create_sector` | `sector_name`, `stock_list` | 创建/更新自定义板块（写操作） |
| `get_stock_name` / `get_last_close` / `get_open_date` | `stock` | 名称 / 昨收（`PreClose`）/ 上市日 int `yyyymmdd`（`OpenDate`） |
| `get_last_volume` / `get_float_caps` | `stock` / `stockcode` | **均为流通股本（股数，= `FloatVolume`）**，不是昨量也不是流通市值 |
| `get_total_share` | `stockcode` | 总股本（= `TotalVolume`） |
| `get_contract_expire_date` | `stock` | 字符串：股票/ETF 返回 `'99999999'`，缺失合约 `'0'` |
| `get_weight_in_index` | `mtkindexcode`, `stockcode` | 指数权重（单位 `%`） |
| `get_close_price` | `market`, `stock_code`, `real_timetag`, `period=86400000`, `divid_type=0` | 指定时点收盘价 |
| `get_risk_free_rate` | `index=-1` | 无风险利率（%），实测恒返终端配置常数 `3.5`，非历史利率序列 |
| `get_svol` / `get_bvol` | `stock` | **盘中窗口内外盘，非当日累计**（股票/ETF 实测 `svol+bvol` 等于末根 1m K 线量，收盘集合竞价落在单侧致另一侧为 0；当日口径需按 tick/K 线自行累计） |
| `get_contract_multiplier` | `stockcode` | 合约乘数；无期货数据终端实测返 int32 哨兵 `2147483647`，客户端遇哨兵显式报错（可用 `get_instrument(code)['VolumeMultiple']` 替代） |
| `get_stock_type` | `stock` | ❌ 恒返回 `0`，客户端显式报错，改用 `get_instrument_type` |
| `get_turn_over_rate` | `stockcode` | ❌ 恒返回 `None`，客户端显式报错；自算可用 `get_ticks()[code]['pvolume'] / get_last_volume(code)`（注意 `pvolume` 单位为股，`volume` 为手相差 100 倍） |

---

## 4. 账户、持仓、委托与运维

本节及第 5、6 节的 `account_id` 均可选（默认服务端账号），亦支持可选 `account_type`（如 `"STOCK"` / `"CREDIT"` / `"FUTURE"` / `"HUGANGTONG"` / `"SHENGANGTONG"` 或 xtconstant 数字，仅在服务端配置列表允许时生效，否则回退默认并记日志）。

| 方法（含别名） | 参数 | 返回与说明 |
|---------------|------|-----------|
| `get_asset`（`query_stock_asset`） | `account_id` | `{"cash", "total_asset", "market_value", "account_id"}`（`ASSET`） |
| `get_positions`（`query_stock_positions`） | `account_id` | `dict[code -> {...}]`（`POSITION`），上下文未绑定降级 `{}` |
| `query_stock_position` | `account_id`, `stock_code` / `code` | 单股持仓 dict 或 `None` |
| `query_orders`（`query_stock_orders`） | `account_id`, `strategy_name=""`, `cancelable_only=False` | `list[OrderSnapshot]`；默认 `strategy_name=""` 查全部（非空须与下单一致） |
| `query_trades`（`query_stock_trades`） | `account_id`, `strategy_name=""` | 成交列表；`strategy_name` 优先取 DEAL 行 `m_strStrategyName`，缺失再回填过滤参数 |
| `describe_trade_detail_fields` | `account_id`, `detail_types=["ORDER","DEAL"]` | 诊断 RPC：仅返回 `rows` 与属性名列表（不返回值），区分「终端未给」与「桥未转发」 |
| `reload_deployment` / `reload_status` | `reason` / 无 | 排期至下一 `adjust` 拍清理 `bigqmt_signal_trader.*` 并重跑 `init()`（约 1s 短暂超时）；无法刷新顶层 `bigqmt_signal_trader_strategy.py` 与 `BIGQMT_VK_DRYRUN.py` |

### 4.5 官方交易查询函数（运行时注入全局）

由大 QMT 注入全局命名空间（不在 `ContextInfo` 桩中），无权限时降级为空列表：

- **订单/新股/历史/汇率**：`get_value_by_order_id(order_id)`、`get_last_order_id`、`get_ipo_data`、`get_new_purchase_limit`、`get_history_trade_detail_data(detail_type, start_date, end_date)`、`get_hkt_exchange_rate`。
- **融资融券**：`get_assure_contract`（担保品）、`get_enable_short_contract`（融券标的）、`get_unclosed_compacts`（未平仓）、`get_closed_compacts`（已平仓）、`get_debt_contract`（负债）。
- **期权持仓**：`get_option_subject_position`、`get_comb_option`。

**信用账户明细两条路径**（注意 `get_trade_detail_data` 的 `strAccountType='CREDIT'` 选账本，`strDatatype` 只能取 `ACCOUNT`/`POSITION`/`POSITION_STATISTICS`/`ORDER`/`DEAL`/`TASK`，切勿传 `'CREDIT'`）：

| 方式 | RPC | 大 QMT 实现 | 特点与字段差异 |
|------|-----|------------|---------------|
| **终端缓存（默认主路径）** | `query_credit_detail` | `get_trade_detail_data(accId, 'CREDIT', 'ACCOUNT')` → `CCreditAccountDetail` | 同步、无限流、无需重启；总负债字段为 `m_dTotalDebit` |
| 查柜台（备用） | `query_credit_account(wait_seconds=1.5)` | `query_credit_account` + `credit_account_callback` → `CCreditDetail` | 异步、30s 限流（超频返旧值标 `stale=true`）；总负债字段为 `m_dTotalDebt`；需真重启策略使入口回调生效 |

- `query_credit_account` 返回 `{"rows", "count", "query_issued", "not_issued_reason", "fresh", "stale", "age_seconds", "seq", "error", "callback_bound"}`；`callback_bound=false` 可能是入口未捕获函数/未重启，也可能是终端确实无此函数（`probe_capabilities` 的 `global_namespace["query_credit_account"]` 可区分）。
- 体检脚本：`python tools/credit_api_report.py`（脱敏只读探测所有两融接口）。

---

## 5. 持仓同步与下单撤单

### `sync_positions`
- 参数：`account_id`（可选）、`reason="rpc"`。主动将 `AccountSnapshot` 写入 VK `bigqmt:positions:{account_id}`；属 `LISTENER_DEFERRED_METHODS`，在 `adjust` 线程执行。

### `submit_order`（别名 `order_stock` / `order_stock_async`，默认关闭）
- 参数：`stock_code`、`action`（`BUY`/`SELL`）或 `order_type`（23/24 等）、`volume`/`order_volume`、`price`、`price_type`（默认 `LIMIT`/11，另有 `LATEST`/5、对手价 44 等）、可选 `account_id` / `strategy_name` / `signal_id` / `remark`/`order_remark` / `wait_settlement`。
- 返回：`{"order_sys_id", "user_order_id"}`；内部调用 `passorder(..., quicktrade=2)`。
- **两融/期权/可转债类型**：直接传 MiniQMT 常量即可（`CREDIT_FIN_BUY`=27 … `CREDIT_DIRECT_CASH_REPAY`=32，专项两融 40–45 自动转 70–75；ETF 期权 50–59、期货 0–15、可转债转股/回售 80–83 原样透传）。无买卖方向的类型（直接还款 32/45、行权/锁定 56–59）无需传 `action`。
  - **直接还款（32）**：金额（整数元）必须传 `order_volume`，`price` 传 `0`（会被忽略）；通常不在委托列表中，到期查不到不报错（`order_sys_id=None`，`order_stock` 返 `-1` 不抛异常，以 `query_credit_detail` 核对）。
- **异步影子结算（`wait_settlement=False`）**：立即应答但服务端仍监控至期限，若终端弹窗拦截未入库则补推 `on_order_error(source="settlement")`。
- **超时拒绝**：服务端按收包时刻计龄，轮到执行已过客户端期限（留 1s 余量，最多期限的 1/4）则拒绝并返回 `RequestExpired`（明确未下单，撤单不受此限）。`passorder` 串行 ~200ms/笔，高并发请降并发或调大超时。

### `cancel_order`（别名 `cancel_order_stock` / `cancel_order_stock_sysid`，默认关闭）
- 参数：`order_sys_id` / `order_sysid` / `order_id`（必填），可选 `user_order_id`、`market`。

---

## 6. MiniQMT 风格别名

| 别名 | 目标方法 | 别名 | 目标方法 |
|------|---------|------|---------|
| `get_full_tick` | `get_ticks` | `query_stock_orders` | `query_orders` |
| `get_instrument_detail` / `get_instrumentdetail` | `get_instrument` | `query_stock_trades` | `query_trades` |
| `getDividFactors` | `get_divid_factors` | `order_stock` / `order_stock_async` | `submit_order` |
| `query_stock_asset` | `get_asset` | `cancel_order_stock` / `cancel_order_stock_sysid` | `cancel_order` |
| `query_stock_positions` | `get_positions` | | |

---

## 7. 大 QMT 能力边界与错误约定

| 能力 | 大 QMT（完整交易端） | MiniQMT / `xtdata` SDK |
|------|---------------------|------------------------|
| 行情快照 / K 线 / 合约详情 / 板块内股票 / 交易日历 | ✅ `ContextInfo` | ✅ `xtdata` |
| 龙虎榜 / 股东 / 换手率 / 期权定价 / 北向港股通 / 基础查询 | ✅ `ContextInfo` | ❌ `xtdata` 无 |
| **板块完整列表**（`get_sector_list`）/ **原始节假日**（`get_holidays`） | ⚠️ 需 fallback（或同机运行 MiniQMT 连行情服务） | ✅ `xtdata` |
| `get_markets` / `get_market_last_trade_date` | 合成 / 日历派生 | 无 |
| 交易（下单 / 撤单 / 持仓） | ✅ `passorder` + `get_trade_detail_data` | ✅ `XtQuantTrader` |

**响应格式**：统一 `{"ok": bool, "data": ..., "error": "..."}`。常见错误：
- `rpc method is not allowed: X`：未在白名单（`rpc_listener_methods`）。
- `order rpc methods are disabled`：`rpc_allow_order_methods` 未开启。
- `RequestExpired: queued Xs ...; NOT dispatched`：排队已超客户端期限，未下单，可安全重试。
- `ContextInfo.X is not available`：当前 QMT 版本无该方法。
- `无法连接行情服务`：原生 `xtdata` SDK 未连上（仅 `get_sector_list`/`get_holidays` SDK 路径）。
