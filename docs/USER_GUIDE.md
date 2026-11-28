# xtquant_big_convert 使用指南

BIGQMT 运行环境里的 RPC 桥接包：将 BIGQMT 内置 Python（行情、交易、持仓）封装为可远程调用的服务，并兼容一组 MiniQMT 方法名，使外部程序无需 XtQuantServer 权限即可驱动 BIGQMT。

- **最短部署路径**：见 [docs/DEPLOY_QUICKSTART.md](DEPLOY_QUICKSTART.md)。
- **详细 API 契约**：见 [docs/RPC_API_REFERENCE.md](RPC_API_REFERENCE.md)。
- **AI 助手 Skill**：内置 [skill/](../skill/)（47 个 CLI 子命令），支持 Claude Code / ZCode / Cursor / Codex 直接查行情、算期权 Greeks、查持仓与下单撤单，详见「[AI 助手 Skill](#ai-助手-skill)」。

---

## 环境要求与安装

客户端、QMT 策略和独立串口桥接均需 **Python 3.12+**。

| 角色 | 运行位置 | 安装方式 |
|------|---------|---------|
| **客户端**（外部程序） | 开发机 / 策略机 | 源码目录：`pip install ".[valkey]"`（若走 PyPI：`pip install "xtquant-big-convert[valkey]"`） |
| **服务端**（QMT 内直连 VK） | QMT 策略所用 Python 3.12+ | `python -m pip install "valkey>=6.1,<7"` |

服务端安装后在对应解释器验证：

```powershell
python -c "import sys, valkey; print(sys.version, valkey.__file__)"
```

- **版本与重试约束**：`[valkey]` 安装 `valkey>=6.1,<7`。RPC 连接默认使用 RESP2 并关闭自动重试（`RPUSH` 失败时服务端可能已收到请求，重发会导致重复投递；服务端亦按 `request_id` 去重）。
- **串口传输**：QMT 策略环境与独立桥接环境同样需要 Python 3.12+，详见 [docs/RPC_TRANSPORTS.md](RPC_TRANSPORTS.md#通过-com0com-接回-vk)。
- **VK 服务**：客户端与服务端连接同一个 VK 实例即可。`BIGQMT_CONFIG["url"]` 与 `BIGQMT_VK_URL` 均接受 `valkey://` 和 `redis://`；Lua 脚本依赖 VK 7.2.5+ 的 `server` API。Windows 可用 [WSL 方式](https://valkey.io/topics/installation/) 或仓库 [部署脚本](../deploy/README.md) 安装 `valkey-windows`，最小 `valkey.conf`：

```conf
bind 127.0.0.1
port 6379
requirepass 换成你自己的强密码
```

```powershell
.\valkey-server.exe .\valkey.conf
```

> **安全提醒**：RPC 层本身无鉴权，队列名 `bigqmt:rpc:queue:<资金账号>` 不算秘密。访问控制仅靠 VK 凭据与 `rpc_allow_order_methods` 开关。**务必设置强密码并默认 `bind 127.0.0.1`**；确需跨机时仅绑内网网卡并配合防火墙白名单，**严禁暴露公网**。

---

## 快速开始

### 文件落位总览

| 文件 | 放置位置 | 来源 |
|------|---------|------|
| `bigqmt_signal_trader`（客户端） | 客户端 site-packages | `pip install ".[valkey]"` |
| `bigqmt_signal_trader_client_config.py` | 客户端（与运行脚本同目录） | `bigqmt-init` 第 2 个目录问题（回车 = 当前目录） |
| `bigqmt_signal_trader/`（整包） | QMT 的 `python` 目录（如 `D:\QMT交易端\python\`） | 手动从安装目录（或源码 `src/`）拷入 |
| `bigqmt_signal_trader_strategy.py` | 同上 | 同上 |
| `bigqmt_signal_trader_vk_rpc_runtime.py` | 同上 | 同上 |
| `BIGQMT_VK_DRYRUN.py` | 同上 | 同上（QMT 加载入口） |
| `bigqmt_signal_trader_qmt_config.py` | 同上 | `bigqmt-init` 第 1 个目录问题（**必须手填**） |
| `BIGQMT_VK_DRYRUN_ALL_IN_ONE.py` | 同上 | 仅单文件部署模式由 `bigqmt-init` 生成（配置已烘焙） |

> 记住三点：① 默认 `package` 模式下 `bigqmt-init` **只写配置、不拷包**；② QMT 目标目录是安装目录下的 `python` 子目录，不是 `bin.x64`；③ 客户端配置跟随外部脚本，不放 QMT 目录。

### 1. 用配置向导生成配置（`bigqmt-init`）

在能写入 QMT `python` 目录的机器上运行（两种写法等价，需在交互式终端中执行，不支持管道输入）：

```powershell
bigqmt-init
# 或（源码检出 / 命令不可用时）
python -m bigqmt_signal_trader.init_config
```

向导依次询问资金账号、账号类型、传输方式、VK 连接信息、是否允许远程下单、部署方式及两个输出目录（完整交互见 [DEPLOY_QUICKSTART.md](DEPLOY_QUICKSTART.md#第-2-步用-bigqmt-init-生成配置)）：

- **两类密码**：VK 密码不回显并写入配置文件；QMT 登录密码默认从环境变量 `BIGQMT_LOGIN_PASSWORD` 读取（不进 `argv`），为空时 `qmt_launcher` 读取启动目录的 `.env`。
- **勿提交配置**：生成文件含账号和凭据，均已在 `.gitignore` 中。
- 如需手写配置，客户端可参照 [`src/bigqmt_signal_trader_client_config.example.py`](../src/bigqmt_signal_trader_client_config.example.py)，服务端核心结构见下方「切换传输层」，并保持 `rpc_background_threads=False`。

### 2. 拷贝服务端文件到 QMT `python` 目录

定位已安装包的目录（源码检出则直接取 `src/`）：

```powershell
python -c "import bigqmt_signal_trader_strategy as m, os; print(os.path.dirname(m.__file__))"
```

将该目录下的 4 项（`bigqmt_signal_trader/`、`bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_vk_rpc_runtime.py`、`BIGQMT_VK_DRYRUN.py`）复制到 QMT 的 `python` 目录。拷完后与第 1 步生成的 `bigqmt_signal_trader_qmt_config.py` 共 5 项。

### 3. 在 QMT 模型交易中运行入口

在 QMT **模型交易**里加载 `BIGQMT_VK_DRYRUN.py` 并运行，正常日志如下：

```text
[bigqmt_shell] 本地 RPC 配置已加载 transport=valkey keys=['host', 'port', 'db', ...]
[bigqmt_shell] 本地账户配置已加载=True
[bigqmt_rpc] transport=valkey mode process_in_listener=True listener_methods=('*',) ...
[bigqmt_rpc] 已启动 queue=bigqmt:rpc:queue:你的账号 background_threads=False
[bigqmt_signal_trader] 初始化成功
```

> 不要在策略编辑器直接点击运行，也不要勾选「独立 Python 进程」，否则 QMT 按普通脚本执行、跳过 `init()` 且不注入原生 API 全局函数。

### 4. 客户端验证与调用

在客户端 `cd` 到存放 `bigqmt_signal_trader_client_config.py` 的目录：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_deployment_info())"
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_full_tick(['000001.SZ']))"
```

**客户端配置查找顺序**：默认 `import bigqmt_signal_trader_client_config`（优先脚本所在目录或当前工作目录，可用 `PYTHONPATH` 或环境变量 `BIGQMT_CLIENT_CONFIG_MODULE` 指定）；找不到时会回退到 `bigqmt_signal_trader_qmt_config`（仅限本机同目录碰巧命中，跨机不可依赖）。

**方式 A：兼容层（推荐，贴近 MiniQMT 写法）**

```python
from bigqmt_signal_trader.xtquant_compat import StockAccount, configure, xt_trader, xtdata

configure()
acc = StockAccount(xt_trader.client.account_id, "STOCK")

ticks = xtdata.get_full_tick(["000001.SZ"])
positions = xt_trader.query_stock_positions(acc)
asset = xt_trader.query_stock_asset(acc)
klines = xtdata.get_market_data_ex(field_list=["close"], stock_list=["000001.SZ"], period="1d", count=5)
```

**方式 B：直接 RPC 调用**

```python
import valkey
from bigqmt_signal_trader.vk_rpc import call_vk_rpc

r = valkey.Valkey(host="192.168.1.100", port=6379, db=5, password="...")
resp = call_vk_rpc(r, "你的账号", "get_full_tick", {"codes": ["000001.SZ"]})
print(resp["data"]["000001.SZ"]["lastPrice"])
```

**方式 C：通过 shim 无缝替换旧 `xtquant` 导入**：详见下文「[保留原 `xtquant` 写法与共存](#保留原-xtquant-写法与共存)」。

### 5. 升级

客户端升级包；服务端重拷上述 4 项并清理 `__pycache__`。若 3 个顶层文件未改动，可调用 `xt_trader.reload_deployment()` 热更新；否则重启策略。详见 [升级已有部署](DEPLOY_QUICKSTART.md#升级已有部署package-模式)。

<details>
<summary>排错参考：入口文件 <code>BIGQMT_VK_DRYRUN.py</code> 内部流程</summary>

1. **定位 `python` 目录（无硬编码）**：优先 `__file__` 所在目录；`exec` 加载无 `__file__` 时在 `sys.path` 中找第一个存在的 `*\python` 目录。
2. **刷新模块**：`importlib.reload` 刷新 `vk_common` / `vk_rpc` / `strategy` / `runtime`（缺失标准 `importlib` 时注册最小兼容模块）。
3. **注入配置与账号**：读取 `BIGQMT_CONFIG` 与 `BIGQMT_ACCOUNT_ID`（缺失时回退 QMT 全局变量 `account`）。
4. **绑定原生 API 并导出回调**：绑定 `passorder` / `cancel` / `get_trade_detail_data`，导出 `init` / `handlebar` / `adjust` 等回调。
5. **编码**：文件头声明 `#coding:gbk` 以匹配中文 Windows QMT 编辑器保存行为，源码保持纯 ASCII 避免乱码。

</details>

---

## 功能一览与核心用法

### RPC 接口总览

白名单包含 **137 个只读方法 + 3 个下单方法（`submit_order` / `submit_orders_batch` / `cancel_order`）+ MiniQMT 风格别名**。行情/参考数据一般调 `xtdata` 同名方法，账户/交易调 `xt_trader` 的 MiniQMT 方法；无同名封装的可走 `xtdata.call_method("方法名", ...)` 或 `xt_trader.client.call("方法名", {...})`。

| 类别 | 方法 |
|------|------|
| **系统** | `ping` |
| **行情快照** | `get_ticks` / `get_full_tick`（五档盘口） |
| **合约/品种** | `get_instrument` / `get_instrument_type` / `get_stock_name` / `get_last_close` / `get_last_volume`（流通股本）/ `get_open_date` / `get_contract_expire_date` / `get_float_caps` / `get_total_share` / `get_weight_in_index` / `get_svol`、`get_bvol`（盘中窗口内外盘，非当日累计）/ `get_contract_multiplier`（遇 int32 哨兵 `2147483647` 报错）/ `get_risk_free_rate` / `is_stock_type` / `get_cb_info`；**实测不可用、客户端显式抛 `NotImplementedError`**：`get_stock_type`（恒 0，请改用 `get_instrument_type`）/ `get_turn_over_rate`（恒 None） |
| **K 线/历史** | `get_market_data` / `get_market_data_ex` / `get_local_data` / `get_close_price` / `get_index_weight` |
| **L2 行情** | `get_l2_quote` / `get_l2_order` / `get_l2_transaction` / `subscribe_l2thousand`（需 L2 权限） |
| **板块** | `get_stock_list_in_sector` / `get_sector_list`* / `get_sector_info` / `create_sector` / `add_sector` / `remove_sector` |
| **交易日历/时段** | `get_trading_dates` / `get_holidays`* / `get_markets`* / `get_market_last_trade_date`* / `get_date_location` / `get_trading_calendar` / `get_trade_times` |
| **数据下载** | `download_history_data` / `download_history_data2` / `download_holiday_data` / `download_etf_info` / `download_cb_data` / `download_history_contracts` / `download_index_weight` / `download_sector_data` |
| **财务/因子** | `get_financial_data` / `download_financial_data` / `download_financial_data2` / `get_raw_financial_data` / `get_factor_data` |
| **ETF/期权/期货** | `get_etf_info` / `get_ipo_info` / `get_option_list` / `get_his_option_list` / `get_his_option_list_batch` / `get_option_detail_data` / `get_option_undl_data` / `get_option_undl` / `get_ETF_list` / `get_main_contract` / `get_his_contract_list` |
| **期权定价** | `bsm_price` / `bsm_iv` / `get_option_iv` |
| **龙虎榜/股东** | `get_longhubang` / `get_top10_share_holder` / `get_holder_num` / `get_turnover_rate`（区间换手率）/ `get_industry` / `get_his_st_data` / `get_his_index_data` |
| **资金流** | `get_north_finance_change`（北向）/ `get_hkt_statistics`（港股通）/ `get_hkt_details` / `get_hkt_exchange_rate` |
| **因子/模型** | `call_formula` / `subscribe_formula` / `unsubscribe_formula` / `get_formula_result` / `gen_factor_index` |
| **时间转换** | `datetime_to_timetag` / `timetag_to_datetime` / `timetagToDateTime`（本地计算） |
| **账户查询** | `get_asset` / `get_positions` / `query_stock_position` / `query_orders` / `query_trades` / `get_history_trade_detail_data` / `get_value_by_order_id` / `get_last_order_id` |
| **新股/打新** | `get_ipo_data` / `get_new_purchase_limit`（客户端另提供 `query_ipo_data` / `ipo_subscribe` / `ipo_subscribe_all`） |
| **融资融券** | `get_assure_contract` / `get_enable_short_contract` / `get_unclosed_compacts` / `get_closed_compacts` / `get_debt_contract`（需两融权限，普通户降级为空） |
| **期权持仓** | `get_option_subject_position` / `get_comb_option` |
| **持仓同步** | `sync_positions`（写回 VK 缓存） |
| **下单/撤单** | `submit_order` / `submit_orders_batch` / `cancel_order`（默认关闭，需显式开启） |

> `*` 标记方法含 fallback：`get_sector_list` 获取失败时抛错，仅 `allow_fallback=True` 才返回 13 个常用板块名；`get_holidays` / `get_market_last_trade_date` 由日历推导；`get_markets` 返回固定集合。详见 [RPC_API_REFERENCE.md](RPC_API_REFERENCE.md)。

**板块名称拼法注意**：`get_stock_list_in_sector` 拼错板块名不报错、只返空列表。BIGQMT 2.1.19.0 实测有效的 13 个默认名称中，**A 股用「上证/深证」、基金和债券用「沪市/深市」**：

| 板块名 | 实测数量 | 易错点 |
|--------|---------|--------|
| `沪深A股` / `上证A股` / `深证A股` | 5220 / 2318 / 2902 | **不是**「沪市A股 / 深市A股」（返回 0） |
| `科创板` / `创业板` | 617 / 1408 | — |
| `沪市基金` / `深市基金` / `沪深ETF` | 1152 / 1124 / 1722 | **不是**「上证基金 / 深证基金」（返回 0） |
| `沪市债券` / `深市债券` | 22351 / 19787 | — |
| `上证期权` / `深证期权` | 674 / 452 | — |
| `中金所` | 0（普通股票户） | 无期货权限，非拼写问题 |

### 客户端兼容层亮点

- **完整 `xtconstant` 枚举**：539 个常量（含原生 MiniQMT 全部 90 个，数值一致），涵盖账号类型、委托类型（股票/期货/信用/期权）、报价类型、委托与账号状态、`ORDER_TYPE_SET`。
- **本地期权 IV/Greeks 计算**：`xtdata.get_option_analytics(option_code)` 与批量 `xtdata.get_option_chain_analytics("510050.SH", "202609")` 基于合约元数据与最新 K 线 close（或显式传入 `option_price` / `underlying_price` 中间价）本地计算 IV 与 Delta/Gamma/Vega/Theta/Rho；违反无套利边界的陈旧价格按合约返回 `analytics_error`，不污染整条链；可设 `include_native_iv=True` 对照原生 `get_option_iv`。
- **委托/成交字段补齐（0.3.8+）**：补齐 `account_type`（按实际部署配置映射数字码）、`instrument_name`、`secu_account`（QMT 不返回，恒为空串防 `AttributeError`）、`offset_flag`、`direction` 及成交的 `commission`；委托快照增量暴露 `price_type` / `traded_price`（旧终端缺省为 `None` / `0.0`）。完整 QMT 交易日 ContextInfo fallback 会把 `SH/SZ` 转成代表指数代码。
  - `strategy_name` 仅对经本桥下单的委托有效（从本桥委托身份库按 `user_order_id` 回填）；终端手工单保持为空。
  - 提供诊断 RPC `describe_trade_detail_fields(account)`，返回 QMT ORDER/DEAL 行的全部属性名（不含值），用于区分「终端未提供」与「桥未转发」。

### 保留原 `xtquant` 写法与共存

`src/xtquant/` 是与官方 `xtquant` 同名的 shim 包（含 `xtdata` / `xttrader` / `xtconstant` / `xttype`），两者按 `sys.path` 顺序生效：

- **方案 A（零改动替换）**：把 `src/` 放到 `PYTHONPATH` 最前（或无官方包时普通安装），`from xtquant import xtdata` 直接走 RPC 桥；代价是同进程内无法再访问官方 `xtquant`。
- **方案 B（渐进共存）**：用 `pip install -e /path/to/xtquant_big_convert`（避免普通 `pip install` 往 `site-packages/xtquant/` 覆盖官方包），新代码显式 `from bigqmt_signal_trader.xtquant_compat import xtdata as bq_xtdata`，旧代码继续用官方 `xtquant`；也可通过虚拟环境 + `PYTHONPATH` 按环境切换。

验证当前命中的包：

```python
import importlib.util
print(importlib.util.find_spec("xtquant").origin)
```

### 异步回报回调与 `*_async` 查询

注册 `XtQuantTraderCallback` 子类并 `connect()` / `subscribe(acc)` 后，通过 VK pub/sub 实时接收委托、成交、错误与账户状态回报：

```python
from bigqmt_signal_trader.xtquant_compat import StockAccount, XtQuantTraderCallback, configure, xt_trader

class MyCallback(XtQuantTraderCallback):
    def on_stock_order(self, order): ...
    def on_stock_trade(self, trade): ...
    def on_order_error(self, err): ...
    def on_cancel_error(self, err): ...
    def on_order_stock_async_response(self, resp): ...
    def on_account_status(self, status): ...

configure()
xt_trader.register_callback(MyCallback())
acc = StockAccount(xt_trader.client.account_id, "STOCK")
xt_trader.connect()
xt_trader.subscribe(acc)
seq = xt_trader.order_stock_async(acc, "600654.SH", 23, 100, 11, 2.95, "rpc_test", "备注")
```

| 回调 | 触发时机 |
|------|---------|
| `on_account_status` | `connect()` / `subscribe()` 后 |
| `on_order_stock_async_response` / `on_cancel_order_stock_async_response` | 异步下单 / 撤单提交响应 |
| `on_stock_order` / `on_stock_trade` | 委托状态变化（已报 50 / 已成 56 / 废单 57）/ 成交回报 |
| `on_order_error` | 服务端检测废单（status=57）；或终端弹窗拦截未入库时由结算兜底补推（`error_msg` 以 `passorder submitted but order not found in system` 开头，`source="settlement"`） |
| `on_cancel_error` | 撤单失败 |

- **事件保序**：`order_stock_async` 响应与 pub/sub 事件走不同线程，客户端按 `order_remark` 设 10 秒超时屏障，待 response/error 到达后按序放行（成交缺 remark 时按委托学到的 `order_sys_id` 关联；复用 remark 按 seq 精确匹配）。同步下单、手工单、无 remark 委托直接透传。
- **`*_async` 查询**：传 `callback` 时对齐 MiniQMT（结果给回调，返回 `None`）；不传则返回 `seq`。
- **必须实盘模式**：QMT 模拟/模型模式下委托不进真实委托队列，`order_stock` 返回 `-1` 且无法收到完整回报。

### 新股申购（打新）

```python
acc = StockAccount("你的账号")
xt_trader.query_ipo_data(acc, stock_type="STOCK")     # 查当日可申购（只读）
xt_trader.query_new_purchase_limit(acc)               # 查各板块申购额度
xt_trader.ipo_subscribe_all(acc, dry_run=True)        # 仅生成申购计划
xt_trader.ipo_subscribe_all(acc)                      # 申购沪深（默认排除北交所）
xt_trader.ipo_subscribe_all(acc, markets=("SH", "SZ", "BJ"))  # 显式含北交所
```

- 返回每只的 `action`（`subscribed` / `planned` / `skipped` / `failed`）与 `reason`，单只失败不影响其余；属于主动调用，如需定时请自行调度。
- 复用 `order_stock` 通道：受 `rpc_allow_order_methods` 控制，使用网关默认 `orderType=1101` / `prType=11` / `quickTrade=2`（回调/定时器中传 `quickTrade=1` 可能静默不发出），主线程执行并完整记账。
- 默认仅打沪深（市值申购不冻资），北交所冻资默认排除；无法识别申购代码前缀（沪 `730/732/780/787/789`、深 `00/30`、北 `920/889/8/4`）一律跳过。
- `query_ipo_data` 与 `dry_run` 已在 BIGQMT 实盘验证，**真实申购委托未在本仓库实测**。

### 全推行情订阅（`subscribe_whole_quote`）

服务端真推送，通过独立 VK pub/sub 增量推送行情：

```python
def on_quote(data):
    for code, tick in data.items():
        print(code, tick.get("lastPrice"))

seq = xtdata.subscribe_whole_quote(["600000.SH", "000001.SZ"], callback=on_quote)
xtdata.unsubscribe_quote(seq)
```

- **机制**：相同标的组合在 QMT 端去重共享；按 `(client_id, sub_id)` 引用计数，全退订或 30s keepalive 超时销毁；客户端启动预拉 `get_full_tick` 快照，推送静默默认 10 轮心跳后自动重放（服务端重启后可自动恢复）。
- **期权兼容**：显式 `.SHO` / `.SZO` 合约自动走 `ContextInfo.subscribe_quote(..., result_type="list")`，与股票/ETF 混合订阅时客户端保持同一订阅号。详见 [SUBSCRIBE_WHOLE_QUOTE_PUSH.md](SUBSCRIBE_WHOLE_QUOTE_PUSH.md) 与 [实盘验证记录](SUBSCRIBE_WHOLE_QUOTE_LIVE_VERIFICATION.md)。

### 全市场快照过滤（`types`）与 K 线字段选择（`field_list`）

1. **`get_full_tick` 市场令牌默认只取股票（0.2.15+，破坏性变更）**：QMT 快照耗时约 0.29ms/只，上交所全量含债券/回购共 26744 只（~7.4s），仅股票 2315 只（~1.08s）。

   ```python
   xtdata.get_full_tick(["SH"])                       # 默认只取股票（~1.08s）
   xtdata.get_full_tick(["SH"], types=["all"])        # 全量 26744 只（~7.4s）
   xtdata.get_full_tick(["SH"], types=["stock", "etf"])
   ```

   支持 `stock`（默认）、`etf`、`fund`、`index`、`convertible`（别名 `cbond`/`cb`）、`all`。请求前即按 FormulaServer 板块清单（~13ms，运行期缓存）收窄；若收窄失败（如 `HK` 无对应板块）则自动回退全量。要全量请用市场令牌 + `types=["all"]`，直接传两万余显式代码会打爆单次 RPC 超时。

2. **`get_market_data_ex` 显式指定 OHLCV 快 ~30 倍**：不传 `field_list` 返回 11 列（含 `preClose` / `suspendFlag` 等），只能走 RPC（~0.97s）；显式传入 `["open", "high", "low", "close", "volume", "amount"]` 走 FormulaServer 直连（~0.03s）。因 FormulaServer 对其余 4 列返回 `NaN`，默认保持走 RPC。

---

## 与 MiniQMT 的兼容性与已知陷阱

### 返回值契约

| 接口 | 返回 | 说明 |
|------|------|------|
| `order_stock()` | `int` | 成功为正数，失败 `-1` |
| `cancel_order_stock()` / `cancel_order_stock_sysid()` | `int` | **`0` 成功，`-1` 失败**（非 bool） |
| `order_stock_async()` / `cancel_order_stock_async()` | `int` | 请求序号 `seq` |
| `connect()` / `start()` / `subscribe()` / `unsubscribe()` | `int` | `0` 成功 |
| `subscribe_quote()` / `subscribe_whole_quote()` | `int` | 订阅号，用于 `unsubscribe_quote()` |
| `query_stock_asset()` / `positions` / `orders` / `trades` | 对象 / `list[对象]` | 含 `.cash` / `.total_asset` 等属性 |
| `get_full_tick()` / `get_market_data_ex()` | `dict` / `dict[str, DataFrame]` | — |

- **`order_id` 双形态**：BIGQMT 仅有字符串合同编号 `m_strOrderSysID`。本桥返回的 `order_id`（含 `XtOrder` / `XtTrade` / 回调对象）为 `int` 子类：既支持 `isinstance(x, int)`、`x > 0`、`x == -1`，又支持 `str(x)` 取原始合同编号。客户端缓存最近 4096 条映射（落库转回普通 `int` 后仍可撤单）；如需纯字符串可直接用 `.order_sysid`。

### 行为差异与扩展接口

| 项目 | MiniQMT | 本项目 |
|------|---------|--------|
| `get_full_tick(["SH"])` | 全市场 | 默认只取股票；全量传 `types=["all"]` |
| `get_instrument_detail()` 查不到 | `None` | `{}`（均 falsy） |
| `download_history_data()` | 无返回 | 返回 `{"finished": n, "total": n}` |
| 账户类型 | 客户端指定 | 以服务端 `BIGQMT_ACCOUNT_TYPE` 为准（列表配置时支持按请求切换） |
| 委托类型常量 | `xtconstant` | 内部翻译为 `passorder` 的 `opType`（含专项两融 40–45 → 70–75） |

**额外扩展（MiniQMT 无）**：`order_stock_result()`、`order_stock_batch()`、`wait_async_orders()`、`ipo_subscribe_all()`、`sync_deployment()`、`get_deployment_info()`、`query_execution_snapshot()`、`local_cache_stats()`、`convert_bond()` / `sell_back_bond()`（可转债转股/回售，对应 `passorder` opType 80–83，按账户类型自动选编号，`order_stock` 直接传 80–83 亦支持）。

### 已知陷阱与内置修复

1. **`query_orders` / `query_trades` 的 `strategy_name` 过滤**：`get_trade_detail_data` 严格按 `strategy_name` 匹配，不一致即返空。本桥默认传 `""` 返回全部委托/成交；如需过滤请显式传入与下单一致的 `strategy_name`。
2. **复权 K 线全 0 与下载等待**：BIGQMT 前/后复权在服务端现场计算，缺少服务端原始 K 线或除权因子时会返回全 0。`download_history_data2(..., dividend_type="front")` 已自动先触发服务端原始数据下载；`get_market_data_ex` / `get_market_data` / `get_local_data` 检测到全 0 时亦会自动触发下载并等待重试一次（极大区间可再重试或先显式下载）。下载接口故意阻塞至数据可见（0.3.37 修复单票重复提交导致等满 60s 的 bug，现冷宽窗约 0.06s）；批量下载仍建议整批传入 `codes` 以摊薄固定开销。
3. **实盘卖出方向误判（`exec_events`）**：实盘 QMT 回调 `m_nDirection` 恒为 48，已改为 `m_nOffsetFlag` → `m_nDirection` → `m_nOpType` 仲裁链，同时正确区分股票现货与期货开平仓方向。
4. **`get_financial_data` 两个前提与冷启动**：① 数据须已在 QMT 界面「数据管理/财务数据」下载到终端本地（BIGQMT 无 `ContextInfo.download_financial_data`，桥上 `download_financial_data` 不可用）；② 必须传 `start_time` / `end_time`（留空直接返 `None`）。此外重启策略后首次调用可能耗时数分钟（实测 346s，之后 <1s），见下文「启动预热」。

---

## 传输层、性能与并发部署

### 切换传输层与线程模式

服务端与客户端配置保持相同的 `transport` 字段即可：

- **VK（默认）**：`{"transport": "valkey"}`（或省略）。
- **串口**：见 [docs/RPC_TRANSPORTS.md](RPC_TRANSPORTS.md)。
- **MySQL（兼容兜底，需 `pymysql` + `DBUtils`）**：

```python
{
    "transport": "mysql",
    "rpc_background_threads": False,
    "mysql": {
        "driver": "pymysql", "host": "192.168.1.100", "port": 3306,
        "user": "root", "password": "...", "database": "bigqmt_rpc", "charset": "utf8mb4",
        "poll_interval_seconds": 0.01,
        "pool_config": {"mincached": 1, "maxcached": 3, "maxshared": 0, "maxconnections": 4},
    },
}
```

**`rpc_background_threads` 一律设为 `False`（drain 模式，0.3.51 起默认）**。旧配置若显式写了 `True`，请改为 `False` 并重启。碰交易上下文的方法（`LISTENER_DEFERRED_METHODS`）无论配置如何都会无条件留在主线程。

**稳态延迟对照**（实盘终端，`schedule_adjust_interval="100nMilliSecond"`，等历史回放结束 `adjust cadence: ticks≈100` 后加随机轮间抖动实测，min / median / max，ms）：

| 传输 + 模式 | `ping` | `query_stock_positions` | 说明 |
|------------|--------|------------------------|------|
| **VK + drain（默认）** | 23 / 102 / 106 | 85 / 103 / 109 | 地板为一拍（并发 `call_async` 单拍最多 20 条） |
| VK + 后台线程 | 199 / 407 / 605 | 102 / 197 / 320 | 跨线程抢 GIL 每条命令付一拍 |
| MySQL | ~104 | — | 轮询兜底 |

> 自行复测务必等日志 `adjust cadence` 回到 `ticks≈100`（策略刚启动的回放窗口可达 5000 拍/秒，测出的是假象）且轮间隔带随机抖动（避免锁相）。详见 [docs/LATENCY_REPORT.md](LATENCY_REPORT.md)。

**重读请求 offload（0.3.53+）**：为防止慢读卡住策略 `adjust` 拍，`get_financial_data`、`call_formula`、市场令牌 `get_full_tick`、>20 代码、`period="tick"` 及按日期窗口取 K 线默认交由工作线程执行，下一拍回包（多付 1~2 拍）；`download_*` 因全程持 GIL 留在主线程。可用 `rpc_heavy_offload: False` 关闭、`rpc_heavy_codes_threshold` 调阈值，`probe_capabilities` 的 `thread_routing.heavy_sample` 可查看路由分布。

### FormulaServer 直连快速路径（只读行情，默认开启）

BIGQMT 的 `58600` 端口是内置 C++ 行情服务 FormulaServer（端口读自 `config/formulaserver/formulaserver.ini`）。客户端对以下 10 个只读方法自动绕开 RPC 与 GIL，实测 **p50 0.07ms**（完整客户端栈 0.145ms）：

`get_instrument` / `get_instrument_detail` / `get_instrumentdetail` / `get_last_volume` / `get_total_share` / `get_contract_multiplier` / `get_main_contract` / `get_weight_in_index` / `get_stock_list_in_sector` / `get_market_data_ex`（仅 `dividend_type="none"`）。

- **能力边界**：不支持账户、持仓、委托、成交、下单和五档盘口（报 `ErrorID 200005`）；因参数或复权语义不一致，`get_trading_dates`、`get_divid_factors`、`get_risk_free_rate` 及前/后复权 K 线刻意走 RPC。
- **零依赖回退**：内置纯 Python BSON 编解码（可选用 `pymongo` 的 `bson`）；任何连接或翻译失败均自动回退 RPC。
- **客户端配置**（默认启用，通常无需写；环境变量 `BIGQMT_FORMULA_ENABLED=0` 可关闭）：

```python
BIGQMT_CONFIG = {
    "formula_server": {
        "enabled": True,
        # "host": "127.0.0.1",               # 跨机需放行 58600 防火墙
        # "port": 58600,
        # "qmt_root": r"D:\QMT交易端",
        # "timeout_seconds": 3.0,
        # "methods": ["get_instrument"],     # 仅路由指定白名单方法
        # "failure_cooldown_seconds": 30.0,
    },
}
```

### 启动预热与卡顿监控

- **自动预热（默认开启）**：启动后在后台守护线程跑一次 `get_financial_data`（不阻塞 `init()`），把重启后的首次几分钟冷启动提前付掉。关闭方式：服务端 `BIGQMT_CONFIG["warm_context_data"] = False`。
- **区分「卡住」与「死了」**：遇到成片超时先搜服务端日志——看到 `STILL RUNNING`（正在执行）或 `slow handler`（执行完补打）说明桥存活，只是串行队列被某个慢调用堵住。

### 多客户端共享同一座桥

| 通道 | 多客户端机制 | 注意事项 |
|------|-------------|---------|
| **RPC 查询** | 按 `request_id` 写入独立响应键，互不串包 | 服务端串行吞吐共享；高频全市场 `get_full_tick(["SH"])` 建议单进程拉取再分发，或开启 `full_tick_cache` |
| **全推行情** | 服务端按 `(client_id, sub_id)` 引用计数，同组合只订一次、pub/sub 广播 | `sub_id` 已含 PID 避免同机多进程撞车；**跨机**共用配置请各设不同 `BIGQMT_QUOTE_CLIENT_ID` |
| **委托/成交回报** | 按账号广播至所有监听客户端 | 请按 `order_remark` / `strategy_name` 自行过滤 |

### 多账号与港股通

RPC 通道按 `account_id` 隔离，客户端侧写法在各模式下完全一致：

1. **同一 QMT 客户端多资金账号**（如股票 + 期货）：单实例配置路由表 `BIGQMT_ACCOUNT_TYPE_MAP`（主账号也需列入）：

   ```python
   BIGQMT_ACCOUNT_ID = "你的股票账号"
   BIGQMT_ACCOUNT_TYPE = "STOCK"
   BIGQMT_ACCOUNT_TYPE_MAP = {
       "你的股票账号": "STOCK",
       "你的期货账号": "FUTURE",
   }
   ```

2. **同一账号多类型（港股通，0.3.54+）**：将类型配为列表（首个为默认），客户端按类型构造 `StockAccount`：

   ```python
   # 服务端（单账号或 MAP 的 value 均可写成列表）
   BIGQMT_ACCOUNT_TYPE = ["STOCK", "HUGANGTONG", "SHENGANGTONG"]

   # 客户端
   acc_hk = StockAccount("你的股票账号", "SHENGANGTONG")  # 沪港通用 "HUGANGTONG"
   xt_trader.query_stock_positions(acc_hk)
   xt_trader.order_stock(acc_hk, "00700.HK", xtconstant.STOCK_BUY, 100, xtconstant.FIX_PRICE, 300.0)
   ```

   类型在允许列表内则按该类型查询、撤单与回填合同编号，不在列表内则回退默认并记日志。

3. **不同 QMT 客户端（多券商 / 多目录 / 多机器）**：在每个客户端各自的 `python` 目录放入同样 4 项文件与独立 `bigqmt_signal_trader_qmt_config.py`，连同一个 VK 即可自动按 `account_id` 隔离。**同一 QMT 客户端内不能跑两个桥实例**（共享 `sys.modules` 会撞配置）。

**客户端同时操作多账号**：为每个账号创建独立的 `BigQmtRpcClient` / `BigQmtXtTrader` / `StockAccount`：

```python
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtTrader, StockAccount

stock_client = BigQmtRpcClient(account_id="股票账号", vk_config={...})
stock_trader = BigQmtXtTrader(account_id="股票账号", vk_client=stock_client.vk_client)
credit_client = BigQmtRpcClient(account_id="信用账号", vk_config={...})
credit_trader = BigQmtXtTrader(account_id="信用账号", vk_client=credit_client.vk_client)

stock_trader.query_stock_asset(StockAccount("股票账号", "STOCK"))
credit_trader.query_stock_positions(StockAccount("信用账号", "CREDIT"))
```

---

## 运维工具与高级部署

### 版本检测、同步与热重载

QMT 跨策略重跑会保留 `sys.modules`，光拷文件不生效。服务端启动会打印版本号与路径，客户端连接时版本不一致会告警，也可主动查询和同步：

```python
xtdata.get_deployment_info()              # 查看服务端版本与目录
xt_trader.sync_deployment(dry_run=True)   # 预览同步变更
xt_trader.sync_deployment()               # 执行同步（客户端侧运行）
```

- 同步特性：目标目录自动取自服务端；**绝不覆写**配置文件（仅更新 `.example.py`）；不新增顶层文件；覆盖前留 `.bak_<时间戳>`；原子写入。设 `BIGQMT_AUTO_SYNC=1` 可在连接时自动同步（默认关）。
- **免重启热重载（0.3.8+）**：

  ```python
  xt_trader.reload_deployment("why")  # 排期至下一 adjust 拍执行，约 0.8s
  xt_trader.reload_status()           # 轮询结果
  ```

  通过清理 `sys.modules` 中所有 `bigqmt_signal_trader.*` 并重跑 `init()` 刷新整个子包（期间约 1 秒查询会超时）。**不能刷新**顶层的 `bigqmt_signal_trader_strategy.py` 与 `BIGQMT_VK_DRYRUN.py`，改动它们仍须重启策略。

### QMT 程序化启停（`qmt_launcher`）

- 若想避免每天重启，可取消 QMT 设置中的「启用自动初始化」；若长期不重启出现断线后持仓不刷新，可在 QMT「行情源」底部勾选**交易中心自动更换服务器**与**自动优选**，或使用下方工具定时重启。
- Win11 无 `wmic`，进程枚举需安装 `pip install psutil`。

```powershell
python -m bigqmt_signal_trader.qmt_launcher status  --dir "D:\QMT交易端"
python -m bigqmt_signal_trader.qmt_launcher restart --dir "D:\QMT交易端"
```

| `mode` | 行为 | 需要登录框交互 |
|--------|------|---------------|
| `exe` | 启动 `XtItClient.exe`，靠终端自动恢复会话 | 否（锁屏可跑） |
| `bat` | 运行指定免密批处理脚本 | 否（锁屏可跑） |
| `login` | 启动后通过物理输入填写账号密码（需 `pywin32` + `pyautogui`） | **是**（需未锁屏交互桌面，锁屏抛 `QmtLauncherError`） |
| `linkmini` | `XtMiniQmt.exe linkMini` | 否 —— **对本项目桥不可用**（仅起迷你端，无策略运行时） |

密码优先读环境变量 `BIGQMT_LOGIN_USER` / `BIGQMT_LOGIN_PASSWORD`（为空时读启动目录 `.env`）。Python API：

```python
from bigqmt_signal_trader.qmt_launcher import (
    close_qmt, open_qmt, restart_qmt, is_qmt_running,
    find_qmt_processes, wait_until_ready, session_is_locked,
)

close_qmt(r"D:\QMT交易端", force_after_seconds=20)  # 按安装目录 bin.x64 隔离，防误杀其他实例
open_qmt(r"D:\QMT交易端", mode="login", credentials={"user": "...", "password": "..."},
         window_title_prefix="QMT", ready_timeout_seconds=180)  # 以 FormulaServer(58600) 可连判就绪
restart_qmt(r"D:\QMT交易端", mode="login", credentials={"user": "...", "password": "..."})
```

### 单文件构建与本地模拟运行

部分券商 QMT 禁止加载外部文件/模块时，可将整包打成单个自包含策略文件（默认串口模式，base64 内嵌，产物约 700–900KB，已 `.gitignore` 勿提交，改源码后重生成）：

```bash
python tools/build_single_file.py
```

构建器读取启动目录 `.env`：
- `BIGQMT_RPC_ALLOW_ORDER_METHODS`：`1` / `true` / `yes` / `on` 开启远程下单撤单（默认关，`bigqmt-init` 亦以此为默认值）。
- `BIGQMT_QMT_SERIAL_PORT`：写入单文件串口配置，默认 `COM10`（独立桥接程序另用 `BIGQMT_SERIAL_PORT` 如 `COM11`）。
- 策略运行时从进程环境读取 `BIGQMT_ACCOUNT_ID`；如需调整可编辑产物顶部的 `BIGQMT_ACCOUNT_TYPE` 与 `BIGQMT_CONFIG`。

**本地模拟验证**（Python 3.12+，内存串口分帧，无需 Windows QMT / VK / 真实 COM）：

```bash
python tools/simulate_qmt.py                                     # 用当前源码临时构建并检查全链路
python tools/simulate_qmt.py src/BIGQMT_VK_DRYRUN_ALL_IN_ONE.py  # 复查指定产物
python tools/simulate_qmt.py --rpc get_full_tick --params '{"codes":["000001.SZ"]}'
```

支持在 IDE 或 `python -m pdb tools/simulate_qmt.py --timeout 60` 中于 `namespace["init"](context)` 下断点；若策略尝试文件或 Socket I/O（即使被捕获）或任一检查失败，退出码为 `1`。真实原生接口与券商限制仍须在目标终端验证。

---

## 本地测试

```powershell
# 单元测试（199 用例）
python -m pytest tests/bigqmt_signal_trader/ -q

# 端到端实盘 API 验证（检测持仓误返空、strategy_name 不匹配、下单静默失败与 server_error）
$env:BIGQMT_ACCOUNT_ID="你的账号"; $env:BIGQMT_VK_HOST="VK地址"; $env:BIGQMT_VK_PORT="6379"; $env:BIGQMT_VK_DB="5"; $env:BIGQMT_VK_PASSWORD="密码"
python test_all_apis.py
# 或复用 QMT 配置：$env:PYTHONPATH="D:\QMT交易端\python;$env:PYTHONPATH"; python test_all_apis.py
```

---

## 日志与排错

### 下单报错速查

| 报错信息 | 原因 | 解决方法 |
|---------|------|---------|
| `ValueError: rpc method is not allowed: order_stock` | 服务端 `rpc_allow_order_methods` 默认 `False` | `xt_trader.client.call("ping")["allow_order_methods"]` 确认；服务端改 `True` 后**重启策略**（`reload_deployment` 刷不了顶层文件） |
| `RuntimeError: passorder is not available in Big QMT runtime` | 被当作普通脚本运行 | 在 QMT **模型交易**运行，且不勾「独立 Python 进程」 |
| `server_error: passorder submitted but order not found in system`（模拟模式） | 模型交易默认「模拟」模式，委托不到券商 | 改为**实盘**模式 |
| `server_error: passorder submitted but order not found in system`（已是实盘） | 终端下单前拦截（资金/仓位不足、越界、无权限），仅弹窗不建单 | 查看 QMT 弹窗；异步下单会收到 `on_order_error`（`source="settlement"`） |
| `order_gateway is not configured` | 策略 `init()` 异常 | 查看启动日志定位根因 |
| `直接还款 (order_type 32): the repayment amount goes in order_volume...` | 还款金额填进了 `price` | 金额（整数元）传给 `order_volume`，`price` 传 `0` |
| `RequestExpired: ... NOT dispatched` / `TimeoutError: ... did NOT place this order` | 并发下单排队超时（`passorder` 串行 ~200ms/笔），桥已拒发 | 可安全重试（超时后客户端会自动查 `get_request_outcome` 明确告知是否已下）；降并发或调大 `timeout_seconds` |

### 日志位置与配置

| 环境 | 路径（按天轮转，默认保留 7 天） |
|------|-------------------------------|
| **QMT 内（服务端）** | `<QMT python 目录>\logs\bigqmt.log` |
| **外部客户端** | `~\.cache\bigqmt\logs\bigqmt.log` |

```powershell
Get-Content "D:\QMT交易端\python\logs\bigqmt.log" -Wait -Tail 50        # 实时跟踪
Get-Content "D:\QMT交易端\python\logs\bigqmt.log" | Select-String "ERROR|WARN"
```

环境变量：`BIGQMT_LOG_ENABLED`（默认 `1`，置 `0` 关文件日志）、`BIGQMT_LOG_TO_STDOUT`（默认 `1`，置 `0` 不输出面板）、`BIGQMT_LOG_RETENTION_DAYS`（默认 `7`）。排错首选日志文件（含 `[bigqmt_diag]` 启动诊断，不随面板清空丢失）。

---

## 安全默认值

- `rpc_allow_order_methods` 默认 `False`，确认风控与接入方后再显式开启。
- 回测桥接永久 `live_ready=false`，不含实盘账户与下单方法。
- 本地配置文件含账号凭据，已在 `.gitignore`，切勿提交。
- RPC 请求负载经 base64 + 数字混淆编码（`encode_rpc_request_payload`），避免 QMT 侧客户端拦截含股票代码的明文。

---

## AI 助手 Skill

仓库内置 [skill/](../skill/)（`SKILL.md`、`scripts/qmt.py`、`references/api_reference.md`），让支持 `SKILL.md` 的 AI 助手通过 47 个确定性 CLI 子命令操作 QMT（默认输出 `{ok, data, ts}` JSON，加 `--table` 切换表格，自动挂载 `src/` 并发现配置），人也可直接手动运行。

**启用方式**：
- 全局安装：`cp -r qmt-trader ~/.claude/skills/qmt-trader`（Claude Code）或 `~/.agents/skills/qmt-trader`（ZCode / 其他助手）。
- 对话指定：让助手先读 `skill/SKILL.md` 再调 `skill/scripts/qmt.py`。
- 手动使用：

```powershell
python skill/scripts/qmt.py ping
python skill/scripts/qmt.py snapshot --table                              # 资产+持仓+委托+成交全景
python skill/scripts/qmt.py tick 600000.SH                                # 五档盘口
python skill/scripts/qmt.py kline 600000.SH --period 1d --count 60 --dividend front
python skill/scripts/qmt.py buy 600000.SH 100 --price 7.50 --dry-run      # 干跑确认
```

| 分类 | 命令 |
|------|------|
| **连通/全景** | `ping` / `snapshot` |
| **账户** | `account` / `positions` / `orders` / `trades` |
| **行情** | `tick` / `kline` / `instrument` / `sector` / `trading-dates` / `north` / `longhubang` / `financial` / `download` / `quote-subscribe` |
| **期权分析** | `option-greeks <option_code>` / `option-greeks 510050.SH --expiry 202609` |
| **扩展查询（25 个）** | `holiday` / `stock-name` / `instrument-type` / `divid-factors` / `market-times` / `trading-calendar` / `option-list` / `bsm-price` / `bsm-iv` / `hkt-stats` / `hkt-details` / `hkt-rate` / `top10-holder` / `holder-num` / `ipo` / `ipo-limit` / `credit-assure` / `credit-short` / `credit-debt` / `his-st` / `index-weight` / `industry` / `sector-info` / `local-data` / `timetag2dt` / `dt2timetag` |
| **交易** | `buy` / `sell` / `cancel`（均支持 `--dry-run`，买卖支持 `--latest` / `--strategy` / `--remark`） |
| **通用兜底** | `rpc <method> [json]`（调用白名单内任意方法） |

**交易安全守则**：下单三命令受 `rpc_allow_order_methods` 控制（关闭时报 `ORDER_DISABLED`）；下单前先 `tick` 看价并 `--dry-run`；遇 `ORDER_TIMEOUT` 切勿盲目重试，先用 `orders` 确认是否已入库；查询全部委托用 `orders --strategy ""`。完整工作流与参数见 [skill/SKILL.md](../skill/SKILL.md) 和 [skill/references/api_reference.md](../skill/references/api_reference.md)。

---

## 为什么不直接连 BIGQMT

官方 `xtquant.xttrader.XtQuantTrader` 依赖 XtQuantServer 通道，当前实测 BIGQMT 直连 `connect()` 返回 `-1`，因此**交易能力**必须驻留在 BIGQMT 内部策略进程中由外部通过 RPC 驱动；而**只读行情**已通过 `58600` 端口的 FormulaServer（即 `qmt_api` 使用的服务）直连加速。若券商后续开放 XtQuantServer 且 `connect()==0`，可再补充交易直连模式。
