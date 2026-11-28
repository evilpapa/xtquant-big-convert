# 大 QMT 信号下单包运行手册

> 更新时间：2026-07-01（早期信号下单 dry-run 阶段运维记录；当前通用部署与 RPC 桥使用见 [USER_GUIDE.md](USER_GUIDE.md) 与 [DEPLOY_QUICKSTART.md](DEPLOY_QUICKSTART.md)）。

## 1. 当前状态与核心原则

本包定位为「**外部系统产出逐笔交易信号，大 QMT 只负责执行**」（非 `qmt_jq_trade` 的目标持仓同步脚本），包含策略入口 `bigqmt_signal_trader_strategy.py` 及行情/持仓/下单适配层，默认 `mode="dryrun"` 不下真实委托。

- **已完成**：本地单测 32 passed、QMT 目录导入与 `init/adjust/sync_positions` 模拟回调、fake `passorder` 参数验证、VK Stream 信号源/状态写回/持仓同步（迁移前已于本机 Redis db5 验证 dry-run）。
- **未完成**：QMT「模型交易」真实实盘委托验证、将 miniQMT 切换为只写 VK 信号、真实账号启用 `passorder`。
- **页面检查（2026-07-01 08:00）**：模型交易当前仅运行「网格策略」，本策略尚未挂入运行。

### 必须遵守的三条 QMT 运行规则

1. **编辑器运行 ≠ 真实交易**：编辑器「运行」仅查 `init/handlebar` 语法与回调；实盘前必须进「模型交易」页面绑定资金账号，先用「模拟信号」观察，全部前置条件通过后再开「实盘交易」。
2. **切勿勾选「启动本地 python」**：勾选后会作为独立脚本运行，不触发 `init/handlebar/order_callback/deal_callback` 且立即退出。
3. **跳过历史 bar**：入口已通过 `if hasattr(ContextInfo, "is_last_bar") and not ContextInfo.is_last_bar(): return None` 防止历史回放阶段误消费实时信号。

---

## 2. 文件部署

将 `<REPO_ROOT>\src` 中的 `bigqmt_signal_trader\`、`bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_dryrun.py`、`bigqmt_signal_trader_vk_dryrun.py` 复制到 `<QMT_PYTHON_DIR>`，并清理 `__pycache__`：

```powershell
Copy-Item -Path '<REPO_ROOT>\src\bigqmt_signal_trader\*' -Destination '<QMT_PYTHON_DIR>\bigqmt_signal_trader' -Recurse -Force
foreach ($f in 'bigqmt_signal_trader_strategy.py','bigqmt_signal_trader_dryrun.py','bigqmt_signal_trader_vk_dryrun.py') {
  Copy-Item -LiteralPath "<REPO_ROOT>\src\$f" -Destination "<QMT_PYTHON_DIR>\$f" -Force
}
Get-ChildItem -LiteralPath '<QMT_PYTHON_DIR>' -Recurse -Filter '__pycache__' -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
```

---

## 3. 编辑器 Dry-Run 与 VK 链路验证

### 3.1 基础 Dry-Run 入口（纯 ASCII，避免编码问题）

在策略编辑器新建策略（周期 `1分钟`/`3分钟`、标的 `000300.SH`、不勾「启动本地 python」与自动交易），保存 → 编译 → 运行，期望输出 `[bigqmt_signal_trader] init ok` 与 `[bigqmt_signal_trader] adjust ok`：

```python
#coding:gbk
from bigqmt_signal_trader_strategy import (
    adjust, configure, deal_callback, handlebar, init, order_callback, set_account_id, sync_positions,
)

try:
    ACCOUNT_ID = account
except NameError:
    ACCOUNT_ID = ""

if ACCOUNT_ID:
    set_account_id(ACCOUNT_ID)
configure(mode="dryrun", account_id=ACCOUNT_ID or "dryrun")
```

### 3.2 VK Dry-Run 安全观察入口

`bigqmt_signal_trader_vk_dryrun.py` 默认使用测试账号 `bigqmt_probe`（`127.0.0.1:6379 db=5`，Stream `bigqmt:signals:bigqmt_probe`，Status `bigqmt:signal_status:bigqmt_probe:{signal_id}`，Position `bigqmt:positions:bigqmt_probe`，`DryRunOrderGateway`）。若 `<QMT_PYTHON_DIR>\bigqmt_signal_trader_qmt_config.py` 存在 `BIGQMT_CONFIG` 则覆盖 VK 连接（含密码勿提交）。

> 切勿将 `ACCOUNT_ID` 设为真实资金账号，以免 dry-run 消费并 ack 真实待执行信号。

写入测试信号并核验状态（期望 `status = DRY_RUN`、`user_order_id = dryrun:bq:...`）：

```powershell
cd <REPO_ROOT>
python -B -c "import sys,valkey; sys.path.insert(0,'src'); from bigqmt_signal_trader.adapters.signal_vk import push_trade_signal; r=valkey.Valkey(host='127.0.0.1',port=6379,db=5); push_trade_signal(r, {'signal_id':'probe-001','account_id':'bigqmt_probe','action':'BUY','stock_code':'600000.SH','amount':100,'price_type':'FIX_PRICE','price':10.0,'created_at':'2026-07-01 09:31:00','expire_at':'2026-07-01 23:59:00','schema_version':1})"
python -B -c "import valkey; r=valkey.Valkey(host='127.0.0.1',port=6379,db=5,decode_responses=True); print(r.hgetall('bigqmt:signal_status:bigqmt_probe:probe-001'))"
```

---

## 4. 模型交易运行与实盘准入

1. **模拟信号阶段**：模型交易新建实例 → 选 dry-run 策略 → 绑定资金账号 → 标的 `000300.SH` / `1分钟` → 选「模拟信号」→ 启动。开盘观察 `init ok`、`adjust ok`、无 traceback、无真实委托（跑 VK dry-run 时可投递 `bigqmt_probe` 测试信号）。
2. **大 QMT Adapter 装配测试**：将策略末尾改为 `configure(mode="bigqmt", account_id=ACCOUNT_ID or "dryrun")`；无真实 `SignalSource` 时仅验证行情/持仓/委托适配器可用，不发单。
3. **开盘观察节奏（9:10 前 / 9:30–9:35 / 9:35 后）**：9:10 前完成部署、编译与模拟启动；9:30–9:35 核查 `init ok`/`adjust ok`、`XtClient_Formula_YYYYMMDD.log`、无真实委托及测试信号状态；稳定后再调整触发周期，保持观察。
4. **切「实盘交易」必须全部满足**：
   - 已验证 Stream 信号源、`StateStore.claim()` 与状态回写；
   - 模拟信号已验证 `passorder` 参数、持仓与撤单查询；
   - 已确认历史 bar 不触发下单；
   - miniQMT 已停止对同账户真实下单（防双系统抢单）；
   - dry-run 未 ack 真实账号的待实盘信号。

---

## 5. 日志排查与参考

日志文件：`<QMT_USERDATA_LOG_DIR>\XtClient_Formula_YYYYMMDD.log`，重点搜索 `bigqmt_signal_trader` / `Traceback` / `ModuleNotFoundError` / `SyntaxError` / `run script failed` / `passorder`。

| 现象 | 原因 | 处理 |
|------|------|------|
| 仅开始/结束运行，无回调日志 | 勾了「启动本地 python」或未进模型交易 | 取消勾选，改用模型交易运行 |
| `No module named dataclasses` | 旧版 QMT Python 不支持 `dataclasses` | 已移除依赖，重拷并清 `__pycache__` |
| `__file__ is not defined` | QMT 编辑器脚本无 `__file__` | 入口勿依赖 `__file__` |
| 编码错误 | 编辑器保存编码与声明不一致 | 文件头用 `#coding:gbk`，源码保持纯 ASCII |
| 启动后处理大量历史 bar | 未过滤历史 K 线 | 已由 `is_last_bar()` 保护 |

**参考链接**：[ThinkTrader 大 QMT 接口文档](https://dict.thinktrader.net/innerApi/interface_operation.html) · [大 QMT Python API 文档](https://qmt.ptradeapi.com/QMT_Python_API_Doc.html) · 参考脚本 `<REPO_ROOT>\src\api\qmt_jq_trade`。
