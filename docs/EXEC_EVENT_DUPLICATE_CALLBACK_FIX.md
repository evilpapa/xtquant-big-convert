# BigQMT 执行回调重复触发修复记录

日期：2026-08-12

## 现象与原因

实盘下单后，同一委托状态和成交回报在客户端各出现两次。策略同时暴露 `on_order` / `on_trade` 和 `order_callback` / `deal_callback`，后者又调用前者；若 QMT 同时触发两套入口，同一事件便会被发布和转发两次。

## 修复

策略入口只保留 `order_callback(ContextInfo, orderInfo)` 和 `deal_callback(ContextInfo, dealInfo)`，直接发布 Valkey 执行事件并转发给本地 runner；运行时和 dry-run 入口不再导入或导出旧别名。相关策略入口、运行时及测试均已更新。

## 验证

当时的定向测试为 38 passed，全量测试为 275 passed、4 skipped：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\bigqmt_signal_trader\test_runner.py tests\bigqmt_signal_trader\test_exec_events.py -q
.\.venv\Scripts\python.exe -m pytest -q
```

`order_stock()` 同步返回 `-1` 而随后收到真实回报属于另一问题：同步路径未及时取得 `order_sys_id`，异步事件稍后才取得。本修复不涉及该问题。
