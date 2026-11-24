# RPC 传输层

RPC 使用 `serial`；`shm` 是占位实现。由 Windows 上独立运行的桥接进程把串口接回 VK 服务。

客户端、QMT 策略和独立串口桥接都需要 Python 3.12+。VK 使用请求队列及响应通道。外部客户端安装 `xtquant-big-convert[valkey]`，直连 VK 的 QMT 策略在实际使用的解释器中安装 `valkey>=6.1,<7`。

- 服务端在 `bigqmt_signal_trader_qmt_config.py` 配置 `BIGQMT_CONFIG`;
- 客户端在 `bigqmt_signal_trader_client_config.py` 配置相应传输。调用 `bigqmt-init` 可生成 VK 配置。

连接已有实例时，可填写 `host`、`port`、`db` 等字段，也可配置 `url`。TCP URL 接受 `valkey://` 和 `redis://`，均交由 `valkey` 包处理。

```python
BIGQMT_CONFIG = {
    "transport": "valkey",
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "rpc_background_threads": False,
    "schedule_adjust": True,
}
```

## 通过 com0com 接回 VK

```text
QMT 模型交易 (COM10) ←→ com0com ←→ Windows 桥接 (COM11) ←→ VK ←→ Linux 策略
```

1. 用 com0com 建立虚拟串口对，例如 `install PortName=COM10 PortName=COM11`。两端波特率和帧大小上限须一致。建议先关闭 `EmuOverrun`，避免缓冲区满时丢字节；若开启 `EmuBR`，大包吞吐会受配置波特率限制。
2. 把更新后的策略、运行时和包同步到 QMT，确保 **QMT 自己的 Python** 能导入 `pyserial`（模块名为 `serial`）。在独立 Windows Python 环境中安装来自当前代码的 `xtquant-big-convert[valkey,serial]`。部署须符合券商要求。
3. 启动 QMT 前设置 `BIGQMT_ACCOUNT_ID` 环境变量，让 QMT 进程能读到它。已有 QMT 进程需要重启才能读到新设置。在 QMT 的 `bigqmt_signal_trader_qmt_config.py` 中读取账号并设置传输；初次验证保持下单关闭：

   ```python
   import os
   import warnings

   BIGQMT_ACCOUNT_ID = os.environ.get("BIGQMT_ACCOUNT_ID", "").strip()
   if not BIGQMT_ACCOUNT_ID:
       warnings.warn("BIGQMT_ACCOUNT_ID is unset or empty; no account is configured.", RuntimeWarning)

   BIGQMT_CONFIG = {
       "transport": "serial",
       "serial": {"port": "COM10", "baudrate": 921600},
       "schedule_adjust_interval": "100nMilliSecond",
       "rpc_allow_order_methods": False,
   }
   ```
4. 在 Windows 上先启动桥接，再启动 QMT 策略。`start_serial_bridge.ps1` 默认会按这个顺序执行：串口就绪后调用 `qmt_launcher` 拉起 QMT，由 QMT 自己完成自动登录和自动运行策略（见[桥接和 QMT 一起启动](SERIAL_VK_BRIDGE.md#桥接和-qmt-一起启动)）。

### 快照缓存和异步下载

- **行情快照**。客户端启用 `BIGQMT_FULL_TICK_CACHE_CONFIG = {"enabled": True}` 后，桥接按现有 VK 需求刷新缓存，相同代码集合共享一次刷新。指定证券和市场快照分别至少间隔 0.5 秒和 3 秒，实际间隔取决于请求队列、QMT 和串口。缓存使用默认证券类型，显式传 `types` 时走普通 RPC。全市场刷新仍受串口单帧上限约束。
- **异步下载**。客户端调用 `xtdata.submit_download_history_data2(stock_list, period, start_time, end_time)` 后立即拿到 `job_id`。通过 `xtdata.get_download_status(job_id)` 或 `xtdata.wait_download(job_id)` 查看状态。桥接使用 VK 保存任务与进度，每次最多向 QMT 提交 10 只标的，可在提交 RPC 中用 `chunk_size` 调小。
- **下载完成条件**。`accepted` 表示 QMT 已受理的标的数，`done` 表示在请求的时间窗内读回至少一条有效记录的标的数。批次内所有标的均通过读回才计入 `done`，这一检查不验证整个时间窗的数据完整性。开始读回后约 15 秒仍读不到数据，任务返回 `failed`。串口断开时若受理结果未知，任务返回 `failed` 并说明原因。重启桥接后可以继续排队中的任务，也可以在读回窗口内检查已确认受理的批次。

## 策略名与重启恢复

配置 VK 时，身份记录从 VK 读取。身份值使用无数字编码，解码后保留原有字段。读取旧记录时，若客户端报告 `Sensitive Data Detected, Forbidden!`（#393），桥接会在 VK 内原子迁移，保留内容及剩余过期时间。该迁移需要 VK 服务
允许 `EVAL`、`GET`、`SETRANGE`；权限不足时原记录保留，错误可从 `probe_order_identity`
的 `lookup_error` 查看。

QMT 策略的本地订单身份记录保存在内存中，`probe_order_identity` 显示
`local_persistence_enabled: false`。串口模式下，策略进程重启会清空这份记录。
直连 VK 时，策略重启后仍可读取七天内保存在 VK 的身份记录。

身份记录里的 `user_order_id` 就是提交时传入的 `order_remark`，同一备注的首个非空策略名优先。
同步新包和顶层策略文件后，须重启策略一次。
