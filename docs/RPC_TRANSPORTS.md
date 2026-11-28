# RPC 传输层

RPC 支持 `valkey`（默认）、`serial`、`pipe`（Windows 本机命名管道）、`copydata`（Windows 窗口消息 `WM_COPYDATA`）和 `shm`（Windows 本机共享内存）。`serial`、`pipe` 与 `copydata` 经 Windows 独立桥接接回 VK；`shm` 不经桥接和 VK。客户端、QMT 策略及独立桥接均需 **Python 3.12+**。

- **配置位置**：服务端在 `bigqmt_signal_trader_qmt_config.py`、客户端在 `bigqmt_signal_trader_client_config.py` 配置 `BIGQMT_CONFIG`（`bigqmt-init` 可生成 VK 配置）。
- **VK 依赖与连接**：外部客户端装 `xtquant-big-convert[valkey]`，直连 VK 的 QMT 策略装 `valkey>=6.1,<7`。连接支持 `host`/`port`/`db` 分项或 `url`（`valkey://`、`redis://` 均由 `valkey` 处理）：

```python
BIGQMT_CONFIG = {
    "transport": "valkey",
    "host": "127.0.0.1", "port": 6379, "db": 5,
    "rpc_background_threads": False,
    "schedule_adjust": True,
}
```

---

## Windows 本机命名管道（pipe）

```text
QMT 模型交易（管道客户端） ←→ \\.\pipe\qmt_bridge ←→ Windows 独立桥接（管道服务端） ←→ VK ←→ Linux 策略
```

仅用于模拟或明确获准的环境。无需虚拟串口或 `pyserial`；QMT 仅打开本机管道句柄，不读写文件、不建 Socket、故障不回退其他传输。服务端拒绝远程管道，桥接与策略须以**同一 Windows 用户**运行（管道名不是认证凭据）。

1. **启动 Windows 桥接**：安装 `pip install ".[valkey]"`，通过本地 `.env` 或环境设 `BIGQMT_ACCOUNT_ID` 和 `BIGQMT_PIPE_VK_URL`（或 `--vk-url`，优先级更高），运行：

   ```powershell
   bigqmt-pipe-bridge --pipe-name qmt_bridge
   # 源码环境：python -m bigqmt_signal_trader.pipe_vk_bridge --pipe-name qmt_bridge
   ```

   `--pipe-name` 支持短名或 `\\.\pipe\...`；桥接与外部客户端须连同一 VK 实例和 DB。
2. **配置 QMT 策略**：使用与桥接一致的 `BIGQMT_ACCOUNT_ID`：

   ```python
   BIGQMT_CONFIG = {
       "transport": "pipe",
       "pipe": {"name": "qmt_bridge"},
       "schedule_adjust_interval": "100nMilliSecond",
       "rpc_allow_order_methods": False,
   }
   ```

   默认单帧 8 MiB（调大需同步改策略 `pipe.max_frame_bytes` 与桥接 `--max-frame-bytes`）。管道线程仅收发字节，交易类 RPC 仍在 `adjust()` 执行。
3. **客户端调用**：外部客户端继续用 `transport="valkey"`，先用 `ping` / `get_full_tick` / `get_asset` 验证。

> QMT 断开后桥接保持监听；桥接重启后须重启 QMT 策略重连。已转发请求不重放，超时不等于未执行。回报、行情推送、快照缓存与异步下载沿用串口桥协议；策略侧不支持 RPC 热重载。

---

## Windows 窗口消息（copydata / WM_COPYDATA）

```text
QMT 模型交易（隐藏窗口） ←→ WM_COPYDATA ←→ Windows 独立桥接（隐藏窗口） ←→ VK ←→ Linux 策略
```

仅用于模拟或明确获准的环境。无需虚拟串口、`pyserial` 或命名管道；外部桥接进程创建一个隐藏 Win32 窗口，QMT 策略侧通过 `user32.FindWindowW` 定位窗口句柄，并使用 `WM_COPYDATA` 在同一 Windows 登录会话内跨进程收发帧字节流。QMT 侧不读写文件、不建 Socket，故障不自动回退其他传输。桥接与策略须在**同一 Windows 登录会话、同一用户桌面**运行（窗口名不是认证凭据）。

1. **启动 Windows 桥接**：安装 `pip install ".[valkey]"`，通过本地 `.env` 或环境设 `BIGQMT_ACCOUNT_ID` 和 `BIGQMT_COPYDATA_VK_URL`（或 `--vk-url`，优先级更高），运行：

   ```powershell
   bigqmt-copydata-bridge --window-name qmt_bridge
   # 源码环境：python -m bigqmt_signal_trader.copydata_vk_bridge --window-name qmt_bridge
   ```

   `--window-name` 支持短名（如 `qmt_bridge`）或 `BigQmtCopyData:...` 标题；桥接与外部客户端须连同一 VK 实例和 DB。
2. **配置 QMT 策略**：使用与桥接一致的 `BIGQMT_ACCOUNT_ID` 和 `window_name`：

   ```python
   BIGQMT_CONFIG = {
       "transport": "copydata",
       "copydata": {"window_name": "qmt_bridge"},
       "schedule_adjust_interval": "100nMilliSecond",
       "rpc_allow_order_methods": False,
   }
   ```

   默认单帧 8 MiB（底层按 64 KiB 分块经 `WM_COPYDATA` 投递；调大上限需同步改策略 `copydata.max_frame_bytes` 与桥接 `--max-frame-bytes`）。窗口消息线程仅负责字节收发，交易类与查询类 RPC 仍在 `adjust()` 主策略线程非阻塞拉取并执行。
3. **客户端调用**：外部客户端继续用 `transport="valkey"`，无需感知 QMT 侧的本地 IPC 切换，先用 `ping` / `get_full_tick` / `get_asset` 验证。

> QMT 断开后桥接保持隐藏窗口监听；桥接重启后须重启 QMT 策略重连。已转发请求不重放，超时不等于未执行。回报、行情推送、快照缓存与异步下载沿用串口桥协议；策略侧不支持 RPC 热重载。

---

## Windows 本机共享内存（shm）

仅用于模拟或明确获准的环境。策略与客户端须在**同一 Windows 机器、同一登录会话**运行，无需 VK 或 `pyserial`，不支持跨机/跨会话，故障不切换传输。

1. **QMT 侧配置**（沿用 `BIGQMT_ACCOUNT_ID`）：

   ```python
   BIGQMT_CONFIG = {
       "transport": "shm",
       "shm": {"namespace": "qmt_shm", "max_response_bytes": 32 * 1024 * 1024},
       "rpc_allow_order_methods": False,
       "schedule_adjust_interval": "100nMilliSecond",
   }
   ```

2. **本机客户端调用**（也可写入客户端 `BIGQMT_CONFIG`）：

   ```python
   from bigqmt_signal_trader import BigQmtRpcClient

   client = BigQmtRpcClient(
       account_id="YOUR_ACCOUNT_ID",
       transport="shm",
       vk_config={"shm": {"namespace": "qmt_shm", "max_response_bytes": 32 * 1024 * 1024}},
   )
   print(client.call("ping"))
   ```

- **对齐约束**：两端 `account_id`、`namespace`、`max_request_bytes`（默认 1 MiB）和 `max_response_bytes`（默认 32 MiB，槽位上限 256 MiB）须一致。双向各占一个有界页文件映射槽位（不创建业务文件），用命名互斥量与事件同步；**映射名和账号不是认证**，勿向不受信任的本机会话开放。
- **行为限制**：单端点同时处理 1 个在途 RPC，仍在 `adjust()` 调度（延迟含终端调度与 JSON 编解码，不保证微秒级）；超时不重发；重启服务前先结束等待中的客户端；首版不支持行情/执行事件推送、VK 快照缓存、异步下载和 RPC 热重载。

---

## 通过 com0com 接回 VK

```text
QMT 模型交易 (COM10) ←→ com0com ←→ Windows 桥接 (COM11) ←→ VK ←→ Linux 策略
```

1. 用 com0com 建虚拟串口对（如 `install PortName=COM10 PortName=COM11`），两端波特率和帧上限须一致；建议关 `EmuOverrun`（防丢字节），注意开启 `EmuBR` 会限制大包吞吐。
2. 同步代码到 QMT，确保 **QMT 的 Python** 可导入 `serial`（`pyserial`），并在独立 Windows Python 环境中安装 `xtquant-big-convert[valkey,serial]`。
3. 启动 QMT 前在进程环境设 `BIGQMT_ACCOUNT_ID`（已有进程须重启），在 `bigqmt_signal_trader_qmt_config.py` 配置串口（初次验证保持下单关闭）：

   ```python
   import os, warnings

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

4. Windows 侧先启桥接再启 QMT 策略（`start_serial_bridge.ps1` 默认按此顺序并拉起 QMT，见[桥接和 QMT 一起启动](SERIAL_VK_BRIDGE.md#桥接和-qmt-一起启动)）。

### 快照缓存与异步下载

- **行情快照缓存**：客户端设 `BIGQMT_FULL_TICK_CACHE_CONFIG = {"enabled": True}` 后，桥接按需刷新并合并相同代码集合请求（指定标的 / 市场快照最小间隔分别为 0.5s / 3s）。缓存仅用于默认证券类型，显式传 `types` 走普通 RPC，全市场仍受串口单帧上限约束。
- **异步下载**：`xtdata.submit_download_history_data2(stock_list, period, start_time, end_time)` 立即返回 `job_id`，用 `get_download_status(job_id)` 或 `wait_download(job_id)` 查询。桥接在 VK 保存进度，单批最多向 QMT 提交 10 只（可传 `chunk_size` 调小）。
- **完成判定与故障**：`accepted` 为 QMT 已受理数，`done` 为时间窗内读回至少一条有效记录的标的数（不校验整窗完整性）。开始读回约 15 秒无数据或串口断开导致受理未知时返回 `failed`。桥接重启后可继续排队任务及复检已受理批次。

---

## 策略名与重启恢复

- **VK 身份记录与迁移**：配置 VK 时身份记录采用无数字编码存储。读取旧记录若触发 `Sensitive Data Detected, Forbidden!`（#393），桥接会在 VK 内原子迁移并保留剩余 TTL（需 VK 允许 `EVAL` / `GET` / `SETRANGE`；权限不足时保留原记录，错误见 `probe_order_identity` 的 `lookup_error`）。
- **重启保持边界**：QMT 策略本地身份记录仅在内存中（`probe_order_identity` 显示 `local_persistence_enabled: false`）：串口模式重启策略即清空，直连 VK 模式重启后仍可读 7 天内的 VK 记录。`user_order_id` 即下单 `order_remark`，同备注首个非空策略名优先；同步新包和顶层策略文件后须重启策略一次。
