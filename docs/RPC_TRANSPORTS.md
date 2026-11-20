# RPC 传输层

RPC 默认使用 `redis`；`shm` 是占位实现。QMT 侧也可使用 `serial`，由 Windows 上独立运行的桥接进程把串口接回 Redis。

- Redis 使用请求队列及响应通道；安装 `xtquant-big-convert[redis]`。

服务端在 `bigqmt_signal_trader_local_config.py` 配置 `BIGQMT_REDIS_CONFIG`，客户端在 `bigqmt_signal_trader_client_config.py` 配置相应传输。调用 `bigqmt-init` 可生成 Redis 配置。

```python
BIGQMT_REDIS_CONFIG = {
    "transport": "redis",
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "rpc_background_threads": False,
    "schedule_adjust": True,
}
```

## 通过 com0com 接回 Redis

```text
QMT 模型交易 (COM10) ←→ com0com ←→ Windows 桥接 (COM11) ←→ Redis ←→ Linux 策略
```

1. 用 com0com 建立虚拟串口对，例如 `install PortName=COM10 PortName=COM11`。两端波特率和帧大小上限须一致。建议先关闭 `EmuOverrun`，避免缓冲区满时丢字节；若开启 `EmuBR`，大包吞吐会受配置波特率限制。
2. 把更新后的策略、运行时和包同步到 QMT，确保 **QMT 自己的 Python** 能导入 `pyserial`（模块名为 `serial`）。在独立 Windows Python 环境中安装来自当前代码的 `xtquant-big-convert[redis,serial]`。部署须符合券商要求。
3. 启动 QMT 前设置 `BIGQMT_ACCOUNT_ID` 环境变量，让 QMT 进程能读到它。已有 QMT 进程需要重启才能读到新设置。在 QMT 的 `bigqmt_signal_trader_local_config.py` 中读取账号并设置传输；初次验证保持下单关闭：

   ```python
   import os
   import warnings

   BIGQMT_ACCOUNT_ID = os.environ.get("BIGQMT_ACCOUNT_ID", "").strip()
   if not BIGQMT_ACCOUNT_ID:
       warnings.warn("BIGQMT_ACCOUNT_ID is unset or empty; no account is configured.", RuntimeWarning)

   BIGQMT_REDIS_CONFIG = {
       "transport": "serial",
       "serial": {"port": "COM10", "baudrate": 921600},
       "schedule_adjust_interval": "100nMilliSecond",
       "rpc_allow_order_methods": False,
   }
   ```
> PS: QMT 端原来依赖 Redis 的快照缓存和异步下载任务会关闭，委托归属改用本地记录。

4. 在 Windows 上先启动桥接，再启动 QMT 策略。桥接默认也读取 `BIGQMT_ACCOUNT_ID`，必须与 QMT 端一致。若 Redis 需要认证，把连接串放在 `BIGQMT_SERIAL_REDIS_URL` 环境变量；省略时使用 `redis://127.0.0.1:6379/5`。

   ```powershell
   $env:BIGQMT_ACCOUNT_ID = 'YOUR_ACCOUNT_ID'  # 换成实际账号；对当前 PowerShell 和子进程生效
   bigqmt-serial-bridge --port COM11 --baudrate 921600
   ```

   仓库根目录的 `start_serial_bridge.ps1` 会自动从 `bigqmt_signal_trader_local_config.py` 读取 Redis 连接配置（避免桥接连 `127.0.0.1` 而客户端用的是另一台 Redis，那会报
   `ERROR Bridge stopped (TimeoutError)`），并在启动前预检 Redis 连通性和串口：

   ```powershell
   .\start_serial_bridge.ps1 -CheckOnly   # 只预检
   .\start_serial_bridge.ps1               # 启动，日志同时在控制台输出
   .\start_serial_bridge.ps1 -LogFile .\bridge.log -RedisPubsub
   ```

5. 确认桥接输出 `QMT serial session ready`，然后用现有 Redis 客户端执行 `ping`、`get_full_tick` 等只读请求。客户端仍配 `transport=redis`，账号、Redis 地址和数据库与桥接一致。默认使用 `bigqmt:rpc:queue:{account_id}` 及原有响应键、列表和频道；使用 Redis pub/sub 发请求的客户端需给桥接加 `--redis-pubsub`。同一账号只能运行一个队列消费者，避免让直连 Redis 的 QMT 策略与串口桥接同时消费。

串口每帧是 `8 位十六进制 CRC32` + `TAB` + `UTF-8 JSON` + `LF`。线程负责分帧和部分写入，QMT 的 `adjust` 从内存队列取请求；委托与成交回报由桥接发布到现有 Redis 频道，行情全推也使用原频道。行情推送拥塞时会丢弃超出队列容量的更新，委托与成交回报使用独立队列。默认单帧上限 8 MiB，大行情按批查询，或者同时调整 QMT 的 `serial.max_frame_bytes` 与桥接的 `--max-frame-bytes`。延迟还取决于 QMT 调度间隔和串口传输量，请在目标终端实测。

一个串口会话服务一个账号。串口模式自动关闭 QMT 侧的 Redis 和原生 `xtdata` 直连；Redis 快照缓存与异步下载任务也不可用。交易请求需使用更新后的 Redis 客户端包：调用接口不变，请求会携带发送时间，让桥接拒绝积压到过期的委托。Windows 和客户端主机应同步时钟。串口异常后关闭并重启相关进程；桥接不会自动重放已取出的请求。若断线时交易状态不明，按备注查询委托和成交再决定是否重试。

## 策略名与重启恢复

RPC 通道与策略归属存储是两件事。配置 Redis 时，身份记录经
Redis 读取；仅切换通道无法解决 QMT 内置 Redis 的 `Sensitive Data Detected, Forbidden!`
响应过滤错误（#393）。身份值现使用无数字编码，解码后原有字段不变；仍存活的旧 JSON
若被过滤，会在 Redis 内原子迁移，保留原内容及剩余过期时间。此兼容路径需要 Redis
允许 `EVAL`、`GET`、`SETRANGE`；权限不足时原记录保留，错误可从 `probe_order_identity`
的 `lookup_error` 查看。

没有配置 Redis 时，服务端在下单前同步追加本地身份 journal，并在启动时恢复七天内的
策略名。默认目录为 QMT 的 `python/state/order_identity`，不可写时回退到用户目录下
`.cache/bigqmt/order_identity`。可在启动 QMT 前设置 `BIGQMT_ORDER_IDENTITY_DIR` 指定
固定目录；文件按账户和账户类型隔离。重装、换运行用户或迁移终端时须保留此目录。
`probe_order_identity` 的 `local_persistence_directory`、`local_persistence_error`
分别显示实际路径及持久化错误。journal 写入失败时当前进程仍使用内存归属，重启恢复可能不完整。

该修复不改变 `order_remark`。身份记录里的 `user_order_id` 就是提交时传入的 remark，
同一 remark 的首个非空策略名仍优先。无 Redis 时的落盘行为从升级后生效；旧进程已丢失且
没有持久记录的归属无法重建。同步新包和顶层策略文件后，须重启策略一次。
