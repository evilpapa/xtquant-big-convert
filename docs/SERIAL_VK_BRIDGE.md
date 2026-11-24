# 独立运行的串口与 VK 桥接

## 通过 com0com 接回 VK

桥接默认也读取 `BIGQMT_ACCOUNT_ID`，必须与 QMT 端一致。若 VK 需要认证，把连接串放在 `BIGQMT_SERIAL_VK_URL` 环境变量；省略时使用 `valkey://127.0.0.1:6379/5`。

   ```powershell
   $env:BIGQMT_ACCOUNT_ID = 'YOUR_ACCOUNT_ID'  # 换成实际账号；对当前 PowerShell 和子进程生效
   bigqmt-serial-bridge --port COM11 --baudrate 921600
   ```

   仓库根目录的 `start_serial_bridge.ps1` 会自动从 `bigqmt_signal_trader_qmt_config.py` 读取 VK 连接配置（避免桥接连 `127.0.0.1` 而客户端用的是另一台 VK 服务器，那会报
   `ERROR 桥接已停止（TimeoutError…）`），并在启动前预检 VK 连通性和串口：

   ```powershell
   .\start_serial_bridge.ps1 -CheckOnly   # 只预检
   .\start_serial_bridge.ps1               # 启动，日志同时在控制台输出
   .\start_serial_bridge.ps1 -LogFile .\bridge.log -VkPubSub
   ```

### 桥接和 QMT 一起启动

QMT 已配置自动登录和自动运行策略时，脚本可以在桥接打开串口后自动拉起大 QMT：

   ```powershell
   # 或在 .env 写 BIGQMT_QMT_DIR=D:/gjzq_qmt_ceshi_new（也可用 -QmtDir / 环境变量）
   .\start_serial_bridge.ps1                 # 先桥接，串口就绪后启动 QMT
   .\start_serial_bridge.ps1 -RestartQmt     # QMT 已在运行时先关掉再一起重启
   .\start_serial_bridge.ps1 -NoQmt          # 只启动桥接（旧行为）
   ```

启动顺序是“桥接先打开串口，再启动 QMT”，调用包内的
`python -m bigqmt_signal_trader.qmt_launcher open --mode exe --no-wait`。看到桥接日志
`QMT 串口会话已就绪` 才表示 QMT 策略已经连上串口；登录框和自动运行策略仍由 QMT 自己处理。
需要让 qmt_launcher 代输账号密码时用 `-QmtMode login`。在运行桥接的 Python 环境中安装
`pywin32` 和 `pyautogui`，并保持 Windows 交互桌面解锁。
`BIGQMT_LOGIN_USER` 和 `BIGQMT_LOGIN_PASSWORD` 优先从进程环境读取，缺失时读取启动目录的
`.env`。凭据不出现在命令行或启动日志中。
如果 `XtItClient.exe` 要求提升权限，请从管理员权限的交互会话运行启动脚本。
这里不依赖 FormulaServer 的 58600 端口就绪：极简模式或只跑策略时该端口可能不监听。

QMT 目录优先级：`-QmtDir` > `.env` 的 `BIGQMT_QMT_DIR` > 环境变量 > 正在运行的 QMT
进程所在目录。脚本只操作该目录下的 QMT 进程，避免误关同机其他账号的终端。
`-CheckOnly` 也会显示 QMT 是否在运行；Ctrl+C 只停止桥接，不关闭 QMT，
下次启动请加 `-RestartQmt`。

### 图形控制台 exe

仓库提供 `tools/serial_bridge_gui.py`，打包后是单文件
`dist/BigQMTSerialController.exe`。主界面有「启动 / 停止 / 重启」，启动后出现两个 tab：

- `serial_vk_bridge 日志`：桥接子进程 stdout/stderr 的实时 tail；
- `QMT 策略日志（FormulaOutput）`：tail `<QMT>\userdata\log\XtClient_FormulaOutput_YYYYMMDD.log`，
  跨天自动切到新文件，按行兼容 QMT 日志里混用的 UTF-8 / GBK。

   ```powershell
   python tools/serial_bridge_gui.py     # 源码运行
   python -m pip install pyinstaller
   python tools/build_gui_exe.py         # 生成 dist\BigQMTSerialController.exe
   ```

行为与 PowerShell 脚本一致：先启动桥接，看到「串口桥接已启动」后再用
`qmt_launcher` 拉起 QMT；看到「QMT 串口会话已就绪」表示策略已连上串口。
「启动时重启已运行的 QMT」默认关闭——QMT 已在跑时只启动桥接并给出警告；
「停止/退出时关闭 QMT」默认开启。所有 QMT 操作都限定在配置的安装目录内。

「QMT 退出后自动拉起」默认开启：启动成功后监控 `XtItClient.exe`（可执行文件名取
`BIGQMT_QMT_EXE`），主进程持续缺失 8 秒就调用 `qmt_launcher open` 重新拉起；连续失败
6 次（10 分钟内）会暂停并提示。点「停止」「重启」或关闭窗口时不会触发；QMT 崩溃后
残留的 `miniquote` / `BrokerProxy` 等子进程会先清理再启动。

控制区右上角的「设置…」可以修改以下内容，保存时写回当前 `.env`（保留注释）：

- 串口：端口、波特率、单帧上限、VK pub/sub；
- BIGQMT 账户：资金账号、QMT 登录账号、QMT 登录密码（密码框掩码，不回显）；
- QMT 软件目录、启动方式（`exe` / `login`）、可执行文件名；
- VK：地址、端口、DB、用户名、密码（用户名 / 密码可留空）。

保存后立即更新内存配置；如果桥接正在运行，会询问是否重启使其生效。
设置面板同时写入 `BIGQMT_SERIAL_VK_URL` 和 `BIGQMT_VK_*` 分项，
所以 PowerShell 脚本和外部客户端仍能连接同一个 VK 实例。

启动时如果检测到已有 `serial_vk_bridge` 进程正占用配置的串口（常见于上次没退干净，
或 PowerShell 脚本还在跑），会弹出确认框；确认后先停止旧进程再启动。
如果旧桥接是以管理员权限运行的，GUI 也要以管理员权限运行才能停止它。
「停止」按钮也会清理占用同一个串口的其他桥接进程。QMT 日志里 UTF-8 / GBK 混排和被
QMT 截断的多字节字符会按行尽量恢复：被截断的字节丢弃，其余中文正常显示，不再整行乱码。

国金 QMT 的 `XtItClient.exe` manifest 是 `requireAdministrator`，非提升进程直接
`CreateProcess` 会得到 `WinError 740`。GUI 启动前会读 manifest 检测到这一点并询问
“是否以管理员身份重新启动”，同意后会触发 UAC，新窗口带 `--autostart` 自动开始。
也可以直接右键 exe 选择「以管理员身份运行」。如果是用 PowerShell 脚本启动 QMT，
同样需要在管理员权限的 PowerShell 会话里运行。

exe 只是前端，桥接和 `qmt_launcher` 仍由外部 Python 执行：优先用 `--python` /
`BIGQMT_PYTHON`，否则用 PATH 里的 `python`，该环境必须已安装 `xtquant-big-convert`
及 `[valkey,serial]` 依赖。exe 会依次查找 `--env-file`、`BIGQMT_ENV_FILE`、exe 同级、
上一级目录（`dist\` 的上一级就是仓库根目录）和当前目录的 `.env`；找不到 QMT 目录时
会尝试已运行的 QMT 进程。`-QmtMode login` 的账号密码沿用
`BIGQMT_LOGIN_USER` / `BIGQMT_LOGIN_PASSWORD`（环境变量或 `.env`），不会写进日志。

确认桥接输出 `QMT 串口会话已就绪`，然后用现有 VK 客户端执行 `ping`、`get_full_tick` 等只读请求。客户端仍配 `transport=valkey`，账号、VK 地址和数据库与桥接一致。桥接按 INFO 级别打印收到的每个 RPC 请求（`收到 RPC 请求 <request_id>：method=... params=...`，参数超过 400 字符会截断），参数里不要放凭据。

脚本会把窗口代码页、PowerShell 的解码编码和桥接进程的 stdio 统一为 UTF-8，并在每次调用外部程序后重新对齐代码页（pyenv 的 `.bat` shim 内部会执行 `chcp`，否则中文日志会乱码）。`-LogFile` 按 UTF-8 写入。直接运行入口而不经过脚本时，若日志中文乱码，可先 `chcp 65001` 并设置 `PYTHONIOENCODING=utf-8`。

QMT 策略默认关闭 `adjust 节拍`、`[adjust_source]` 和 `[tick_app_hist]` 的周期输出。在策略的 `BIGQMT_CONFIG` 中设 `"adjust_log_enabled": True` 开启，设 `"adjust_log_interval_seconds": 60` 可每隔约 60 秒打印一次。间隔默认 10 秒，须为正数。保存配置后重启策略。

默认使用 `bigqmt:rpc:queue:{account_id}` 及原有响应键、列表和频道；使用 VK pub/sub 发请求的客户端需给桥接加 `--vk-pubsub`。同一账号只能运行一个队列消费者，避免让直连 VK 的 QMT 策略与串口桥接同时消费。

串口每帧是 `8 位十六进制 CRC32` + `TAB` + `UTF-8 JSON` + `LF`。线程负责分帧和部分写入，QMT 的 `adjust` 从内存队列取请求；委托与成交回报由桥接发布到现有 VK 频道，行情全推也使用原频道。行情推送拥塞时会丢弃超出队列容量的更新，委托与成交回报使用独立队列。默认单帧上限 8 MiB，大行情按批查询，或者同时调整 QMT 的 `serial.max_frame_bytes` 与桥接的 `--max-frame-bytes`。延迟还取决于 QMT 调度间隔和串口传输量，请在目标终端实测。

一个串口会话服务一个账号。串口模式关闭 QMT 侧的 VK 和原生 `xtdata` 直连，快照缓存和异步历史下载由独立桥接进程管理。交易请求需使用更新后的 VK 客户端包：调用接口不变，请求会携带发送时间，让桥接拒绝积压到过期的委托。Windows 和客户端主机应同步时钟。

QMT 写超时时会中止当前帧的发送，并在同一串口会话中继续收发。该帧对应的客户端调用仍可能超时。若 COM 端口断开，请检查端口后重启受影响的进程。桥接不会自动重放未确认的请求。若交易状态不明，按备注查询委托和成交再决定是否重试。
