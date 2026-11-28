# 独立运行的串口与 VK 桥接

## 通过 com0com 接回 VK

桥接默认读取 `BIGQMT_ACCOUNT_ID`（须与 QMT 端一致）；VK 连接串通过 `BIGQMT_SERIAL_VK_URL` 配置（默认 `valkey://127.0.0.1:6379/5`）：

```powershell
$env:BIGQMT_ACCOUNT_ID = 'YOUR_ACCOUNT_ID'
bigqmt-serial-bridge --port COM11 --baudrate 921600
```

仓库根目录的 `start_serial_bridge.ps1` 会从 `bigqmt_signal_trader_qmt_config.py` 读取 VK 配置，并在启动前预检 VK 连通性与串口：

```powershell
.\start_serial_bridge.ps1 -CheckOnly                    # 仅预检（含 QMT 运行状态）
.\start_serial_bridge.ps1                               # 启动并控制台输出日志
.\start_serial_bridge.ps1 -LogFile .\bridge.log -VkPubSub
```

- **编码**：脚本会将控制台代码页、PowerShell 解码及桥接 stdio 统一为 UTF-8（含外部程序调用后重新对齐，防 pyenv `.bat` shim 改代码页），`-LogFile` 按 UTF-8 写入。直接运行命令若中文乱码，先执行 `chcp 65001` 并设 `PYTHONIOENCODING=utf-8`。
- **联调验证**：确认桥接输出 `QMT 串口会话已就绪` 后，使用 `transport=valkey` 的客户端发 `ping` / `get_full_tick` 验证。桥接以 INFO 级别打印收到的 RPC 请求（超 400 字符截断），请勿在参数中放凭据。

### 桥接和 QMT 一起启动

QMT 配好自动登录与自动运行策略后，脚本可在串口就绪后调用 `python -m bigqmt_signal_trader.qmt_launcher open --mode exe --no-wait` 拉起大 QMT（不依赖 FormulaServer 58600 端口）：

```powershell
# 可在 .env 中设置 BIGQMT_QMT_DIR=D:/gjzq_qmt_ceshi_new
.\start_serial_bridge.ps1             # 先开串口桥接，再拉起 QMT
.\start_serial_bridge.ps1 -RestartQmt # 先关已有 QMT 再一起重启
.\start_serial_bridge.ps1 -NoQmt      # 仅启动桥接
```

- **目录优先级**：`-QmtDir` > `.env` 的 `BIGQMT_QMT_DIR` > 环境变量 > 当前运行 QMT 进程目录；只操作该目录下的 QMT 进程，避免误关同机其他终端。
- **代输登录**：传 `-QmtMode login`（需 `pywin32` + `pyautogui` 且桌面未锁屏），凭据优先取进程环境 `BIGQMT_LOGIN_USER` / `BIGQMT_LOGIN_PASSWORD`，缺失时读启动目录 `.env`（不进命令行和日志）。
- **权限与退出**：若 `XtItClient.exe` 需要管理员权限，请以管理员身份运行脚本；`Ctrl+C` 只停桥接不关 QMT，下次联动请加 `-RestartQmt`。

### 图形控制台 exe

`tools/serial_bridge_gui.py` 可打包为单文件 `dist/BigQMTSerialController.exe`，提供「启动 / 停止 / 重启」及两个日志页签：`serial_vk_bridge 日志`（实时 tail）和 `QMT 策略日志（FormulaOutput）`（自动跨天切换 `<QMT>\userdata\log\XtClient_FormulaOutput_YYYYMMDD.log`，按行兼容 UTF-8/GBK 混排与截断多字节字符）。

```powershell
python tools/serial_bridge_gui.py     # 源码运行
python -m pip install pyinstaller
python tools/build_gui_exe.py         # 构建 dist\BigQMTSerialController.exe
```

- **运行环境**：exe 仅为前端，实际由外部 Python（优先级：`--python` / `BIGQMT_PYTHON` / `PATH` 中的 `python`，须安装 `xtquant-big-convert[valkey,serial]`）执行桥接与 `qmt_launcher`；`.env` 按 `--env-file`、`BIGQMT_ENV_FILE`、exe 同级、上一级目录、当前目录顺序查找。
- **启停与守护**：
  - 默认「启动时重启已运行的 QMT」关闭、「停止/退出时关闭 QMT」开启，操作限定在配置目录内。
  - **QMT 守护**（默认开）：主进程（文件名取 `BIGQMT_QMT_EXE`）持续缺失 8 秒后清理残留子进程（如 `miniquote` / `BrokerProxy`）并自动拉起；10 分钟内连续失败 6 次暂停提示。手动停止/重启/退出不触发。
  - **桥接守护**：端口打开后每 20 秒巡检桥接子进程，异常退出时自动拉起（不中断 QMT），恢复以 `QMT 串口会话已就绪` 为准。
  - **串口冲突与提权**：启动检测到串口被旧桥接占用会弹窗确认清理；「停止」也会清理占用同一串口的其他桥接进程。检测到 `XtItClient.exe` manifest 含 `requireAdministrator`（避免 `WinError 740`）时提示以管理员身份带 `--autostart` 重启。
- **设置面板**：右上角「设置…」可改串口（端口/波特率/单帧上限/VK pub/sub）、账户（资金账号/登录账密）、QMT（目录/`exe` 或 `login`/exe 名）和 VK（地址/端口/DB/账密/桥接命令超时），保留注释写回 `.env`（同步写 `BIGQMT_SERIAL_VK_URL` 与 `BIGQMT_VK_*`），运行中会询问是否重启生效。

## 协议与运行约束

- **帧与队列**：串口帧格式为 `8 位十六进制 CRC32` + `TAB` + `UTF-8 JSON` + `LF`，默认单帧上限 8 MiB（调大需同步改 QMT `serial.max_frame_bytes` 与桥接 `--max-frame-bytes`）。默认沿用 `bigqmt:rpc:queue:{account_id}` 与原有回调/行情频道（VK pub/sub 发请求的客户端需给桥接加 `--vk-pubsub`）。行情推送拥塞时会丢弃超容量更新，委托/成交回报走独立队列。
- **单账号与防冲突**：一个串口会话服务一个账号，同一账号只允许一个队列消费者（勿与直连 VK 策略并跑）。串口模式下关闭 QMT 侧 VK 和原生 `xtdata` 直连，快照缓存与异步下载由桥接管理；交易请求携带发送时间戳以便桥接拒收过期积压委托，两端主机应同步时钟。
- **策略周期日志**：QMT 策略默认关闭 `adjust 节拍` / `[adjust_source]` / `[tick_app_hist]` 周期日志；可在 `BIGQMT_CONFIG` 中设 `"adjust_log_enabled": True` 与 `"adjust_log_interval_seconds": 60`（默认 10 秒，正数）后重启开启。
- **串口写超时与断开**：单帧写超时会中止当前帧并继续会话（客户端可能超时）；连续写超时约 30 次且无对端数据时，QMT 会提示「桥接进程可能已停止或未读取串口」。COM 断开需检查端口后重启受影响进程；桥接不自动重放未确认请求，交易状态不明请先按备注查委托/成交。

## VK 闪断与超时

- **自动退避恢复**：VK 命令超时或闪断时，桥接不会退出，而是暂停拉取新请求、保持串口收发，并按 1s→2s→4s…（上限 10s）退避重试（日志打印 `VK … 暂时不可用` / `已恢复`）。故障期间无法发布的串口应答会被丢弃并记日志。
- **单条命令超时**：默认 5 秒（范围 `(0, 60]`，支持小数），可通过 GUI「设置… → VK → 桥接命令超时（秒）」（`.env` 中 `BIGQMT_SERIAL_VK_TIMEOUT`）、`--vk-timeout` 或 `-VkTimeout` 配置，优先级高于 VK URL 中的 `socket_timeout`。
- **网络建议与 exe 同步**：WiFi 偶发 >2s 抖动易引发超时，建议改用有线或关闭无线节能。使用 `dist/BigQMTSerialController.exe` 时须随源码重新执行 `python tools/build_gui_exe.py`，避免旧版 exe 缺少桥接自动拉起能力。
