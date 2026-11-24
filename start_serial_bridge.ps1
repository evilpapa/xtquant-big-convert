#Requires -Version 5.1
<#
.SYNOPSIS
  启动 bigqmt-serial-bridge（Windows 串口 <-> Valkey 桥接），并按需拉起大 QMT。

.DESCRIPTION
  链路：QMT 模型交易(如 COM10) <-com0com-> 本脚本的串口(如 COM11) <-串口帧-> 桥接 <-Valkey-> 策略。

  优先使用当前目录 .env 中的账号和 Valkey 地址。未配置地址时读取
  bigqmt_signal_trader_qmt_config.py 中的 Valkey 配置。
  启动前预检 Valkey 连通性和串口能否打开；-CheckOnly 只预检不启动。

  默认在桥接串口打开后（日志出现“串口桥接已启动”）调用
  bigqmt_signal_trader.qmt_launcher 启动大 QMT，由 QMT 自己完成自动登录和
  自动运行策略。判断是否真正接通看桥接日志里的“QMT 串口会话已就绪”。
  QMT 目录默认读 .env 的 BIGQMT_QMT_DIR，其次用正在运行的 QMT 进程所在目录。

.PARAMETER ValkeyUrl
  valkey://[user:password@]host:port/db，也接受 redis://。
  优先级：本参数 > .env > -LocalConfig 的 BIGQMT_CONFIG > 环境变量。

.PARAMETER Account
  资金账号。默认读取 .env 或环境变量中的 BIGQMT_ACCOUNT_ID。

.PARAMETER Port
  桥接这一侧的串口，默认 COM11。

.PARAMETER Baudrate
  默认 921600，必须与 QMT 侧 serial.baudrate 一致。

.PARAMETER MaxFrameBytes
  单帧上限，默认 8388608，须与 QMT 侧 serial.max_frame_bytes 一致。

.PARAMETER LocalConfig
  QMT 侧本地配置文件；默认依次在脚本目录、脚本目录\src、当前目录查找。

.PARAMETER ValkeyPubsub
  客户端用 Valkey pub/sub 发请求时加上（等价 --valkey-pubsub）。

.PARAMETER LogFile
  输出同时追加写入该文件。

.PARAMETER CheckOnly
  只预检账号 / Valkey / 串口 / QMT 目录和运行状态，不启动桥接。

.PARAMETER Force
  预检失败时仍然启动（仅供确认桥接自身报错用，一般不要加）。

.PARAMETER BridgeExe
  指定入口程序（bigqmt-serial-bridge 或 python.exe）；默认自动查找。

.PARAMETER QmtDir
  大 QMT 安装目录（根目录、bin.x64 或 XtItClient.exe）。
  优先级：本参数 > .env 的 BIGQMT_QMT_DIR > 环境变量 > 正在运行的 QMT 进程目录。

.PARAMETER NoQmt
  只启动桥接，不启动 QMT（与旧版行为一致）。

.PARAMETER RestartQmt
  QMT 已在运行时先关闭它（qmt_launcher close），再随桥接一起重启。
  桥接重启后旧策略的串口会话不会自动恢复，通常需要加这个开关。

.PARAMETER QmtMode
  交给 qmt_launcher 的启动方式，默认 exe：直接运行 XtItClient.exe，依赖 QMT
  自身的自动登录和自动运行策略。login：由 qmt_launcher 在登录框里自动输入
  账号密码（需要 pywin32 和 pyautogui，账号密码从环境变量
  BIGQMT_LOGIN_USER / BIGQMT_LOGIN_PASSWORD 读取，缺失时从启动目录的 .env 读取）。

.PARAMETER QmtExe
  -QmtMode exe/login 启动的可执行文件名，默认 XtItClient.exe。

.EXAMPLE
  .\start_serial_bridge.ps1 -CheckOnly

.EXAMPLE
  .\start_serial_bridge.ps1

.EXAMPLE
  .\start_serial_bridge.ps1 -LogFile .\bridge.log

.EXAMPLE
  .\start_serial_bridge.ps1 -RestartQmt

.EXAMPLE
  .\start_serial_bridge.ps1 -NoQmt -Port COM11 -Baudrate 921600 `
      -ValkeyUrl valkey://192.168.2.112:6379/5 -ValkeyPubsub

.NOTES
  Ctrl+C 退出。桥接停止后 QMT 侧串口会话失效，脚本不会顺手关掉 QMT；
  下次启动请加 -RestartQmt，把 QMT 和桥接一起重启。
  已从 Valkey 取出的请求不会重放，重启前先查委托和成交。
  QMT 2.1.x 的 XtItClient.exe manifest 要求管理员权限；非管理员会话启动 QMT 会得到
  WinError 740，预检会拒绝启动。请用管理员 PowerShell 运行，或加 -NoQmt 只启动桥接。
#>
param(
    [string]$ValkeyUrl = "",
    [string]$Account = "",
    [string]$Port = "COM11",
    [int]$Baudrate = 921600,
    [int]$MaxFrameBytes = 8388608,
    [string]$LocalConfig = "",
    [switch]$ValkeyPubsub,
    [string]$LogFile = "",
    [switch]$CheckOnly,
    [switch]$Force,
    [string]$BridgeExe = "",
    [string]$QmtDir = "",
    [switch]$NoQmt,
    [switch]$RestartQmt,
    [ValidateSet("exe", "login")][string]$QmtMode = "exe",
    [string]$QmtExe = "XtItClient.exe"
)
$ErrorActionPreference = "Stop"

# ---- 0. 统一按 UTF-8 输出 ----------------------------------------------------
# PowerShell 用 [Console]::OutputEncoding 解码外部程序输出，而 Python 往管道写日志
# 时默认用系统 locale 编码（如 cp936），两者不一致就会把中文显示成乱码。
# 另外 pyenv 的 .bat shim 会执行 chcp（先 1250 再 65001），把窗口代码页从 936 改掉，
# 而 .NET 缓存的 [Console]::OutputEncoding 不会跟着变，于是 PowerShell 按 GBK 写、
# 控制台按 UTF-8 显示。这里统一成 UTF-8：控制台代码页、PowerShell 解码/输出和
# Python 的 stdio 都用 UTF-8，之后每次调用外部程序再把代码页对齐回来。
function Set-ConsoleCodePage([int]$CodePage) {
    if ([Console]::IsOutputRedirected) { return }   # 没有控制台可设置
    $comspec = if ($env:ComSpec) { $env:ComSpec } else { "cmd.exe" }
    try {
        # 用 chcp 而不是 [Console]::OutputEncoding：.NET 的设置器不会调用
        # SetConsoleOutputCP，改不了控制台真正使用的代码页。
        & $comspec /c "chcp $CodePage > NUL 2>&1" | Out-Null
    }
    catch { }
}

$script:OriginalOutputEncoding = [Console]::OutputEncoding
$script:OriginalCodePage = $script:OriginalOutputEncoding.CodePage
$script:OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$script:PythonIoEncoding = "utf-8"
try { [Console]::OutputEncoding = $script:OutputEncoding } catch { }
Set-ConsoleCodePage 65001

function Sync-ConsoleEncoding {
    # shim 可能又改了代码页，这里把控制台和 .NET 的编码对齐回 UTF-8。
    try { [Console]::OutputEncoding = $script:OutputEncoding } catch { }
    Set-ConsoleCodePage 65001
}

function Restore-ConsoleEncoding {
    try { [Console]::OutputEncoding = $script:OriginalOutputEncoding } catch { }
    Set-ConsoleCodePage $script:OriginalCodePage
}

function Exit-Script([int]$Code) {
    Restore-ConsoleEncoding
    exit $Code
}

function Say-Step([string]$m) { Write-Host ""; Write-Host ("== " + $m) -ForegroundColor Cyan }
function Say-Ok([string]$m) { Write-Host ("   [通过] " + $m) -ForegroundColor Green }
function Say-Warn([string]$m) { Write-Host ("   [warn] " + $m) -ForegroundColor Yellow }
function Say-Fail([string]$m) { Write-Host ("   [fail] " + $m) -ForegroundColor Red }
function Say-Detail([string]$m) { Write-Host ("          " + $m) -ForegroundColor DarkGray }

function Say-Probe([string[]]$Lines) {
    # python 的 traceback 太长，默认只留最后几行；-Verbose 时全打
    $clean = @($Lines | Where-Object { $_ -and $_.Trim() -ne "System.Management.Automation.RemoteException" })
    if ($VerbosePreference -eq "SilentlyContinue" -and $clean.Count -gt 3) {
        $clean = $clean[($clean.Count - 3)..($clean.Count - 1)]
        Say-Detail "(完整回溯用 -Verbose 查看)"
    }
    foreach ($line in $clean) { Say-Detail $line }
}

# ---- 1. 读取 QMT 侧本地配置 -------------------------------------------------
function Find-LocalConfig([string]$Explicit) {
    if ($Explicit) {
        if (-not (Test-Path -LiteralPath $Explicit)) { throw ("找不到 -LocalConfig: " + $Explicit) }
        return (Resolve-Path -LiteralPath $Explicit).Path
    }
    $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
    foreach ($candidate in @(
            (Join-Path $scriptDir "bigqmt_signal_trader_qmt_config.py"),
            (Join-Path $scriptDir "src\bigqmt_signal_trader_qmt_config.py"),
            (Join-Path (Get-Location).Path "bigqmt_signal_trader_qmt_config.py"))) {
        if (Test-Path -LiteralPath $candidate) { return (Resolve-Path -LiteralPath $candidate).Path }
    }
    return ""
}

function Get-ValkeyConfigBlock([string]$Text) {
    if (-not $Text) { return "" }
    $start = $Text.IndexOf("BIGQMT_CONFIG")
    if ($start -lt 0) { $start = $Text.IndexOf("BIGQMT_CONFIG") }
    if ($start -lt 0) { return "" }
    $open = $Text.IndexOf("{", $start)
    if ($open -lt 0) { return "" }
    $depth = 0
    for ($i = $open; $i -lt $Text.Length; $i++) {
        $ch = $Text[$i]
        if ($ch -eq "{") { $depth++ }
        elseif ($ch -eq "}") {
            $depth--
            if ($depth -eq 0) { return $Text.Substring($open, $i - $open + 1) }
        }
    }
    return ""
}

function Get-ConfigValue([string]$Block, [string]$Key) {
    if (-not $Block) { return "" }
    $pattern = "(?m)^\s*['""]?" + [regex]::Escape($Key) + "['""]?\s*:\s*(?<value>[^,#\r\n]*)"
    $m = [regex]::Match($Block, $pattern)
    if (-not $m.Success) { return "" }
    return $m.Groups["value"].Value.Trim().Trim([char]0x22, [char]0x27).Trim()
}

function New-ValkeyUrlFromBlock([string]$Block) {
    $url = Get-ConfigValue $Block "url"
    if ($url) { return $url }
    $h = Get-ConfigValue $Block "host"
    if (-not $h) { return "" }
    $p = Get-ConfigValue $Block "port"; if (-not $p) { $p = "6379" }
    $d = Get-ConfigValue $Block "db"; if (-not $d) { $d = "0" }
    $u = Get-ConfigValue $Block "username"
    $w = Get-ConfigValue $Block "password"
    $auth = ""
    if ($w) {
        if ($u) { $auth = [uri]::EscapeDataString($u) + ":" + [uri]::EscapeDataString($w) + "@" }
        else { $auth = ":" + [uri]::EscapeDataString($w) + "@" }
    }
    return ("valkey://" + $auth + $h + ":" + $p + "/" + $d)
}

function Format-ValkeyUrl([string]$Url) {
    # 日志里隐藏用户名/密码
    return [regex]::Replace($Url, "//[^/@]*@", "//***@")
}

# ---- 2. 解析启动入口 --------------------------------------------------------
function Resolve-BridgeEntry([string]$Explicit) {
    if ($Explicit) {
        $leaf = [System.IO.Path]::GetFileName($Explicit)
        if ($leaf -match "^pythonw?([0-9]+(\.[0-9]+)?)?\.exe$") {
            return @{ Exe = $Explicit; Prefix = @("-m", "bigqmt_signal_trader.serial_valkey_bridge") }
        }
        return @{ Exe = $Explicit; Prefix = @() }
    }
    $cmd = Get-Command "bigqmt-serial-bridge" -ErrorAction SilentlyContinue
    if ($cmd) { return @{ Exe = $cmd.Source; Prefix = @() } }
    $py = Get-Command "python" -ErrorAction SilentlyContinue
    if ($py) { return @{ Exe = $py.Source; Prefix = @("-m", "bigqmt_signal_trader.serial_valkey_bridge") } }
    throw "找不到 bigqmt-serial-bridge，也没有 python。请先 pip install -e . 或 pip install `"xtquant-big-convert[valkey,serial]`"。"
}

function Resolve-Python($Entry) {
    if ($Entry.Prefix.Count -gt 0 -and $Entry.Prefix[0] -eq "-m") { return $Entry.Exe }
    if ([System.IO.Path]::GetFileName($Entry.Exe) -match "^pythonw?([0-9]+(\.[0-9]+)?)?\.exe$") { return $Entry.Exe }
    $py = Get-Command "python" -ErrorAction SilentlyContinue
    if ($py) { return $py.Source }
    return ""
}

function Invoke-Capture([string]$Exe, [string[]]$Arguments) {
    # 5.1 里 native 命令的 stderr 重定向会生成 ErrorRecord，EAP=Stop 会把整段语句打断。
    $previous = $ErrorActionPreference
    $previousIoEncoding = $env:PYTHONIOENCODING
    $ErrorActionPreference = "Continue"
    $env:PYTHONIOENCODING = $script:PythonIoEncoding
    try {
        $output = & $Exe @Arguments 2>&1 | ForEach-Object { [string]$_ }
        return @{ Code = $LASTEXITCODE; Output = @($output) }
    }
    finally {
        $ErrorActionPreference = $previous
        if ($null -eq $previousIoEncoding) {
            Remove-Item Env:\PYTHONIOENCODING -ErrorAction SilentlyContinue
        }
        else {
            $env:PYTHONIOENCODING = $previousIoEncoding
        }
        Sync-ConsoleEncoding
    }
}

# ---- 3. 大 QMT 终端 ---------------------------------------------------------
function Resolve-QmtBinDir([string]$Path) {
    # 接受安装根目录、bin.x64 或其中的 exe；统一返回 bin.x64 目录。
    if (-not $Path) { return "" }
    $p = $Path.Trim().Trim([char]0x22, [char]0x27)
    if (Test-Path -LiteralPath $p -PathType Leaf) { $p = Split-Path -Parent $p }
    if (-not (Test-Path -LiteralPath $p -PathType Container)) { return "" }
    $p = (Resolve-Path -LiteralPath $p).Path
    if ((Split-Path -Leaf $p) -ne "bin.x64") {
        $candidate = Join-Path $p "bin.x64"
        if (Test-Path -LiteralPath $candidate -PathType Container) {
            $p = (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    return $p
}

function Find-RunningQmtBinDir {
    # 本机可能并存多个 QMT 安装，只把正在运行的那个当作默认值。
    $names = @("XtItClient.exe", "XtMiniQmt.exe")
    $found = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $names -contains $_.Name -and $_.ExecutablePath })
    if ($found.Count -eq 0) { return "" }
    return (Split-Path -Parent $found[0].ExecutablePath)
}

function Test-QmtRunning([string]$BinDir) {
    # 只看指定安装目录，避免同机其他账号的 QMT 干扰判断。
    if (-not $BinDir) { return $false }
    $names = @("XtItClient.exe", "XtMiniQmt.exe", "miniquote.exe", "BrokerProxy.exe", "minibroker.exe")
    $found = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $names -contains $_.Name -and $_.ExecutablePath -and
            ((Split-Path -Parent $_.ExecutablePath) -eq $BinDir)
        })
    return ($found.Count -gt 0)
}

function Invoke-QmtLauncher([string]$Python, [string[]]$Arguments) {
    return Invoke-Capture $Python (@("-m", "bigqmt_signal_trader.qmt_launcher") + $Arguments)
}

function Test-IsAdmin {
    return ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
            ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-RequiresElevation([string]$ExePath) {
    # XtItClient.exe 的 manifest 是 requireAdministrator；非提升进程启动会得到 WinError 740。
    if (-not (Test-Path -LiteralPath $ExePath -PathType Leaf)) { return $false }
    try {
        $stream = [System.IO.File]::OpenRead($ExePath)
        try {
            $buffer = New-Object byte[] (2 * 1024 * 1024)
            $carry = ""
            while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                $text = $carry + [System.Text.Encoding]::ASCII.GetString($buffer, 0, $count)
                if ($text.IndexOf("requireAdministrator", [System.StringComparison]::Ordinal) -ge 0) {
                    return $true
                }
                $carry = $text.Substring([Math]::Max(0, $text.Length - 32))
            }
        }
        finally { $stream.Dispose() }
    }
    catch { }
    return $false
}

# ---- 4. 解析参数 ------------------------------------------------------------
Say-Step "配置"

$entry = Resolve-BridgeEntry $BridgeExe
$pyExe = Resolve-Python $entry
$dotenv = $null
if ((-not $ValkeyUrl -or [string]::IsNullOrWhiteSpace($Account) -or -not $QmtDir -or $QmtMode -eq "login") -and
        (Test-Path -LiteralPath ".env" -PathType Leaf)) {
    if (-not $pyExe) { throw "读取 .env 需要外部 Python，请检查 PATH 或指定 -BridgeExe。" }
    $result = Invoke-Capture $pyExe @("-c", "import json; from bigqmt_signal_trader.local_env import local_serial_settings; print(json.dumps(local_serial_settings()))")
    if ($result.Code -ne 0) { throw "无法读取 .env，请确认同一 Python 环境已安装最新版 xtquant-big-convert。" }
    $dotenv = $result.Output[-1] | ConvertFrom-Json
}

$configPath = Find-LocalConfig $LocalConfig
$configText = ""
$valkeyBlock = ""
$configTransport = ""
if ($configPath) {
    $configText = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8
    $valkeyBlock = Get-ValkeyConfigBlock $configText
    $configTransport = Get-ConfigValue $valkeyBlock "transport"
}

$valkeySource = "命令行 -ValkeyUrl"
if (-not $ValkeyUrl -and $dotenv.valkey_url) {
    $ValkeyUrl = $dotenv.valkey_url
    $valkeySource = ".env"
}
if (-not $ValkeyUrl) {
    $ValkeyUrl = New-ValkeyUrlFromBlock $valkeyBlock
    if ($ValkeyUrl) { $valkeySource = "本地配置" }
}
if (-not $ValkeyUrl -and $env:BIGQMT_SERIAL_VALKEY_URL) {
    $ValkeyUrl = $env:BIGQMT_SERIAL_VALKEY_URL
    $valkeySource = "环境变量 BIGQMT_SERIAL_VALKEY_URL"
}
if (-not $ValkeyUrl -and $env:BIGQMT_VALKEY_URL) {
    $ValkeyUrl = $env:BIGQMT_VALKEY_URL
    $valkeySource = "环境变量 BIGQMT_VALKEY_URL"
}
if (-not $ValkeyUrl) {
    $ValkeyUrl = "valkey://127.0.0.1:6379/5"
    $valkeySource = "内置默认值（未找到 QMT 侧配置）"
}

$accountSource = "命令行 -Account"
if ([string]::IsNullOrWhiteSpace($Account) -and $dotenv.account_id) {
    $Account = $dotenv.account_id
    $accountSource = ".env"
}
if ([string]::IsNullOrWhiteSpace($Account)) {
    $Account = $env:BIGQMT_ACCOUNT_ID
    $accountSource = "环境变量 BIGQMT_ACCOUNT_ID"
}
if ([string]::IsNullOrWhiteSpace($Account)) {
    Write-Warning "缺少资金账号：请在 .env 或环境变量中设置 BIGQMT_ACCOUNT_ID，或传入 -Account。"
    Exit-Script 1
}
$Account = $Account.Trim()

# QMT 终端：-NoQmt 时完全不碰；否则先确定安装目录，桥接串口就绪后再启动。
$qmtBinDir = ""
$qmtExePath = ""
$qmtSource = ""
$qmtAction = "off"
$qmtEnabled = -not $NoQmt
if ($qmtEnabled) {
    $qmtCandidate = $QmtDir.Trim()
    $qmtSource = "命令行 -QmtDir"
    if (-not $qmtCandidate -and $dotenv.qmt_dir) {
        $qmtCandidate = [string]$dotenv.qmt_dir
        $qmtSource = ".env BIGQMT_QMT_DIR"
    }
    if (-not $qmtCandidate -and $env:BIGQMT_QMT_DIR) {
        $qmtCandidate = $env:BIGQMT_QMT_DIR
        $qmtSource = "环境变量 BIGQMT_QMT_DIR"
    }
    if (-not $qmtCandidate) {
        $qmtCandidate = Find-RunningQmtBinDir
        if ($qmtCandidate) { $qmtSource = "正在运行的 QMT 进程" }
    }
    if (-not $qmtCandidate) {
        Say-Warn "找不到大 QMT 安装目录：本次只启动桥接；需要一起启动请在 .env 设置 BIGQMT_QMT_DIR 或传入 -QmtDir。"
        $qmtEnabled = $false
    }
    else {
        $qmtBinDir = Resolve-QmtBinDir $qmtCandidate
        if (-not $qmtBinDir) {
            Say-Warn ("QMT 目录无效：" + $qmtCandidate + "（" + $qmtSource + "）：本次只启动桥接。")
            $qmtEnabled = $false
        }
        else {
            $qmtExePath = Join-Path $qmtBinDir $QmtExe
            if (-not (Test-Path -LiteralPath $qmtExePath -PathType Leaf)) {
                Say-Warn ("在 " + $qmtBinDir + " 下找不到 " + $QmtExe +
                    "：本次只启动桥接；可用 -QmtDir / -QmtExe 指定。")
                $qmtEnabled = $false
            }
        }
    }
}

Write-Host ("   账号      : " + $Account + "   (" + $accountSource + ")")
Write-Host ("   串口      : " + $Port + " @ " + $Baudrate + " baud, 帧上限 " + $MaxFrameBytes)
Write-Host ("   Valkey     : " + (Format-ValkeyUrl $ValkeyUrl) + "   (" + $valkeySource + ")")
Write-Host ("   本地配置  : " + $(if ($configPath) { $configPath } else { "未找到（可用 -LocalConfig 指定）" }))
Write-Host ("   启动入口  : " + $entry.Exe + " " + (@($entry.Prefix) -join " "))
if ($NoQmt) {
    Write-Host "   QMT 终端  : 未启用（-NoQmt）"
}
elseif (-not $qmtEnabled) {
    Write-Host "   QMT 终端  : 未找到（本次只启动桥接）"
}
else {
    Write-Host ("   QMT 终端  : " + $qmtExePath + "   (" + $qmtSource + "，-QmtMode " + $QmtMode + ")")
}

if ($configTransport -and $configTransport -ne "serial") {
    Say-Warn ('本地配置里的 transport 是 "' + $configTransport + '"，串口桥接需要 QMT 侧 BIGQMT_CONFIG 写 "transport": "serial" 并配好 "serial": {"port": "COM10", "baudrate": 921600}。')
}
elseif ($configPath -and -not $configTransport) {
    Say-Warn '本地配置里没有 "transport": "serial"，QMT 侧默认仍走 Valkey，不会用到本桥接。'
}

# ---- 5. 预检 ----------------------------------------------------------------
Say-Step "预检"
$preflightOk = $true

if (-not $pyExe) {
    Say-Warn "找不到 python，跳过 Valkey/串口预检（桥接启动时会再检查一遍）。"
}
else {
    $env:BIGQMT_PREFLIGHT_VALKEY_URL = $ValkeyUrl
    $probe = Invoke-Capture $pyExe @("-c", "import os; from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client; build_valkey_client(dict(url=os.environ['BIGQMT_PREFLIGHT_VALKEY_URL'], socket_timeout=2, socket_connect_timeout=2)).ping(); print('ok')")
    if ($probe.Code -eq 0) {
        Say-Ok ("Valkey 可达: " + (Format-ValkeyUrl $ValkeyUrl))
    }
    else {
        $preflightOk = $false
        Say-Fail ("Valkey 不可达: " + (Format-ValkeyUrl $ValkeyUrl))
        Say-Probe $probe.Output
        Say-Detail "桥接默认连 valkey://127.0.0.1:6379/5，请用 -ValkeyUrl 或 BIGQMT_SERIAL_VALKEY_URL 指向 QMT 策略用的同一个 Valkey 和 db。"
        if (($probe.Output -join "`n") -match "No module named 'valkey'") {
            Say-Detail '缺少 valkey 客户端：pip install "xtquant-big-convert[valkey,serial]"'
        }
    }

    $env:BIGQMT_PREFLIGHT_PORT = $Port
    $env:BIGQMT_PREFLIGHT_BAUD = [string]$Baudrate
    $probe = Invoke-Capture $pyExe @("-c", "import os, serial; p = serial.Serial(port=os.environ['BIGQMT_PREFLIGHT_PORT'], baudrate=int(os.environ['BIGQMT_PREFLIGHT_BAUD']), timeout=0, write_timeout=0.1); p.close(); print('ok')")
    if ($probe.Code -eq 0) {
        Say-Ok ("串口可打开: " + $Port)
    }
    else {
        $preflightOk = $false
        Say-Fail ("串口打不开: " + $Port)
        Say-Probe $probe.Output
        Say-Detail ("确认 com0com 已建立端口对（如 COM10 <-> COM11），端口名拼写正确，且 QMT 或另一个桥接没有占用 " + $Port + " 这一端。")
        if (($probe.Output -join "`n") -match "No module named 'serial'") {
            Say-Detail '缺少 pyserial：pip install "xtquant-big-convert[valkey,serial]"'
        }
    }

    Remove-Item Env:\BIGQMT_PREFLIGHT_VALKEY_URL -ErrorAction SilentlyContinue
    Remove-Item Env:\BIGQMT_PREFLIGHT_PORT -ErrorAction SilentlyContinue
    Remove-Item Env:\BIGQMT_PREFLIGHT_BAUD -ErrorAction SilentlyContinue
}

if ($qmtEnabled) {
    if (-not $pyExe) {
        $preflightOk = $false
        Say-Fail "启动 QMT 需要 Python 运行 qmt_launcher，但 PATH 中找不到 python。请用 -BridgeExe 指定，或加 -NoQmt。"
    }
    else {
        $status = Invoke-QmtLauncher $pyExe @("status", "--dir", $qmtBinDir)
        $statusText = ($status.Output -join "`n")
        if ($statusText -match "Traceback|No module named") {
            $preflightOk = $false
            Say-Fail "无法调用 qmt_launcher（同一 Python 环境需要安装 xtquant-big-convert）"
            Say-Probe $status.Output
            Say-Detail "只启动桥接可加 -NoQmt。"
        }
        elseif ($status.Code -eq 0) {
            if ($RestartQmt) {
                $qmtAction = "restart"
                Say-Ok "QMT 正在运行，启动前会先关闭（-RestartQmt）"
            }
            else {
                $qmtAction = "keep"
                Say-Warn "QMT 已在运行：旧策略的串口会话不会自动恢复；需要一起重启请加 -RestartQmt。"
            }
        }
        elseif ($status.Code -eq 1) {
            $qmtAction = "launch"
            Say-Ok ("QMT 未运行，桥接串口就绪后自动启动 " + $QmtExe)
        }
        else {
            $preflightOk = $false
            Say-Fail ("无法查询 QMT 进程状态，exit=" + $status.Code)
            Say-Probe $status.Output
            Say-Detail "可 pip install psutil（qmt_launcher 优先用它枚举进程），或只启动桥接加 -NoQmt。"
        }
    }
}

if ($qmtEnabled -and ($qmtAction -eq "launch" -or $qmtAction -eq "restart") -and
        -not (Test-IsAdmin) -and (Test-RequiresElevation $qmtExePath)) {
    $preflightOk = $false
    Say-Fail "XtItClient.exe 的 manifest 要求管理员权限（WinError 740）；当前 PowerShell 不是管理员"
    Say-Detail "请以管理员身份运行 PowerShell，或加 -NoQmt 只启动桥接；GUI 版会自动提示提升权限。"
}

if ($qmtEnabled -and $QmtMode -eq "login") {
    # qmt_launcher 优先读环境变量，缺失时自己读启动目录的 .env；这里只做预检，不打印值。
    $loginUser = if ($env:BIGQMT_LOGIN_USER) { $env:BIGQMT_LOGIN_USER } else { [string]$dotenv.login_user }
    $loginPassword = if ($env:BIGQMT_LOGIN_PASSWORD) { $env:BIGQMT_LOGIN_PASSWORD } else { [string]$dotenv.login_password }
    if (-not $loginUser -or -not $loginPassword) {
        $preflightOk = $false
        Say-Fail "QmtMode login 缺少登录凭据"
        Say-Detail "在 .env（启动目录）或环境变量设置 BIGQMT_LOGIN_USER / BIGQMT_LOGIN_PASSWORD；也可改用 -QmtMode exe 依赖 QMT 自己的自动登录。"
    }
    else {
        Say-Ok "登录凭据已配置（环境变量或 .env，不显示）"
    }
    if ($pyExe) {
        $probe = Invoke-Capture $pyExe @("-c", "import win32gui, pyautogui")
        if ($probe.Code -eq 0) {
            Say-Ok "login 所需的 pywin32 / pyautogui 可用"
        }
        else {
            $preflightOk = $false
            Say-Fail "QmtMode login 需要 pywin32 和 pyautogui"
            Say-Probe $probe.Output
            Say-Detail "在桥接的 Python 环境安装：pip install pywin32 pyautogui"
        }
    }
}

if ($CheckOnly) {
    Write-Host ""
    if ($preflightOk) { Say-Ok "预检通过（-CheckOnly，未启动桥接）。"; Exit-Script 0 }
    Say-Fail "预检未通过（-CheckOnly，未启动桥接）。"
    Exit-Script 1
}

if (-not $preflightOk -and -not $Force) {
    Write-Host ""
    Say-Fail "预检未通过，已放弃启动（确认无误可加 -Force 强行启动）。"
    Exit-Script 1
}

# ---- 6. 启动 ----------------------------------------------------------------
if ($qmtAction -eq "restart") {
    Say-Step "关闭正在运行的 QMT"
    $closed = Invoke-QmtLauncher $pyExe @("close", "--dir", $qmtBinDir)
    if ($closed.Code -ne 0) {
        Say-Fail ("关闭 QMT 失败，exit=" + $closed.Code + "。")
        Say-Probe $closed.Output
        if (-not $Force) {
            Say-Detail "未启动桥接。确认可以强行继续时加 -Force，或先手动关闭 QMT。"
            Exit-Script 1
        }
        Say-Warn "继续启动（-Force），但旧 QMT 进程可能仍占用端口。"
    }
    else {
        $lastLine = @($closed.Output | Select-Object -Last 1)
        Say-Ok $(if ($lastLine.Count) { [string]$lastLine[0] } else { "QMT 已关闭" })
    }
}

Say-Step "启动 bigqmt-serial-bridge"

$bridgeArgs = @($entry.Prefix) + @(
    "--port", $Port,
    "--baudrate", [string]$Baudrate,
    "--account-id", $Account,
    "--max-frame-bytes", [string]$MaxFrameBytes,
    "--valkey-url", $ValkeyUrl
)
if ($ValkeyPubsub) { $bridgeArgs += "--valkey-pubsub" }

$printed = (@($entry.Exe) + $bridgeArgs) -join " "
if ($ValkeyUrl -ne (Format-ValkeyUrl $ValkeyUrl)) { $printed = $printed.Replace($ValkeyUrl, (Format-ValkeyUrl $ValkeyUrl)) }
Write-Host ("   " + $printed)
Write-Host "   Ctrl+C 停止。" -ForegroundColor DarkGray
Write-Host ""

# 同一个 Valkey 也通过环境变量传一份，方便子进程/后续排错保持一致
$env:BIGQMT_SERIAL_VALKEY_URL = $ValkeyUrl
$env:PYTHONUNBUFFERED = "1"

if ($LogFile) {
    $logDir = Split-Path -Parent $LogFile
    if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
        New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    }
    $header = "# " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " 启动 " + $printed
    Add-Content -LiteralPath $LogFile -Value $header -Encoding UTF8
}

# 原生程序的 stderr 在 5.1 里会变成 ErrorRecord；这里逐行转成字符串再输出，
# 避免 Tee-Object 把整条 "NativeCommandError" 和 UTF-16 编码写进日志。
$previousIoEncoding = $env:PYTHONIOENCODING
$env:PYTHONIOENCODING = $script:PythonIoEncoding
$ErrorActionPreference = "Continue"
$qmtLaunched = $false
$qmtStartupSeen = $false
$qmtSessionReady = $false
try {
    $consoleSynced = $false
    & $entry.Exe @bridgeArgs 2>&1 | ForEach-Object {
        $line = [string]$_
        if (-not $consoleSynced) {
            # 入口可能是 pyenv 的 .bat shim（内部会 chcp），首行输出时控制台
            # 代码页已经变了，先对齐再显示。
            $consoleSynced = $true
            Sync-ConsoleEncoding
        }
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            Write-Host $line -ForegroundColor Red
        }
        else {
            Write-Host $line
        }
        if ($LogFile) { Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8 }

        # 桥接在打开串口之后才打印这一行，此时再启动 QMT，避免策略先开串口。
        if (-not $qmtStartupSeen -and $line -match "串口桥接已启动") {
            $qmtStartupSeen = $true
            if ($qmtAction -eq "launch" -or $qmtAction -eq "restart") {
                $qmtLaunched = $true
                Write-Host ""
                Say-Step ("启动大 QMT（" + $QmtMode + "）")
                $qmtArgs = @("open",
                    "--dir", $qmtBinDir,
                    "--mode", $QmtMode,
                    "--exe", $QmtExe,
                    "--no-wait")
                $launch = Invoke-QmtLauncher $pyExe $qmtArgs
                if ($launch.Code -eq 0) {
                    Say-Ok ("已启动 " + $QmtExe + "（" + $qmtBinDir + "），等待 QMT 自动登录并运行策略")
                    Say-Detail "看到「QMT 串口会话已就绪」即表示 QMT 策略已连上桥接。"
                }
                else {
                    Say-Fail ("启动 QMT 失败，exit=" + $launch.Code)
                    Say-Probe $launch.Output
                    Say-Detail "桥接继续运行；可手动启动 QMT 后查看串口会话日志。"
                }
            }
        }
        if (-not $qmtSessionReady -and $line -match "QMT 串口会话已就绪") {
            $qmtSessionReady = $true
            Say-Ok "QMT 策略已连上串口会话"
        }
    }
    $code = $LASTEXITCODE
}
finally {
    if ($null -eq $previousIoEncoding) {
        Remove-Item Env:\PYTHONIOENCODING -ErrorAction SilentlyContinue
    }
    else {
        $env:PYTHONIOENCODING = $previousIoEncoding
    }
    Sync-ConsoleEncoding
}
if ($null -eq $code) { $code = 0 }

Write-Host ""
if ($qmtAction -ne "off" -and -not $qmtSessionReady) {
    Say-Warn "本次没有看到「QMT 串口会话已就绪」：确认 QMT 已登录并运行了串口策略；下次可加 -RestartQmt 一起重启。"
}
if ($qmtLaunched) {
    if (Test-QmtRunning $qmtBinDir) {
        Say-Detail ("QMT 仍在运行（" + $qmtBinDir + "）。桥接退出后它的串口会话已失效，下次用 -RestartQmt 重启。")
    }
    else {
        Say-Detail ("QMT 已退出（" + $qmtBinDir + "）。")
    }
}
if ($code -eq 0) {
    Say-Ok "桥接已退出（Ctrl+C）。QMT 侧串口会话已失效，下次启动桥接后要重启 QMT 策略。"
}
else {
    Say-Fail ("桥接异常退出，exit=" + $code + "。对照上面的 ERROR 行：")
    Say-Detail "无法连接 Valkey：请检查地址、端口、数据库、服务状态和防火墙。"
    Say-Detail "无法打开串口：请检查串口名称、端口占用及 com0com 端口对。"
    Say-Detail "桥接已停止：请检查串口和 Valkey，查询委托及成交后再重启桥接和 QMT 策略。"
}
Exit-Script $code
