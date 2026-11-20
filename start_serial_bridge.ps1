#Requires -Version 5.1
<#
.SYNOPSIS
  启动 bigqmt-serial-bridge：Windows 串口 <-> Redis 桥接进程。

.DESCRIPTION
  链路：QMT 模型交易(如 COM10) <-com0com-> 本脚本的串口(如 COM11) <-串口帧-> 桥接 <-Redis-> 策略。

  默认从 bigqmt_signal_trader_local_config.py 读取 QMT 侧在用的 Redis 地址，
  避免桥接连到 127.0.0.1 而 QMT 连到别的机器 —— 这是
  "ERROR Bridge stopped (TimeoutError)" 最常见的原因。
  启动前预检 Redis 连通性和串口能否打开；-CheckOnly 只预检不启动。

.PARAMETER RedisUrl
  redis://[user:password@]host:port/db。
  优先级：本参数 > -LocalConfig 的 BIGQMT_REDIS_CONFIG > 环境变量 BIGQMT_SERIAL_REDIS_URL。

.PARAMETER Account
  资金账号。默认读取环境变量 BIGQMT_ACCOUNT_ID；可用本参数覆盖。

.PARAMETER Port
  桥接这一侧的串口，默认 COM11。

.PARAMETER Baudrate
  默认 921600，必须与 QMT 侧 serial.baudrate 一致。

.PARAMETER MaxFrameBytes
  单帧上限，默认 8388608，须与 QMT 侧 serial.max_frame_bytes 一致。

.PARAMETER LocalConfig
  QMT 侧本地配置文件；默认依次在脚本目录、脚本目录\src、当前目录查找。

.PARAMETER RedisPubsub
  客户端用 Redis pub/sub 发请求时加上（等价 --redis-pubsub）。

.PARAMETER LogFile
  输出同时追加写入该文件。

.PARAMETER CheckOnly
  只预检账号 / Redis / 串口，不启动桥接。

.PARAMETER Force
  预检失败时仍然启动（仅供确认桥接自身报错用，一般不要加）。

.PARAMETER BridgeExe
  指定入口程序（bigqmt-serial-bridge 或 python.exe）；默认自动查找。

.EXAMPLE
  .\start_serial_bridge.ps1 -CheckOnly

.EXAMPLE
  .\start_serial_bridge.ps1

.EXAMPLE
  .\start_serial_bridge.ps1 -LogFile .\bridge.log

.EXAMPLE
  .\start_serial_bridge.ps1 -Port COM11 -Baudrate 921600 `
      -RedisUrl redis://192.168.2.112:6379/5 -RedisPubsub

.NOTES
  Ctrl+C 退出。桥接停止后 QMT 侧串口会话失效，需重启 QMT 策略；
  已从 Redis 取出的请求不会重放，重启前先查委托和成交。
#>
param(
    [string]$RedisUrl = "",
    [string]$Account = "",
    [string]$Port = "COM11",
    [int]$Baudrate = 921600,
    [int]$MaxFrameBytes = 8388608,
    [string]$LocalConfig = "",
    [switch]$RedisPubsub,
    [string]$LogFile = "",
    [switch]$CheckOnly,
    [switch]$Force,
    [string]$BridgeExe = ""
)
$ErrorActionPreference = "Stop"

function Say-Step([string]$m) { Write-Host ""; Write-Host ("== " + $m) -ForegroundColor Cyan }
function Say-Ok([string]$m) { Write-Host ("   [ok]   " + $m) -ForegroundColor Green }
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
            (Join-Path $scriptDir "bigqmt_signal_trader_local_config.py"),
            (Join-Path $scriptDir "src\bigqmt_signal_trader_local_config.py"),
            (Join-Path (Get-Location).Path "bigqmt_signal_trader_local_config.py"))) {
        if (Test-Path -LiteralPath $candidate) { return (Resolve-Path -LiteralPath $candidate).Path }
    }
    return ""
}

function Get-RedisConfigBlock([string]$Text) {
    if (-not $Text) { return "" }
    $start = $Text.IndexOf("BIGQMT_REDIS_CONFIG")
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

function New-RedisUrlFromBlock([string]$Block) {
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
    return ("redis://" + $auth + $h + ":" + $p + "/" + $d)
}

function Format-RedisUrl([string]$Url) {
    # 日志里隐藏用户名/密码
    return [regex]::Replace($Url, "//[^/@]*@", "//***@")
}

# ---- 2. 解析启动入口 --------------------------------------------------------
function Resolve-BridgeEntry([string]$Explicit) {
    if ($Explicit) {
        $leaf = [System.IO.Path]::GetFileName($Explicit)
        if ($leaf -match "^pythonw?([0-9]+(\.[0-9]+)?)?\.exe$") {
            return @{ Exe = $Explicit; Prefix = @("-m", "bigqmt_signal_trader.serial_redis_bridge") }
        }
        return @{ Exe = $Explicit; Prefix = @() }
    }
    $cmd = Get-Command "bigqmt-serial-bridge" -ErrorAction SilentlyContinue
    if ($cmd) { return @{ Exe = $cmd.Source; Prefix = @() } }
    $py = Get-Command "python" -ErrorAction SilentlyContinue
    if ($py) { return @{ Exe = $py.Source; Prefix = @("-m", "bigqmt_signal_trader.serial_redis_bridge") } }
    throw "找不到 bigqmt-serial-bridge，也没有 python。请先 pip install -e . 或 pip install `"xtquant-big-convert[redis,serial]`"。"
}

function Resolve-Python($Entry) {
    if ($Entry.Prefix.Count -gt 0 -and $Entry.Prefix[0] -eq "-m") { return $Entry.Exe }
    if ([System.IO.Path]::GetFileName($Entry.Exe) -match "^pythonw?([0-9]+(\.[0-9]+)?)?\.exe$") { return $Entry.Exe }
    $py = Get-Command "python" -ErrorAction SilentlyContinue
    if ($py) { return $py.Source }
    return ""
}

# ---- 3. 解析参数 ------------------------------------------------------------
Say-Step "配置"

$configPath = Find-LocalConfig $LocalConfig
$configText = ""
$redisBlock = ""
$configTransport = ""
if ($configPath) {
    $configText = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8
    $redisBlock = Get-RedisConfigBlock $configText
    $configTransport = Get-ConfigValue $redisBlock "transport"
}

$redisSource = "命令行 -RedisUrl"
if (-not $RedisUrl) {
    $RedisUrl = New-RedisUrlFromBlock $redisBlock
    if ($RedisUrl) { $redisSource = "本地配置" }
}
if (-not $RedisUrl -and $env:BIGQMT_SERIAL_REDIS_URL) {
    $RedisUrl = $env:BIGQMT_SERIAL_REDIS_URL
    $redisSource = "环境变量 BIGQMT_SERIAL_REDIS_URL"
}
if (-not $RedisUrl) {
    $RedisUrl = "redis://127.0.0.1:6379/5"
    $redisSource = "内置默认值（未找到 QMT 侧配置）"
}

$accountSource = "命令行 -Account"
if ([string]::IsNullOrWhiteSpace($Account)) {
    $Account = $env:BIGQMT_ACCOUNT_ID
    $accountSource = "环境变量 BIGQMT_ACCOUNT_ID"
}
if ([string]::IsNullOrWhiteSpace($Account)) {
    Write-Warning "缺少资金账号：请设置环境变量 BIGQMT_ACCOUNT_ID，或传入 -Account。"
    exit 1
}
$Account = $Account.Trim()

$entry = Resolve-BridgeEntry $BridgeExe

Write-Host ("   账号      : " + $Account + "   (" + $accountSource + ")")
Write-Host ("   串口      : " + $Port + " @ " + $Baudrate + " baud, 帧上限 " + $MaxFrameBytes)
Write-Host ("   Redis     : " + (Format-RedisUrl $RedisUrl) + "   (" + $redisSource + ")")
Write-Host ("   本地配置  : " + $(if ($configPath) { $configPath } else { "未找到（可用 -LocalConfig 指定）" }))
Write-Host ("   启动入口  : " + $entry.Exe + " " + (@($entry.Prefix) -join " "))

if ($configTransport -and $configTransport -ne "serial") {
    Say-Warn ('本地配置里的 transport 是 "' + $configTransport + '"，串口桥接需要 QMT 侧 BIGQMT_REDIS_CONFIG 写 "transport": "serial" 并配好 "serial": {"port": "COM10", "baudrate": 921600}。')
}
elseif ($configPath -and -not $configTransport) {
    Say-Warn '本地配置里没有 "transport": "serial"，QMT 侧默认仍走 Redis，不会用到本桥接。'
}

function Invoke-Capture([string]$Exe, [string[]]$Arguments) {
    # 5.1 里 native 命令的 stderr 重定向会生成 ErrorRecord，EAP=Stop 会把整段语句打断。
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Exe @Arguments 2>&1 | ForEach-Object { [string]$_ }
        return @{ Code = $LASTEXITCODE; Output = @($output) }
    }
    finally {
        $ErrorActionPreference = $previous
    }
}

# ---- 4. 预检 ----------------------------------------------------------------
Say-Step "预检"
$pyExe = Resolve-Python $entry
$preflightOk = $true

if (-not $pyExe) {
    Say-Warn "找不到 python，跳过 Redis/串口预检（桥接启动时会再检查一遍）。"
}
else {
    $env:BIGQMT_PREFLIGHT_REDIS_URL = $RedisUrl
    $probe = Invoke-Capture $pyExe @("-c", "import os, redis; redis.Redis.from_url(os.environ['BIGQMT_PREFLIGHT_REDIS_URL'], socket_timeout=2, socket_connect_timeout=2).ping(); print('ok')")
    if ($probe.Code -eq 0) {
        Say-Ok ("Redis 可达: " + (Format-RedisUrl $RedisUrl))
    }
    else {
        $preflightOk = $false
        Say-Fail ("Redis 不可达: " + (Format-RedisUrl $RedisUrl))
        Say-Probe $probe.Output
        Say-Detail "桥接默认连 redis://127.0.0.1:6379/5，请用 -RedisUrl 或 BIGQMT_SERIAL_REDIS_URL 指向 QMT 策略用的同一个 Redis 和 db。"
        if (($probe.Output -join "`n") -match "No module named 'redis'") {
            Say-Detail '缺少 redis 客户端：pip install "xtquant-big-convert[redis,serial]"'
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
            Say-Detail '缺少 pyserial：pip install "xtquant-big-convert[redis,serial]"'
        }
    }

    Remove-Item Env:\BIGQMT_PREFLIGHT_REDIS_URL -ErrorAction SilentlyContinue
    Remove-Item Env:\BIGQMT_PREFLIGHT_PORT -ErrorAction SilentlyContinue
    Remove-Item Env:\BIGQMT_PREFLIGHT_BAUD -ErrorAction SilentlyContinue
}

if ($CheckOnly) {
    Write-Host ""
    if ($preflightOk) { Say-Ok "预检通过（-CheckOnly，未启动桥接）。"; exit 0 }
    Say-Fail "预检未通过（-CheckOnly，未启动桥接）。"
    exit 1
}

if (-not $preflightOk -and -not $Force) {
    Write-Host ""
    Say-Fail "预检未通过，已放弃启动（确认无误可加 -Force 强行启动）。"
    exit 1
}

# ---- 5. 启动 ----------------------------------------------------------------
Say-Step "启动 bigqmt-serial-bridge"

$bridgeArgs = @($entry.Prefix) + @(
    "--port", $Port,
    "--baudrate", [string]$Baudrate,
    "--account-id", $Account,
    "--max-frame-bytes", [string]$MaxFrameBytes,
    "--redis-url", $RedisUrl
)
if ($RedisPubsub) { $bridgeArgs += "--redis-pubsub" }

$printed = (@($entry.Exe) + $bridgeArgs) -join " "
if ($RedisUrl -ne (Format-RedisUrl $RedisUrl)) { $printed = $printed.Replace($RedisUrl, (Format-RedisUrl $RedisUrl)) }
Write-Host ("   " + $printed)
Write-Host "   Ctrl+C 停止。" -ForegroundColor DarkGray
Write-Host ""

# 同一个 Redis 也通过环境变量传一份，方便子进程/后续排错保持一致
$env:BIGQMT_SERIAL_REDIS_URL = $RedisUrl
$env:PYTHONUNBUFFERED = "1"

if ($LogFile) {
    $logDir = Split-Path -Parent $LogFile
    if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
        New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    }
    $header = "# " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " start " + $printed
    Add-Content -LiteralPath $LogFile -Value $header -Encoding UTF8
}

# 原生程序的 stderr 在 5.1 里会变成 ErrorRecord；这里逐行转成字符串再输出，
# 避免 Tee-Object 把整条 "NativeCommandError" 和 UTF-16 编码写进日志。
$ErrorActionPreference = "Continue"
& $entry.Exe @bridgeArgs 2>&1 | ForEach-Object {
    $line = [string]$_
    if ($_ -is [System.Management.Automation.ErrorRecord]) {
        Write-Host $line -ForegroundColor Red
    }
    else {
        Write-Host $line
    }
    if ($LogFile) { Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8 }
}
$code = $LASTEXITCODE
if ($null -eq $code) { $code = 0 }

Write-Host ""
if ($code -eq 0) {
    Say-Ok "桥接已退出（Ctrl+C）。QMT 侧串口会话已失效，下次启动桥接后要重启 QMT 策略。"
}
else {
    Say-Fail ("桥接异常退出，exit=" + $code + "。对照上面的 ERROR 行：")
    Say-Detail "Cannot reach Redis ...     Redis 地址/端口/db 不对，或 Redis 没启动、被防火墙挡住。"
    Say-Detail "Cannot open serial port ... 串口名不对，或被 QMT/另一个桥接占用，或 com0com 端口对不存在。"
    Say-Detail "Bridge stopped ...         运行中串口或 Redis 断链；重启桥接和 QMT 策略，重启前先查委托和成交。"
}
exit $code
