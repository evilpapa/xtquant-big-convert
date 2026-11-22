#Requires -Version 5.1
<#
.SYNOPSIS
  在 Windows 上部署 xtquant-big-convert 的 Valkey 桥接。
.DESCRIPTION
  适用于已安装并登录大 QMT 的 2 核 4 GB Windows 服务器。
  可重复运行，脚本会跳过已完成的步骤。
.PARAMETER QmtDir   大 QMT 安装目录，例如 C:\JianghaiQMT
.PARAMETER QmtPython QMT 策略使用的 Python 解释器（默认 <QmtDir>\bin.x64\python.exe）
.PARAMETER Account  资金账号，例如 1234567890
.PARAMETER WorkDir  部署目录（默认 C:\qmt_bridge）
.PARAMETER ValkeyZip 包含 ValkeyService.exe 的 Cygwin 离线压缩包
.PARAMETER ValkeyUrl Valkey Windows 压缩包的备用下载地址
.PARAMETER ValkeyPort Valkey 服务的本机端口（默认 6379）
.PARAMETER BridgePackage 桥接安装包或源码仓库的路径
.PARAMETER Proxy    下载使用的 HTTP 代理，例如 http://127.0.0.1:7897
.PARAMETER AllowOrders  在服务端配置中启用远程下单和撤单（默认关闭）
#>
param(
    [Parameter(Mandatory=$true)][string]$QmtDir,
    [Parameter(Mandatory=$true)][string]$Account,
    [string]$WorkDir = "C:\qmt_bridge",
    [string]$ValkeyZip = "",
    [string]$ValkeyUrl = "",
    [ValidateRange(1, 65535)][int]$ValkeyPort = 6379,
    [string]$BridgePackage = "",
    [string]$Proxy = "",
    [switch]$CheckOnly,
    [switch]$AllowOrders,
    [switch]$Conda,
    [string]$MinicondaUrl = "",
    [string]$PipIndex = "https://pypi.tuna.tsinghua.edu.cn/simple",
    [switch]$WithStrategy,
    [string]$StrategySrc = "",
    [string]$QmtPython = ""
)
$ErrorActionPreference = "Stop"
$valkeyRelease = "9.1.4"
$archiveName = "Valkey-$valkeyRelease-Windows-x64-cygwin-with-Service"
$rdir = "$WorkDir\valkey"
$binDir = "$rdir\$archiveName"
$valkeyService = "$binDir\ValkeyService.exe"
$valkeyCli = "$binDir\valkey-cli.exe"
$pwFile = "$rdir\valkey_password.txt"
$conf = "$rdir\valkey.conf"
$dataDir = "$rdir\data"
$serviceName = "ValkeyBigQMT"

function Step($m) { Write-Host ""
  Write-Host ("== " + $m) -ForegroundColor Cyan }
function Ok($m)   { Write-Host ("   [ok] " + $m) -ForegroundColor Green }
function Info($m) { Write-Host ("   .. " + $m) }

function Assert-PythonVersion([string]$Executable, [string]$Role) {
    & $Executable -c "import sys; sys.exit(0 if sys.version_info >= (3, 12) else 1)"
    if ($LASTEXITCODE -ne 0) { throw "${Role}需要 Python 3.12 或更高版本。请检查解释器 $Executable。" }
}

# ---- 0. 检查部署条件 -----------------------------------------------------
Step "0/7 检查部署条件"
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin -and -not $CheckOnly) { throw "请以管理员身份运行 PowerShell。" }
if ($CheckOnly) {
    Info "检查模式，仅显示当前部署状态"
    $items = [ordered]@{
        "QMT 安装目录"            = Test-Path "$QmtDir\bin.x64\XtItClient.exe"
        "xtquant 库"        = Test-Path "$QmtDir\bin.x64\Lib\site-packages\xtquant"
        "服务端文件"       = Test-Path "$QmtDir\python\BIGQMT_VALKEY_DRYRUN.py"
        "服务端配置"= Test-Path "$QmtDir\python\bigqmt_signal_trader_qmt_config.py"
        "客户端环境"         = ((Test-Path "$WorkDir\envs\bigqmt-client\Scripts\python.exe") -or (Test-Path "$WorkDir\envs\bigqmt\python.exe"))
        "Valkey 程序"     = (Test-Path $valkeyService) -and (Test-Path $valkeyCli)
        "Valkey 密码文件"= Test-Path $pwFile
        "Valkey 配置"       = Test-Path $conf
        "Valkey 服务"      = (Get-Service $serviceName -ErrorAction SilentlyContinue).Status -eq "Running"
        "客户端配置"      = Test-Path "$WorkDir\client\bigqmt_signal_trader_client_config.py"
        "qmt_cli"            = Test-Path "$WorkDir\client\qmt_cli.py"
    }
    $missing = 0
    foreach ($k in $items.Keys) {
        $mark = if ($items[$k]) { "[ok]     " } else { "[缺失]"; $missing++ }
        Write-Host ("   $mark $k")
    }
    if ($missing -gt 0) { Write-Host "   缺少 $missing 项。请以管理员身份去掉 -CheckOnly 后重新运行。" }
    else { Write-Host "   部署已完成" }
    return
}
if (-not (Test-Path "$QmtDir\bin.x64\XtItClient.exe")) { throw "在 $QmtDir 下找不到大 QMT" }
if (-not (Test-Path "$QmtDir\bin.x64\Lib\site-packages\xtquant")) {
    throw "缺少 xtquant 库。请先在 QMT 中执行「下载 Python 库」。"
}
if (-not $QmtPython) { $QmtPython = "$QmtDir\bin.x64\python.exe" }
if (-not (Test-Path -LiteralPath $QmtPython -PathType Leaf)) {
    throw "请用 -QmtPython 指定 QMT 策略实际使用的解释器。"
}
Assert-PythonVersion $QmtPython "QMT 策略"
& $QmtPython -c "from valkey import Valkey; from valkey.backoff import NoBackoff; from valkey.retry import Retry"
if ($LASTEXITCODE -ne 0) { throw '请在 QMT 策略环境安装 Valkey 客户端：python -m pip install "valkey>=6.1,<7"' }
if (-not (Get-Service $serviceName -ErrorAction SilentlyContinue) -and
    (Get-NetTCPConnection -State Listen -LocalPort $ValkeyPort -ErrorAction SilentlyContinue)) {
    throw "端口 $ValkeyPort 已被占用，请用 -ValkeyPort 指定空闲端口。"
}
$pyExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $pyExe -and -not $Conda) { throw "PATH 中找不到 Python。请安装 Python 3.12 或更高版本，或用 -Conda 使用 Miniconda。" }
Ok "已找到 QMT 和 xtquant。系统 Python：$pyExe"
New-Item -ItemType Directory -Path $WorkDir -Force | Out-Null

# ---- 1. 准备客户端环境（Conda 或 venv） --------------------------
Step "1/7 准备客户端环境（xtquant-big-convert 和 pandas）"
if ($Conda) {
    $condaExe = (Get-Command conda -ErrorAction SilentlyContinue).Source
    if (-not $condaExe) {
        foreach ($cand in @("$env:USERPROFILE\miniconda3\Scripts\conda.exe",
                            "$env:USERPROFILE\anaconda3\Scripts\conda.exe",
                            "C:\ProgramData\miniconda3\Scripts\conda.exe")) {
            if (Test-Path $cand) { $condaExe = $cand; break }
        }
    }
    if (-not $condaExe) {
        Info "找不到 Conda，正在安装 Miniconda 到 $WorkDir\miniconda3"
        $condaHome = "$WorkDir\miniconda3".TrimEnd("\\")
        if ($condaHome.Contains(" ")) { throw "Miniconda 静默安装要求路径不含空格，请更换 -WorkDir。" }
        $condaExe  = "$condaHome\Scripts\conda.exe"
        $installer = "$WorkDir\Miniconda3-latest-Windows-x86_64.exe"
        if (-not (Test-Path $installer)) {
            $url = if ($MinicondaUrl) { $MinicondaUrl } else { "https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Windows-x86_64.exe" }
            $dlArgs = @("-L","--max-time","900","-o",$installer,$url)
            if ($Proxy) { $dlArgs = @("-x",$Proxy) + $dlArgs }
            & curl.exe @dlArgs
            if ($LASTEXITCODE -ne 0) { throw "Miniconda 下载失败。可用 -MinicondaUrl <本地安装程序路径> 或 -Proxy http://... 重试。" }
        }
        # 静默安装时，/D= 必须是最后一个参数，路径不加引号，末尾不加反斜杠。
        Start-Process -FilePath $installer -ArgumentList "/InstallationType=JustMe","/AddToPath=0","/RegisterPython=0","/S","/D=$condaHome" -Wait
        if (-not (Test-Path $condaExe)) { throw "Miniconda 静默安装失败，找不到 $condaExe" }
        Ok "Miniconda 已安装到 $condaHome"
    }
    $venv = "$WorkDir\envs\bigqmt"
    $vpy  = "$venv\python.exe"
    if (-not (Test-Path $vpy)) {
        New-Item -ItemType Directory -Force "$WorkDir\envs" | Out-Null
        if ($Proxy) { $env:HTTP_PROXY = $Proxy; $env:HTTPS_PROXY = $Proxy }
        # Conda 25 及以上版本需先接受默认频道的服务条款。旧版本可能没有此子命令。
        foreach ($ch in @("https://repo.anaconda.com/pkgs/main",
                          "https://repo.anaconda.com/pkgs/r",
                          "https://repo.anaconda.com/pkgs/msys2")) {
            & $condaExe tos accept --override-channels --channel $ch *> $null
        }
        & $condaExe create -y -p $venv -c https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main --override-channels python=3.13
        if ($LASTEXITCODE -ne 0) { throw "Conda 环境创建失败" }
    }
    Ok "Conda 环境：$venv（Python 3.13）"
} else {
    $venv = "$WorkDir\envs\bigqmt-client"
    $vpy  = "$venv\Scripts\python.exe"
    if (-not (Test-Path $vpy)) {
        Assert-PythonVersion $pyExe "创建客户端环境的系统 Python"
        New-Item -ItemType Directory -Force "$WorkDir\envs" | Out-Null
        & $pyExe -m venv $venv
        if ($LASTEXITCODE -ne 0) { throw "venv 创建失败" }
    }
}
Assert-PythonVersion $vpy "客户端环境"
& $vpy -m pip --version *> $null
if ($LASTEXITCODE -ne 0) { & $vpy -m ensurepip --upgrade }
$pipArgs = @("-m","pip","install","-i",$PipIndex,"--upgrade","pip")
if ($Proxy) { $pipArgs += @("--proxy",$Proxy) }
& $vpy @pipArgs *> $null
if (-not $BridgePackage) {
    $repoDir = Split-Path -Parent $PSScriptRoot
    $BridgePackage = if (Test-Path "$repoDir\pyproject.toml") { $repoDir } else { "xtquant-big-convert" }
}
$installArgs = @("-m","pip","install","-i",$PipIndex,"${BridgePackage}[valkey]","pandas")
if ($Proxy) { $installArgs += @("--proxy",$Proxy) }
& $vpy @installArgs
if ($LASTEXITCODE -ne 0) { throw "pip 安装失败，请检查 -BridgePackage 和网络或代理。" }
& $vpy -c "import valkey; import importlib.util; assert importlib.util.find_spec('BIGQMT_VALKEY_DRYRUN')"
if ($LASTEXITCODE -ne 0) { throw "需要支持 Valkey 的桥接包。请用 -BridgePackage 指定源码仓库或 wheel 文件。" }
Ok "客户端环境已就绪：$vpy"

# ---- 2. 安装服务端文件 ----------------------------------
Step "2/7 复制桥接服务端文件到 QMT 的 python 目录"
$src = (& $vpy -c "import bigqmt_signal_trader_strategy as m, os; print(os.path.dirname(m.__file__))").Trim()
$dst = "$QmtDir\python"
if ($src -eq $dst) {
    throw ("导入到已部署的文件，源目录与 QMT 的 python 目录相同（" + $src + "）。请切换到其他目录运行此脚本，例如 deploy 目录。")
}
foreach ($item in @("bigqmt_signal_trader","bigqmt_signal_trader_strategy.py",
                    "bigqmt_signal_trader_valkey_rpc_runtime.py","BIGQMT_VALKEY_DRYRUN.py")) {
    Copy-Item (Join-Path $src $item) (Join-Path $dst $item) -Recurse -Force
}
Ok "已复制到 $dst"

# ---- 3. 下载并解压 Valkey -------------------------------------------
Step "3/7 准备 Windows 版 Valkey $valkeyRelease"
$zip = "$rdir\$archiveName.zip"
Add-Type -AssemblyName System.IO.Compression.FileSystem
function Test-ValkeyZip([string]$Path) {
    if (-not (Test-Path $Path)) { return $false }
    try {
        $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)
        try {
            return ($null -ne $archive.GetEntry("$archiveName/valkey-server.exe")) -and
                   ($null -ne $archive.GetEntry("$archiveName/valkey-cli.exe")) -and
                   ($null -ne $archive.GetEntry("$archiveName/ValkeyService.exe"))
        }
        finally { $archive.Dispose() }
    } catch { return $false }
}
$explicitZip = [bool]$ValkeyZip
if (-not $ValkeyZip) { $ValkeyZip = $zip }
if (-not (Test-ValkeyZip $ValkeyZip)) {
    # 保留调用方指定的压缩包。默认压缩包缺失或损坏时重新下载。
    if ($explicitZip) { throw "压缩包无效，请提供 $archiveName.zip：$ValkeyZip" }
    New-Item -ItemType Directory -Force $rdir | Out-Null
    $downloadZip = "$zip.download"
    # 兼容 Windows PowerShell 5.1，先确定下载地址，再构造参数数组。
    $source = "https://github.com/valkey-windows/valkey-windows/releases/download/$valkeyRelease/$archiveName.zip"
    if ($ValkeyUrl) { $source = $ValkeyUrl }
    $dlArgs = @("-L","--connect-timeout","20","--retry","3","--retry-delay","2","--max-time","1800",
      "-o",$downloadZip,$source)
    if ($Proxy) { $dlArgs = @("-x",$Proxy) + $dlArgs }
    & curl.exe @dlArgs
    if ($LASTEXITCODE -ne 0 -or -not (Test-ValkeyZip $downloadZip)) {
        throw "Valkey 下载失败或文件不完整。可用 -ValkeyZip <路径>、-ValkeyUrl <备用地址> 或 -Proxy http://... 重试。"
    }
    Move-Item -LiteralPath $downloadZip -Destination $ValkeyZip -Force
}
if (-not (Test-Path $valkeyService) -or -not (Test-Path $valkeyCli) -or
    -not (Test-Path "$binDir\valkey-server.exe")) {
    Expand-Archive -LiteralPath $ValkeyZip -DestinationPath $rdir -Force
}
Ok "Valkey 程序目录：$binDir"

# ---- 4. 配置 Valkey 服务及密码 -----------------------------------
Step "4/7 配置 Valkey 服务（监听 127.0.0.1，启用密码，内存上限 192 MB）"
New-Item -ItemType Directory -Force $dataDir | Out-Null
$svc = Get-Service $serviceName -ErrorAction SilentlyContinue
if ($svc -and (-not (Test-Path $pwFile) -or -not (Test-Path $conf))) {
    throw "服务 $serviceName 已存在，但 $rdir 中缺少配置或密码文件。"
}
if (Test-Path $pwFile) { $pw = (Get-Content $pwFile -Raw).Trim() }
else {
    $secretBytes = New-Object byte[] 24
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($secretBytes) }
    finally { $rng.Dispose() }
    $pw = [BitConverter]::ToString($secretBytes).Replace("-", "").ToLowerInvariant()
    Set-Content $pwFile $pw -NoNewline -Encoding ASCII
}
if ($pw -notmatch '\A[0-9a-f]{48}\z') { throw "Valkey 密码文件无效：$pwFile" }
if (-not $svc) {
    @"
bind 127.0.0.1
port $ValkeyPort
requirepass $pw
maxmemory 192mb
maxmemory-policy noeviction
"@ | Set-Content -LiteralPath $conf -Encoding ASCII
    & $valkeyService install -c $conf --dir $dataDir --port $ValkeyPort --service-name $serviceName
    if ($LASTEXITCODE -ne 0) { throw "Valkey 服务安装失败" }
} elseif (-not (Select-String -LiteralPath $conf -Pattern "^\s*port\s+$ValkeyPort\s*$" -Quiet)) {
    throw "服务 $serviceName 已使用其他端口，请以 $conf 中的端口为准。"
}
$svc = Get-Service $serviceName
if ($svc.Status -ne "Running") { Start-Service $serviceName }
$previousAuth = $env:VALKEYCLI_AUTH
try {
    $env:VALKEYCLI_AUTH = $pw
    $pong = & $valkeyCli -h 127.0.0.1 -p $ValkeyPort -n 5 PING
    if ($LASTEXITCODE -ne 0 -or $pong -ne "PONG") {
        throw "Valkey 身份验证失败（127.0.0.1:$ValkeyPort）。"
    }
} finally {
    $env:VALKEYCLI_AUTH = $previousAuth
}
Ok "Valkey 服务验证通过（127.0.0.1:$ValkeyPort）"

# ---- 5. 生成服务端配置 -------------------------------------------------
Step "5/7 写入服务端配置"
# 新部署默认只允许查询，传入 -AllowOrders 才启用远程下单和撤单。
$allowOrdersPy = if ($AllowOrders) { "True" } else { "False" }
$localCfg = @"
# coding: utf-8
BIGQMT_ACCOUNT_ID = "$Account"
BIGQMT_ACCOUNT_TYPE = "STOCK"

BIGQMT_CONFIG = {
    "host": "127.0.0.1",
    "port": $ValkeyPort,
    "db": 5,
    "username": "",
    "password": "$pw",
    "rpc_allow_order_methods": $allowOrdersPy,
    "rpc_process_in_listener": True,
    "rpc_listener_methods": ("*",),
    "rpc_background_threads": False,
    "schedule_adjust": True,
    "schedule_adjust_interval": "100nMilliSecond",
    "full_tick_cache_enabled": False,
    "download_jobs_enabled": False,
    "exec_events_enabled": True,
    "exec_events_debug_raw_fields": False,
}
"@
Set-Content "$dst\bigqmt_signal_trader_qmt_config.py" $localCfg -Encoding UTF8
Ok "$dst\bigqmt_signal_trader_qmt_config.py"

# ---- 6. 安装客户端配置和命令行工具 ------------------------------------------------
Step "6/7 安装客户端配置和命令行工具"
$cdir = "$WorkDir\client"
New-Item -ItemType Directory -Force $cdir | Out-Null
$cliSrc = Join-Path $PSScriptRoot "qmt_cli.py"
if (-not (Test-Path $cliSrc)) { throw "请将部署包中的 qmt_cli.py 放在本脚本旁。" }
Copy-Item $cliSrc "$cdir\qmt_cli.py" -Force
$clientCfg = @"
# coding: utf-8
BIGQMT_ACCOUNT_ID = "$Account"
BIGQMT_RPC_TIMEOUT_SECONDS = 30.0

BIGQMT_CONFIG = {
    "host": "127.0.0.1",
    "port": $ValkeyPort,
    "db": 5,
    "username": "",
    "password": "$pw",
    "transport": "valkey",
    "protocol": 2,
}

BIGQMT_FULL_TICK_CACHE_CONFIG = {"enabled": False}
BIGQMT_FORMULA_SERVER_CONFIG = {"enabled": False}
BIGQMT_LOCAL_CACHE_CONFIG = {
    "enabled": True,
    "dir": r"$cdir\.bigqmt_cache",
    "fallback_rpc": False,
    "format": "auto",
}
"@
Set-Content "$cdir\bigqmt_signal_trader_client_config.py" $clientCfg -Encoding UTF8
Ok "客户端配置和 qmt_cli.py 已写入 $cdir"

# ---- 7. 复制自定义策略目录（可选） -----------------------------------------
if ($WithStrategy) {
    Step "7/7 复制自定义策略目录"
    if (-not $StrategySrc -or -not (Test-Path $StrategySrc)) { throw "使用 -WithStrategy 时须传入 -StrategySrc <目录>。" }
    Copy-Item "$StrategySrc\*" "$WorkDir\my_code" -Recurse -Force
    Ok "策略已复制到 $WorkDir\my_code，请检查 order_config.json 中的 account_num。"
}

# ---- 部署后的手动操作 ----------------------------------------------------
Write-Host ""
Write-Host "================ 部署文件已就绪，请完成以下操作 ================" -ForegroundColor Yellow
Write-Host @"
1. 在 QMT 的「模型交易」中添加策略，加载
   $dst\BIGQMT_VALKEY_DRYRUN.py
   将运行模式设为「实盘」并启动策略。
   在输出面板确认 RPC 已启动（[bigqmt_rpc]）。
2. 从 QMT 进程外验证：
   & "$vpy" "$cdir\qmt_cli.py" ping
   & "$vpy" "$cdir\qmt_cli.py" asset
3. Valkey 密码文件： $pwFile
4. 2 核 4 GB 服务器的内存预算：QMT 约 1.5 GB，Valkey 上限 192 MB，Python 约 400 MB。可关闭闲置的 QMT 面板。
"@ -ForegroundColor Yellow
if ($AllowOrders) {
    Write-Host "5. 远程下单和撤单已开启（-AllowOrders），可调用 buy、sell 和 cancel。" -ForegroundColor Yellow
} else {
    Write-Host "5. 远程下单和撤单默认关闭，buy、sell、cancel 返回 ORDER_DISABLED。可传入 -AllowOrders 重新部署，或在 $dst\bigqmt_signal_trader_qmt_config.py 中设置 rpc_allow_order_methods=True 后重启策略。" -ForegroundColor Yellow
}
