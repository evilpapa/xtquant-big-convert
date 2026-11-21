#Requires -Version 5.1
<#
.SYNOPSIS
  One-shot deployment of the xtquant_big_convert Valkey bridge on a fresh Windows box.
.DESCRIPTION
  Target: 2C4G Windows server with Big QMT installed and logged in.
  Idempotent - safe to re-run. Skips completed steps.
.PARAMETER QmtDir   Big QMT install root, e.g. C:\JianghaiQMT
.PARAMETER QmtPython Python executable used by the QMT strategy (default: <QmtDir>\bin.x64\python.exe)
.PARAMETER Account  Capital account id (digits), e.g. 1234567890
.PARAMETER WorkDir  Deployment root (default C:\qmt_bridge)
.PARAMETER ValkeyZip Offline Cygwin archive with ValkeyService.exe
.PARAMETER ValkeyUrl Alternate download URL for the Valkey Windows archive
.PARAMETER ValkeyPort Loopback port for the Valkey service (default 6379)
.PARAMETER BridgePackage Path to the bridge wheel or source checkout
.PARAMETER Proxy    HTTP proxy for downloads, e.g. http://127.0.0.1:7897
.PARAMETER AllowOrders  Enable order RPCs in the generated server config (off by default)
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
    if ($LASTEXITCODE -ne 0) { throw "$Role requires Python 3.12 or newer. Check the interpreter at $Executable." }
}

# ---- 0. preconditions -----------------------------------------------------
Step "0/7 preconditions"
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin -and -not $CheckOnly) { throw "Please run from an elevated (Administrator) PowerShell." }
if ($CheckOnly) {
    Info "check-only mode: reporting state, no writes"
    $items = [ordered]@{
        "QMT dir"            = Test-Path "$QmtDir\bin.x64\XtItClient.exe"
        "xtquant lib"        = Test-Path "$QmtDir\bin.x64\Lib\site-packages\xtquant"
        "server files"       = Test-Path "$QmtDir\python\BIGQMT_VALKEY_DRYRUN.py"
        "server local config"= Test-Path "$QmtDir\python\bigqmt_signal_trader_qmt_config.py"
        "client env"         = ((Test-Path "$WorkDir\envs\bigqmt-client\Scripts\python.exe") -or (Test-Path "$WorkDir\envs\bigqmt\python.exe"))
        "valkey binaries"     = (Test-Path $valkeyService) -and (Test-Path $valkeyCli)
        "valkey password file"= Test-Path $pwFile
        "valkey config"       = Test-Path $conf
        "valkey service"      = (Get-Service $serviceName -ErrorAction SilentlyContinue).Status -eq "Running"
        "client config"      = Test-Path "$WorkDir\client\bigqmt_signal_trader_client_config.py"
        "qmt_cli"            = Test-Path "$WorkDir\client\qmt_cli.py"
    }
    $missing = 0
    foreach ($k in $items.Keys) {
        $mark = if ($items[$k]) { "[ok]     " } else { "[missing]"; $missing++ }
        Write-Host ("   $mark $k")
    }
    if ($missing -gt 0) { Write-Host "   $missing item(s) missing -> run without -CheckOnly (as Administrator)" }
    else { Write-Host "   deployment complete" }
    return
}
if (-not (Test-Path "$QmtDir\bin.x64\XtItClient.exe")) { throw "Big QMT not found under $QmtDir" }
if (-not (Test-Path "$QmtDir\bin.x64\Lib\site-packages\xtquant")) {
    throw "xtquant library missing. Open QMT and run its 'download python libraries' first."
}
if (-not $QmtPython) { $QmtPython = "$QmtDir\bin.x64\python.exe" }
if (-not (Test-Path -LiteralPath $QmtPython -PathType Leaf)) {
    throw "Pass -QmtPython with the executable used by the QMT strategy."
}
Assert-PythonVersion $QmtPython "The QMT strategy"
& $QmtPython -c "from valkey import Valkey; from valkey.backoff import NoBackoff; from valkey.retry import Retry"
if ($LASTEXITCODE -ne 0) { throw 'Install valkey in the QMT strategy environment: python -m pip install "valkey>=6.1,<7".' }
if (-not (Get-Service $serviceName -ErrorAction SilentlyContinue) -and
    (Get-NetTCPConnection -State Listen -LocalPort $ValkeyPort -ErrorAction SilentlyContinue)) {
    throw "Port $ValkeyPort is already listening. Choose an unused -ValkeyPort for Valkey."
}
$pyExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $pyExe -and -not $Conda) { throw "System Python not found on PATH (>= 3.12), or pass -Conda to use miniconda." }
Ok "QMT found; xtquant present; python: $pyExe"
New-Item -ItemType Directory -Path $WorkDir -Force | Out-Null

# ---- 1. client environment (conda py313 or venv) --------------------------
Step "1/7 client env (xtquant-big-convert + pandas)"
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
        Info "conda not found - auto-installing Miniconda into $WorkDir\miniconda3"
        $condaHome = "$WorkDir\miniconda3".TrimEnd("\\")
        if ($condaHome.Contains(" ")) { throw "WorkDir path contains spaces; Miniconda silent install cannot handle it - choose a space-free -WorkDir." }
        $condaExe  = "$condaHome\Scripts\conda.exe"
        $installer = "$WorkDir\Miniconda3-latest-Windows-x86_64.exe"
        if (-not (Test-Path $installer)) {
            $url = if ($MinicondaUrl) { $MinicondaUrl } else { "https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Windows-x86_64.exe" }
            $dlArgs = @("-L","--max-time","900","-o",$installer,$url)
            if ($Proxy) { $dlArgs = @("-x",$Proxy) + $dlArgs }
            & curl.exe @dlArgs
            if ($LASTEXITCODE -ne 0) { throw "miniconda download failed (offline? pre-download and pass -MinicondaUrl <local exe path>, or add -Proxy http://...)" }
        }
        # silent install; /D= must be the LAST argument, unquoted, no trailing backslash
        Start-Process -FilePath $installer -ArgumentList "/InstallationType=JustMe","/AddToPath=0","/RegisterPython=0","/S","/D=$condaHome" -Wait
        if (-not (Test-Path $condaExe)) { throw "miniconda silent install failed - $condaExe missing" }
        Ok "miniconda installed: $condaHome"
    }
    $venv = "$WorkDir\envs\bigqmt"
    $vpy  = "$venv\python.exe"
    if (-not (Test-Path $vpy)) {
        New-Item -ItemType Directory -Force "$WorkDir\envs" | Out-Null
        if ($Proxy) { $env:HTTP_PROXY = $Proxy; $env:HTTPS_PROXY = $Proxy }
        # conda >= 25 requires accepting defaults-channel ToS before
        # non-interactive use; older builds lack the subcommand (ignore errors).
        foreach ($ch in @("https://repo.anaconda.com/pkgs/main",
                          "https://repo.anaconda.com/pkgs/r",
                          "https://repo.anaconda.com/pkgs/msys2")) {
            & $condaExe tos accept --override-channels --channel $ch *> $null
        }
        & $condaExe create -y -p $venv -c https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main --override-channels python=3.13
        if ($LASTEXITCODE -ne 0) { throw "conda create failed" }
    }
    Ok "conda env: $venv (python 3.13)"
} else {
    $venv = "$WorkDir\envs\bigqmt-client"
    $vpy  = "$venv\Scripts\python.exe"
    if (-not (Test-Path $vpy)) {
        Assert-PythonVersion $pyExe "The system Python used to create the client environment"
        New-Item -ItemType Directory -Force "$WorkDir\envs" | Out-Null
        & $pyExe -m venv $venv
        if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
    }
}
Assert-PythonVersion $vpy "The client environment"
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
if ($LASTEXITCODE -ne 0) { throw "pip install failed (check -BridgePackage and network/proxy)" }
& $vpy -c "import valkey; import importlib.util; assert importlib.util.find_spec('BIGQMT_VALKEY_DRYRUN')"
if ($LASTEXITCODE -ne 0) { throw "A Valkey-enabled bridge build is required. Pass -BridgePackage with the source checkout or wheel." }
Ok "venv ready: $vpy"

# ---- 2. server files into QMT python dir ----------------------------------
Step "2/7 copy bridge server files into QMT python dir"
$src = (& $vpy -c "import bigqmt_signal_trader_strategy as m, os; print(os.path.dirname(m.__file__))").Trim()
$dst = "$QmtDir\python"
if ($src -eq $dst) {
    throw ("source resolved to the QMT python dir itself (" + $src + ") - the import picked up already-deployed files. Run the script from any other directory (e.g. the deploy folder).")
}
foreach ($item in @("bigqmt_signal_trader","bigqmt_signal_trader_strategy.py",
                    "bigqmt_signal_trader_valkey_rpc_runtime.py","BIGQMT_VALKEY_DRYRUN.py")) {
    Copy-Item (Join-Path $src $item) (Join-Path $dst $item) -Recurse -Force
}
Ok "copied to $dst"

# ---- 3. Valkey download / unzip -------------------------------------------
Step "3/7 Valkey $valkeyRelease for Windows"
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
    # An explicitly passed archive is the caller's: never download over it.
    # The default path is ours: missing means first run, broken means an
    # interrupted download -- both go to the download below.
    if ($explicitZip) { throw "Expected the $archiveName.zip release archive: $ValkeyZip" }
    New-Item -ItemType Directory -Force $rdir | Out-Null
    $downloadZip = "$zip.download"
    # Windows PowerShell 5.1 (what the README runs this with) has no
    # if-expression: `(if ...)` inside an array is parsed as a command
    # named "if". Pick the source first, then build the argument list.
    $source = "https://github.com/valkey-windows/valkey-windows/releases/download/$valkeyRelease/$archiveName.zip"
    if ($ValkeyUrl) { $source = $ValkeyUrl }
    $dlArgs = @("-L","--connect-timeout","20","--retry","3","--retry-delay","2","--max-time","1800",
      "-o",$downloadZip,$source)
    if ($Proxy) { $dlArgs = @("-x",$Proxy) + $dlArgs }
    & curl.exe @dlArgs
    if ($LASTEXITCODE -ne 0 -or -not (Test-ValkeyZip $downloadZip)) {
        throw "Valkey download failed or incomplete (pass -ValkeyZip <path> for offline use, -ValkeyUrl <mirror> for an alternate source, or add -Proxy http://...)"
    }
    Move-Item -LiteralPath $downloadZip -Destination $ValkeyZip -Force
}
if (-not (Test-Path $valkeyService) -or -not (Test-Path $valkeyCli) -or
    -not (Test-Path "$binDir\valkey-server.exe")) {
    Expand-Archive -LiteralPath $ValkeyZip -DestinationPath $rdir -Force
}
Ok "Valkey binaries: $binDir"

# ---- 4. valkey password / conf / service -----------------------------------
Step "4/7 Valkey service (bind 127.0.0.1 + password + 192mb cap)"
New-Item -ItemType Directory -Force $dataDir | Out-Null
$svc = Get-Service $serviceName -ErrorAction SilentlyContinue
if ($svc -and (-not (Test-Path $pwFile) -or -not (Test-Path $conf))) {
    throw "$serviceName exists but its config or password file is missing from $rdir."
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
if ($pw -notmatch '\A[0-9a-f]{48}\z') { throw "Invalid Valkey password file: $pwFile" }
if (-not $svc) {
    @"
bind 127.0.0.1
port $ValkeyPort
requirepass $pw
maxmemory 192mb
maxmemory-policy noeviction
"@ | Set-Content -LiteralPath $conf -Encoding ASCII
    & $valkeyService install -c $conf --dir $dataDir --port $ValkeyPort --service-name $serviceName
    if ($LASTEXITCODE -ne 0) { throw "Valkey service installation failed" }
} elseif (-not (Select-String -LiteralPath $conf -Pattern "^\s*port\s+$ValkeyPort\s*$" -Quiet)) {
    throw "$serviceName already exists with a different port. Use the port in $conf."
}
$svc = Get-Service $serviceName
if ($svc.Status -ne "Running") { Start-Service $serviceName }
$previousAuth = $env:VALKEYCLI_AUTH
try {
    $env:VALKEYCLI_AUTH = $pw
    $pong = & $valkeyCli -h 127.0.0.1 -p $ValkeyPort -n 5 PING
    if ($LASTEXITCODE -ne 0 -or $pong -ne "PONG") {
        throw "Cannot authenticate to Valkey at 127.0.0.1:$ValkeyPort."
    }
} finally {
    $env:VALKEYCLI_AUTH = $previousAuth
}
Ok "Valkey service verified at 127.0.0.1:$ValkeyPort"

# ---- 5. server-side config -------------------------------------------------
Step "5/7 write server-side local config"
# Order RPCs stay OFF unless -AllowOrders is given -- the repo's safety
# default: a fresh bridge answers queries only, order placement is an
# explicit human decision (qmt-trader SKILL.md 安全须知).
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

# ---- 6. client config + CLI ------------------------------------------------
Step "6/7 client config + CLI"
$cdir = "$WorkDir\client"
New-Item -ItemType Directory -Force $cdir | Out-Null
$cliSrc = Join-Path $PSScriptRoot "qmt_cli.py"
if (-not (Test-Path $cliSrc)) { throw "qmt_cli.py must sit next to this script (deploy bundle)." }
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
Ok "client config + qmt_cli.py in $cdir"

# ---- 7. optional: strategy folder -----------------------------------------
if ($WithStrategy) {
    Step "7/7 strategy folder"
    if (-not $StrategySrc -or -not (Test-Path $StrategySrc)) { throw "-WithStrategy needs -StrategySrc <dir>" }
    Copy-Item "$StrategySrc\*" "$WorkDir\my_code" -Recurse -Force
    Ok "strategy copied to $WorkDir\my_code (check order_config.json account_num)"
}

# ---- done: manual steps ----------------------------------------------------
Write-Host ""
Write-Host "================ DEPLOYMENT DONE - manual steps left ================" -ForegroundColor Yellow
Write-Host @"
1. In QMT: model trading -> add strategy -> load
   $dst\BIGQMT_VALKEY_DRYRUN.py
   set run mode to LIVE (实盘), start it.
   Output panel must show: [bigqmt_rpc] started channel=bigqmt:rpc:req:$Account
2. Verify from outside:
   & "$vpy" "$cdir\qmt_cli.py" ping
   & "$vpy" "$cdir\qmt_cli.py" asset
3. valkey password saved at: $pwFile
4. Memory budget on 2C4G: QMT ~1.5G + valkey <=192M + python ~400M. Close QMT panels you do not use.
"@ -ForegroundColor Yellow
if ($AllowOrders) {
    Write-Host "5. order RPCs: ENABLED (-AllowOrders). buy/sell/cancel will work." -ForegroundColor Yellow
} else {
    Write-Host "5. order RPCs: OFF (default). buy/sell/cancel answer ORDER_DISABLED; re-run with -AllowOrders or set rpc_allow_order_methods=True in $dst\bigqmt_signal_trader_qmt_config.py and restart the strategy." -ForegroundColor Yellow
}
