# deploy/ — 一键部署包（2核4G Windows 服务器适用）

把整个 `deploy\` 文件夹拷到新服务器，按下面执行。

如果服务器已有 VK 实例，按[部署快速开始](../docs/DEPLOY_QUICKSTART.md)配置好客户端和 QMT 策略即可复用。本页的一键脚本用于安装并管理独立的 `ValkeyBigQMT` 服务。

## 前提（脚本不负责的部分）
1. 已安装大 QMT 客户端并登录交易
2. QMT 菜单里点过 **下载 python库**（脚本会检查 `bin.x64\Lib\site-packages\xtquant`）。策略运行环境需要 Python 3.12+，在实际运行策略的解释器中执行 `python -m pip install "valkey>=6.1,<7"`。如使用自定义解释器，将其路径传给 `-QmtPython`。
3. Python 二选一：
   - **miniconda/anaconda（推荐，加 `-Conda` 参数）**——已装则直接用；**没装会自动下载
     Miniconda 官方安装器静默装到 `<WorkDir>\miniconda3`**（官方源国内直连可达；
     离线机器可提前下载安装器后传 `-MinicondaUrl "C:\temp\Miniconda3-....exe"`）
   - 或系统 Python ≥ 3.12 在 PATH 上（默认走 venv）
   - 注意：`-WorkDir` 路径不要含空格（Miniconda 静默安装的限制）
4. 管理员 PowerShell

## 一键部署

```powershell
cd <deploy目录>
powershell -ExecutionPolicy Bypass -File .\deploy_qmt_bridge.ps1 `
    -QmtDir  "C:\你的券商QMT目录" `
    -Account "资金账号" `
    -WorkDir "C:\qmt_bridge" `
    -Conda `
    -Proxy   "http://127.0.0.1:7897"        # 没有代理就删掉这行
```

脚本可重复运行：创建客户端环境并安装包，复制服务端文件，安装 VK 服务，生成配置和 `qmt_cli.py`。VK 默认绑定 `127.0.0.1`，使用随机密码，内存上限为 192 MB。pip、conda 和 Miniconda 默认使用清华镜像；可传 `-PipIndex https://pypi.org/simple` 切换 pip 源，其他选项见脚本。

脚本使用 [valkey-windows 社区发行包](https://github.com/valkey-windows/valkey-windows/releases/tag/9.1.4)，安装 `ValkeyBigQMT` 服务。下载受阻时，可传 `-VkZip "C:\path\Valkey-9.1.4-Windows-x64-cygwin-with-Service.zip"` 或 `-VkUrl` 指定镜像。

从源码运行时，脚本安装当前检出的桥接包。单独复制 `deploy\` 时，可用 `-BridgePackage` 指定支持 VK 的源码目录或 wheel。已有服务占用 `6379` 时，可用 `-VkPort` 选择空闲端口，脚本会同步写入两份配置。

**下单默认关闭**：生成的服务端配置 `rpc_allow_order_methods=False`，新部署只应答查询；
确认风控后加 `-AllowOrders` 重跑（或改配置为 True 并重启策略）才开放 buy/sell/cancel。

## 部署后只剩两步人工操作
1. QMT → 模型交易 → 加载 `<QMT目录>\python\BIGQMT_VK_DRYRUN.py` → **运行模式切"实盘"** → 启动
2. 验证（python 路径按所用路线二选一——`-Conda` 用第一行，系统 python venv 用第二行）：
   ```powershell
   & "C:\qmt_bridge\envs\bigqmt\python.exe" "C:\qmt_bridge\client\qmt_cli.py" ping
   & "C:\qmt_bridge\envs\bigqmt-client\Scripts\python.exe" "C:\qmt_bridge\client\qmt_cli.py" ping
   ```
   > PowerShell 里执行带引号的路径必须以 `&` 开头；行尾不要多引号。

## 检查状态（不写任何东西）

```powershell
powershell -File .\deploy_qmt_bridge.ps1 -QmtDir "..." -Account "..." -WorkDir "..." -CheckOnly
```

## 2核4G 内存预算
| 组件 | 占用 |
|---|---|
| 大 QMT（登录+行情） | ~1.5G（关掉不用的面板/自选股能省不少） |
| VK（已限 192mb noeviction） | ≤200M |
| Python 策略进程（pandas+aiohttp） | ~400M |
| 系统剩余 | ~1.5G |

合计约 3.2-3.5G，可用但别再跑其他重负载。新部署的 VK 密码在 `<WorkDir>\valkey\vk_password.txt`，已有部署可继续使用原来的 `valkey_password.txt`。

## 策略代码（可选）
加 `-WithStrategy -StrategySrc "C:\path\to\my_code"` 会把策略目录一起拷到 `<WorkDir>\my_code`；
注意检查 `order_config.json` 里的 `account_num` 与 `-Account` 一致。

## 细节文档
部署原理、传输层对比、排错见 [docs/USER_GUIDE.md](../docs/USER_GUIDE.md) 与 [docs/DEPLOY_QUICKSTART.md](../docs/DEPLOY_QUICKSTART.md)。
