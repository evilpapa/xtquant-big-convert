# 部署快速开始（单账号）

> 面向第一次部署和升级已有部署的最短路径。传输层和多账号说明见 [使用指南](USER_GUIDE.md)；排错见「日志与排错」。

整条链路只有 4 件东西：

- **QMT 侧的服务端：BIGQMT**（一个策略文件加一个包，跑在 QMT 内置 python 进程里，绑定本地 COM 端口）
- **QMT 侧的独立 Com2VK 代理: serial_vk_bridge** (com 端口和 VK 通信)
- **外部的客户端：LOCAL**（pip 装的包，你本地策略系统的 python 环境，可和 QMT 分数不同的环境，没有机器要求，可使用 linux/mac 环境）
- **中间的 VK 服务**（已有实例可直接使用）

服务端放进 QMT，由独立桥接进程通过 COM 端口与 QMT 通信。桥接进程和外部客户端使用同一组 VK 连接参数。

从旧版迁移时，先将两端的连接地址指向同一个 VK 实例。
新配置使用 `BIGQMT_CONFIG`、`BIGQMT_VK_*` 和 `transport="valkey"`。升级时，同步修改 `.env` 和启动环境，把 `BIGQMT_VALKEY_*`、`BIGQMT_SERIAL_VALKEY_*` 分别改成 `BIGQMT_VK_*`、`BIGQMT_SERIAL_VK_*`。串口启动参数使用 `--vk-url`、`--vk-pubsub`，PowerShell 中对应 `-VkUrl`、`-VkPubSub`。
连接 `url` 接受 `valkey://` 或 `redis://`，两种写法都由 `valkey` 包连接 VK 服务。
更新两端代码后，在 QMT 中重新加载下面的新入口。`bigqmt:*` 键名和 RPC 消息格式沿用原格式。

## 部署方式

| 方式 | 放进 QMT 的是什么 | 什么时候选 |
|---|---|---|
| `package`（默认） | 一个包目录 + 三个顶层 `.py` + 一个入口文件，共 4 项 | 升级时可以 `reload_deployment` 热更新，不用重启策略 |
| `single_file` | 一个 `BIGQMT_VK_DRYRUN_ALL_IN_ONE.py`，包和配置都 base64 内嵌在里面 | QMT 目录不方便放多个文件、或想一个文件拷来拷去 |

单文件模式的代价：包内嵌在文件里，升级只能整个文件替换再重启策略，`reload_deployment` 刷不动它。

## 文件汇总

客户端、QMT 策略和独立串口桥接都需要 Python 3.12 或更高版本。直连 VK 的策略还需在实际运行的解释器中安装 `valkey`。

LOCAL 侧文件汇总

| 文件 | 放哪 | 谁放 |
|---|---|---|
| `bigqmt_signal_trader` 包（客户端用的那份） | QMT-PYTHON 的 site-packages | 第 1 步 `pip install` |
| `bigqmt_signal_trader_client_config.py` | 客户端：**和你运行的脚本同一目录** | 第 2 步 `bigqmt-init` 第二个目录问题（回车 = 当前目录） |

BIGQMT `package` 模式下文件汇总
| 文件 | 放哪 | 谁放 |
|---|---|---|
| `bigqmt_signal_trader/`（整个包目录） | QMT 的 `python` 目录 | 第 3 步**你手动拷** |
| `bigqmt_signal_trader_strategy.py` | 同上 | 同上 |
| `bigqmt_signal_trader_vk_rpc_runtime.py` | 同上 | 同上 |
| `BIGQMT_VK_DRYRUN.py`（入口） | 同上 | 同上 |
| `bigqmt_signal_trader_qmt_config.py` | 同上 | 第 2 步 `bigqmt-init` 第一个目录问题（**必须手填**，回车 = 当前目录） |

BIGQMT `single` 模式下文件汇总
| 文件 | 放哪 | 谁放 |
|---|---|---|
| `BIGQMT_VK_DRYRUN_ALL_IN_ONE.py` | QMT 的 `python` 目录 | 第 3 步**你手动拷** |

`bigqmt-init` **只写两份配置**，不拷包、不装依赖、不会自己去找 QMT 装在哪。「QMT 的 `python` 目录」指 QMT 安装目录下叫 `python` 的子目录，不是 `bin.x64`。

## 第 1 步：LOCAL 装包

```powershell
pip install ".[valkey]"  # 在源码根目录执行
```

直连 VK 时，也在 **QMT 策略实际使用的解释器**中运行 `python -m pip install "valkey>=6.1,<7"`，再用 `python -c "import valkey"` 验证。

## 第 2 步：用 `bigqmt-init` 生成配置

不要手抄 `.example.py`。在**能写到 QMT 的 python 目录的机器上**跑，两种写法等价：

```powershell
bigqmt-init
```

```powershell
python -m bigqmt_signal_trader.init_config
```

第一种是 pip 装包时注册的命令；`bigqmt-init` 找不到（PATH 没带上 Scripts 目录）或者你用的是源码检出，就用第二种。

选项题输数字或直接输名字都认，`STOCK`、`valkey`、`package` 这样写也行。每个问题的说明：

| 问题 | 说明 |
|---|---|
| 资金账号 | 服务端和客户端会写同一个，对不上是「查询全空」的头号原因 |
| 账号类型（STOCK） | STOCK / CREDIT / FUTURE / STOCK_OPTION / HUGANGTONG / SHENGANGTONG。信用账户选 CREDIT，选错查出来是整行 0。股票户兼做港股通：生成后把 `BIGQMT_ACCOUNT_TYPE` 手工改成列表 `["STOCK", "HUGANGTONG", "SHENGANGTONG"]` |
| VK 地址 / 端口 / db / 用户名 / 密码 | 选 `valkey` 才问。密码输入不回显，会写进配置文件 |
| 允许远程下单/撤单？（否） | 打开前它会警告：任何能连上这条通道的程序都可以下单。首次部署先留 `否`，验证通过再打开 |
| 部署方式（package） | 见上表 |
| **QMT 的 python 目录（回车则写到当前目录）** | **这里最容易出错。** 填 QMT 安装目录下的 `python`，如 `D:\gjzq_qmt\python`。直接回车会写到你当前所在的目录，服务端找不到配置 |
| 客户端配置写到哪个目录（回车则当前目录） | **你外部程序（要运行的脚本）所在目录。** 客户端靠 `import bigqmt_signal_trader_client_config` 找它，得在 `sys.path` 上——和脚本同目录最省事；放别处要加 `PYTHONPATH` 或设 `BIGQMT_CLIENT_CONFIG_MODULE`。放进 QMT 的 python 目录只有从那里运行才碰巧能用（退回读服务端配置） |

已存在的文件会先问再覆盖，`--force` 跳过询问。

## 第 3 步：拷服务端文件到 QMT（`package` 模式）

单文件模式跳过本步。

先找到 pip 装的包在哪：

```powershell
python -c "import bigqmt_signal_trader_strategy as m, os; print(os.path.dirname(m.__file__))"
```

## 第 4 步：QMT 里运行入口

QMT 策略编辑器**加载并运行 `BIGQMT_VK_DRYRUN.py`**（单文件模式是 `..._ALL_IN_ONE.py`），运行模式切到**实盘**。输出面板看到这两行即成功：

```
[bigqmt_shell] bigqmt_signal_trader 0.3.37 已从 D:\...\python\bigqmt_signal_trader 加载
[bigqmt_rpc] 已启动 queue=bigqmt:rpc:queue:你的账号 background_threads=False
```

第一行的版本号和目录**就是实际加载的**，以后升级看它。

两个会让它看起来启动了、其实没起来的坑：在**策略编辑器界面**直接运行、勾了**「独立 python 进程」**。两种情况下 QMT 不注入任何 API 全局，文件被当普通脚本执行完就结束，`init()` 永远不会调用。面板里 `download globals bound=[]` 是空的就是这个。

## 第 5 步：验证

在客户端机器上，**先 `cd` 到放 `bigqmt_signal_trader_client_config.py` 的目录**（`python -c`
从当前目录找配置）：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_deployment_info())"
```

它回答的是**服务端**在跑哪个版本、从哪个目录加载：

```
{'version': '0.4.0', 'package_dir': 'D:\\...\\python\\bigqmt_signal_trader', 'python_version': '3.12.0', ...}
```

版本对得上，再拉一次行情：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_full_tick(['000001.SZ']))"
```

能打出五档盘口即部署成功。

## 部署后建议跑一次能力探测

不同券商 QMT 暴露的 callable 不一样（下载全局、信用接口、L2 等），一条命令列出本机能力：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); import json; print(json.dumps(xtdata.call_method('probe_capabilities'), ensure_ascii=False, indent=2))"
```

输出三部分：`qmt_globals`（下载/信用/交易全局函数是否绑定）、`contextinfo_methods`（ContextInfo 方法存在性）、`credit_probe`（信用接口只读试调结果）。

另有 `download_probe`（#277）：财务下载「接口暴露」和「独立更新可用」分开报。它真发一次小范围下载（`000001.SZ`、`Capital`、30 天窗口），`functions.<名字>.verdict` 是结论：`update_usable` = 下载真的跑通；`exposed_but_service_unreachable` = SDK 函数在、行情服务不在（大 QMT 里 miniQMT 没开就是这样，`sdk_call.error` 带 SDK 原话，通常是 `无法连接行情服务`）；`not_exposed` = 哪条通道都没有。`readback_existing_rows` 是已有财务行的读取结果，**读得到不等于能更新**，所以单独一个键。服务不在时那次拨号要付 2～3 秒超时，`probe_capabilities` 传 `{"download_probe": false}` 可以跳过，此时 verdict 是 `exposed_untested`。

## 升级已有部署（`package` 模式）

客户端升级只是 `pip install -U`。服务端升级是文件拷贝，有几个地方会踩坑，按这个顺序做，收盘后做。

**1. 看清线上现在是什么。** 先问一遍：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_deployment_info()['version'])"
```

**2. 确认线上没有你没记住的本地改动。** 把线上的包和它声称的那个版本的 tag 逐文件比对。**先把换行符归一化**：Windows 上的文件多半是 CRLF，git 里是 LF，不归一化会看到每一行都不同，把正常状态误判成大量私改。

```powershell
# 在仓库 clone 里，对每个 .py：tr -d '\r' 后比 sha256
tr -d '\r' < src/bigqmt_signal_trader/vk_rpc.py | sha256sum
tr -d '\r' < D:\...\python\bigqmt_signal_trader\vk_rpc.py | sha256sum
```

对不上的文件再和后面几个 tag 比。线上经常是「基线版本 + 几个文件手动更新到了更新的版本」这种混搭，只要每个文件都能对上某个 tag，就是干净的，可以覆盖。对不上任何 tag 的文件才是本地改动，停下来搞清楚。

**3. 确认三个顶层文件有没有变。** `bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_vk_rpc_runtime.py`、`BIGQMT_VK_DRYRUN.py` 是 QMT 自己 exec 的，`reload_deployment` 刷不动它们。新版本里这三个和线上相同，就走热更新；有一个不同，就得重启策略。

**4. 用 QMT 策略实际使用的 Python 编译一遍要变的文件。** 直连 VK 时还需验证该解释器能导入 `valkey`。如果用没有控制台的 `pythonw.exe`，可将编译结果写进文件：

```python
# qmt_compile_check.py
import sys, py_compile
out = ["python " + sys.version.split()[0]]
for path in sys.argv[1:]:
    try:
        py_compile.compile(path, doraise=True); out.append("OK   " + path)
    except Exception as exc:
        out.append("FAIL %s -> %s" % (path, exc))
open(r"C:\temp\qmt_compile_result.txt", "w").write("\n".join(out))
```

```powershell
& "D:\...\bin.x64\pythonw.exe" qmt_compile_check.py "新版本\bigqmt_signal_trader\xtquant_compat.py" "新版本\bigqmt_signal_trader\exec_events.py"
type C:\temp\qmt_compile_result.txt
```

第一行显示执行检查的 Python 版本。有编译失败时，先解决该环境的语法兼容问题。

**5. 备份。** 整个包目录拷到 QMT 的 `python` 目录**之外**，比如 `D:\...\_deploy_backup_20260911_173558`。放在 `python` 目录里 QMT 会扫到。

**6. 拷贝新包，清 `__pycache__`。** 只拷 `.py`，逐字节校验一遍。两个 Python 版本可能不同，部署时使用目标环境生成的字节码。

**7. 热更新。** 三个顶层文件没变的前提下：

```python
xt_trader.reload_deployment("deploy 0.3.37")   # 排期到下一个 adjust tick，立刻返回
xt_trader.reload_status()                       # 1~2 秒后看这个
```

`reload_status` 回 `{'ok': True, 'version_before': '0.3.34', 'version_after': '0.3.37', 'modules_purged': 31, 'seconds': 1.15}` 就是成了。期间约 1 秒的查询会超时。

**8. 验证。** `get_deployment_info()` 的版本变了、终端进程 PID 没变（说明没重启）、日志里没有新的 ERROR、拉一次行情和持仓正常。

**回滚**：把备份目录拷回去，再 `reload_deployment` 一次。

## 常见问题（部署期 90% 的问题都在这里）

| 现象 | 原因与处理 |
|------|-----------|
| 服务端找不到配置 | `bigqmt-init` 问「QMT 的 python 目录」时直接回车了，配置写到了当前目录。把 `bigqmt_signal_trader_qmt_config.py` 挪到 QMT 的 python 目录 |
| 客户端报 `account_id` 为空 / 连到 127.0.0.1 / 连的是别的账号 | 客户端没找到 `bigqmt_signal_trader_client_config.py`：它靠 import 找，必须和运行的脚本同目录（或在 `PYTHONPATH` 上、或 `BIGQMT_CLIENT_CONFIG_MODULE` 指定）。`python -c` 是从**当前目录**找。找不到时退回服务端那份 `bigqmt_signal_trader_qmt_config.py`，所以在 QMT 目录里跑能通、换个目录就不通 |
| 查询全空但账户有数据 | 账号没对上（服务端 `BIGQMT_ACCOUNT_ID` vs 客户端），或 QMT 不在实盘模式 |
| 信用账户查出来整行 0 | `bigqmt-init` 的账号类型选了 STOCK，应选 CREDIT |
| QMT 提示需要 Python 3.12 或更高版本 | 检查并升级 QMT 策略实际使用的解释器 |
| 启动面板没有任何输出 | 文件没拷全（4 项缺一不可），或加载的是 runtime 文件而不是 DRYRUN 入口 |
| 面板像正常启动然后"结束运行"，外部连不上 | 看 `download globals bound=[]` 是不是**空的**、有没有 `init ok`。空的说明 QMT 没注入任何 API 全局——文件被当**普通脚本**执行。两个已知原因：在**策略编辑器界面**运行、勾了**「独立 python 进程」**。0.3.8 起入口会直接把这段话打出来（issue #123） |
| 改了代码但没生效 | QMT 跨重跑保留 `sys.modules`，**拷贝本身不生效**。先用 `get_deployment_info()` 确认跑的是哪个 build，再 `reload_deployment` |
| `reload_deployment` 后版本没变 | 改的是三个顶层文件之一，热更新刷不动，重启策略 |
| 升级后策略报 `SyntaxError` | 用 QMT 策略实际使用的解释器编译变更文件，定位不兼容的语法 |

## 改完代码怎么生效

**0.3.8 起不用重启**：

```python
xt_trader.reload_deployment("why")   # 排期到下一个 adjust tick
xt_trader.reload_status()            # -> {'ok': True, 'modules_purged': 28,
                                     #     'version_before': ..., 'version_after': ...}
```

约 1 秒，期间约 1 秒的查询会超时（服务正在重建）。

**但改这三个文件仍然要重启策略**：`bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_vk_rpc_runtime.py`、`BIGQMT_VK_DRYRUN.py`——QMT 自己 exec 它们，模块没法 reload 自己所在的模块。单文件部署整个都是这种情况。

更细的排错（日志位置、日志保留策略、启动诊断字段）见 [使用指南](USER_GUIDE.md)「日志与排错」。
