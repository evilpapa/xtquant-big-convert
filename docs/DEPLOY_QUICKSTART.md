# 部署快速开始（单账号）

> 面向首次部署和升级已有部署的最短路径。传输层与多账号见 [使用指南](USER_GUIDE.md)；详细排错见「[日志与排错](USER_GUIDE.md#日志与排错)」。

整条链路包含 4 个组件（客户端、QMT 策略、独立串口桥接均需 **Python 3.12+**）：

1. **QMT 侧服务端（BIGQMT）**：策略入口 + 包，运行在 QMT 内置 Python 进程，绑定本地 COM 端口（或直连 VK）。
2. **独立代理（`serial_vk_bridge`）**：在 QMT 进程外负责 COM 端口与 VK 通信。
3. **外部客户端（LOCAL）**：你的策略环境（不限系统，支持 Linux / macOS / Windows）。
4. **VK 服务**：桥接与客户端连接同一实例（`url` 接受 `valkey://` 或 `redis://`，均由 `valkey` 包处理）。

> **旧版迁移提示**：新配置使用 `BIGQMT_CONFIG`、`BIGQMT_VK_*`、`transport="valkey"`；升级时把 `.env` 与环境里的 `BIGQMT_VALKEY_*` / `BIGQMT_SERIAL_VALKEY_*` 改为 `BIGQMT_VK_*` / `BIGQMT_SERIAL_VK_*`，串口参数改用 `--vk-url` / `--vk-pubsub`（PowerShell 为 `-VkUrl` / `-VkPubSub`）。`bigqmt:*` 键名与 RPC 格式不变。

---

## 部署方式与文件落位

| 方式 | 放入 QMT `python` 目录的内容 | 适用场景与代价 |
|------|-----------------------------|---------------|
| `package`（默认） | `bigqmt_signal_trader/`、`bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_vk_rpc_runtime.py`、`BIGQMT_VK_DRYRUN.py`（共 4 项）+ `bigqmt_signal_trader_qmt_config.py` | 支持 `reload_deployment()` 热更新，无需重启策略 |
| `single_file` | `BIGQMT_VK_DRYRUN_ALL_IN_ONE.py`（包与配置均 base64 内嵌） | 单文件拷贝方便；升级须整文件替换并重启策略，不支持热更新 |

- **LOCAL 客户端**：包安装在客户端 Python 的 `site-packages`；`bigqmt_signal_trader_client_config.py` 放在**运行脚本同目录**（由 `bigqmt-init` 第 2 个目录问题生成）。
- 注意：`bigqmt-init` **只写配置、不拷包、不装依赖**；QMT 目标目录是安装目录下的 `python` 子目录，不是 `bin.x64`。

---

## 第 1 步：LOCAL 装包

```powershell
pip install ".[valkey]"   # 在源码根目录执行
```

若 QMT 策略直连 VK，在 QMT 策略实际解释器中执行 `python -m pip install "valkey>=6.1,<7"` 并用 `python -c "import valkey"` 验证。

## 第 2 步：用 `bigqmt-init` 生成配置

在能写入 QMT `python` 目录的机器上运行（两种写法等价）：

```powershell
bigqmt-init
# 或（PATH 无 Scripts / 源码检出时）
python -m bigqmt_signal_trader.init_config
```

选项题支持输编号或名称（如 `STOCK` / `valkey` / `package`）；已存在文件会先询问覆盖（`--force` 跳过）。关键问题说明：

| 问题 | 说明 |
|------|------|
| 资金账号 | 服务端与客户端须一致，对不上是「查询全空」首要原因 |
| 账号类型（`STOCK`） | `STOCK` / `CREDIT` / `FUTURE` / `STOCK_OPTION` / `HUGANGTONG` / `SHENGANGTONG`；信用户必选 `CREDIT`；A 股兼港股通可在生成后改为列表 `["STOCK", "HUGANGTONG", "SHENGANGTONG"]` |
| VK 参数 | 选 `valkey` 时询问（地址/端口/db/用户名/密码，密码不回显） |
| 允许远程下单/撤单？（否） | 首次建议保持「否」，验证通过后再开 |
| 部署方式（`package`） | 见上表 |
| **QMT 的 python 目录** | **务必手填**（如 `D:\gjzq_qmt\python`）；直接回车会写到当前目录导致服务端找不到配置 |
| 客户端配置目录 | 填外部脚本所在目录（回车 = 当前目录）；放别处需配 `PYTHONPATH` 或 `BIGQMT_CLIENT_CONFIG_MODULE` |

## 第 3 步：拷服务端文件到 QMT（`package` 模式）

单文件模式跳过本步。定位已安装包的目录（源码检出则用 `src/`），将上述 4 项复制到 QMT 的 `python` 目录：

```powershell
python -c "import bigqmt_signal_trader_strategy as m, os; print(os.path.dirname(m.__file__))"
```

## 第 4 步：QMT 里运行入口

在 QMT **模型交易**中加载并运行 `BIGQMT_VK_DRYRUN.py`（单文件为 `..._ALL_IN_ONE.py`），模式切为**实盘**。面板出现以下两行即成功（第一行为实际加载的版本与目录）：

```text
[bigqmt_shell] bigqmt_signal_trader 0.3.37 已从 D:\...\python\bigqmt_signal_trader 加载
[bigqmt_rpc] 已启动 queue=bigqmt:rpc:queue:你的账号 background_threads=False
```

> 勿在策略编辑器直接运行，也勿勾选「独立 python 进程」，否则 QMT 不注入 API 全局（面板显示 `download globals bound=[]`），`init()` 不会被调用。

## 第 5 步：验证与能力探测

在客户端 `cd` 到存放 `bigqmt_signal_trader_client_config.py` 的目录：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_deployment_info())"
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); print(xtdata.get_full_tick(['000001.SZ']))"
```

确认版本号匹配且返回五档盘口后，建议跑一次能力探测，查看当前券商终端实际暴露的 `qmt_globals`、`contextinfo_methods`、`credit_probe` 与 `download_probe`：

```powershell
python -c "from bigqmt_signal_trader.xtquant_compat import configure, xtdata; configure(); import json; print(json.dumps(xtdata.call_method('probe_capabilities'), ensure_ascii=False, indent=2))"
```

> `download_probe` 会发一次小范围财务下载（`000001.SZ` / `Capital` / 30 天）区分 `update_usable`、`exposed_but_service_unreachable`（如 miniQMT 行情服务未开）和 `not_exposed`；`readback_existing_rows` 仅表示本地有旧行，不等同于可更新。传入 `{"download_probe": false}` 可跳过 2–3 秒探测（verdict 为 `exposed_untested`）。

---

## 升级已有部署（`package` 模式）

建议收盘后按序执行（客户端升级直接 `pip install -U`）：

1. **查线上版本**：`xtdata.get_deployment_info()['version']`。
2. **排查本地私改**：先归一化 CRLF 再比对 sha256，确认每个 `.py` 都能对应到仓库某 tag：

   ```powershell
   tr -d '\r' < src/bigqmt_signal_trader/vk_rpc.py | sha256sum
   tr -d '\r' < D:\...\python\bigqmt_signal_trader\vk_rpc.py | sha256sum
   ```

3. **检查 3 个顶层文件**：`bigqmt_signal_trader_strategy.py`、`bigqmt_signal_trader_vk_rpc_runtime.py`、`BIGQMT_VK_DRYRUN.py` 均无变化可走热更新，任一变化须重启策略。
4. **目标解释器编译检查**：用 QMT 实际 Python 校验语法（直连 VK 时顺便检查 `import valkey`）；使用无控制台的 `pythonw.exe` 可写结果到文件：

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
   & "D:\...\bin.x64\pythonw.exe" qmt_compile_check.py "新版本\bigqmt_signal_trader\xtquant_compat.py"
   type C:\temp\qmt_compile_result.txt
   ```

5. **目录外备份**：将原包目录备份到 QMT `python` 目录**之外**（放里面会被 QMT 扫描）。
6. **拷贝与清理**：仅拷 `.py` 逐字节校验，删除旧 `__pycache__`。
7. **热更新（0.3.8+，约 1 秒，期间查询短暂超时）** 或重启策略：

   ```python
   xt_trader.reload_deployment("deploy 0.3.37")
   xt_trader.reload_status()   # {'ok': True, 'version_before': ..., 'version_after': ..., 'modules_purged': ...}
   ```

8. **验证与回滚**：确认 `get_deployment_info()` 版本已变、PID 未变、无新 ERROR 且行情/持仓正常。回滚只需拷回备份目录再 `reload_deployment()`。

---

## 常见问题速查

| 现象 | 原因与处理 |
|------|-----------|
| 服务端找不到配置 | `bigqmt-init` 第一个目录直接回车写到了当前目录；移到 QMT `python` 目录 |
| 客户端报 `account_id` 为空 / 连错主机或账号 | 未找到 `bigqmt_signal_trader_client_config.py`（须在脚本同目录 / `PYTHONPATH` / `BIGQMT_CLIENT_CONFIG_MODULE`；`python -c` 查当前目录），退回读了服务端配置 |
| 查询全空但账户有数据 | 账号不匹配或 QMT 未切实盘模式 |
| 信用账户查出整行 0 | 账号类型选了 `STOCK`，应选 `CREDIT` |
| 提示需要 Python 3.12+ | 检查并升级 QMT 策略实际解释器 |
| 面板无输出，或立刻「结束运行」 | 4 项未拷全、加载了错误文件，或在编辑器运行 / 勾了「独立 python 进程」（`download globals bound=[]`） |
| 改代码 / `reload_deployment` 未生效 | QMT 跨运行保留 `sys.modules`，须执行 `reload_deployment()`；若改了 3 个顶层文件或单文件模式，则须重启策略 |
| 升级后 `SyntaxError` | 用 QMT 实际 Python 编译变更文件定位不兼容语法 |

更细的排错（日志位置、日志保留策略、启动诊断字段）见 [使用指南](USER_GUIDE.md)「日志与排错」。
