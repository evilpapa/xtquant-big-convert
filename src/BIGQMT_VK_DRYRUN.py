#coding:utf-8
"""按路径加载桥接模块的 QMT 策略入口。

部分券商的导入白名单会拒绝项目包名。此入口对项目内模块按路径编译，
包括包内相对导入。标准库和 QMT 模块沿用正常导入机制。
"""
import builtins as _builtins
import os
import sys
import types

if sys.version_info < (3, 12):
    raise RuntimeError("xtquant-big-convert requires Python 3.12 or newer.")

try:
    import importlib as _importlib
except ImportError:
    # 部分旧版 QMT 的 python36.zip 缺少 importlib。
    # 本地加载器仅需 import_module 和 reload，因此只提供这两个兼容入口。
    _importlib = types.ModuleType("importlib")

    def _fallback_import_module(name, package=None):
        if package or str(name).startswith("."):
            raise ImportError("relative import requires the standard importlib package")
        return _builtins.__import__(name, globals(), locals(), ("*",), 0)

    def _fallback_reload(module):
        return module

    _importlib.import_module = _fallback_import_module
    _importlib.reload = _fallback_reload
    sys.modules["importlib"] = _importlib


_LOCAL_ROOTS = (
    "bigqmt_signal_trader",
    "bigqmt_signal_trader_strategy",
    "bigqmt_signal_trader_vk_rpc_runtime",
    "bigqmt_signal_trader_qmt_config",
)
_ORIGINAL_IMPORT        = _builtins.__import__
_ORIGINAL_IMPORT_MODULE = _importlib.import_module
_ORIGINAL_RELOAD        = _importlib.reload


def _known_qmt_python_dir():
    # 从 sys.path 查找 QMT 的 python 目录，兼容不同安装位置和启动方式。
    # 未找到时返回空字符串。
    for p in sys.path:
        if p and r"\python" in p and os.path.isdir(p):
            return p
    return ""


try:
    _SOURCE_ROOT = os.path.dirname(os.path.abspath(__file__))
except Exception:
    _SOURCE_ROOT = _known_qmt_python_dir()
if not _SOURCE_ROOT:
    _SOURCE_ROOT = _known_qmt_python_dir()


def _is_local_module(name):
    return any(name == root or name.startswith(root + ".") for root in _LOCAL_ROOTS)


def _resolve_name(name, module_globals, level):
    if not level:
        return name
    package = (module_globals or {}).get("__package__") or (module_globals or {}).get("__name__", "")
    if not package:
        raise ImportError("relative import without package")
    for unused in range(level - 1):
        if "." not in package:
            raise ImportError("relative import beyond top-level package")
        package = package.rsplit(".", 1)[0]
    return package + ("." + name if name else "")


def _find_local_source(name):
    relative = name.replace(".", os.sep)
    dirs = []
    if _SOURCE_ROOT:
        dirs.append(_SOURCE_ROOT)
    for p in sys.path:
        if p and os.path.isdir(p) and p not in dirs:
            dirs.append(p)
    for d in dirs:
        package_init = os.path.join(d, relative, "__init__.py")
        if os.path.isfile(package_init):
            return package_init, True
        module_file = os.path.join(d, relative + ".py")
        if os.path.isfile(module_file):
            return module_file, False
    raise ModuleNotFoundError("local source not found: %s" % name, name=name)


def _set_parent_attribute(name, module):
    if "." not in name:
        return
    parent_name, child_name = name.rsplit(".", 1)
    parent = _load_local_module(parent_name)
    setattr(parent, child_name, module)


# 策略文件损坏时可能只报 NameError，并显示约 200 字符的令牌（#102）。
# 一次实际报错如下：
#
#     File "...\bigqmt_signal_trader_strategy.py", line 1, in <module>
#         MiFBOecYoHXT4UUBBOIr3m5aTVbA5Rbt6OnG52cfBT5EAtPG9kA7kQnEsKuDUOORy...
#     NameError: name 'MiFBOecYoHXT4UUB...' is not defined
#
# Python 将首行当作变量名。此时保存下来的可能是下载页面或代理返回的令牌，
# 需要重新取得源码。更换版本无法修复已经损坏的文件。
_TOKEN_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_=+/")


def _looks_like_a_token_not_python(source):
    """首个非空行看起来是下载令牌时返回 True。

    只识别无空白且由 base64 字符组成的长行。即便误判，该行作为孤立标识符也会执行失败。
    """
    try:
        if isinstance(source, bytes):
            source = source.decode("utf-8", "replace")
        for line in source.splitlines():
            line = line.strip()
            if not line:
                continue
            return (len(line) >= 40
                    and not any(char.isspace() for char in line)
                    and all(char in _TOKEN_CHARS for char in line))
    except Exception:
        pass
    return False


def _load_local_module(name):
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    source_path, is_package = _find_local_source(name)
    if "." in name:
        _load_local_module(name.rsplit(".", 1)[0])
    module = types.ModuleType(name)
    module.__file__ = source_path
    module.__package__ = name if is_package else name.rpartition(".")[0]
    if is_package:
        module.__path__ = [os.path.dirname(source_path)]
    module_builtins = dict(_builtins.__dict__)
    module_builtins["__import__"] = _local_import
    module.__dict__["__builtins__"] = module_builtins
    module.__dict__["__bigqmt_load_local_module"] = _load_local_module
    sys.modules[name] = module
    # QMT 白名单会拒绝根包加载时立即导出的部分内容。
    if name == "bigqmt_signal_trader":
        return module
    source = None
    try:
        with open(source_path, "rb") as source_file:
            source = source_file.read()
        exec(compile(source, source_path, "exec"), module.__dict__)
    except Exception as exc:
        sys.modules.pop(name, None)
        # 禁用策略 FileIO 的终端会在 open() 处抛出 PermissionError（#420）。
        # 此时 source 尚未读入，避免诊断代码用 UnboundLocalError 掩盖原异常。
        if source is not None and _looks_like_a_token_not_python(source):
            raise RuntimeError(
                "%s is not Python source -- its first line is one long token, "
                "so the file was saved from something other than the code "
                "(a download page, a mirror that served a token). Fetch it "
                "again: pip install --upgrade xtquant-big-convert then "
                "xt_trader.sync_deployment(), or copy src/ from the release. "
                "A different version will not help; every version fails the "
                "same way on this file." % source_path)
        print("[bigqmt_shell] 加载失败 module=%s path=%s error=%s：%s"
              % (name, source_path, type(exc).__name__, exc))
        raise
    _set_parent_attribute(name, module)
    return module


# 记住已经确认不是模块的名字，避免每次都遍历 sys.path。
_MISSING_LOCAL_MODULES = set()


def _local_import(name, module_globals=None, module_locals=None, fromlist=(), level=0):
    absolute_name = _resolve_name(name, module_globals, level)
    if not _is_local_module(absolute_name):
        return _ORIGINAL_IMPORT(name, module_globals, module_locals, fromlist, level)
    module = _load_local_module(absolute_name)
    for child in fromlist or ():
        if child != "*":
            child_name = absolute_name + "." + child
            # fromlist 中通常是模块属性。未命中时需要扫描源码目录和 sys.path。
            # 缓存未命中的名字后，实测 ping 往返从约 1800 毫秒降至 95～200 毫秒。
            if child_name in _MISSING_LOCAL_MODULES:
                continue
            try:
                _load_local_module(child_name)
            except ModuleNotFoundError:
                _MISSING_LOCAL_MODULES.add(child_name)
    if fromlist:
        return module
    return _load_local_module(absolute_name.split(".", 1)[0])


def _local_import_module(name, package=None):
    if _is_local_module(name):
        return _load_local_module(name)
    return _ORIGINAL_IMPORT_MODULE(name, package)


def _local_reload(module):
    if _is_local_module(getattr(module, "__name__", "")):
        return _load_local_module(module.__name__)
    return _ORIGINAL_RELOAD(module)


def _clear_local_modules():
    for name in list(sys.modules):
        if _is_local_module(name):
            sys.modules.pop(name, None)


def _stop_previous_rpc_service():
    """Release the previous QMT strategy's socket before clearing its module.

    QMT can re-execute this entry in the same Python process.  The old strategy
    module owns the RPC service and its ZMQ ROUTER socket, so dropping that
    module from ``sys.modules`` first would make the service unreachable and
    leave its port bound for the next strategy start.
    """
    previous = sys.modules.get("bigqmt_signal_trader_strategy")
    reset = getattr(previous, "reset_app", None)
    if not callable(reset):
        return
    try:
        reset()
        print("[bigqmt_shell] 已停止之前的 RPC 服务")
    except Exception as exc:
        # 即使旧实例停止失败，也继续重新加载，让 QMT 报告实际启动错误。
        print("[bigqmt_shell] 停止之前的 RPC 服务失败：%s" % exc)


_stop_previous_rpc_service()
_clear_local_modules()
_importlib.import_module = _local_import_module
_importlib.reload = _local_reload
print("[bigqmt_shell] importlib 入口 source_root=%s" % _SOURCE_ROOT)


def _fallback_account_id():
    for name in ("BIGQMT_ACCOUNT_ID", "account", "account_id", "accountID"):
        value = globals().get(name)
        if value:
            return str(value)
    return ""


try:
    _local_import("bigqmt_signal_trader.adapters.vk_common", globals(), fromlist=("*",))
    _local_import("bigqmt_signal_trader.vk_rpc", globals(), fromlist=("*",))
    _strategy = _local_import("bigqmt_signal_trader_strategy", globals(), fromlist=("*",))
    _strategy.reset_app()
except Exception as bridge_preload_error:
    print("[bigqmt_shell] 桥接预加载失败：%s" % bridge_preload_error)

_runtime = _local_import("bigqmt_signal_trader_vk_rpc_runtime", globals(), fromlist=("*",))


def _load_local_config():
    return _local_import("bigqmt_signal_trader_qmt_config", globals(), fromlist=("*",))


try:
    _config = _load_local_config()
    BIGQMT_CONFIG = _runtime.local_vk_config(_config)
    forced_transport = globals().get("BIGQMT_FORCE_TRANSPORT")
    if forced_transport:
        BIGQMT_CONFIG = dict(BIGQMT_CONFIG or {})
        BIGQMT_CONFIG["transport"] = str(forced_transport)
    print("[bigqmt_shell] 本地 RPC 配置已加载 transport=%s keys=%s" % (
        (BIGQMT_CONFIG or {}).get("transport", "valkey"),
        sorted((BIGQMT_CONFIG or {}).keys()),
    ))
    _runtime.configure_runtime_vk(BIGQMT_CONFIG)
except Exception as rpc_config_error:
    print("[bigqmt_shell] 本地 RPC 配置加载失败：%s" % rpc_config_error)

try:
    _config = _load_local_config()
    BIGQMT_ACCOUNT_ID = getattr(_config, "BIGQMT_ACCOUNT_ID", "")
    print("[bigqmt_shell] 本地账户配置已加载=%s" % bool(BIGQMT_ACCOUNT_ID))
    _runtime.configure_runtime_account(BIGQMT_ACCOUNT_ID)
except Exception as account_config_error:
    print("[bigqmt_shell] 本地账户配置加载失败：%s" % account_config_error)
    account_id = _fallback_account_id()
    if account_id:
        _runtime.configure_runtime_account(account_id)

try:
    # QMT 只向挂载文件注入全局函数，因此在此处传入 globals()。
    # 使用策略模块维护的统一名单，避免手工复制时遗漏 query_credit_account（#202）。
    qmt_extra = _runtime.capture_qmt_injected_funcs(globals())
    print("[bigqmt_shell] 下载相关全局函数已绑定=%s" % sorted(k for k in qmt_extra if "download" in k or "down_history" in k))
    _runtime.bind_runtime_api(
        passorder_func=globals().get("passorder"),
        cancel_func=globals().get("cancel"),
        get_trade_detail_data_func=globals().get("get_trade_detail_data"),
        extra_funcs=qmt_extra or None,
    )
    # 作为策略运行时，QMT 会注入下单、交易查询和下载函数。
    # 普通脚本也会执行模块顶层，但不会调用 init() 或启动 RPC（#123）。
    # 文件声明 UTF-8，以便在 QMT 策略输出面板显示中文诊断。
    if not any(name in globals() for name in
               ("passorder", "get_trade_detail_data", "download_history_data")):
        print("[bigqmt_shell] 警告：QMT 未注入任何 API 全局函数"
              "（passorder / get_trade_detail_data / download_history_data），"
              "当前文件正作为普通脚本运行，而不是策略。init() 不会被调用，"
              "RPC 服务不会启动，运行将在此结束且没有监听器。"
              "两个已知原因：从策略编辑窗口直接运行，而不是添加到模型交易；"
              "或启用了“独立 Python 进程”，使 QMT 以 __main__ 方式执行文件"
              "但不调用 init()。参见问题 #123。")
except NameError:
    pass


init = _runtime.init
handlebar = _runtime.handlebar
stop = _runtime.stop
adjust = _runtime.adjust
order_callback = _runtime.order_callback
deal_callback = _runtime.deal_callback
# QMT 只向挂载文件调用回调，此处需暴露信用账户柜台查询回调（#202）。
credit_account_callback = _runtime.credit_account_callback
