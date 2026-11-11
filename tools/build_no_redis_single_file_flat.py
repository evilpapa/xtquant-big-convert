# -*- coding: utf-8 -*-
"""Shared flat-code embedding helpers for the named-pipe builder."""
import ast
import io
import os
import tokenize

import build_single_file as bsf

# This script lives in tools/ in a source checkout; a byte-identical copy ships
# inside the wheel as package data so bigqmt-init works from a pip install too.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDENT = "    "


def _decode(data):
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def indent_protected(text):
    """把模块源码缩进 INDENT 放进函数体，但保持多行字符串内部行原样，
    避免缩进改变字符串字面量的内容。"""
    lines = text.splitlines(keepends=True)
    keep_flat = set()  # 0-based 行号：多行字符串 token 的"非首行"都要保持原样
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except Exception:
        toks = []
    for tok in toks:
        if tok.type == tokenize.STRING and tok.start[0] != tok.end[0]:
            # 多行字符串：从第二行到末行都视为字符串内部（首行是代码+引号）
            s, e = tok.start[0] - 1, tok.end[0] - 1
            for ln in range(s + 1, e + 1):
                keep_flat.add(ln)
    out = []
    for i, line in enumerate(lines):
        if not line.strip():
            out.append(line)
        elif i in keep_flat:
            out.append(line)
        else:
            out.append(INDENT + line)
    return "".join(out)


def module_level_names(source):
    """AST 收集模块顶层绑定的名字。

    这些名字在 def _mod_N() 函数体里必须 global 化，否则被内层函数/类
    方法闭包引用时会被编译器变成 cell 变量，模块代码里 global 更新
    module.__dict__ 不会同步到 cell —— 造成模块级状态错位。
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    names = set()

    def collect_target(node):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, (ast.Tuple, ast.List)):
            for elt in node.elts:
                collect_target(elt)
        elif isinstance(node, ast.Starred):
            collect_target(node.value)

    def walk_body(stmts):
        for node in stmts:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    names.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name != "*":
                        names.add(alias.asname or alias.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    collect_target(target)
            elif isinstance(node, ast.AnnAssign):
                collect_target(node.target)
            elif isinstance(node, ast.AugAssign):
                collect_target(node.target)
            elif isinstance(node, (ast.For, ast.AsyncFor)):
                collect_target(node.target)
                walk_body(node.body)
                walk_body(node.orelse)
            elif isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    if item.optional_vars is not None:
                        collect_target(item.optional_vars)
                walk_body(node.body)
            elif isinstance(node, ast.If):
                walk_body(node.body)
                walk_body(node.orelse)
            elif isinstance(node, ast.While):
                walk_body(node.body)
                walk_body(node.orelse)
            elif isinstance(node, ast.Try):
                walk_body(node.body)
                walk_body(node.orelse)
                for handler in node.handlers:
                    if handler.name:
                        names.add(handler.name)
                    walk_body(handler.body)
                walk_body(node.finalbody)
            elif isinstance(node, ast.Global):
                names.update(node.names)
            elif isinstance(node, ast.Delete):
                for target in node.targets:
                    collect_target(target)

    walk_body(tree.body)
    # 排除 shell 注入的名字（它们不在模块源码里，且不属于模块级绑定）
    for excluded in ("__export_module__", "_SHELL_CONFIG", "__builtins__"):
        names.discard(excluded)
    return names


def global_decl_lines(names, per_line=8):
    """把模块级名字分组生成 "global a, b, ..." 行（global 不支持换行）。"""
    out = []
    for i in range(0, len(names), per_line):
        chunk = ", ".join(sorted(names)[i:i + per_line])
        out.append(INDENT + "global " + chunk)
    return out


def module_funcs_block(sources, extra):
    """生成: 若干 def _mod_N(): 函数体 + _MODULE_FUNCS 映射表。"""
    funcs = []  # [ (index, rel, source_text) ]
    all_items = sorted(sources.items()) + sorted(extra.items())
    for idx, (rel, data) in enumerate(all_items):
        text = _decode(data).rstrip("\n")
        funcs.append((idx, rel, text))
    lines = []
    for idx, rel, text in funcs:
        lines.append("def _mod_%d():" % idx)
        try:
            has_stmt = bool(ast.parse(text).body)
        except SyntaxError:
            has_stmt = False
        if text.strip() and has_stmt:
            global_names = module_level_names(text)
            if global_names:
                lines.extend(global_decl_lines(global_names))
            lines.append(indent_protected(text))
        else:
            lines.append(INDENT + "pass")
        lines.append("")
    lines.append("")
    lines.append("_MODULE_FUNCS = {")
    for idx, rel, text in funcs:
        lines.append('    "%s": _mod_%d,' % (rel, idx))
    lines.append("}")
    return "\n".join(lines)


FLAT_TEMPLATE = r'''#coding:gbk
"""Single-file self-contained Big QMT dry-run strategy (named-pipe version).

FLAT build: the embedded sources are written inline as REAL Python code
(NOT strings, NOT base64). Each module source is indented inside a
``def _mod_N():`` body; at runtime its ``__code__`` is executed in a fresh
module namespace (``exec(func.__code__, module.__dict__)``).

So you can search / read / edit any embedded module as normal code, e.g.
bigqmt_signal_trader_redis_rpc_runtime, with IDE syntax highlighting and
indentation support, and no triple-quote escaping issues.

All custom modules the bridge needs are embedded into this file and
loaded in-memory at runtime:

    * bigqmt_signal_trader package (all submodules)
    * bigqmt_signal_trader_strategy
    * bigqmt_signal_trader_redis_rpc_runtime
    * bigqmt_signal_trader_local_config  (generated from the config block below)

The RPC transport is forced to the Windows named pipe (ctypes/kernel32 only).
No redis or outbound socket connections: this build is for
QMT sandboxes that kill the process when it dials out. The redis connection
fields in the config below are ignored in pipe mode.

This file never imports any other custom module from disk. The embedded
loader below resolves relative imports against the in-memory modules, so you
can copy this one file to a QMT python directory and run it as a strategy
without shipping the package alongside.

Callbacks: the pipe has no push channel. on_stock_order / on_stock_trade /
on_order_error are synthesized client-side by polling queries (#372, default
1s, BIGQMT_EXEC_POLL_SECONDS). Whole-quote push (subscribe_whole_quote) is
disabled in this build -- poll get_full_tick instead.

Edit BIGQMT_ACCOUNT_ID below before running.
"""
import builtins as _builtins
import importlib as _importlib
import os
import sys
import types


# =========================== config block ===========================
BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"

BIGQMT_ACCOUNT_TYPE = "STOCK"

BIGQMT_REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "username": "",
    "password": "",
    # Keep order RPC disabled unless you explicitly want remote order/cancel.
    # This build cannot import redis at all, so the redis block must not be
    # emitted -- otherwise every consumer dials a host that is not there
    # (issues #145 / #147).
    "redis_enabled": False,
    "rpc_allow_order_methods": False,
    # 命名管道传输（本机同主机；ctypes/kernel32，零第三方依赖，不碰 socket）。
    "transport": "pipe",
    # 原生 xtdata SDK 的调用会拨 127.0.0.1:58610；QMT 把它判为「非法IP」并弹
    # 模态框，卡住策略线程。pipe 构建默认关掉（显式写出来，避免任何 provider
    # 忘了按 transport 推导时回落到构造器默认的 True）。
    "native_xtdata_enabled": False,
    "pipe": {
        "pipe_name": "bigqmt_rpc",   # 管道名前缀，账号 id 自动拼在后面；客户端要一致
    },
    # Requests drain through QMT's run_time("adjust", ...) callback, on the main
    # strategy thread. get_trade_detail_data returns EMPTY off that thread, so
    # order/query methods must not be moved to a background thread.
    "rpc_process_in_listener": True,
    "rpc_listener_methods": ("*",),
    "rpc_background_threads": False,
    "schedule_adjust": True,
    "schedule_adjust_interval": "100nMilliSecond",
    "full_tick_cache_enabled": False,
    "full_tick_demand_ttl_seconds": 10,
    "full_tick_cache_ttl_seconds": 10,
    "full_tick_refresh_interval_seconds": 0.5,
    "full_tick_market_refresh_interval_seconds": 3,
    "full_tick_refresh_max_wall_seconds": 0.3,
    "full_tick_max_requests": 8,
    "download_jobs_enabled": False,
    "download_job_chunk_size": 10,
    "download_job_max_wall_seconds": 0.5,
    "download_job_ttl_seconds": 3600,
    "exec_events_enabled": True,
    "exec_events_debug_raw_fields": False,
}
# ===================================================================


# ========================= flat embedded modules ====================
# NOTE: real code. Each module source lives in a def _mod_N(): body below
# and is executed via its __code__ in a dedicated module namespace.
# Build-time AST analysis injects "global <name>" declarations at the top
# of every _mod_N() body so that module-level names resolve through the
# module dict exactly like a real module (no stale closure-cell snapshots).
_SHELL_CONFIG = {
    "BIGQMT_ACCOUNT_ID": BIGQMT_ACCOUNT_ID,
    "BIGQMT_ACCOUNT_TYPE": BIGQMT_ACCOUNT_TYPE,
    "BIGQMT_REDIS_CONFIG": BIGQMT_REDIS_CONFIG,
}

__MODULE_FUNCS_BLOCK__

def _mod_local_config():
    # Read straight from the shell config block and write into the module dict
    # via global. Do NOT rely on __export_module__/closure cells/module lookup:
    # QMT's per-process reload can leave those paths stale, silently yielding an
    # empty account and disabling the RPC service.
    global BIGQMT_ACCOUNT_ID, BIGQMT_ACCOUNT_TYPE, BIGQMT_REDIS_CONFIG
    BIGQMT_ACCOUNT_ID = _SHELL_CONFIG.get("BIGQMT_ACCOUNT_ID", "")
    BIGQMT_ACCOUNT_TYPE = _SHELL_CONFIG.get("BIGQMT_ACCOUNT_TYPE", "STOCK")
    BIGQMT_REDIS_CONFIG = _SHELL_CONFIG.get("BIGQMT_REDIS_CONFIG", {})

_MODULE_FUNCS["bigqmt_signal_trader_local_config.py"] = _mod_local_config
# ===================================================================


_LOCAL_ROOTS = (
    "bigqmt_signal_trader",
    "bigqmt_signal_trader_strategy",
    "bigqmt_signal_trader_redis_rpc_runtime",
    "bigqmt_signal_trader_local_config",
    "bigqmt_no_redis",
)
_ORIGINAL_IMPORT = _builtins.__import__
_ORIGINAL_IMPORT_MODULE = _importlib.import_module
_ORIGINAL_RELOAD = _importlib.reload

# QMT runs the strategy via exec, where __file__ may be absent; fall back safely.
_STRATEGY_FILE_DIR = os.path.dirname(os.path.abspath(globals().get("__file__", os.getcwd())))


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
    relative = name.replace(".", "/")
    package_init = relative + "/__init__.py"
    if package_init in _MODULE_FUNCS:
        return package_init, True
    module_file = relative + ".py"
    if module_file in _MODULE_FUNCS:
        return module_file, False
    raise ModuleNotFoundError("local source not found: %s" % name, name=name)


def _set_parent_attribute(name, module):
    if "." not in name:
        return
    parent_name, child_name = name.rsplit(".", 1)
    parent = _load_local_module(parent_name)
    setattr(parent, child_name, module)


def _export_module(ns):
    """Only used by the hand-written _mod_local_config() body: its frame
    locals() are re-synced into the module namespace dict."""
    module = sys.modules.get(ns.get("__name__", ""))
    if module is None:
        return
    for key, value in ns.items():
        if key in ("__export_module__", "__builtins__"):
            continue
        if key.startswith("__") and key not in ("__name__", "__file__", "__package__", "__path__"):
            continue
        module.__dict__[key] = value


def _load_local_module(name):
    # Only reuse modules this file built itself. QMT reloads strategies in the
    # same process, so sys.modules may still hold a same-named module from a
    # previous load (e.g. an older bigqmt_signal_trader_local_config without
    # the account set). Reusing that stale module silently disables the RPC
    # service, so any module without our marker is rebuilt and replaced.
    existing = sys.modules.get(name)
    if existing is not None and getattr(existing, "__bigqmt_flat_builtin__", False):
        return existing
    source_path, is_package = _find_local_source(name)
    if "." in name:
        _load_local_module(name.rsplit(".", 1)[0])
    module = types.ModuleType(name)
    module.__bigqmt_flat_builtin__ = True
    # Point __file__ under the strategy file dir so adapters that walk up the
    # tree (e.g. market_bigqmt locating the native xtdata SDK) still work when
    # this file lives inside a QMT python directory.
    module.__file__ = os.path.join(_STRATEGY_FILE_DIR, source_path.replace("/", os.sep))
    module.__package__ = name if is_package else name.rpartition(".")[0]
    if is_package:
        module.__path__ = [os.path.dirname(module.__file__)]
    module_builtins = dict(_builtins.__dict__)
    module_builtins["__import__"] = _local_import
    module.__dict__["__builtins__"] = module_builtins
    module.__dict__["__bigqmt_load_local_module"] = _load_local_module
    module.__dict__["__export_module__"] = _export_module
    module.__dict__["_SHELL_CONFIG"] = _SHELL_CONFIG
    sys.modules[name] = module
    # QMT native allowlist rejects the root package eager exports.
    if name == "bigqmt_signal_trader":
        return module
    try:
        func = _MODULE_FUNCS[source_path]
        exec(func.__code__, module.__dict__)
    except Exception:
        sys.modules.pop(name, None)
        raise
    _set_parent_attribute(name, module)
    return module


def _local_import(name, module_globals=None, module_locals=None, fromlist=(), level=0):
    absolute_name = _resolve_name(name, module_globals, level)
    if not _is_local_module(absolute_name):
        return _ORIGINAL_IMPORT(name, module_globals, module_locals, fromlist, level)
    module = _load_local_module(absolute_name)
    for child in fromlist or ():
        if child != "*":
            try:
                _load_local_module(absolute_name + "." + child)
            except ModuleNotFoundError:
                pass
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
    previous = sys.modules.get("bigqmt_signal_trader_strategy")
    reset = getattr(previous, "reset_app", None)
    if not callable(reset):
        return
    try:
        reset()
        print("[bigqmt_shell] previous rpc service stopped")
    except Exception as exc:
        print("[bigqmt_shell] previous rpc service stop failed: %s" % exc)


_stop_previous_rpc_service()
_clear_local_modules()
_importlib.import_module = _local_import_module
_importlib.reload = _local_reload
print("[bigqmt_shell] all-in-one no-redis strategy flat modules=%d" % len(_MODULE_FUNCS))


def _fallback_account_id():
    for name in ("BIGQMT_ACCOUNT_ID", "account", "account_id", "accountID"):
        value = globals().get(name)
        if value:
            return str(value)
    return ""


try:
    _local_import("bigqmt_signal_trader.exec_events", globals(), fromlist=("*",))
except Exception as exec_events_preload_error:
    print("[bigqmt_shell] exec_events preload failed: %s" % exec_events_preload_error)
try:
    _local_import("bigqmt_signal_trader.adapters.market_bigqmt", globals(), fromlist=("*",))
    _local_import("bigqmt_signal_trader.adapters.order_bigqmt", globals(), fromlist=("*",))
    _local_import("bigqmt_signal_trader.adapters.position_bigqmt", globals(), fromlist=("*",))
    _strategy = _local_import("bigqmt_signal_trader_strategy", globals(), fromlist=("*",))
    _strategy.reset_app()
except Exception as bridge_preload_error:
    print("[bigqmt_shell] bridge preload failed: %s" % bridge_preload_error)

_runtime = _local_import("bigqmt_signal_trader_redis_rpc_runtime", globals(), fromlist=("*",))


def _load_local_config():
    # Build the local config module directly from the shell config block and
    # force it into sys.modules. This bypasses _mod_N() execution, closure
    # cells, __export_module__ and any stale same-named module a previous
    # strategy load may have left in the QMT process.
    account = _SHELL_CONFIG.get("BIGQMT_ACCOUNT_ID", "")
    redis_config = _SHELL_CONFIG.get("BIGQMT_REDIS_CONFIG", {})
    module = sys.modules.get("bigqmt_signal_trader_local_config")
    if module is None or not getattr(module, "__bigqmt_flat_builtin__", False):
        module = types.ModuleType("bigqmt_signal_trader_local_config")
        module.__bigqmt_flat_builtin__ = True
        sys.modules["bigqmt_signal_trader_local_config"] = module
    module.BIGQMT_ACCOUNT_ID = account
    module.BIGQMT_REDIS_CONFIG = redis_config
    if not account:
        print("[bigqmt_shell] WARN local account config empty (shell account=%r)" % (_SHELL_CONFIG.get("BIGQMT_ACCOUNT_ID"),))
    return module


try:
    _config = _load_local_config()
    BIGQMT_REDIS_CONFIG = getattr(_config, "BIGQMT_REDIS_CONFIG", {})
    # Force pipe transport (this is the named-pipe build: no socket anywhere).
    BIGQMT_REDIS_CONFIG = dict(BIGQMT_REDIS_CONFIG or {})
    BIGQMT_REDIS_CONFIG["transport"] = "pipe"
    # drain 轮询（adjust 拍），后台线程在这类沙箱里只有 GIL 代价没有收益。
    BIGQMT_REDIS_CONFIG.setdefault("rpc_background_threads", False)
    # Nothing here may dial out: redis 块不下发（懒 client 首个命令就 connect，
    # EDR 抓到就杀进程，2026-09-24 实盘）。
    BIGQMT_REDIS_CONFIG["redis_enabled"] = False
    # pipe 没有推送线：全推推送关掉（客户端轮询 get_full_tick / 执行回调由
    # 客户端轮询合成 #372）；下载队列和快照缓存需要 redis，一并关。
    BIGQMT_REDIS_CONFIG["quote_push"] = {"enabled": False}
    BIGQMT_REDIS_CONFIG.setdefault("download_jobs_enabled", False)
    BIGQMT_REDIS_CONFIG.setdefault("full_tick_cache_enabled", False)
    print("[bigqmt_shell] pipe mode: transport=pipe background_threads=%s "
          "redis_enabled=False quote_push=False" % BIGQMT_REDIS_CONFIG["rpc_background_threads"])
    _runtime.configure_runtime_redis(BIGQMT_REDIS_CONFIG)
except Exception as redis_config_error:
    print("[bigqmt_shell] local redis config load failed: %s" % redis_config_error)

try:
    _config = _load_local_config()
    BIGQMT_ACCOUNT_ID = getattr(_config, "BIGQMT_ACCOUNT_ID", "")
    print("[bigqmt_shell] local account config loaded=%s" % bool(BIGQMT_ACCOUNT_ID))
    _runtime.configure_runtime_account(BIGQMT_ACCOUNT_ID)
except Exception as account_config_error:
    print("[bigqmt_shell] local account config load failed: %s" % account_config_error)
    account_id = _fallback_account_id()
    if account_id:
        _runtime.configure_runtime_account(account_id)

try:
    # Same capture every mounted entry uses (#202): the strategy module owns
    # the name list (_QMT_INJECTED_GLOBAL_FUNCS). The hand-copied tuple that
    # used to live here had been stale since 0.2.12 -- it stopped at
    # get_hkt_exchange_rate, so every single-file build silently shipped
    # without the download globals (download_history_data /
    # download_history_data2 / down_history_data), query_credit_account and
    # the formula family.
    qmt_extra = _strategy.capture_qmt_injected_funcs(globals())
    _runtime.bind_runtime_api(
        passorder_func=globals().get("passorder"),
        cancel_func=globals().get("cancel"),
        get_trade_detail_data_func=globals().get("get_trade_detail_data"),
        extra_funcs=qmt_extra or None,
    )
except NameError:
    pass


init = _runtime.init
handlebar = _runtime.handlebar
adjust = _runtime.adjust
order_callback = _runtime.order_callback
deal_callback = _runtime.deal_callback
# Credit-account counter query callback. QMT only calls back into the
# namespace of the file it mounted, so the single-file entry has to
# re-export it too or query_credit_account can never answer (#202).
credit_account_callback = _runtime.credit_account_callback
'''


if __name__ == "__main__":
    raise SystemExit("Use build_pipe_single_file_flat.py for the named-pipe build")
