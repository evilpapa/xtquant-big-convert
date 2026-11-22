# coding: utf-8
"""大 QMT 策略的生命周期回调和桥接初始化。"""

import datetime
import importlib as _importlib
import sys
import threading
import time
import traceback as _traceback

# 策略重跑时刷新 adapter_factory，使 build_app 的修改生效。
# 保留适配器类的身份，避免其他模块的 isinstance 判断失效。
_af_mod = sys.modules.get("bigqmt_signal_trader.adapter_factory")
if _af_mod is not None:
    try:
        _importlib.reload(_af_mod)
    except Exception as _reload_err:
        print("[bigqmt_signal_trader] 重新加载 adapter_factory 失败：%s" % _reload_err)

try:
    _load_bridge_module = __bigqmt_load_local_module
except NameError:
    _load_bridge_module = None

if _load_bridge_module is not None:
    _adapter_factory     = _load_bridge_module("bigqmt_signal_trader.adapter_factory")
    _valkey_settings     = _load_bridge_module("bigqmt_signal_trader.adapters.valkey_common")
    _runner              = _load_bridge_module("bigqmt_signal_trader.runner")
    _runtime_bigqmt      = _load_bridge_module("bigqmt_signal_trader.runtime_bigqmt")
    _order_watch         = _load_bridge_module("bigqmt_signal_trader.order_watch")
    _default_build_app   = _adapter_factory.build_app
    forward_order_event  = _runner.forward_order_event
    forward_trade_event  = _runner.forward_trade_event
    init_app             = _runner.init_app
    _reset_runner_app    = _runner.reset_app
    sync_positions_app   = _runner.sync_positions_app
    tick_app             = _runner.tick_app
    BigQmtRuntimeAdapter = _runtime_bigqmt.BigQmtRuntimeAdapter

else:
    from bigqmt_signal_trader.adapters import valkey_common as _valkey_settings
    from bigqmt_signal_trader.adapter_factory import build_app as _default_build_app
    from bigqmt_signal_trader.runner import (
        forward_order_event,
        forward_trade_event,
        init_app,
        reset_app as _reset_runner_app,
        sync_positions_app,
        tick_app,
    )
    from bigqmt_signal_trader.runtime_bigqmt import BigQmtRuntimeAdapter
    from bigqmt_signal_trader import order_watch as _order_watch


# 委托跟踪表在模块加载时创建，由 C++ 回调线程写入、adjust 线程读取（#164）。
_order_watch_table = _order_watch.OrderWatchTable()


def _note_order_watch(order_info):
    """从 QMT 原始 orderInfo 记录备注到柜台编号、柜台编号到状态的映射。"""
    try:
        _order_watch_table.note(
            _exec_events.normalize_order_event(order_info, ""))
    except Exception:
        pass


# 在模块加载时预加载 exec_events。QMT 的委托和成交回调运行在 C++ 线程上，
# 在该线程上首次执行模块可能触发 SystemError。已载入 sys.modules 的模块可正常读取（#76）。
def _import_exec_events():
    # 在 QMT 沙箱中沿用适配器的加载器，避开 C 层 __import__ 的相同限制。
    if _load_bridge_module is not None:
        return _load_bridge_module("bigqmt_signal_trader.exec_events")
    from bigqmt_signal_trader import exec_events

    return exec_events


def _load_exec_events():
    try:
        return _import_exec_events()
    except Exception:
        direct_error = _traceback.format_exc()
    # 直接加载失败后，交给拥有完整 Python 线程状态的 threading.Thread 重试。
    # 导入锁保证结果可以安全交回。
    holder = {}

    def _target():
        try:
            holder["module"] = _import_exec_events()
        except Exception:
            pass

    try:
        worker = threading.Thread(target=_target)
        worker.start()
        worker.join()
    except Exception:
        pass
    if holder.get("module") is not None:
        return holder["module"]
    print(
        "[bigqmt_signal_trader] exec_events 加载失败；本次运行将禁用成交回报推送：\n%s"
        % direct_error
    )
    return None


_exec_events = _load_exec_events()


_app_factory   = None
_account_id    = ""
_config        = {}
_qmt_api       = {}
_adjust_logged = False

# QMT 调了多少次 credit_account_callback。0 说明 QMT 压根没回调，
# 大于 0 但 handlers 里没数据说明是转交那一步断了（#202）。
_credit_callback_fired      =     0
_rpc_service                = None
_quote_subscription_service = None  # (QuoteSubscriptionManager, QuotePushChannel)
_exec_event_valkey_client   = None  # reused;                    building          a new client per trade callback leaks
_scheduled_adjust           = False

# 大 QMT 进程内的延迟诊断。缩短 GIL 切换间隔可减少后台 RPC 线程的等待。
# 心跳线程独立测量解释器无法调度它的时长。
_GIL_SWITCH_INTERVAL              = 0.001
_LATENCY_PROBE_ENABLED            = False
_LATENCY_PROBE_THRESHOLD_MS       =  50.0
_latency_probe_started            = False
_last_full_tick_refresh_at        =   0.0
_last_full_tick_market_refresh_at =   0.0

# 记录 adjust 的实际周期，用于识别 run_time 被限制到 K 线周期等调度问题。
_adjust_tick_stats = {"last_ts": 0.0, "count": 0, "window_start": 0.0, "sum": 0.0, "min": 0.0, "max": 0.0}


def set_app_factory(factory):
    global _app_factory
    _app_factory = factory


def set_account_id(account_id):
    global _account_id
    _account_id = str(account_id or "")


def configure(**kwargs):
    _config.update(kwargs)


def bind_qmt_api(passorder_func=None, cancel_func=None, get_trade_detail_data_func=None,
                 extra_funcs=None):
    if passorder_func is not None:
        _qmt_api["passorder"] = passorder_func
    if cancel_func is not None:
        _qmt_api["cancel"] = cancel_func
    if get_trade_detail_data_func is not None:
        _qmt_api["get_trade_detail_data"] = get_trade_detail_data_func
    # 捕获 QMT 运行时注入的额外全局函数（融资融券查询、IPO、期权持仓等）。
    # 这些函数和 passorder 一样由 Big QMT 进程在运行时注入到全局命名空间，
    # 不在 _PyContextInfo.py 桩里，需在 DRYRUN 入口捕获后传入。
    if extra_funcs:
        for name, func in extra_funcs.items():
            if func is not None:
                _qmt_api[name] = func


# reload_deployment 先应答，再由后续 adjust 重载，避免 reset_app() 提前关闭传输。
# not_before 是最短等待时间，_wait_for_responses_to_flush 再检查应答是否已发出。
_RELOAD_GRACE_SECONDS         =         1.5
_RELOAD_FLUSH_TIMEOUT_SECONDS =         5.0
_reload_request               = {"pending": False, "requested_at": 0.0, "not_before": 0.0, "by": ""}
_reload_result                = {}


def request_reload(reason=""):
    """Schedule a package reload for the next adjust tick.

    What it refreshes: everything under bigqmt_signal_trader/, by purging it
    from sys.modules and re-running init(). That covers the adapters, the RPC
    handlers, the models and the transports -- where nearly all changes land.

    What it CANNOT refresh, and no amount of importlib will: this file and the
    BIGQMT_VALKEY_DRYRUN entry. QMT execs those itself, and a module cannot
    reload the module it is running in. Those still need a strategy restart.

    Deliberately explicit: reloading a live trading process is not free. QMT's
    order/deal callbacks run on a C++ thread, and the first exec of a
    not-yet-imported module there fails in the C layer without setting a Python
    exception (SystemError: error return without exception set). The reload runs
    on the adjust thread and the callback path holds its own reference to
    exec_events from module load, so a callback landing mid-reload keeps using
    the old module rather than importing anything -- but the window is real,
    which is why this never fires on its own.
    """
    _reload_request["pending"] = True
    _reload_request["requested_at"] = time.time()
    _reload_request["not_before"] = time.time() + _RELOAD_GRACE_SECONDS
    _reload_request["by"] = str(reason or "")
    return {
        "scheduled": True,
        "note": "reload runs on the next adjust tick; poll get_deployment_info "
                "or reload_status to see the result",
        "version_before": _package_version(),
    }


def reload_status():
    """The outcome of the last reload, or what is still pending."""
    status = dict(_reload_result)
    status["pending"] = bool(_reload_request["pending"])
    status["requested_at"] = _reload_request["requested_at"]
    return status


def _package_version():
    try:
        if _load_bridge_module is not None:
            module = _load_bridge_module("bigqmt_signal_trader.version")
        else:
            from bigqmt_signal_trader import version as module
        return str(getattr(module, "__version__", ""))
    except Exception:
        return ""


def _purge_package_modules():
    """Drop every bigqmt_signal_trader module so the next import reads source.

    Purging beats importlib.reload here: reload has to run in dependency order
    (order_bigqmt does `from ..models import OrderSnapshot` at import time, so
    reloading models after it leaves the old class bound), and getting that
    order wrong fails silently. A purge has no order.
    """
    names = [name for name in list(sys.modules)
             if name == "bigqmt_signal_trader"
             or name.startswith("bigqmt_signal_trader.")]
    for name in names:
        sys.modules.pop(name, None)
    return sorted(names)


def _rebind_module_level_imports():
    """Re-point the names this module bound at import time.

    Purging sys.modules does nothing for references already held here --
    _default_build_app, the runner functions, BigQmtRuntimeAdapter and
    _exec_events would all keep pointing at the old objects, and the reload
    would look like it worked while changing nothing.
    """
    global _adapter_factory, _runner, _runtime_bigqmt, _default_build_app
    global forward_order_event, forward_trade_event, init_app, _reset_runner_app
    global sync_positions_app, tick_app, BigQmtRuntimeAdapter, _exec_events

    if _load_bridge_module is not None:
        _adapter_factory = _load_bridge_module("bigqmt_signal_trader.adapter_factory")
        _runner = _load_bridge_module("bigqmt_signal_trader.runner")
        _runtime_bigqmt = _load_bridge_module("bigqmt_signal_trader.runtime_bigqmt")
        _default_build_app = _adapter_factory.build_app
        forward_order_event = _runner.forward_order_event
        forward_trade_event = _runner.forward_trade_event
        init_app = _runner.init_app
        _reset_runner_app = _runner.reset_app
        sync_positions_app = _runner.sync_positions_app
        tick_app = _runner.tick_app
        BigQmtRuntimeAdapter = _runtime_bigqmt.BigQmtRuntimeAdapter
    else:
        from bigqmt_signal_trader.adapter_factory import build_app as _bp
        from bigqmt_signal_trader import runner as _rn
        from bigqmt_signal_trader.runtime_bigqmt import BigQmtRuntimeAdapter as _ra

        _default_build_app = _bp
        forward_order_event = _rn.forward_order_event
        forward_trade_event = _rn.forward_trade_event
        init_app = _rn.init_app
        _reset_runner_app = _rn.reset_app
        sync_positions_app = _rn.sync_positions_app
        tick_app = _rn.tick_app
        BigQmtRuntimeAdapter = _ra
    _exec_events = _load_exec_events()


def _pending_response_count():
    """How many RPC replies the transport has queued but not yet sent."""
    transport = getattr(_rpc_service, "_transport", None) if _rpc_service else None
    pending = getattr(transport, "_response_queue", None)
    if pending is None:
        return 0
    try:
        return pending.qsize()
    except Exception:
        return 0


def _wait_for_responses_to_flush(timeout_seconds=None):
    """Let the transport send what is queued before reset_app() tears it down.

    The reply to reload_deployment is put on the ZMQ transport's response queue
    by the handler, and the ROUTER thread sends it at the top of its next loop
    -- a loop that can be sitting in recv_multipart for up to RCVTIMEO (1s),
    and that needs the GIL this thread is holding. Sleeping yields both.

    Sending from here instead is not an option: the ROUTER socket belongs to
    that thread and ZMQ sockets are not thread-safe -- the same reason it closes
    its own socket in its finally block.

    Without this the reload SUCCEEDS and the caller still sees a timeout, which
    is indistinguishable from a reload that killed the bridge. That is what the
    first two live attempts did: "responded method=reload_deployment ok=True"
    and "[bigqmt_reload] ok purged=28" in the terminal, TransportTimeout at the
    client.
    """
    if timeout_seconds is None:
        timeout_seconds = _RELOAD_FLUSH_TIMEOUT_SECONDS
    deadline = time.time() + max(0.0, timeout_seconds)
    while time.time() < deadline:
        if _pending_response_count() == 0:
            # 发送者出队后仍需完成发送，再让出一次执行时间。
            time.sleep(0.2)
            return True
        time.sleep(0.05)
    return False


def _perform_reload(context_info):
    """在 adjust 线程清除模块缓存，重新导入并初始化。"""
    global _reload_result
    _reload_request["pending"] = False
    started = time.time()
    before = _package_version()
    result = {"ok": False, "version_before": before, "version_after": "",
              "modules_purged": 0, "seconds": 0.0, "error": "",
              "replies_flushed": False, "by": _reload_request["by"]}
    try:
        result["replies_flushed"] = _wait_for_responses_to_flush()
        if not result["replies_flushed"]:
            print("[bigqmt_reload] 警告：%.0fs 后仍有 %d 个应答在队列中；"
                  "重新加载仍会继续，但调用方可能看到超时"
                  % (_RELOAD_FLUSH_TIMEOUT_SECONDS, _pending_response_count()))
        # 重载会重建处理器。保留旧委托归属记录，供新处理器继续回填策略名（#393）。
        identity_journal = None
        old_handlers = getattr(_rpc_service, "handlers", None)
        if old_handlers is not None:
            identity_journal = getattr(old_handlers, "_order_identity_local", None)
        reset_app()
        result["modules_purged"] = len(_purge_package_modules())
        _rebind_module_level_imports()
        init(context_info)
        if identity_journal:
            new_handlers = getattr(_rpc_service, "handlers", None)
            if new_handlers is not None:
                # 合并初始化恢复的记录和旧的内存记录，优先保留仍有效的首次归属。
                recovered = getattr(new_handlers, "_order_identity_local", None) or {}
                now = time.time()
                ttl = getattr(new_handlers, "_ORDER_IDENTITY_LOCAL_TTL_SECONDS", 7 * 86400)
                for key, entry in recovered.items():
                    old_entry = identity_journal.get(key)
                    if (old_entry is None or now - old_entry[0] > ttl
                            or (entry[1] and not old_entry[1])
                            or (entry[1] == old_entry[1] and entry[0] > old_entry[0])):
                        identity_journal[key] = entry
                limit = getattr(new_handlers, "_ORDER_IDENTITY_LOCAL_LIMIT", 5000)
                while len(identity_journal) > limit:
                    identity_journal.popitem(last=False)
                new_handlers._order_identity_local = identity_journal
                print("[bigqmt_reload] 委托身份日志已跨重载保留（%d 条）"
                      % len(identity_journal))
        result["version_after"] = _package_version()
        result["ok"] = True
    except Exception as exc:
        result["error"] = "%s: %s" % (type(exc).__name__, exc)
        _log_err("reload", "重新加载失败：%s\n%s"
                          % (exc, _traceback.format_exc()))
    result["seconds"] = round(time.time() - started, 3)
    _reload_result = result
    print("[bigqmt_reload] %s：清理模块=%d，版本 %s -> %s，耗时 %.2fs%s"
          % ("成功" if result["ok"] else "失败", result["modules_purged"],
             before or "?", result["version_after"] or "?", result["seconds"],
             "" if result["ok"] else "  (" + result["error"] + ")；请重启策略"))
    return result


def reset_app():
    global _adjust_logged, _rpc_service, _scheduled_adjust, _last_full_tick_refresh_at, _last_full_tick_market_refresh_at
    global _quote_subscription_service, _exec_event_valkey_client
    _adjust_logged = False
    _scheduled_adjust = False
    _last_full_tick_refresh_at = 0.0
    _last_full_tick_market_refresh_at = 0.0
    _adjust_tick_stats.update({"last_ts": 0.0, "count": 0, "window_start": 0.0, "sum": 0.0, "min": 0.0, "max": 0.0})
    if _rpc_service is not None:
        try:
            _rpc_service.stop()
        except Exception:
            pass
    _rpc_service = None
    # 停止行情推送并取消全推订阅，避免重跑时发布端口仍被占用或旧订阅仍触发回调。
    if _quote_subscription_service is not None:
        try:
            manager, channel = _quote_subscription_service
            for key in list(getattr(manager, "_combos", {}).keys()):
                combo = manager._combos.get(key)
                if combo is not None:
                    try:
                        manager._close_source(combo.handle)
                    except Exception:
                        pass
            manager._combos.clear()
            manager._sub_index.clear()
            channel.stop()
        except Exception:
            pass
    _quote_subscription_service = None
    # 清除缓存的成交回报客户端，供下次启动重新创建。
    _exec_event_valkey_client = None
    _reset_runner_app()


def _resolve_runtime_name(name):
    if name in _qmt_api:
        return _qmt_api[name]
    if name in globals():
        return globals()[name]
    try:
        import builtins
        return getattr(builtins, name)
    except Exception:
        return None


def _detect_account_id(context_info=None):
    if _account_id:
        return _account_id
    try:
        import importlib
        import bigqmt_signal_trader_qmt_config as _local_config

        _local_config = importlib.reload(_local_config)
        value = str(
            getattr(_local_config, "BIGQMT_ACCOUNT_ID", "")
            or _valkey_settings.local_valkey_config(_local_config).get("account_id")
            or ""
        )
        if value:
            return value
    except Exception:
        pass
    for name in ("account", "account_id", "accountID"):
        value = _resolve_runtime_name(name)
        if value:
            return str(value)
    if context_info is not None:
        for name in ("account", "account_id", "accountID", "m_strAccountID"):
            value = getattr(context_info, name, None)
            if value:
                return str(value)
        for name in ("get_account", "get_account_id", "getAccountID"):
            func = getattr(context_info, name, None)
            if callable(func):
                try:
                    value = func()
                except Exception:
                    value = None
                if value:
                    return str(value)
    return ""


# 大 QMT 启动时注入的全局函数，经 RPC 暴露。延迟查找函数，便于在终端外导入测试。
_EXTRA_QMT_GLOBAL_FUNCS = (
    "get_history_trade_detail_data",  # 历史成交明细
    "get_value_by_order_id",          # 按 order_id 查委托详情
    "get_last_order_id",              # 最近委托号
    "get_ipo_data",                   # 新股数据
    "get_new_purchase_limit",         # 新股申购额度
    "get_assure_contract",            # 融资标的（担保品）合约
    "get_enable_short_contract",      # 融券标的合约
    "get_unclosed_compacts",          # 未平仓合约（负债）
    "get_closed_compacts",            # 已平仓合约
    "get_debt_contract",              # 负债合约
    "query_credit_account",           # 信用账户明细，查柜台的那条路。异步：结果只从 credit_account_callback 出来，见下面的 credit_account_callback 和 #202。
    "get_option_subject_position",    # 期权标的持仓
    "get_comb_option",                # 组合期权
    "get_hkt_exchange_rate",          # 港股通汇率
    "download_history_data",          # 和 passorder 一样，从 QMT 注入的命名空间获取下载函数（#32）。
    "download_history_data2",
    "down_history_data",              # 部分 QMT 只提供四参数的单标的下载函数 down_history_data（#54）。
    "call_formula",                   # 公式族也是运行时全局（官方文档原型就是全局函数，#374）：不在捕获名单里的话 qmt_api 永远拿不到，适配器的「全局优先」就没有可优先的对象。
    "subscribe_formula",
    "unsubscribe_formula",
    "get_formula_result",
    "gen_factor_index",
)

# QMT 注入函数的统一名单，策略入口通过 capture_qmt_injected_funcs 读取。
_QMT_INJECTED_GLOBAL_FUNCS = (
    "passorder", "cancel", "get_trade_detail_data",
) + _EXTRA_QMT_GLOBAL_FUNCS


def capture_qmt_injected_funcs(namespace):
    """从 QMT 挂载文件的命名空间收集已注入的函数。

    入口传入自己的 globals()，再将结果交给 bind_qmt_api。跳过不可调用的值。
    """
    captured = {}
    for name in _QMT_INJECTED_GLOBAL_FUNCS:
        func = (namespace or {}).get(name)
        if callable(func):
            captured[name] = func
    return captured


def _build_config():
    config = dict(_config)
    if str((config.get("rpc") or {}).get("transport") or "").lower() == "serial":
        config["valkey"] = {}
        config["schedule_adjust"] = True
        for key in ("signal_source_type", "source_type", "state_store_type", "position_sync_type"):
            if str(config.get(key) or "").lower() == "valkey":
                config[key] = ""
        for key in ("download_jobs", "full_tick_cache"):
            config[key] = dict(config.get(key) or {}, enabled=False)
    if _account_id:
        config["account_id"] = _account_id
    qmt_api = dict(config.get("qmt_api") or {})
    for name in ("passorder", "cancel", "get_trade_detail_data"):
        if qmt_api.get(name) is None:
            qmt_api[name] = _resolve_runtime_name(name)
    # 解析其余官方全局函数（存在则注入，不存在保持 None）。
    for name in _EXTRA_QMT_GLOBAL_FUNCS:
        if qmt_api.get(name) is None:
            qmt_api[name] = _resolve_runtime_name(name)
    config["qmt_api"] = qmt_api
    return config


def _build_app(context_info):
    if _app_factory is not None:
        return _app_factory(context_info)
    return _default_build_app(context_info, _build_config())


def _config_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _native_xtdata_enabled(rpc_config):
    """Resolve whether the RPC-side market provider may use native xtdata."""
    rpc_config = dict(rpc_config or {})
    if str(rpc_config.get("transport") or "").lower() == "serial":
        return False
    enabled = rpc_config.get("native_xtdata_enabled")
    if enabled is None:
        enabled = True
    return bool(enabled)


_VALKEY_TRANSPORT_NAMES = ("valkey", "", "default")


def _is_valkey_transport(transport_name):
    return str(transport_name or "valkey").lower() in _VALKEY_TRANSPORT_NAMES


def _resolve_background_threads(transport_name, configured):
    """Decide whether the RPC service runs its own background receive threads.

    ``configured`` is None when the local config never set
    ``rpc_background_threads`` (the runtime only forwards the key when it was
    given explicitly). That used to mean the historical default -- receiver
    threads on -- for every non-valkey transport. Since #343 it means the
    adjust drain wherever the transport can drain: measured on the live
    terminal, drain is <= 1 tick on every transport while a background thread
    pays a tick per GIL acquisition (zmq+thread 490ms for a deferred query,
    zmq+drain 88ms). Only a transport with no drain keeps its thread.

    An explicit False opts into the adjust-driven drain instead: the transport
    is polled with a non-blocking recv from drain_request_queue on each adjust
    tick, and the reply is sent on the adjust thread too. That removes every
    cross-thread handoff from the round trip -- measured on the live terminal,
    each time a background thread acquires the GIL costs ~1 adjust tick
    (~100ms), which is where the round trip actually goes (#104).

    Only transports that implement a *real* drain_request_queue may honor the
    override; anything else keeps the receiver thread it needs to be polled by.
    Valkey honors either value (blocking brpop path AND an adjust lpop drain).

    Ask the transport class whether it implements draining.
    """
    normalized = str(transport_name or "valkey").lower()
    if normalized == "serial":
        return False
    if _is_valkey_transport(normalized):
        return bool(configured)
    if not _transport_can_drain(normalized):
        return True
    return bool(configured)


def _transport_can_drain(transport_name):
    """这个传输自己实现了 drain_request_queue 吗。

    问传输类本身，而不是维护另一张名单。
    基类的实现返回 0（占位），所以只认子类自己覆盖过的。
    """
    try:
        from bigqmt_signal_trader.transports.factory import transport_supports_drain
    except Exception:
        # 导入不到就退回历史名单，别让一个 import 失败改变行为
        return str(transport_name or "").lower() in ("mysql",)
    return transport_supports_drain(transport_name)


def _build_quote_subscription_service(context_info, config, transport_name, account_id,
                                      valkey_client, push_channel=None):
    """Assemble the server-side whole-quote push service (manager + channel).

    Returns ``(manager, channel)`` or ``None`` when disabled. The channel publisher
    is started in ``_start_rpc_service`` once the RPC service is up; the manager's
    reaper is fed from ``_drain_rpc_service``.
    """
    quote_config = dict(config.get("quote_push") or {})
    enabled = _config_bool(quote_config.get("enabled"), True)
    if not enabled:
        return None
    if _load_bridge_module is not None:
        _qsm = _load_bridge_module("bigqmt_signal_trader.quote_subscription_manager")
    else:
        from bigqmt_signal_trader import quote_subscription_manager as _qsm
    import importlib

    _qsm = importlib.reload(_qsm)
    heartbeat_timeout = float(quote_config.get("heartbeat_timeout_seconds", 30.0))
    return _qsm.build_quote_subscription_service(
        context_info,
        transport_name=transport_name,
        account_id=account_id,
        valkey_client=valkey_client,
        enabled=True,
        heartbeat_timeout_seconds=heartbeat_timeout,
        push_channel=push_channel,
    )


def _build_rpc_service(context_info, app, config):
    global _exec_events
    rpc_config = dict(config.get("rpc") or {})
    enabled = _config_bool(config.get("enable_rpc"), False) or _config_bool(rpc_config.get("enabled"), False)
    if not enabled:
        return None
    transport_name = str(rpc_config.get("transport") or "valkey").lower()
    valkey_transport = transport_name in ("valkey", "", "default")

    import importlib
    if _load_bridge_module is not None:
        _market_bigqmt         = _load_bridge_module("bigqmt_signal_trader.adapters.market_bigqmt")
        _position_bigqmt       = _load_bridge_module("bigqmt_signal_trader.adapters.position_bigqmt")
        _valkey_rpc            = _load_bridge_module("bigqmt_signal_trader.valkey_rpc")
        BigQmtRpcHandlers      = _valkey_rpc.BigQmtRpcHandlers
        ValkeyPubSubRpcService = _valkey_rpc.ValkeyPubSubRpcService
    else:
        from bigqmt_signal_trader.adapters import market_bigqmt as _market_bigqmt
        from bigqmt_signal_trader.adapters import position_bigqmt as _position_bigqmt
        from bigqmt_signal_trader import valkey_rpc as _valkey_rpc
        BigQmtRpcHandlers = _valkey_rpc.BigQmtRpcHandlers
        ValkeyPubSubRpcService = _valkey_rpc.ValkeyPubSubRpcService

    # QMT 重跑策略时会沿用进程内的模块缓存，重载适配器以应用已同步的修改。
    _market_bigqmt = importlib.reload(_market_bigqmt)
    _position_bigqmt = importlib.reload(_position_bigqmt)
    # 一并重载延迟导入的模块，使其修改也能在策略重跑时生效。
    for _mod_name in (
        "bigqmt_signal_trader.full_tick_cache",
        "bigqmt_signal_trader.download_jobs",
        "bigqmt_signal_trader.order_remarks",
        "bigqmt_signal_trader.exec_events",
    ):
        try:
            importlib.reload(importlib.import_module(_mod_name))
        except Exception as _reload_err:
            print("[bigqmt_rpc] 重新加载 %s 失败：%s" % (_mod_name, _reload_err))
    # 复用 RPC 已加载的回报模块，避免在 C++ 回调中导入。
    _exec_events = _valkey_rpc._exec_events
    BigQmtMarketDataProvider = _market_bigqmt.BigQmtMarketDataProvider
    BigQmtPositionProvider = _position_bigqmt.BigQmtPositionProvider

    qmt_api = dict(config.get("qmt_api") or {})
    valkey_client = None
    response_valkey_client = None
    if valkey_transport:
        if _load_bridge_module is not None:
            _valkey_common = _load_bridge_module("bigqmt_signal_trader.adapters.valkey_common")
        else:
            from bigqmt_signal_trader.adapters import valkey_common as _valkey_common

        _valkey_common = importlib.reload(_valkey_common)
        valkey_config = dict(config.get("valkey") or {})
        valkey_config.update(dict(rpc_config.get("valkey") or {}))
        listen_valkey_config = dict(valkey_config)
        # Valkey 直连模式的 LPOP 与监听共用客户端。设定 Socket 超时，
        # 避免 Valkey 无响应时长时间阻塞策略线程。BRPOP 自身仍有 1 秒命令超时。
        if listen_valkey_config.get("socket_timeout") in (None, ""):
            listen_valkey_config["socket_timeout"] = 10
        valkey_client = rpc_config.get("valkey_client") or config.get("valkey_client") or _valkey_common.build_valkey_client(listen_valkey_config)
        response_valkey_client = (
            rpc_config.get("response_valkey_client")
            or config.get("response_valkey_client")
            or _valkey_common.build_valkey_client(valkey_config)
        )
    account_id = str(rpc_config.get("account_id") or config.get("account_id") or _account_id or "")
    if not account_id:
        print("[bigqmt_rpc] 已禁用：account_id 为空")
        return None
    transport = None
    if not valkey_transport:
        if _load_bridge_module is not None:
            build_transport = _load_bridge_module("bigqmt_signal_trader.transports.factory").build_transport
        else:
            from bigqmt_signal_trader.transports.factory import build_transport

        transport = build_transport(transport_name, rpc_config, account_id=account_id)
    allow_order_methods = _config_bool(rpc_config.get("allow_order_methods"), False)
    global _quote_subscription_service
    _quote_subscription_service = _build_quote_subscription_service(
        context_info, config, transport_name, account_id, valkey_client,
        transport if transport_name == "serial" else None,
    )
    quote_manager = (
        _quote_subscription_service[0] if _quote_subscription_service is not None else None
    )
    handlers = BigQmtRpcHandlers(
        account_id=account_id,
        market_data=BigQmtMarketDataProvider(
            context_info,
            qmt_api=qmt_api,
            # RPC 处理器自行创建行情提供器，显式传入是否允许访问原生 xtdata。
            native_xtdata_enabled=_native_xtdata_enabled(rpc_config),
        ),
        position_provider=BigQmtPositionProvider(
            get_trade_detail_data_func=qmt_api.get("get_trade_detail_data"),
            account_type=config.get("account_type", "STOCK"),
        ),
        order_gateway=getattr(app, "order_gateway", None),
        position_sync_sink=getattr(app, "position_sync_sink", None),
        allow_order_methods=allow_order_methods,
        allowed_methods=rpc_config.get("allowed_methods"),
        qmt_api=qmt_api,
        # 默认由后续 adjust 确认委托。仅在无法通过 adjust 重试确认时，才需要同步确认。
        settle_orders_inline=(transport_name != "serial"
                              and _config_bool(rpc_config.get("settle_orders_inline"), False)),
        order_settle_timeout_seconds=float(rpc_config.get("order_settle_timeout_seconds", 3.0)),
        quote_subscription_manager=quote_manager,
        # 空字符串表示让报单来源留空（#154）。
        default_strategy_name=rpc_config.get("default_strategy_name"),
    )
    # 下载任务和委托归属共用已配置的 Valkey 客户端（#133）。
    # 串口模式下 _exec_event_valkey 返回 None，相关存储功能随之停用。
    _store_valkey                         = response_valkey_client or valkey_client or _exec_event_valkey(config)
    handlers.download_job_valkey_client   = _store_valkey
    handlers.order_identity_valkey_client = _store_valkey
    handlers.order_remark_valkey_client   = _exec_events.bounded_identity_client(_store_valkey)
    handlers.order_watch_table            = _order_watch_table      # 确认委托时优先查回调写入的跟踪表（#164）。
    handlers.reload_hook                  = request_reload # 通过钩子将 reload_deployment 交给策略模块处理。
    handlers.reload_status_hook           = reload_status
    handlers.exec_event_publisher         = _publish_synthesized_cancel_error  # QMT 拒绝撤单时没有回调，确认路径通过此钩子发布 cancel_error（#389）。
    handlers.exec_events_enabled          = _config_bool((config.get("exec_events") or {}).get("enabled"), True)
    handlers.download_jobs_enabled        = _config_bool((config.get("download_jobs") or {}).get("enabled"), False)
    handlers.download_job_chunk_size      = int((config.get("download_jobs") or {}).get("chunk_size") or 10)
    handlers.download_job_ttl_seconds     = int((config.get("download_jobs") or {}).get("job_ttl_seconds") or 3600)
    process_in_listener                   = _config_bool(rpc_config.get("process_in_listener"), True)
    listener_methods                      = rpc_config.get("listener_methods") or ("*",)
    configured_bg                         = rpc_config.get("background_threads")  # 未配置 rpc_background_threads 时，由传输方式决定默认值。
    if configured_bg is not None:
        configured_bg = _config_bool(configured_bg, False)
    background_threads = _resolve_background_threads(transport_name, configured_bg)
    if background_threads and not configured_bg:
        print("[bigqmt_rpc] transport=%s 不支持 drain，已强制启用 background_threads"
              % transport_name)
    print(
        "[bigqmt_rpc] transport=%s 模式配置 process_in_listener=%s listener_methods=%s allow_order_methods=%s background_threads=%s"
        % (transport_name, process_in_listener, listener_methods, allow_order_methods, background_threads)
    )
    service = ValkeyPubSubRpcService(
        valkey_client=valkey_client,
        response_valkey_client=response_valkey_client,
        handlers=handlers,
        account_id=account_id,
        request_channel_template=rpc_config.get("request_channel_template", "bigqmt:rpc:req:{account_id}"),
        response_channel_template=rpc_config.get("response_channel_template", "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_key_template=rpc_config.get("response_key_template", "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_ttl_seconds=int(rpc_config.get("response_ttl_seconds", 60)),
        max_queue_size=int(rpc_config.get("max_queue_size", 200)),
        process_in_listener=process_in_listener,
        listener_methods=listener_methods,
        background_threads=background_threads,
        debug_log_limit=int(rpc_config.get("debug_log_limit", 5)),
        transport=transport,
        # adjust 处理队列时，可把大查询移交给工作线程（#351）。
        heavy_offload=_config_bool(rpc_config.get("heavy_offload"), True),
        heavy_codes_threshold=int(rpc_config.get("heavy_codes_threshold", 20) or 20),
    )
    if transport_name == "serial":
        return service
    # BIGQMT_ACCOUNT_TYPE_MAP 含多个账号时，每个账号创建一个 RPC 服务，共用处理器。
    try:
        from bigqmt_signal_trader.multi_account import build_multi_account_rpc_service
        multi_service = build_multi_account_rpc_service(
            context_info, app, config,
            # 复用已创建的主账号服务，由多账号构建器创建附属服务。
            lambda ctx, app, cfg: service if ctx is context_info else None,
        )
        if multi_service is not service:
            # 多账号服务由 MultiAccountRpcServiceManager 管理。
            return multi_service
    except Exception as _ma_err:
        print("[bigqmt_rpc] multi_account 检查失败，已回退到单账户模式：%s" % _ma_err)
    return service


def _start_rpc_service(context_info, app, config):
    global _rpc_service
    if _rpc_service is not None:
        return _rpc_service
    service = _build_rpc_service(context_info, app, config)
    if service is not None:
        service.start()
        _rpc_service = service
        if _quote_subscription_service is not None:
            try:
                _quote_subscription_service[1].start_publisher()
                print("[bigqmt_quote_push] 行情推送发布器已启动 transport=%s"
                      % str(dict(config.get("rpc") or {}).get("transport") or "valkey"))
            except Exception as exc:
                _log_err("quote_push", "行情推送发布器启动失败：%s" % exc)
    return _rpc_service


def _adjust_interval_seconds(config):
    """schedule_adjust_interval as seconds, or None when unparseable."""
    text = str(config.get("schedule_adjust_interval") or "3000nMilliSecond").strip().lower()
    digits = ""
    for ch in text:
        if ch.isdigit() or ch == ".":
            digits += ch
        else:
            break
    unit = text[len(digits):].strip()
    if unit.startswith("n"):
        unit = unit[1:]
    scale = {"millisecond": 0.001, "ms": 0.001, "second": 1.0, "s": 1.0,
             "minute": 60.0, "m": 60.0, "hour": 3600.0, "h": 3600.0}.get(unit)
    if not digits or scale is None:
        return None
    try:
        return float(digits) * scale
    except ValueError:
        return None


def _drain_budget_seconds(config, rpc_config):
    """How long one adjust tick may keep the strategy thread draining (#303).

    ``rpc.drain_budget_seconds`` when set; else one tick interval, never
    under 0.5s. A 100ms tick gets 0.5s -- a couple of passorders, then the
    thread goes back to QMT and the replies go out. A 3s tick keeps 3s, the
    same work per tick as the old unbounded batch. 0 or a negative value
    disables the bound.
    """
    explicit = rpc_config.get("drain_budget_seconds")
    if explicit is not None and str(explicit).strip() != "":
        value = float(explicit)
        return value if value > 0 else None
    interval = _adjust_interval_seconds(config)
    if interval is None:
        return None
    return max(0.5, interval)


def _drain_rpc_service(config):
    if _rpc_service is None:
        return 0
    rpc_config = dict(config.get("rpc") or {})
    max_items = int(rpc_config.get("drain_max_items", 20))
    processed = 0
    if hasattr(_rpc_service, "drain_request_queue"):
        processed += _rpc_service.drain_request_queue(max_items=max_items)
    try:
        processed += _rpc_service.drain_pending(
            max_items=max_items, budget_seconds=_drain_budget_seconds(config, rpc_config))
    except TypeError:
        # A service object from before the budget (a partial reload).
        processed += _rpc_service.drain_pending(max_items=max_items)
    if _quote_subscription_service is not None:
        try:
            _quote_subscription_service[0].reap_expired()
        except Exception as exc:
            _log_err("quote_push", "清理过期推送资源失败：%s" % exc)
    # 从 RPC 处理器取得行情提供器，释放闲置的预热订阅（#310）。
    # 兼容尚无清理接口的旧处理器。
    prune = getattr(getattr(getattr(_rpc_service, "handlers", None), "market_data", None),
                    "prune_tick_subscriptions", None)
    if callable(prune):
        try:
            prune()
        except Exception as exc:
            _log_err("full_tick", "全量逐笔预热订阅清理失败：%s" % exc)
    return processed


def _refresh_full_tick_cache(context_info, config):
    global _last_full_tick_refresh_at, _last_full_tick_market_refresh_at
    cache_config = dict(config.get("full_tick_cache") or {})
    if not _config_bool(cache_config.get("enabled"), True):
        return 0
    account_id = str(cache_config.get("account_id") or config.get("account_id") or _account_id or "")
    if not account_id:
        return 0
    # Symbol-list demands are cheap and refresh on the fast interval; whole-market
    # (SH/SZ/BJ/HK) demands are heavy and refresh on a slower cadence so a ~50k row
    # snapshot is not pulled every fast tick.
    symbol_interval = float(cache_config.get("refresh_interval_seconds") or 0.5)
    market_interval = float(cache_config.get("market_refresh_interval_seconds") or 3.0)
    max_wall = cache_config.get("refresh_max_wall_seconds")
    max_wall = float(max_wall) if max_wall else None
    now = time.time()
    do_symbol = now - _last_full_tick_refresh_at >= symbol_interval
    do_market = now - _last_full_tick_market_refresh_at >= market_interval
    if not do_symbol and not do_market:
        return 0
    valkey_client = getattr(_rpc_service, "valkey", None)
    if valkey_client is None:
        valkey_config = dict(config.get("valkey") or {})
        if not valkey_config:
            return 0
        from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client

        valkey_client = build_valkey_client(valkey_config)
    demand_ttl = float(cache_config.get("demand_ttl_seconds") or 10)
    cache_ttl = float(cache_config.get("cache_ttl_seconds") or 10)
    max_requests = int(cache_config.get("max_requests") or 8)
    from bigqmt_signal_trader.full_tick_cache import refresh_full_tick_cache

    refreshed = 0
    # Symbol and market refreshes are throttled independently, so each advances its
    # own timestamp and runs in its own try: a symbol-refresh error must not starve
    # the market refresh nor leave it retrying every fast tick (unthrottled).
    if do_symbol:
        _last_full_tick_refresh_at = now
        try:
            refreshed += refresh_full_tick_cache(
                valkey_client,
                context_info,
                account_id,
                demand_ttl_seconds=demand_ttl,
                cache_ttl_seconds=cache_ttl,
                max_requests=max_requests,
                kind="symbol",
                max_wall_seconds=max_wall,
            )
        except Exception as exc:
            _log_err("full_tick_cache", "证券级行情缓存刷新失败：%s" % exc)
    if do_market:
        _last_full_tick_market_refresh_at = now
        try:
            refreshed += refresh_full_tick_cache(
                valkey_client,
                context_info,
                account_id,
                demand_ttl_seconds=demand_ttl,
                cache_ttl_seconds=cache_ttl,
                max_requests=max_requests,
                kind="market",
                max_wall_seconds=max_wall,
            )
        except Exception as exc:
            _log_err("full_tick_cache", "全市场行情缓存刷新失败：%s" % exc)
    return refreshed


def _schedule_adjust_if_needed(context_info, config):
    global _scheduled_adjust
    if _scheduled_adjust:
        return
    if not _config_bool(config.get("schedule_adjust"), False):
        return
    interval = str(config.get("schedule_adjust_interval") or "3000nMilliSecond")
    if not hasattr(context_info, "run_time"):
        print(
            "[bigqmt_signal_trader] 警告：ContextInfo.run_time 不可用；"
            "RPC drain 将回退到 K 线节拍（请求的 interval=%s 无法应用）"
            % interval
        )
        return
    start_time = (datetime.datetime.now() + datetime.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        context_info.run_time("adjust", interval, start_time)
        _scheduled_adjust = True
        print(
            "[bigqmt_signal_trader] 已安排 adjust interval=%s"
            "（实际节拍请查看“adjust 节拍”日志）" % interval
        )
    except Exception as exc:
        print(
            "[bigqmt_signal_trader] 警告：安排 adjust 调度失败（%s）；"
            "RPC drain 将回退到 K 线节拍，请求的 interval=%s 无法应用"
            % (exc, interval)
        )


# Where each adjust() call came from, and what tick_app actually costs.
# The question this answers: handlebar is documented as tick-driven in live
# trading, but the observed cadence is a flat ~50/10s -- exactly the run_time
# timer and nothing else. Splitting the counters shows whether handlebar fires
# at all once the historical replay ends, and the tick_app histogram shows what
# it would cost to let it drive the strategy body rather than just the drain.
_adjust_source_stats = {"handlebar": 0, "timer": 0, "window_start": 0.0}
# Bucket upper bounds in ms; the last bucket is everything above.
_TICK_APP_BUCKETS = (1, 5, 20, 50, 100, 250, 500, 1000, 2000)
_tick_app_hist = [0] * (len(_TICK_APP_BUCKETS) + 1)
_tick_app_max_ms = [0.0]


def _record_adjust_source(source):
    """Count adjust() calls per trigger source, logged on the cadence window."""
    stats = _adjust_source_stats
    now = time.time()
    if stats["window_start"] <= 0:
        stats["window_start"] = now
    stats[source] = stats.get(source, 0) + 1
    if now - stats["window_start"] >= 10.0:
        print(
            "[adjust_source] handlebar=%d timer=%d，统计窗口 %.0fs"
            % (stats["handlebar"], stats["timer"], now - stats["window_start"])
        )
        stats.update({"handlebar": 0, "timer": 0, "window_start": now})


def _record_tick_app_ms(ms):
    """Histogram of tick_app cost.

    tick_app is the expensive half of adjust() and is skipped while
    is_last_bar() is False, so the replay-time cost (~0.2ms/call) says nothing
    about what it costs at the live edge. Before letting ticks drive it we need
    the real distribution, not just the >50ms outliers the phase logger prints.
    """
    for index, bound in enumerate(_TICK_APP_BUCKETS):
        if ms <= bound:
            _tick_app_hist[index] += 1
            break
    else:
        _tick_app_hist[-1] += 1
    if ms > _tick_app_max_ms[0]:
        _tick_app_max_ms[0] = ms


def _format_tick_app_hist():
    labels = []
    previous = 0
    for index, bound in enumerate(_TICK_APP_BUCKETS):
        labels.append("%d-%dms=%d" % (previous, bound, _tick_app_hist[index]))
        previous = bound
    labels.append(">%dms=%d" % (_TICK_APP_BUCKETS[-1], _tick_app_hist[-1]))
    return " ".join(labels)


def _record_adjust_tick():
    """Track and periodically log the real interval between adjust triggers."""
    stats = _adjust_tick_stats
    now = time.time()
    last = stats["last_ts"]
    stats["last_ts"] = now
    if last <= 0:
        stats["window_start"] = now
        return
    delta = now - last
    stats["count"] += 1
    stats["sum"] += delta
    stats["min"] = delta if stats["min"] <= 0 else min(stats["min"], delta)
    stats["max"] = max(stats["max"], delta)
    if now - stats["window_start"] >= 10.0 and stats["count"] > 0:
        avg = stats["sum"] / stats["count"]
        print(
            "[bigqmt_signal_trader] adjust 节拍：ticks=%d avg=%.3fs min=%.3fs max=%.3fs，统计窗口 %.0fs"
            % (stats["count"], avg, stats["min"], stats["max"], now - stats["window_start"])
        )
        if sum(_tick_app_hist) > 0:
            print("[tick_app_hist] %s max=%.0fms"
                  % (_format_tick_app_hist(), _tick_app_max_ms[0]))
        stats.update({"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0, "window_start": now})


def _gil_probe_loop():
    """Heartbeat: sleep 5ms in a loop and measure the ACTUAL elapsed time. sleep()
    releases the GIL; if returning from it takes much longer than 5ms, the thread
    was starved -- i.e. the interpreter (this whole process) was stalled holding
    the GIL elsewhere. Summarize gaps over a 10s window so we can see how often /
    how long the process freezes, independent of any RPC request."""
    step = 0.005
    threshold = _LATENCY_PROBE_THRESHOLD_MS / 1000.0
    window_start = time.time()
    gaps = []
    while True:
        t0 = time.time()
        time.sleep(step)
        gap = time.time() - t0 - step
        if gap > threshold:
            gaps.append(gap * 1000.0)
        now = time.time()
        if now - window_start >= 10.0:
            if gaps:
                gaps.sort()
                print(
                    "[gil_probe] 最近 %.0fs：%d 次卡顿>%.0fms，max=%.0fms p50=%.0fms total=%.0fms"
                    % (now - window_start, len(gaps), _LATENCY_PROBE_THRESHOLD_MS,
                       gaps[-1], gaps[len(gaps) // 2], sum(gaps))
                )
            else:
                print("[gil_probe] 最近 %.0fs：没有超过 %.0fms 的卡顿（正常）"
                      % (now - window_start, _LATENCY_PROBE_THRESHOLD_MS))
            window_start = now
            gaps = []


def _start_latency_probe():
    global _latency_probe_started
    if _latency_probe_started or not _LATENCY_PROBE_ENABLED:
        return
    _latency_probe_started = True
    t = threading.Thread(target=_gil_probe_loop, name="bigqmt-gil-probe", daemon=True)
    t.start()
    print("[gil_probe] 已启动（threshold=%.0fms）" % _LATENCY_PROBE_THRESHOLD_MS)


def _apply_gil_tuning():
    try:
        sys.setswitchinterval(_GIL_SWITCH_INTERVAL)
        print("[bigqmt_signal_trader] GIL 切换间隔已设置为 %.4fs" % _GIL_SWITCH_INTERVAL)
    except Exception as exc:
        print("[bigqmt_signal_trader] setswitchinterval 设置失败：%s" % exc)


def init(ContextInfo):
    detected_account_id = _detect_account_id(ContextInfo)
    if detected_account_id and not _account_id:
        set_account_id(detected_account_id)
    if _account_id and hasattr(ContextInfo, "set_account"):
        try:
            ContextInfo.set_account(_account_id)
        except Exception as exc:
            _log_startup_error("set_account 设置失败：%s" % exc)
    _apply_gil_tuning()
    _start_latency_probe()
    config = _build_config()
    runtime = BigQmtRuntimeAdapter(ContextInfo)
    app = None
    try:
        app = init_app(runtime, _build_app)
    except Exception as exc:
        # A build failure (e.g. missing valkey package) must not kill the strategy
        # before the RPC service even starts — log it and continue without app.
        _log_startup_error("init_app/build_app 初始化失败：%s" % exc)
    try:
        _start_rpc_service(ContextInfo, app, config)
    except Exception as exc:
        # e.g. zmq port conflict -> TransportError. Log it so the user sees why
        # the RPC service didn't start instead of QMT silently exiting.
        _log_startup_error("RPC 服务启动失败：%s" % exc)
    try:
        _schedule_adjust_if_needed(ContextInfo, config)
    except Exception as exc:
        _log_startup_error("adjust 调度设置失败：%s" % exc)
    print("[bigqmt_signal_trader] 初始化成功")

    # 启动时自动诊断：检测服务状态 + 关键函数绑定，方便发现问题
    _diag_startup(ContextInfo, config)
    try:
        _start_context_warmup(ContextInfo, config)
    except Exception as exc:
        _log_startup_error("上下文预热启动失败：%s" % exc)
    return app


def stop(ContextInfo):
    reset_app()


def _log_startup_error(message):
    """Log a startup error to file AND the QMT panel; never raises."""
    try:
        from bigqmt_signal_trader.logging_setup import get_logger
        get_logger("init").error("%s", message)
    except Exception:
        pass
    try:
        print("[bigqmt_signal_trader] 初始化错误：%s" % message)
    except Exception:
        pass


def _log_err(tag, message):
    """Log a runtime error to the rotating file AND the QMT panel; never raises.

    Centralizes error visibility: the QMT output panel scrolls away, but the
    log file (logs/bigqmt.log, kept 7 days) survives restarts and crashes.
    """
    try:
        from bigqmt_signal_trader.logging_setup import get_logger
        get_logger(tag).error("%s", message)
    except Exception:
        pass
    try:
        print("[bigqmt_signal_trader] %s: %s" % (tag, message))
    except Exception:
        pass


def _diag_bar_driver(context_info):
    """Report what drives handlebar: the strategy's own symbol, period, and
    whether any quote subscription exists.

    handlebar is documented as firing per incoming tick in live trading, but it
    goes quiet here once the historical replay ends. This strategy never calls
    subscribe_quote / subscribe_whole_quote / set_universe, so the leading
    suspect is that nothing is feeding it ticks. Print what QMT actually has so
    the next live session settles it instead of us guessing.
    """
    fields = (
        ("stockcode", "品种"),
        ("stock_code", "品种(alt)"),
        ("period", "周期"),
        ("do_back_test", "回测模式"),
        ("start", "起始"),
        ("end", "结束"),
    )
    parts = []
    for name, label in fields:
        try:
            value = getattr(context_info, name, None)
            if callable(value):
                value = value()
            if value not in (None, ""):
                parts.append("%s=%s" % (label, value))
        except Exception:
            continue
        print("[bigqmt_diag] K 线驱动：%s" % (" ".join(parts) or "<无法读取>"))

    for name in ("subscribe_quote", "subscribe_whole_quote", "set_universe", "is_last_bar"):
        print("[bigqmt_diag]   %-22s %s"
              % (name, "可用" if callable(getattr(context_info, name, None)) else "不可用"))
    try:
        print("[bigqmt_diag]   is_last_bar() 当前值    %s" % context_info.is_last_bar())
    except Exception as exc:
        print("[bigqmt_diag]   is_last_bar() 调用失败  %s" % exc)
    print("[bigqmt_diag]   本策略未订阅任何行情 -> handlebar 预计仅由历史回放驱动")


def _diag_startup(ContextInfo, config):
    """Startup diagnostics: check service status and key function bindings.

    Prints a summary to the QMT log so users can quickly see if the RPC service
    is up, which transport is active, and whether key QMT functions (passorder,
    get_trade_detail_data) are bound. Helps diagnose "service won't start" issues.
    """
    print("=" * 60)
    print("[bigqmt_diag] 启动诊断")
    print("=" * 60)

    _diag_bar_driver(ContextInfo)

    # 1. RPC service status
    rpc_config = dict(config.get("rpc") or {})
    transport = rpc_config.get("transport", "valkey")
    print("[bigqmt_diag] 传输方式=%s" % transport)
    if _rpc_service is not None:
        print("[bigqmt_diag] rpc_service=运行中（type=%s）" % type(_rpc_service).__name__)
    else:
        print("[bigqmt_diag] rpc_service=未启动（请检查 enable_rpc 或上方错误）")

    # 2. Key QMT function bindings
    qmt_api = dict(config.get("qmt_api") or {})
    for name in ("passorder", "cancel", "get_trade_detail_data", "down_history_data"):
        bound = qmt_api.get(name) is not None
        print("[bigqmt_diag] %s bound=%s" % (name, bound))

    # 3. Quick connectivity test (get_full_tick)
    try:
        tick = ContextInfo.get_full_tick(["000001.SZ"])
        if tick:
            print("[bigqmt_diag] get_full_tick=正常（keys=%d）" % len(tick))
        else:
            print("[bigqmt_diag] get_full_tick=空（市场可能已休市）")
    except Exception as e:
        print("[bigqmt_diag] get_full_tick=失败：%s" % str(e)[:60])

    print("[bigqmt_diag] 诊断完成")
    print("=" * 60)


# ContextInfo families whose FIRST call after a restart can cost minutes.
#
# get_full_tick is already exercised by _diag_startup, on the main thread, and
# comes back in milliseconds. get_financial_data is not, and it was measured at
# 346 SECONDS on its first call after a restart -- while QMT itself was healthy
# (whole-quote data flowing, threadpool alive) and the main strategy thread was
# idle. Every later call that day took under a second, including codes and
# tables never asked for before, so it is a one-time cost and not a per-code
# cache miss.
#
# That block lands on the RPC listener thread, which serves one request at a
# time, so it takes the whole bridge down with it: every queued request times
# out and the client sees a dead bridge.
#
# Warming does not make the cost cheaper. It moves it to a known moment, onto a
# thread nobody is waiting on, with a log line saying what is happening --
# instead of arriving as an unexplained freeze the first time a caller asks.
#
# Deliberately NOT on the main thread: _diag_startup runs there during init, and
# a 346-second call in init would freeze startup before the adjust timer is even
# scheduled -- worse than the problem.
def _warm_financial_data(context_info):
    """Fetch a real slice, not an empty one.

    An EMPTY date range is accepted and returns None instantly. The first
    version of this warmup passed "" for both and reported "warm in 0.00s"
    while exercising nothing at all -- a warmup that silently no-ops is worse
    than none, because the log says it worked. Measured against the live
    terminal, same code and stock:

        dotted field + real range   0.75s  DataFrame rows=159
        dotted field + empty range  0.17s  None
        whole table  + real range   0.41s  DataFrame rows=159
        whole table  + empty range  0.20s  Series rows=6
    """
    end = datetime.date.today()
    start = end - datetime.timedelta(days=365)
    return context_info.get_financial_data(
        ["CAPITALSTRUCTURE.total_capital"], ["000001.SZ"],
        start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), "report_time")


CONTEXT_WARMUP_PROBES = (
    ("get_financial_data", _warm_financial_data),
)


def _warmup_row_count(result):
    """How much a probe brought back, or -1 when that cannot be told."""
    if result is None:
        return 0
    try:
        return len(result)
    except Exception:
        return -1


def _context_warmup_loop(context_info):
    for name, probe in CONTEXT_WARMUP_PROBES:
        started = time.time()
        print("[bigqmt_warmup] %s：重启后的首次调用可能耗时数分钟；"
              "现在提前执行，以免调用方等待" % name)
        try:
            result = probe(context_info)
            elapsed = time.time() - started
        except Exception as exc:
            print("[bigqmt_warmup] %s 在 %.1fs 后失败：%s"
                  % (name, time.time() - started, str(exc)[:120]))
            continue
        rows = _warmup_row_count(result)
        if rows == 0:
            # Warming nothing while reporting success is the failure mode this
            # check exists to catch; it already happened once.
            print("[bigqmt_warmup] %s 在 %.2fs 内没有返回数据；"
                  "预热请求未走到预期路径，首个真实调用仍需等待"
                  % (name, elapsed))
        elif elapsed > 10.0:
            print("[bigqmt_warmup] %s 已在 %.1fs 后预热完成（%s 行）；"
                  "本次等待已结束，后续调用应在亚秒级返回"
                  % (name, elapsed, rows))
        else:
            print("[bigqmt_warmup] %s 预热完成，耗时 %.2fs（%s 行）"
                  % (name, elapsed, rows))


def _start_context_warmup(context_info, config):
    """Kick the warmup onto a daemon thread. Never blocks init."""
    flag = dict(config.get("rpc") or {}).get("warm_context_data", True)
    if isinstance(flag, str):
        flag = flag.strip().lower() not in ("0", "false", "no", "off", "")
    if not flag:
        return
    thread = threading.Thread(
        target=_context_warmup_loop, args=(context_info,),
        name="bigqmt-context-warmup", daemon=True)
    thread.start()


def _pump_download_jobs(context_info, config):
    """Advance any queued async download job by a bounded slice on this thread."""
    job_config = dict(config.get("download_jobs") or {})
    if not _config_bool(job_config.get("enabled"), True):
        return None
    account_id = str(job_config.get("account_id") or config.get("account_id") or _account_id or "")
    if not account_id:
        return None
    # Reuse one client. This runs on every adjust tick, so building a client
    # here leaked one connection pool per tick -- at a 100ms interval that is
    # ten per second. The symptom is easy to miss: the pools are garbage
    # collected, and valkey-py's __del__ then raises
    # "AttributeError: 'Valkey' object has no attribute 'connection'", which
    # Python swallows as "Exception ignored in". It never reaches a log the
    # package writes; it only shows up in the QMT panel.
    #
    # _exec_event_valkey already learned this lesson and caches; this path was
    # missed. Both only build a client when _rpc_service has none, which is the
    # zmq-transport case.
    valkey_client = _exec_event_valkey(config)
    if valkey_client is None:
        return None
    market_data = getattr(getattr(_rpc_service, "handlers", None), "market_data", None)
    if market_data is None:
        from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider

        market_data = BigQmtMarketDataProvider(
            context_info,
            qmt_api=dict(config.get("qmt_api") or {}),
            native_xtdata_enabled=_native_xtdata_enabled(config.get("rpc")),
        )
    try:
        from bigqmt_signal_trader.download_jobs import pump_download_jobs

        return pump_download_jobs(
            valkey_client,
            market_data,
            account_id,
            chunk_size=int(job_config.get("chunk_size") or 10),
            max_wall_seconds=float(job_config.get("max_wall_seconds") or 0.5),
            job_ttl_seconds=int(job_config.get("job_ttl_seconds") or 3600),
        )
    except Exception as exc:
        _log_err("download_jobs", "下载任务泵送失败：%s" % exc)
        return None


def _adjust_phase(name, fn, *args):
    """Time one adjust phase; log only if it exceeds 50ms. Pinpoints which part of
    the 500ms adjust cycle holds the GIL (the gil_probe shows the stall exists;
    this shows WHERE). The finally-log never alters the call's result/exception.

    Guard with except: an exception here (e.g. valkey outage inside the LPOP
    drain) must NOT propagate into adjust/handlebar — QMT stops the strategy on
    a callback raise, which is the 'auto-exit' users report. Log and continue.
    """
    t0 = time.perf_counter()
    try:
        return fn(*args)
    except Exception:
        import traceback as _tb
        try:
            from bigqmt_signal_trader.logging_setup import get_logger
            get_logger("adjust").error("adjust 阶段 %s 执行失败：\n%s", name, _tb.format_exc())
        except Exception:
            pass
        return None
    finally:
        ms = (time.perf_counter() - t0) * 1000.0
        if ms > 50.0:
            print("[adjust_phase] %s %.0fms" % (name, ms))


def adjust(ContextInfo, _source="timer"):
    global _adjust_logged
    _record_adjust_tick()
    _record_adjust_source(_source)
    config = _build_config()
    _adjust_phase("drain", _drain_rpc_service, config)
    _adjust_phase("exec_hold", _flush_held_presysid_orders, config)
    # AFTER the drain, and not on the tick that scheduled it. The drain is what
    # flushes queued RPC responses, and reset_app() tears the transport down
    # with any reply still in it -- reloading first killed the reply to
    # reload_deployment itself, which the server had already logged as
    # "responded ok=True" while the client sat there until it timed out.
    if _reload_request["pending"] and time.time() >= _reload_request["not_before"]:
        _adjust_phase("reload", _perform_reload, ContextInfo)
        return None
    _adjust_phase("full_tick", _refresh_full_tick_cache, ContextInfo, config)
    _adjust_phase("download", _pump_download_jobs, ContextInfo, config)
    try:
        if hasattr(ContextInfo, "is_last_bar") and not ContextInfo.is_last_bar():
            return None
    except Exception:
        pass
    if not _adjust_logged:
        print("[bigqmt_signal_trader] adjust 完成")
        _adjust_logged = True
    _tick_app_t0 = time.perf_counter()
    try:
        return _adjust_phase("tick_app", tick_app, ContextInfo, datetime.datetime.now())
    finally:
        # Every call, not just the >50ms ones _adjust_phase prints: deciding
        # whether ticks may drive this needs the whole distribution.
        _record_tick_app_ms((time.perf_counter() - _tick_app_t0) * 1000.0)


def handlebar(ContextInfo):
    """Standard Big QMT bar callback.

    Documented as tick-driven during live trading ("再在每个tick数据来后驱动运行
    一次"), but the observed cadence is a flat ~50/10s -- the run_time timer
    alone. Tagging the source tells us whether this ever fires once the
    historical replay ends; the strategy subscribes to no quote, which is the
    leading suspect.
    """
    return adjust(ContextInfo, _source="handlebar")


# Valkey is preferred for exec events, but only while it actually works.
# "Configured" is not "reachable": valkey-py builds a client lazily and does not
# dial until the first command, so a stale valkey block in the config yields a
# perfectly good-looking client that times out on every publish -- and the zmq
# push channel sitting right next to it never gets used (issue #145).
_EXEC_VALKEY_FAILURE_LIMIT = 3
_exec_sink_state = {"valkey_failures": 0, "reports": 0, "demoted": False}

# issue #161: QMT fires the order callback once when the order row appears and
# again when m_strOrderSysID is populated (#152's window) -- the client then
# logs two identical 已报 events, the first degenerate (no sysid, order_id=0).
# A sysid-less order event is held for a short window; if its sysid-bearing
# twin arrives the held one is dropped, otherwise the adjust tick publishes it.
_held_presysid_orders = {}      # key -> (event, held_at, raw_obj)
_HELD_PRESYSID_DEFAULT_SECONDS = 0.8
_instrument_name_cache = {}     # stock_code -> name (only non-empty cached)


def _presysid_key(event):
    """Identity for pairing a sysid-less event with its sysid-bearing twin."""
    remark = str(event.get("user_order_id") or event.get("remark") or "").strip()
    if remark:
        return ("remark", remark)
    stock = str(event.get("stock_code") or "")
    if not stock:
        return None  # cannot key safely -- publish immediately
    return ("fields", (
        stock, event.get("price"),
        event.get("volume") or event.get("order_volume"),
        event.get("direction"),
    ))


def _hold_presysid_order(event, event_config, raw_obj=None):
    """Hold a sysid-less order event instead of publishing it. True if held."""
    if str(event.get("order_sys_id") or ""):
        return False
    hold_s = float(event_config.get("hold_presysid_order_seconds",
                                    _HELD_PRESYSID_DEFAULT_SECONDS) or 0)
    if hold_s <= 0:
        return False
    key = _presysid_key(event)
    if key is None:
        return False
    _held_presysid_orders[key] = (event, time.time(), raw_obj)
    return True


def _drop_held_presysid_twin(event):
    """A sysid-bearing event supersedes its held sysid-less twin."""
    if not str(event.get("order_sys_id") or ""):
        return
    key = _presysid_key(event)
    if key is not None:
        _held_presysid_orders.pop(key, None)


def _flush_held_presysid_orders(config):
    """Publish held events whose window expired. Runs on the adjust tick."""
    if not _held_presysid_orders:
        return
    event_config = dict(config.get("exec_events") or {})
    hold_s = float(event_config.get("hold_presysid_order_seconds",
                                    _HELD_PRESYSID_DEFAULT_SECONDS) or 0)
    now = time.time()
    expired = [key for key, entry in _held_presysid_orders.items()
               if now - entry[1] >= hold_s]
    if not expired:
        return
    exec_events = _exec_events
    account_id = str(event_config.get("account_id") or config.get("account_id")
                     or _account_id or "")
    sink = _exec_event_sink(config)
    if exec_events is None or sink is None or not account_id:
        for key in expired:
            _held_presysid_orders.pop(key, None)
        return
    for key in expired:
        entry = _held_presysid_orders.pop(key, None)
        if entry is None:
            continue
        event, _held_at, raw_obj = entry
        _publish_one(exec_events, sink, account_id, event, "order", config)
        try:
            status = int(event.get("status") or 0)
        except (TypeError, ValueError):
            status = 0
        if status == 57:
            # the held event turns out to be a junk: it still owes the
            # order_error twin the non-held path would have published.
            _publish_one(exec_events, sink, account_id,
                         exec_events.normalize_order_error_event(raw_obj, account_id),
                         "order_error", config)


def _event_instrument_name(context_info, stock_code):
    """Resolve the instrument name for an event, cached; empty never cached."""
    code = str(stock_code or "")
    if not code:
        return ""
    cached = _instrument_name_cache.get(code)
    if cached:
        return cached
    name = ""
    getter = getattr(context_info, "get_stock_name", None) if context_info is not None else None
    if getter is not None:
        try:
            name = str(getter(code) or "")
        except Exception:
            name = ""
    if name:
        _instrument_name_cache[code] = name
    return name


def _push_channel_sink():
    transport = getattr(_rpc_service, "_transport", None)
    if getattr(transport, "name", None) == "serial":
        return transport
    if _quote_subscription_service is None:
        return None
    try:
        return _quote_subscription_service[1]          # (manager, channel)
    except Exception:
        return None


def _exec_event_sink(config):
    """Where exec events go: a Valkey client, or the quote push channel.

    Exec events were Valkey-only, so a zmq deployment with no Valkey configured
    silently delivered no order/trade callbacks at all -- _publish_exec_event
    simply returned (issue #76). zmq deployments already run a push channel for
    whole-quote data, so reuse it rather than opening a second socket.

    Valkey stays first *while it works*: its channels carry streams for short
    replay, which the push channel has no equivalent of. After
    _EXEC_VALKEY_FAILURE_LIMIT consecutive publish failures it is demoted and
    the push channel takes over, because an unreachable valkey was otherwise
    swallowing every callback while a working channel stood idle (issue #145).
    """
    if not _exec_sink_state["demoted"]:
        valkey_client = _exec_event_valkey(config)
        if valkey_client is not None:
            return valkey_client
    return _push_channel_sink()


def _note_exec_publish_failure(kind, exc):
    """Count a failure, demote valkey once it is clearly not coming back, and
    keep the log readable.

    The full traceback is deliberate -- issue #76 took a day because str(exc)
    alone read "error return without exception set" with no origin. But a
    persistently unreachable valkey prints that for EVERY order and deal, which
    buries the log it is meant to explain. Full detail the first few times,
    a one-liner after that.
    """
    state = _exec_sink_state
    state["valkey_failures"] += 1
    state["reports"] += 1
    if state["reports"] <= 3:
        _log_err(
            "exec_events",
            "发布 %s 失败：%s（%s）\n%s"
            % (kind, exc, exc.__class__.__name__, _traceback.format_exc()),
        )
    elif state["reports"] % 50 == 0:
        _log_err(
            "exec_events",
            "发布 %s 已连续失败 %d 次：%s（%s）"
            % (kind, state["reports"], exc, exc.__class__.__name__),
        )
    if not state["demoted"] and state["valkey_failures"] >= _EXEC_VALKEY_FAILURE_LIMIT:
        state["demoted"] = bool(_push_channel_sink())
        if state["demoted"]:
            print("[bigqmt_exec_events] valkey 已连续失败 %d 次；委托和成交回调"
                  "将改用行情推送通道。若不需要该能力，可从本地配置中删除 "
                  "valkey 配置块。"
                  % state["valkey_failures"])


def _publish_one(exec_events, sink, account_id, event, kind, config):
    """Publish one event, falling back to the push channel if the sink fails.

    Without the fallback a failed publish simply lost the callback -- the
    client never learns the order happened. Trying the other channel costs one
    extra attempt and only on the failure path.
    """
    try:
        exec_events.publish_exec_event(sink, account_id, event)
        _exec_sink_state["valkey_failures"] = 0
        return True
    except Exception as exc:
        _note_exec_publish_failure(kind, exc)
    fallback = _push_channel_sink()
    if fallback is None or fallback is sink:
        return False
    try:
        exec_events.publish_exec_event(fallback, account_id, event)
        return True
    except Exception as exc:
        _note_exec_publish_failure("%s (push fallback)" % kind, exc)
        return False


def _publish_synthesized_cancel_error(account_id, event):
    """Publish a bridge-synthesized cancel_error through the exec machinery.

    #389: QMT logs a refused cancel (已成不可撤 etc.) but fires NO callback,
    so the handlers' settle path builds the event and this hook publishes it
    on the same sink -- with the same push-channel fallback -- as native
    order/trade events. Installed on the handlers as exec_event_publisher.
    """
    try:
        config = _build_config()
        event_config = dict(config.get("exec_events") or {})
        if not _config_bool(event_config.get("enabled"), True):
            return False
        exec_events = _exec_events
        if exec_events is None:
            return False
        account_id = str(account_id or event_config.get("account_id")
                         or config.get("account_id") or _account_id or "")
        if not account_id:
            return False
        sink = _exec_event_sink(config)
        if sink is None:
            return False
        return _publish_one(exec_events, sink, account_id, event,
                            "cancel_error", config)
    except Exception:
        return False


def _exec_event_valkey(config):
    """Return a valkey client for exec-event publishing, reusing one instance.

    Previously a new client was built per order/trade callback when the RPC
    service had none (the zmq-transport case), leaking a connection pool per
    event. Reuse one; build failure returns None so publishing just skips.

    A non-Valkey transport may retain a Valkey block for optional download jobs
    or exec-event replay. Skip that block only when both consumers are
    explicitly disabled; omitted flags retain the legacy enabled behavior.
    """
    global _exec_event_valkey_client
    if str((config.get("rpc") or {}).get("transport") or "").lower() == "serial":
        return None
    existing = getattr(_rpc_service, "valkey", None) if _rpc_service is not None else None
    if existing is not None:
        return existing
    if _exec_event_valkey_client is not None:
        return _exec_event_valkey_client
    if not _config_bool((config.get("download_jobs") or {}).get("enabled"), True) and not _config_bool(
        (config.get("exec_events") or {}).get("enabled"), True
    ):
        return None
    valkey_config = dict(config.get("valkey") or {})
    if not valkey_config:
        return None
    try:
        from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client

        _exec_event_valkey_client = build_valkey_client(valkey_config)
    except Exception as exc:
        # 静默 None 会让事件悄悄全丢（issue #71 的 protocol 崩溃就是这样没的），
        # 必须留痕。
        try:
            from bigqmt_signal_trader.logging_setup import get_logger

            get_logger("strategy").error("成交回报 Valkey 客户端创建失败：%s", exc)
        except Exception:
            pass
        return None
    return _exec_event_valkey_client


def _local_identity_strategy_name(account_id, event):
    """strategy_name from the in-process submit journal, or "".

    The journal (#156) is written by the RPC handlers at submit time and is
    all a deployment has when valkey is absent -- or, the reported case in
    #174, configured but not reachable, which fails silently.
    """
    service = _rpc_service
    handlers = getattr(service, "handlers", None) if service is not None else None
    journal = getattr(handlers, "_order_identity_local", None)
    if not journal:
        return ""
    remark = str(event.get("user_order_id") or event.get("remark") or "").strip()
    if not remark:
        return ""
    try:
        entry = journal.get((str(account_id or ""), remark))
        if not entry:
            return ""
        stamped, name = entry
        ttl = getattr(handlers, "_ORDER_IDENTITY_LOCAL_TTL_SECONDS", 86400.0)
        if name and (time.time() - float(stamped)) <= float(ttl):
            return str(name)
    except Exception:
        return ""
    return ""


def _enrich_event_identity(exec_events, config, account_id, event):
    """Put the strategy name back on an event QMT could not name (#174).

    Most events no longer get here: normalize_*_event reads the name off
    报单来源 (m_strSource), which is passorder's own strategyName argument
    coming back, so an order this bridge placed names itself and returns at the
    first line below. m_strStrategyName really is absent (120 and 47 attributes
    on a live terminal, in neither) -- concluding from that alone that the name
    was absent too is what made this function the only way back.

    It stays as the fallback for what the row cannot answer: a terminal that
    blanks the source, and orders whose name only the bridge ever knew.

    The in-process journal is consulted FIRST, and deliberately so. This runs
    on QMT's C++ callback thread, and for an order this process submitted the
    journal already holds the same string that was written to valkey -- so
    asking valkey first would buy nothing and cost a round trip per event. It
    costs a lot when valkey is configured and not reachable: valkey-py does not
    dial until the first command, so every callback would pay the full
    timeout (#145's shape). That is precisely the deployment #174 was reported
    from.

    Valkey is the fallback rather than the primary because its extra reach --
    naming an order some OTHER process submitted -- is the rarer case.

    Both branches call this. Enriching only orders is what left
    on_stock_trade's strategy_name permanently empty.

    The row's own strategy-name field is NOT authoritative (#216): QMT fills
    it with the QMT-side strategy's registered name (this bridge process),
    not the strategy_name the caller passed at order time. So no early return
    on a non-empty row value -- a submit-time identity with a name WINS.
    """
    name = _local_identity_strategy_name(account_id, event)
    if name:
        event["strategy_name"] = name
        return event
    valkey_client = _exec_event_valkey(config)
    if valkey_client is not None:
        try:
            event = exec_events.enrich_order_identity(valkey_client, account_id, event)
        except Exception:
            pass
    return event


def _event_account_id(obj):
    """The account a native order / deal object names, or "" (#320).

    Reading an attribute off a QMT object can itself raise (#76's
    SystemError), and this runs on the C++ callback thread before the
    guarded block below -- so it must never let anything out.
    """
    for name in ("m_strAccountID", "account_id"):
        try:
            value = getattr(obj, name, None)
        except Exception:
            return ""
        if value:
            return str(value).strip()
    return ""


def _recover_callback_remark(kind, obj):
    """Record native associations independently of push enablement/success."""
    try:
        event = (_exec_events.normalize_trade_event(obj, _account_id) if kind == "trade"
                 else _exec_events.normalize_order_event(obj, _account_id))
        handlers = getattr(_rpc_service, "handlers", None)
        if handlers is not None:
            return handlers._recover_order_remarks(event.get("account_id"), [event])[0]
        return event
    except Exception as exc:
        print("[bigqmt_exec] 委托备注观测失败：%s" % type(exc).__name__)
        return None


def _publish_exec_event(kind, obj, context_info=None, normalized_event=None):
    """Push a normalized order/trade event to Valkey for real-time client callbacks."""
    config = _build_config()
    event_config = dict(config.get("exec_events") or {})
    # Raw-field diagnostics run BEFORE every other check (and before the
    # enabled/account_id early returns), because the point is to observe the
    # object exactly as QMT handed it over — even when publishing is off.
    raw_fields = None
    exec_events = _exec_events
    if exec_events is None:
        # Already reported once at module load; a per-callback log would flood.
        return
    if _config_bool(event_config.get("debug_raw_fields"), False):
        try:
            print(exec_events.format_raw_snapshot(kind, obj))
            raw_fields = exec_events.raw_field_snapshot(obj)
        except Exception as exc:
            print("[bigqmt_exec_raw] 原始快照 %s 获取失败：%s" % (kind, exc))
    if not _config_bool(event_config.get("enabled"), True):
        return
    # The channel is per account, so it has to be THIS event's account: a
    # callback for a secondary account (方式一) published under the
    # configured primary landed where nobody was listening (#320, #322).
    # The row names its account; the configured one is only the fallback.
    account_id = str(
        _event_account_id(obj)
        or event_config.get("account_id") or config.get("account_id") or _account_id or "")
    if not account_id:
        return
    sink = _exec_event_sink(config)
    if sink is None:
        return
    try:
        if kind == "trade":
            event = (normalized_event if normalized_event is not None
                     else exec_events.normalize_trade_event(obj, account_id))
            event = _enrich_event_identity(exec_events, config, account_id, event)
            if not event.get("instrument_name"):
                event["instrument_name"] = _event_instrument_name(
                    context_info, event.get("stock_code"))
            if raw_fields:
                event["raw_fields"] = raw_fields
            _publish_one(exec_events, sink, account_id, event, kind, config)
        else:
            event = (normalized_event if normalized_event is not None
                     else exec_events.normalize_order_event(obj, account_id))
            # Identity enrichment reads the remark->identity map that
            # remember_order_identity wrote. On a push channel the event goes
            # out un-enriched rather than not at all; order_sys_id and remark
            # are already on it.
            event = _enrich_event_identity(exec_events, config, account_id, event)
            if not event.get("instrument_name"):
                event["instrument_name"] = _event_instrument_name(
                    context_info, event.get("stock_code"))
            # QMT fires this callback once with the row pre-sysid and again
            # once the id lands (#152's window) -- two identical 已报 events,
            # the first degenerate (issue #161). Hold the sysid-less one; the
            # twin drops it, the adjust flush publishes it if no twin comes.
            if _hold_presysid_order(event, event_config, obj):
                return
            _drop_held_presysid_twin(event)
            if raw_fields:
                event["raw_fields"] = raw_fields
            _publish_one(exec_events, sink, account_id, event, kind, config)
            # 废单 (status=57 ENTRUST_STATUS_JUNK) 推送 order_error，让客户端
            # on_order_error 能感知下单被拒。
            try:
                status = int(event.get("status") or 0)
            except (TypeError, ValueError):
                status = 0
            if status == 57:
                err_event = exec_events.normalize_order_error_event(obj, account_id)
                if raw_fields:
                    err_event["raw_fields"] = raw_fields
                _publish_one(exec_events, sink, account_id, err_event,
                             "order_error", config)
    except Exception as exc:
        # Publishing itself is handled (and throttled) inside _publish_one, so
        # anything reaching here came from normalizing the QMT object. str(exc)
        # alone reads "error return without exception set" with no hint of where
        # it came from -- that is what made issue #76 take a day to pin down.
        _log_err(
            "exec_events",
            "构建 %s 成交回报失败：%s（%s）\n%s"
            % (kind, exc, exc.__class__.__name__, _traceback.format_exc()),
        )


def order_callback(ContextInfo, orderInfo):
    """Standard Big QMT order callback."""
    # Settlement feeds on this even when client push is off (issue #164).
    _note_order_watch(orderInfo)
    event = _recover_callback_remark("order", orderInfo)
    _publish_exec_event("order", orderInfo, ContextInfo, normalized_event=event)
    return forward_order_event(BigQmtRuntimeAdapter.to_order_event(orderInfo))


def deal_callback(ContextInfo, dealInfo):
    """Standard Big QMT deal callback."""
    event = _recover_callback_remark("trade", dealInfo)
    _publish_exec_event("trade", dealInfo, ContextInfo, normalized_event=event)
    return forward_trade_event(BigQmtRuntimeAdapter.to_trade_event(dealInfo))


def credit_account_callback(ContextInfo, seq, result):
    """Standard Big QMT credit-account callback (官方参考 6.13).

    信用账户明细只有这一条查柜台的路：query_credit_account 立刻返回，结果从这里
    出来。QMT 只往被挂载的那个文件的命名空间里回调，所以每个入口文件都要像
    order_callback / deal_callback 那样再导出一次这个名字（#202）。

    回调跑在 QMT 的 C++ 线程上，所以这里只把结果交给 handlers 存着，绝不做需要
    主线程上下文的事，也绝不让异常逃出去 —— 回调里抛异常在 QMT 那边表现为没有
    堆栈的 SystemError（issue #76）。
    """
    # 这里以前成功和失败都不出声，于是「QMT 根本没回调」和「回调来了但没送到
    # handlers」从外面看一模一样 —— 正是 CLAUDE.md 里那条「a failed operation
    # looks exactly like one that never ran」，栽在自己的新代码上。所以每一次
    # 触发都留痕：计数交给 handlers（probe_capabilities 报出来），送不到时
    # 一定要吼出来。
    global _credit_callback_fired
    _credit_callback_fired += 1
    try:
        handlers = getattr(_rpc_service, "handlers", None) if _rpc_service else None
        if handlers is None:
            _log_err("credit_account_callback",
                     "QMT 已送达信用账户明细（seq=%s），但没有可接收的 RPC 服务；"
                     "该结果将被丢弃。"
                     % (seq,))
            return False
        if not hasattr(handlers, "note_credit_account"):
            _log_err("credit_account_callback",
                     "QMT 已送达信用账户明细（seq=%s），但当前部署的 handlers "
                     "没有 note_credit_account；桥接包版本早于入口文件。"
                     "请重新同步并重启策略。" % (seq,))
            return False
        return handlers.note_credit_account(seq, result)
    except Exception as exc:
        _log_err("credit_account_callback",
                 "记录信用账户明细失败：%s（%s）"
                 % (exc, exc.__class__.__name__))
        return False


def sync_positions(ContextInfo):
    return sync_positions_app("manual")
