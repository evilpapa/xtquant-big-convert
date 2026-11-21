# coding: utf-8
"""Big QMT Valkey Pub/Sub RPC strategy entry.

This entry does not consume trade signals. RPC order methods are disabled by
default; read-only methods and position sync are enabled.
"""

import os
import sys


# QMT loads strategy scripts via exec, so __file__ may be undefined. Build a
# list of candidate directories (script dir guesses + cwd) and put any that
# holds bigqmt_signal_trader_strategy.py on sys.path[0]. This keeps the package
# and bigqmt_signal_trader_qmt_config importable regardless of how QMT
# invokes the script.
_CANDIDATE_DIRS = []
try:
    _CANDIDATE_DIRS.append(os.path.dirname(os.path.abspath(__file__)))
except Exception:
    pass
_CANDIDATE_DIRS.append(os.getcwd())
for _up in (".", ".."):
    _CANDIDATE_DIRS.append(os.path.abspath(os.path.join(os.getcwd(), _up)))
for _dir in _CANDIDATE_DIRS:
    if os.path.exists(os.path.join(_dir, "bigqmt_signal_trader_strategy.py")):
        if _dir not in sys.path:
            sys.path.insert(0, _dir)
        break


from bigqmt_signal_trader.adapters.valkey_common import local_valkey_config


try:
    _load_bridge_module = __bigqmt_load_local_module
except NameError:
    _load_bridge_module = None

if _load_bridge_module is not None:
    _strategy_module = _load_bridge_module("bigqmt_signal_trader_strategy")
    adjust = _strategy_module.adjust
    bind_qmt_api = _strategy_module.bind_qmt_api
    capture_qmt_injected_funcs = _strategy_module.capture_qmt_injected_funcs
    configure = _strategy_module.configure
    # 信用账户查柜台的回调；QMT 只往被挂载的文件回调，所以这里必须再导出一次，
    # 和 order_callback / deal_callback 同理 (#202)。
    credit_account_callback = _strategy_module.credit_account_callback
    deal_callback = _strategy_module.deal_callback
    handlebar = _strategy_module.handlebar
    init = _strategy_module.init
    order_callback = _strategy_module.order_callback
    reset_app = _strategy_module.reset_app
    set_account_id = _strategy_module.set_account_id
    stop = _strategy_module.stop
    sync_positions = _strategy_module.sync_positions
else:
    from bigqmt_signal_trader_strategy import (  # noqa: E402
        adjust,
        bind_qmt_api,
        capture_qmt_injected_funcs,
        configure,
        credit_account_callback,
        deal_callback,
        handlebar,
        init,
        order_callback,
        reset_app,
        set_account_id,
        stop,
        sync_positions,
    )

# QMT mounts this file by exec and injects trade/download/query globals into
# THIS module's namespace (same mechanism as passorder). They are invisible to
# the strategy module -- its _EXTRA_QMT_GLOBAL_FUNCS resolution checks the
# strategy module's globals and builtins only -- so without the capture below
# every trade/download/query RPC is a silent no-op (verified live 2026-09-01:
# get_ipo_info -> NotImplementedError, download_history_data -> False). A plain
# import of this module (e.g. the DRYRUN loader) sees none of these names and
# skips silently; DRYRUN's own explicit bind_runtime_api still governs there.
# The capture goes through the strategy's capture_qmt_injected_funcs -- the
# name list lives there and nowhere else, so no local hand-copied tuple.

# QMT re-runs the strategy by re-exec'ing this file while the strategy module
# stays cached in sys.modules -- reset its state (stop the old RPC service,
# drop stale subscriptions) or the re-run leaks them.
try:
    reset_app()
except NameError:
    pass

_qmt_injected = capture_qmt_injected_funcs(globals())
if _qmt_injected:
    bind_qmt_api(extra_funcs=_qmt_injected)
    print("[bigqmt_runtime] 已绑定 QMT 注入的全局函数：%s" % sorted(_qmt_injected))


ACCOUNT_ID = ""
# 账号类型：STOCK(股票) / CREDIT(信用/两融) / FUTURE(期货) / OPTION(期权)
# 对应 xtconstant 枚举：SECURITY_ACCOUNT=2 / CREDIT_ACCOUNT=3 / FUTURE_ACCOUNT=1
ACCOUNT_TYPE = "STOCK"
# 这台机器上到底有没有 valkey 可用。默认 True 只是为了不改变既有部署的行为；
# 它并不代表「配了 valkey」—— 下面 VALKEY_HOST/PORT 有默认值，所以光看配置永远
# 分辨不出「我配了 valkey」和「我什么都没写」（issue #147）。
#
# 设成 False 时 configure_runtime 干脆不发 valkey 块，于是 config["valkey"] 为空，
# 五个使用方（委托身份库、异步下载任务、全推快照缓存、exec 事件、身份回填）
# 各自的 `if not valkey_config: return None` 自然生效，一次都不会去连。
#
# 用在 QMT 无法加载 RESP 客户端的环境（issue #145），
# transport=valkey 时这个开关被忽略 ——
# 那种部署没有 valkey 就没有 RPC。
VALKEY_ENABLED = True
VALKEY_HOST = os.environ.get("BIGQMT_VALKEY_HOST") or "127.0.0.1"
VALKEY_PORT = int(os.environ.get("BIGQMT_VALKEY_PORT") or 6379)
VALKEY_DB = int(os.environ.get("BIGQMT_VALKEY_DB") or 5)
VALKEY_USERNAME = os.environ.get("BIGQMT_VALKEY_USERNAME") or ""
VALKEY_PASSWORD = os.environ.get("BIGQMT_VALKEY_PASSWORD") or ""
VALKEY_URL = os.environ.get("BIGQMT_VALKEY_URL")
VALKEY_PROTOCOL = os.environ.get("BIGQMT_VALKEY_PROTOCOL") or 2
RPC_ALLOW_ORDER_METHODS = False
# 未指定策略名时委托带的 投资备注。QMT 会把它显示在 委托 列表的 报单来源 列
# （issue #154），所以这不是内部字段。设为 "" 可让该列留空，和手动下单一样。
RPC_DEFAULT_STRATEGY_NAME = None
RPC_PROCESS_IN_LISTENER = True
RPC_BACKGROUND_THREADS = False
# "*" expands to read-only RPC methods only. Order/cancel/sync methods still go
# through the queue fallback and require schedule_adjust=True when enabled.
RPC_LISTENER_METHODS = ("*",)
# Transport selection: valkey or shm.
# Backend-specific configuration is forwarded to the factory.
RPC_TRANSPORT = "valkey"
RPC_SERIAL_CONFIG = {}
# None defaults to enabled; explicit True/False overrides it.
RPC_NATIVE_XTDATA_ENABLED = None
QUOTE_PUSH_CONFIG = {}
SCHEDULE_ADJUST_ENABLED = True
# How often the strategy thread drains the RPC queue (via adjust). Lower = less
# queue wait for read RPCs. Verify on the live box that run_time honors sub-3s
# intervals (see the adjust cadence log) before trusting a low value.
SCHEDULE_ADJUST_INTERVAL = "500nMilliSecond"
# How long one adjust tick may keep the strategy thread draining RPC requests
# (#303). None = one adjust interval, never under 0.5s; 0 disables the bound.
RPC_DRAIN_BUDGET_SECONDS = None
# Post-submit/-cancel settle window before the RPC answers without a broker id
# (#396 follow-up). None = the strategy/handlers default (3.0s) owns it; set
# order_settle_timeout_seconds in the valkey block to override. Forwarded only
# when named, so the default keeps living in one place.
RPC_ORDER_SETTLE_TIMEOUT_SECONDS = None
# Heavy reads (downloads, financial data, market-token get_full_tick, tick
# period / date-window get_market_data_ex, > RPC_HEAVY_CODES_THRESHOLD codes)
# run on one worker thread instead of the adjust thread in drain mode (#351),
# so a 1.8s read is not the strategy's own tick stopped for 1.8s. Their reply
# pays ~1-2 ticks; everything else stays at one tick. False = 0.3.52 behavior.
RPC_HEAVY_OFFLOAD = True
RPC_HEAVY_CODES_THRESHOLD = 20
FULL_TICK_CACHE_ENABLED = False
FULL_TICK_DEMAND_TTL_SECONDS = 10
FULL_TICK_CACHE_TTL_SECONDS = 10
# Symbol-list demands refresh fast; whole-market (SH/SZ/BJ/HK) demands refresh on
# a slower cadence so a ~50k row snapshot is not pulled on every fast tick.
FULL_TICK_REFRESH_INTERVAL_SECONDS = 0.5
FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = 3.0
# Wall-clock budget for one refresh round to avoid stalling the strategy thread.
FULL_TICK_REFRESH_MAX_WALL_SECONDS = 0.3
FULL_TICK_MAX_REQUESTS = 8
# Async download jobs: the strategy thread drains one queued job at a time,
# downloading DOWNLOAD_JOB_CHUNK_SIZE symbols per tick (capped by the wall-clock
# budget), so a long download never blocks the RPC pump. chunk_size is the
# smallest per-tick block, so keep it modest if per-symbol downloads are slow.
# DISABLED by default: the full Big QMT terminal's embedded xtdata SDK has no
# reachable data service to download through (raises "无法连接行情服务"). Supplement
# history via the terminal's 数据管理/补充数据 UI, then read it over RPC with
# get_market_data_ex / get_local_data. Re-enable only where a MiniQMT/xtdata data
# service is connectable (set download_jobs_enabled=True in the local config).
DOWNLOAD_JOBS_ENABLED = False
DOWNLOAD_JOB_CHUNK_SIZE = 10
DOWNLOAD_JOB_MAX_WALL_SECONDS = 0.5
DOWNLOAD_JOB_TTL_SECONDS = 3600
# Push order_callback/deal_callback details to Valkey so clients get real-time
# on_stock_order/on_stock_trade callbacks (MiniQMT style) instead of polling.
EXEC_EVENTS_ENABLED = True
# Dump the raw order_callback/deal_callback object fields to the QMT output panel
# (and into the published event as "raw_fields"). Off by default — it prints on
# every callback. Turn on to settle what m_nDirection/m_nOffsetFlag actually
# carry live, which the buy/sell direction mapping currently assumes.
EXEC_EVENTS_DEBUG_RAW_FIELDS = False
# QMT fires order_callback once with the row pre-sysid and again once the id
# lands (#152's window) -- the client then sees two identical 已报 events
# (issue #161). Sysid-less order events are held this many seconds waiting for
# the twin; 0 disables (every event publishes immediately, old behaviour).
EXEC_EVENTS_HOLD_PRESYSID_SECONDS = 0.8

try:
    # Keep optional account-type config backward compatible: an old local
    # config without BIGQMT_ACCOUNT_TYPE must not discard the account ID or
    # Valkey settings that are present in that same module.
    import bigqmt_signal_trader_qmt_config as _local_config

    BIGQMT_ACCOUNT_ID = getattr(_local_config, "BIGQMT_ACCOUNT_ID", "")
    BIGQMT_CONFIG = local_valkey_config(_local_config)
except Exception:
    BIGQMT_ACCOUNT_ID = ""
    BIGQMT_CONFIG = {}
try:
    from bigqmt_signal_trader_qmt_config import BIGQMT_ACCOUNT_TYPE
except Exception:
    BIGQMT_ACCOUNT_TYPE = ""

ACCOUNT_ID = str(BIGQMT_ACCOUNT_ID or ACCOUNT_ID or "")

# Account type. A credit account read as STOCK returns an all-zero asset row
# rather than an error, so getting this wrong is silent (issue #92).
#
# Three places look plausible and only one used to work:
#   - BIGQMT_ACCOUNT_TYPE in the local config          -- worked
#   - account_type inside BIGQMT_CONFIG          -- was ignored
#   - editing ACCOUNT_TYPE in this file                -- silently overwritten
#     below, because the shipped example config sets BIGQMT_ACCOUNT_TYPE.
# Both of the others are now honoured, and the resolved value is printed at
# startup so a setting that did not take effect is visible instead of showing
# up later as zero assets.
# The constant above ships as "STOCK", so it only counts as something the user
# chose once it has been edited away from that; otherwise every credit setup
# would report a phantom conflict with it.
_ACCOUNT_TYPE_EDITED_HERE = ACCOUNT_TYPE if ACCOUNT_TYPE != "STOCK" else ""
_account_type_sources = [
    ("BIGQMT_ACCOUNT_TYPE", BIGQMT_ACCOUNT_TYPE),
    ("BIGQMT_CONFIG['account_type']", BIGQMT_CONFIG.get("account_type")),
    ("ACCOUNT_TYPE in bigqmt_signal_trader_valkey_rpc_runtime.py",
     _ACCOUNT_TYPE_EDITED_HERE),
]
ACCOUNT_TYPE_SOURCE = "default"
ACCOUNT_TYPE = "STOCK"
for _source_name, _source_value in _account_type_sources:
    if _source_value:
        # A LIST names every type the account may be addressed as (港股通:
        # ["STOCK", "HUGANGTONG", "SHENGANGTONG"]); the first is what this
        # deployment trades as by default, the rest are honoured per request
        # (account_type_map.request_account_type).
        if isinstance(_source_value, (list, tuple)):
            _source_value = next((v for v in _source_value if str(v or "").strip()), "")
            if not _source_value:
                continue
        ACCOUNT_TYPE = str(_source_value).strip().upper()
        ACCOUNT_TYPE_SOURCE = _source_name
        break


def _report_deployment():
    """Print which build is actually running, before anything else happens.

    A deploy is a file copy and QMT keeps modules across strategy re-runs, so
    "the copy never happened" and "the copy happened but was not picked up"
    look identical from the outside. This line tells them apart at a glance.
    """
    try:
        # Submodule, not the root package: the QMT sandbox never execs
        # bigqmt_signal_trader/__init__.py, so anything defined there is
        # invisible in exactly the environment this line describes.
        from bigqmt_signal_trader.version import deployment_report

        version, directory = deployment_report()
        print("[bigqmt_shell] bigqmt_signal_trader %s 已从 %s 加载"
              % (version, directory))
    except Exception as exc:
        print("[bigqmt_shell] 无法获取版本信息：%s" % exc)


def _report_account_type():
    """Say which account type won and where it came from."""
    extra = ""
    if isinstance(BIGQMT_ACCOUNT_TYPE, (list, tuple)) and len(BIGQMT_ACCOUNT_TYPE) > 1:
        extra = " also answers as %s per request" % "/".join(
            str(v).strip().upper() for v in BIGQMT_ACCOUNT_TYPE[1:] if str(v or "").strip())
    print("[bigqmt_shell] account_type=%s（来源：%s）%s"
          % (ACCOUNT_TYPE, ACCOUNT_TYPE_SOURCE, extra))

    def _first(value):
        if isinstance(value, (list, tuple)):
            value = next((v for v in value if str(v or "").strip()), "")
        return str(value or "").strip().upper()

    conflicting = [
        name for name, value in _account_type_sources
        if _first(value) and _first(value) != ACCOUNT_TYPE
    ]
    if conflicting:
        print("[bigqmt_shell] 已忽略以下来源中冲突的 account_type：%s"
              % ", ".join(conflicting))


_report_deployment()
_report_account_type()
VALKEY_ENABLED = bool(BIGQMT_CONFIG.get("valkey_enabled", VALKEY_ENABLED))
VALKEY_ENABLED_EXPLICIT = "valkey_enabled" in BIGQMT_CONFIG
# Keep direct runtime configuration in sync with the selected transport.
RPC_TRANSPORT = str(BIGQMT_CONFIG.get("transport", RPC_TRANSPORT)).lower()
RPC_SERIAL_CONFIG = dict(BIGQMT_CONFIG.get("serial") or {})
if BIGQMT_CONFIG.get("native_xtdata_enabled") is not None:
    RPC_NATIVE_XTDATA_ENABLED = bool(BIGQMT_CONFIG.get("native_xtdata_enabled"))
VALKEY_HOST = BIGQMT_CONFIG.get("host", VALKEY_HOST)
VALKEY_PORT = int(BIGQMT_CONFIG.get("port", VALKEY_PORT))
VALKEY_DB = int(BIGQMT_CONFIG.get("db", VALKEY_DB))
VALKEY_USERNAME = BIGQMT_CONFIG.get("username", VALKEY_USERNAME)
VALKEY_PASSWORD = BIGQMT_CONFIG.get("password", VALKEY_PASSWORD)
VALKEY_URL = BIGQMT_CONFIG.get("url", VALKEY_URL)
VALKEY_PROTOCOL = BIGQMT_CONFIG.get("protocol", VALKEY_PROTOCOL)
RPC_ALLOW_ORDER_METHODS = bool(BIGQMT_CONFIG.get("rpc_allow_order_methods", RPC_ALLOW_ORDER_METHODS))
RPC_DEFAULT_STRATEGY_NAME = BIGQMT_CONFIG.get(
    "rpc_default_strategy_name", RPC_DEFAULT_STRATEGY_NAME)
RPC_PROCESS_IN_LISTENER = bool(
    BIGQMT_CONFIG.get("rpc_process_in_listener", RPC_PROCESS_IN_LISTENER and not RPC_ALLOW_ORDER_METHODS)
)
RPC_BACKGROUND_THREADS = bool(BIGQMT_CONFIG.get("rpc_background_threads", RPC_BACKGROUND_THREADS))
# The strategy side only honors an EXPLICIT rpc_background_threads on non-valkey
# transports (absent = historical default, receiver threads on). Remember
# whether the local config named it so _apply_config can forward accordingly.
RPC_BACKGROUND_THREADS_EXPLICIT = "rpc_background_threads" in BIGQMT_CONFIG
RPC_LISTENER_METHODS = tuple(BIGQMT_CONFIG.get("rpc_listener_methods", RPC_LISTENER_METHODS))
QUOTE_PUSH_CONFIG = dict(BIGQMT_CONFIG.get("quote_push", QUOTE_PUSH_CONFIG))
SCHEDULE_ADJUST_ENABLED = bool(BIGQMT_CONFIG.get("schedule_adjust", SCHEDULE_ADJUST_ENABLED))
if not RPC_BACKGROUND_THREADS:
    SCHEDULE_ADJUST_ENABLED = True
SCHEDULE_ADJUST_INTERVAL = str(BIGQMT_CONFIG.get("schedule_adjust_interval", SCHEDULE_ADJUST_INTERVAL))
RPC_DRAIN_BUDGET_SECONDS = BIGQMT_CONFIG.get("drain_budget_seconds", RPC_DRAIN_BUDGET_SECONDS)
RPC_ORDER_SETTLE_TIMEOUT_SECONDS = BIGQMT_CONFIG.get(
    "order_settle_timeout_seconds", RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
RPC_HEAVY_OFFLOAD = bool(BIGQMT_CONFIG.get("rpc_heavy_offload", RPC_HEAVY_OFFLOAD))
RPC_HEAVY_CODES_THRESHOLD = int(BIGQMT_CONFIG.get("rpc_heavy_codes_threshold", RPC_HEAVY_CODES_THRESHOLD))
FULL_TICK_CACHE_ENABLED = bool(BIGQMT_CONFIG.get("full_tick_cache_enabled", FULL_TICK_CACHE_ENABLED))
FULL_TICK_DEMAND_TTL_SECONDS = float(
    BIGQMT_CONFIG.get("full_tick_demand_ttl_seconds", FULL_TICK_DEMAND_TTL_SECONDS)
)
FULL_TICK_CACHE_TTL_SECONDS = float(
    BIGQMT_CONFIG.get("full_tick_cache_ttl_seconds", FULL_TICK_CACHE_TTL_SECONDS)
)
FULL_TICK_REFRESH_INTERVAL_SECONDS = float(
    BIGQMT_CONFIG.get("full_tick_refresh_interval_seconds", FULL_TICK_REFRESH_INTERVAL_SECONDS)
)
FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = float(
    BIGQMT_CONFIG.get("full_tick_market_refresh_interval_seconds", FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS)
)
FULL_TICK_REFRESH_MAX_WALL_SECONDS = float(
    BIGQMT_CONFIG.get("full_tick_refresh_max_wall_seconds", FULL_TICK_REFRESH_MAX_WALL_SECONDS)
)
FULL_TICK_MAX_REQUESTS = int(BIGQMT_CONFIG.get("full_tick_max_requests", FULL_TICK_MAX_REQUESTS))
DOWNLOAD_JOBS_ENABLED = bool(BIGQMT_CONFIG.get("download_jobs_enabled", DOWNLOAD_JOBS_ENABLED))
DOWNLOAD_JOB_CHUNK_SIZE = int(BIGQMT_CONFIG.get("download_job_chunk_size", DOWNLOAD_JOB_CHUNK_SIZE))
DOWNLOAD_JOB_MAX_WALL_SECONDS = float(
    BIGQMT_CONFIG.get("download_job_max_wall_seconds", DOWNLOAD_JOB_MAX_WALL_SECONDS)
)
DOWNLOAD_JOB_TTL_SECONDS = int(BIGQMT_CONFIG.get("download_job_ttl_seconds", DOWNLOAD_JOB_TTL_SECONDS))
EXEC_EVENTS_ENABLED = bool(BIGQMT_CONFIG.get("exec_events_enabled", EXEC_EVENTS_ENABLED))
EXEC_EVENTS_DEBUG_RAW_FIELDS = bool(
    BIGQMT_CONFIG.get("exec_events_debug_raw_fields", EXEC_EVENTS_DEBUG_RAW_FIELDS)
)
# issue #161: sysid-less order events are held this long waiting for their
# sysid-bearing twin; 0 publishes every event immediately (old behaviour).
EXEC_EVENTS_HOLD_PRESYSID_SECONDS = float(
    BIGQMT_CONFIG.get("exec_events_hold_presysid_seconds", 0.8)
)


def _valkey_block():
    """The valkey settings, or {} when this deployment has no valkey (#147).

    An empty dict is the whole point: every consumer already guards with
    `if not valkey_config: return None`, and those guards were dead because
    this block was emitted unconditionally with module defaults.

    transport=valkey overrides the switch -- that deployment cannot run without
    valkey, so honouring valkey_enabled=False there would break the RPC bridge
    itself rather than the optional extras.

    """
    if RPC_TRANSPORT == "serial" or (not VALKEY_ENABLED and RPC_TRANSPORT not in ("valkey", "", "default")):
        return {}
    return {
        "host": VALKEY_HOST,
        "port": VALKEY_PORT,
        "db": VALKEY_DB,
        "username": VALKEY_USERNAME,
        "password": VALKEY_PASSWORD,
        "url": VALKEY_URL,
        "protocol": VALKEY_PROTOCOL,
        "position_key_template": "bigqmt:positions:{account_id}",
        "position_event_stream_template": "bigqmt:position_events:{account_id}",
    }


def _apply_config(account_id):
    account_id = str(account_id or "")
    if account_id:
        set_account_id(account_id)
    rpc_block = {
        "enabled": True,
        "account_id": account_id,
        "allow_order_methods": RPC_ALLOW_ORDER_METHODS,
        "default_strategy_name": RPC_DEFAULT_STRATEGY_NAME,
        "request_channel_template": "bigqmt:rpc:req:{account_id}",
        "response_channel_template": "bigqmt:rpc:resp:{account_id}:{request_id}",
        "response_key_template": "bigqmt:rpc:resp:{account_id}:{request_id}",
        "response_ttl_seconds": 60,
        "drain_max_items": 20,
        "drain_budget_seconds": RPC_DRAIN_BUDGET_SECONDS,
        "heavy_offload": RPC_HEAVY_OFFLOAD,
        "heavy_codes_threshold": RPC_HEAVY_CODES_THRESHOLD,
        "process_in_listener": RPC_PROCESS_IN_LISTENER,
        "listener_methods": RPC_LISTENER_METHODS,
        # Transport selection (default valkey). Forwarded from the local
        # config for the transport factory.
        "transport": RPC_TRANSPORT,
        # Native xtdata can be explicitly disabled.
        "native_xtdata_enabled": (bool(RPC_NATIVE_XTDATA_ENABLED)
                                  if RPC_NATIVE_XTDATA_ENABLED is not None
                                  else True),
    }
    if RPC_TRANSPORT == "serial":
        rpc_block["serial"] = dict(RPC_SERIAL_CONFIG)
    # Forward order_settle_timeout_seconds ONLY when the local config named
    # it (#396 follow-up): the strategy/handlers default (3.0s) owns the unset
    # case, and forwarding None would crash its float() -- the chain used to
    # drop the key outright, so a configured value never took effect.
    if RPC_ORDER_SETTLE_TIMEOUT_SECONDS is not None:
        rpc_block["order_settle_timeout_seconds"] = float(
            RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
    # Forward background_threads ONLY when the local config named it: on
    # non-valkey transports the strategy treats an absent key as "historical
    # default" (receiver threads on) and an explicit False as the opt-in to
    # the adjust-driven drain (#104).
    if RPC_BACKGROUND_THREADS_EXPLICIT:
        rpc_block["background_threads"] = RPC_BACKGROUND_THREADS
    configure(
        mode="bigqmt",
        account_id=account_id,
        account_type=ACCOUNT_TYPE,
        position_sync_type="valkey" if RPC_TRANSPORT in ("valkey", "", "default") else "",
        enable_rpc=True,
        schedule_adjust=SCHEDULE_ADJUST_ENABLED or RPC_TRANSPORT == "serial",
        schedule_adjust_interval=SCHEDULE_ADJUST_INTERVAL,
        valkey=_valkey_block(),
        rpc=rpc_block,
        quote_push=QUOTE_PUSH_CONFIG,
        full_tick_cache={
            "enabled": FULL_TICK_CACHE_ENABLED,
            "account_id": account_id,
            "demand_ttl_seconds": FULL_TICK_DEMAND_TTL_SECONDS,
            "cache_ttl_seconds": FULL_TICK_CACHE_TTL_SECONDS,
            "refresh_interval_seconds": FULL_TICK_REFRESH_INTERVAL_SECONDS,
            "market_refresh_interval_seconds": FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS,
            "refresh_max_wall_seconds": FULL_TICK_REFRESH_MAX_WALL_SECONDS,
            "max_requests": FULL_TICK_MAX_REQUESTS,
        },
        download_jobs={
            "enabled": DOWNLOAD_JOBS_ENABLED,
            "account_id": account_id,
            "chunk_size": DOWNLOAD_JOB_CHUNK_SIZE,
            "max_wall_seconds": DOWNLOAD_JOB_MAX_WALL_SECONDS,
            "job_ttl_seconds": DOWNLOAD_JOB_TTL_SECONDS,
        },
        exec_events={
            "enabled": EXEC_EVENTS_ENABLED,
            "account_id": account_id,
            "debug_raw_fields": EXEC_EVENTS_DEBUG_RAW_FIELDS,
            "hold_presysid_order_seconds": EXEC_EVENTS_HOLD_PRESYSID_SECONDS,
        },
    )


def configure_runtime_account(account_id):
    _apply_config(account_id)


def configure_runtime_valkey(valkey_config):
    global VALKEY_ENABLED, VALKEY_HOST, VALKEY_PORT, VALKEY_DB, VALKEY_USERNAME, VALKEY_PASSWORD, RPC_ALLOW_ORDER_METHODS, RPC_DEFAULT_STRATEGY_NAME, RPC_PROCESS_IN_LISTENER, RPC_BACKGROUND_THREADS, RPC_LISTENER_METHODS, SCHEDULE_ADJUST_ENABLED, SCHEDULE_ADJUST_INTERVAL, FULL_TICK_CACHE_ENABLED, FULL_TICK_DEMAND_TTL_SECONDS, FULL_TICK_CACHE_TTL_SECONDS, FULL_TICK_REFRESH_INTERVAL_SECONDS, FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS, FULL_TICK_REFRESH_MAX_WALL_SECONDS, FULL_TICK_MAX_REQUESTS, RPC_TRANSPORT, QUOTE_PUSH_CONFIG, DOWNLOAD_JOBS_ENABLED, DOWNLOAD_JOB_CHUNK_SIZE, DOWNLOAD_JOB_MAX_WALL_SECONDS, DOWNLOAD_JOB_TTL_SECONDS, EXEC_EVENTS_ENABLED, EXEC_EVENTS_DEBUG_RAW_FIELDS, EXEC_EVENTS_HOLD_PRESYSID_SECONDS, RPC_BACKGROUND_THREADS_EXPLICIT, RPC_DRAIN_BUDGET_SECONDS, RPC_HEAVY_OFFLOAD, RPC_HEAVY_CODES_THRESHOLD, VALKEY_ENABLED_EXPLICIT, RPC_NATIVE_XTDATA_ENABLED, RPC_ORDER_SETTLE_TIMEOUT_SECONDS
    global RPC_SERIAL_CONFIG, VALKEY_URL, VALKEY_PROTOCOL
    valkey_config = valkey_config or {}
    RPC_SERIAL_CONFIG = dict(valkey_config.get("serial") or {})
    RPC_BACKGROUND_THREADS_EXPLICIT = "rpc_background_threads" in valkey_config
    VALKEY_ENABLED_EXPLICIT = "valkey_enabled" in valkey_config
    if valkey_config.get("native_xtdata_enabled") is not None:
        RPC_NATIVE_XTDATA_ENABLED = bool(valkey_config.get("native_xtdata_enabled"))
    VALKEY_ENABLED = bool(valkey_config.get("valkey_enabled", VALKEY_ENABLED))
    VALKEY_HOST = valkey_config.get("host", VALKEY_HOST)
    VALKEY_PORT = int(valkey_config.get("port", VALKEY_PORT))
    VALKEY_DB = int(valkey_config.get("db", VALKEY_DB))
    VALKEY_USERNAME = valkey_config.get("username", VALKEY_USERNAME)
    VALKEY_PASSWORD = valkey_config.get("password", VALKEY_PASSWORD)
    VALKEY_URL = valkey_config.get("url", VALKEY_URL)
    VALKEY_PROTOCOL = valkey_config.get("protocol", VALKEY_PROTOCOL)
    RPC_ALLOW_ORDER_METHODS = bool(valkey_config.get("rpc_allow_order_methods", RPC_ALLOW_ORDER_METHODS))
    RPC_DEFAULT_STRATEGY_NAME = valkey_config.get(
        "rpc_default_strategy_name", RPC_DEFAULT_STRATEGY_NAME)
    RPC_PROCESS_IN_LISTENER = bool(
        valkey_config.get("rpc_process_in_listener", RPC_PROCESS_IN_LISTENER and not RPC_ALLOW_ORDER_METHODS)
    )
    RPC_BACKGROUND_THREADS = bool(valkey_config.get("rpc_background_threads", RPC_BACKGROUND_THREADS))
    RPC_LISTENER_METHODS = tuple(valkey_config.get("rpc_listener_methods", RPC_LISTENER_METHODS))
    RPC_TRANSPORT = str(valkey_config.get("transport", RPC_TRANSPORT)).lower()
    QUOTE_PUSH_CONFIG = dict(valkey_config.get("quote_push", QUOTE_PUSH_CONFIG))
    # schedule_adjust must stay ON for all transports.
    # run_time("adjust", interval) is what THROTTLES QMT's strategy callback: with
    # it, adjust fires on the configured cadence (e.g. 500ms); WITHOUT it QMT calls
    # adjust in a hot loop (~2500/s) that pegs the GIL and starves the
    # background thread (RPC then times out entirely). It also sets the GIL-release
    # rhythm the background transport threads rely on. So keep the original rule:
    # honor an explicit value, default on, and force on when not background-threaded.
    SCHEDULE_ADJUST_ENABLED = bool(valkey_config.get("schedule_adjust", SCHEDULE_ADJUST_ENABLED))
    if not RPC_BACKGROUND_THREADS:
        SCHEDULE_ADJUST_ENABLED = True
    SCHEDULE_ADJUST_INTERVAL = str(valkey_config.get("schedule_adjust_interval", SCHEDULE_ADJUST_INTERVAL))
    RPC_DRAIN_BUDGET_SECONDS = valkey_config.get("drain_budget_seconds", RPC_DRAIN_BUDGET_SECONDS)
    RPC_ORDER_SETTLE_TIMEOUT_SECONDS = valkey_config.get(
        "order_settle_timeout_seconds", RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
    RPC_HEAVY_OFFLOAD = bool(valkey_config.get("rpc_heavy_offload", RPC_HEAVY_OFFLOAD))
    RPC_HEAVY_CODES_THRESHOLD = int(valkey_config.get("rpc_heavy_codes_threshold", RPC_HEAVY_CODES_THRESHOLD))
    FULL_TICK_CACHE_ENABLED = bool(valkey_config.get("full_tick_cache_enabled", FULL_TICK_CACHE_ENABLED))
    FULL_TICK_DEMAND_TTL_SECONDS = float(
        valkey_config.get("full_tick_demand_ttl_seconds", FULL_TICK_DEMAND_TTL_SECONDS)
    )
    FULL_TICK_CACHE_TTL_SECONDS = float(valkey_config.get("full_tick_cache_ttl_seconds", FULL_TICK_CACHE_TTL_SECONDS))
    FULL_TICK_REFRESH_INTERVAL_SECONDS = float(
        valkey_config.get("full_tick_refresh_interval_seconds", FULL_TICK_REFRESH_INTERVAL_SECONDS)
    )
    FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = float(
        valkey_config.get("full_tick_market_refresh_interval_seconds", FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS)
    )
    FULL_TICK_REFRESH_MAX_WALL_SECONDS = float(
        valkey_config.get("full_tick_refresh_max_wall_seconds", FULL_TICK_REFRESH_MAX_WALL_SECONDS)
    )
    FULL_TICK_MAX_REQUESTS = int(valkey_config.get("full_tick_max_requests", FULL_TICK_MAX_REQUESTS))
    DOWNLOAD_JOBS_ENABLED = bool(valkey_config.get("download_jobs_enabled", DOWNLOAD_JOBS_ENABLED))
    DOWNLOAD_JOB_CHUNK_SIZE = int(valkey_config.get("download_job_chunk_size", DOWNLOAD_JOB_CHUNK_SIZE))
    DOWNLOAD_JOB_MAX_WALL_SECONDS = float(
        valkey_config.get("download_job_max_wall_seconds", DOWNLOAD_JOB_MAX_WALL_SECONDS)
    )
    DOWNLOAD_JOB_TTL_SECONDS = int(valkey_config.get("download_job_ttl_seconds", DOWNLOAD_JOB_TTL_SECONDS))
    EXEC_EVENTS_ENABLED = bool(valkey_config.get("exec_events_enabled", EXEC_EVENTS_ENABLED))
    EXEC_EVENTS_DEBUG_RAW_FIELDS = bool(
        valkey_config.get("exec_events_debug_raw_fields", EXEC_EVENTS_DEBUG_RAW_FIELDS)
    )
    EXEC_EVENTS_HOLD_PRESYSID_SECONDS = float(
        valkey_config.get("exec_events_hold_presysid_seconds", EXEC_EVENTS_HOLD_PRESYSID_SECONDS)
    )
    _apply_config(ACCOUNT_ID)


def bind_runtime_api(passorder_func=None, cancel_func=None, get_trade_detail_data_func=None,
                     extra_funcs=None):
    bind_qmt_api(
        passorder_func=passorder_func,
        cancel_func=cancel_func,
        get_trade_detail_data_func=get_trade_detail_data_func,
        extra_funcs=extra_funcs,
    )


_apply_config(ACCOUNT_ID)
