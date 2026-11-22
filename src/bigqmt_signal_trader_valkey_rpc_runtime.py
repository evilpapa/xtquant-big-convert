# coding: utf-8
"""大 QMT Valkey Pub/Sub RPC 策略入口。

此入口不消费交易信号。RPC 下单接口默认关闭，只读接口和持仓同步默认开启。
"""

import os
import sys


# QMT 通过 exec 加载策略，__file__ 可能未定义。检查脚本目录和工作目录等候选位置，
# 将包含 bigqmt_signal_trader_strategy.py 的目录加到 sys.path 开头，供导入策略和配置。
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

# QMT 通过 exec 把交易、下载和查询函数注入当前入口的全局命名空间。
# 策略模块看不到这些名字，因此用 capture_qmt_injected_funcs 统一收集。
# 普通导入时没有这些全局函数，由 DRYRUN 入口通过 bind_runtime_api 显式绑定。

# QMT 重跑本文件时，策略模块仍可能缓存在 sys.modules 中。
# 先停止旧 RPC 服务并清理订阅，避免重复运行时遗留资源。
try:
    reset_app()
except NameError:
    pass

_qmt_injected = capture_qmt_injected_funcs(globals())
if _qmt_injected:
    bind_qmt_api(extra_funcs=_qmt_injected)
    print("[bigqmt_runtime] 已绑定 QMT 注入的全局函数：%s" % sorted(_qmt_injected))


ACCOUNT_ID                                = ""
# 账号类型：STOCK(股票) / CREDIT(信用/两融) / FUTURE(期货) / OPTION(期权)
# 对应 xtconstant 枚举：SECURITY_ACCOUNT=2 / CREDIT_ACCOUNT=3 / FUTURE_ACCOUNT=1
ACCOUNT_TYPE                              = "STOCK"
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
VALKEY_ENABLED                            = True
VALKEY_HOST                               = os.environ.get("BIGQMT_VALKEY_HOST") or "127.0.0.1"
VALKEY_PORT                               = int(os.environ.get("BIGQMT_VALKEY_PORT") or 6379)
VALKEY_DB                                 = int(os.environ.get("BIGQMT_VALKEY_DB") or 5)
VALKEY_USERNAME                           = os.environ.get("BIGQMT_VALKEY_USERNAME") or ""
VALKEY_PASSWORD                           = os.environ.get("BIGQMT_VALKEY_PASSWORD") or ""
VALKEY_URL                                = os.environ.get("BIGQMT_VALKEY_URL")
VALKEY_PROTOCOL                           = os.environ.get("BIGQMT_VALKEY_PROTOCOL") or 2
RPC_ALLOW_ORDER_METHODS                   = False
# 未指定策略名时委托带的 投资备注。QMT 会把它显示在 委托 列表的 报单来源 列
# （issue #154），所以这不是内部字段。设为 "" 可让该列留空，和手动下单一样。
RPC_DEFAULT_STRATEGY_NAME                 = None
RPC_PROCESS_IN_LISTENER                   = True
RPC_BACKGROUND_THREADS                    = False
# "*" 展开为只读 RPC 方法。委托、撤单和持仓同步走队列，由 adjust 处理。
RPC_LISTENER_METHODS                      = ("*",)
# 将所选传输方式和对应配置传给传输工厂。
RPC_TRANSPORT                             = "valkey"
RPC_SERIAL_CONFIG                         = {}
# None 表示采用默认启用行为，也可显式传 True 或 False。
RPC_NATIVE_XTDATA_ENABLED                 = None
QUOTE_PUSH_CONFIG                         = {}
SCHEDULE_ADJUST_ENABLED                   = True
# 主策略线程通过 adjust 处理 RPC 队列的间隔。缩短间隔可减少排队等待。
# 设为 3 秒以下时，用调度日志确认目标终端的 run_time 确实按该间隔触发。
SCHEDULE_ADJUST_INTERVAL                  = "500nMilliSecond"
# 每次 adjust 处理 RPC 的时间上限（#303）。
# None 表示取调度间隔且至少 0.5 秒，0 表示不设上限。
RPC_DRAIN_BUDGET_SECONDS                  = None
# 提交或撤单后等待委托确认的时间（#396）。未设置时由处理器使用默认的 3 秒。
# 显式设置 order_settle_timeout_seconds 才覆盖默认值。
RPC_ORDER_SETTLE_TIMEOUT_SECONDS          = None
# 队列模式可把符合条件的大型读取交给单独线程（#351），例如财务数据、
# 全市场快照、逐笔或区间行情，以及超过标的数阈值的查询。
# 这可减少 adjust 被占用的时间，重读取应答约多等 1～2 个调度周期。
RPC_HEAVY_OFFLOAD                         = True
RPC_HEAVY_CODES_THRESHOLD                 = 20
FULL_TICK_CACHE_ENABLED                   = False
FULL_TICK_DEMAND_TTL_SECONDS              = 10
FULL_TICK_CACHE_TTL_SECONDS               = 10
# 标的列表刷新较快，全市场（SH/SZ/BJ/HK）刷新较慢，避免频繁读取约 5 万行行情。
FULL_TICK_REFRESH_INTERVAL_SECONDS        = 0.5
FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = 3.0
# 限制单轮刷新耗时，避免长时间占用策略线程。
FULL_TICK_REFRESH_MAX_WALL_SECONDS        = 0.3
FULL_TICK_MAX_REQUESTS                    = 8
# 异步下载按标的分批推进，单轮受耗时预算限制。若单个标的下载较慢，应调小批量。
# 默认关闭。完整大 QMT 内置的 xtdata SDK 可能无法连接行情服务，可通过终端的
#「数据管理 / 补充数据」准备历史数据，再用 get_market_data_ex 或 get_local_data 读取。
DOWNLOAD_JOBS_ENABLED                     = False
DOWNLOAD_JOB_CHUNK_SIZE                   = 10
DOWNLOAD_JOB_MAX_WALL_SECONDS             = 0.5
DOWNLOAD_JOB_TTL_SECONDS                  = 3600
# 推送委托与成交回报，让客户端实时触发 MiniQMT 风格的回调。
EXEC_EVENTS_ENABLED                       = True
# 将委托或成交原始字段打印到 QMT 面板，并随事件附上 raw_fields。
# 每次回调都会输出，仅在排查 m_nDirection、m_nOffsetFlag 等字段时开启。
EXEC_EVENTS_DEBUG_RAW_FIELDS              = False
# QMT 分别在委托号生成前后触发回调，客户端可能收到两条「已报」事件（#161）。
# 暂存无委托号事件并等待后续回调。设为 0 时立即发布每条事件。
EXEC_EVENTS_HOLD_PRESYSID_SECONDS         = 0.8

try:
    # 兼容未设置 BIGQMT_ACCOUNT_TYPE 的旧配置，仍读取账号和 Valkey 参数。
    import bigqmt_signal_trader_qmt_config as _local_config

    BIGQMT_ACCOUNT_ID = getattr(_local_config, "BIGQMT_ACCOUNT_ID", "")
    BIGQMT_CONFIG     = local_valkey_config(_local_config)
except Exception:
    BIGQMT_ACCOUNT_ID = ""
    BIGQMT_CONFIG     = {}
try:
    from bigqmt_signal_trader_qmt_config import BIGQMT_ACCOUNT_TYPE
except Exception:
    BIGQMT_ACCOUNT_TYPE = ""

ACCOUNT_ID = str(BIGQMT_ACCOUNT_ID or ACCOUNT_ID or "")

# 信用账户按 STOCK 查询可能悄悄返回全零资产（#92），因此启动时打印实际类型和来源。
# 按下列顺序取第一个有效值。本文件原有的 STOCK 默认值不算显式配置。
_ACCOUNT_TYPE_EDITED_HERE = ACCOUNT_TYPE if ACCOUNT_TYPE != "STOCK" else ""
_account_type_sources = [
    ("BIGQMT_ACCOUNT_TYPE", BIGQMT_ACCOUNT_TYPE),
    ("BIGQMT_CONFIG['account_type']", BIGQMT_CONFIG.get("account_type")),
    ("ACCOUNT_TYPE in bigqmt_signal_trader_valkey_rpc_runtime.py",
     _ACCOUNT_TYPE_EDITED_HERE),
]
ACCOUNT_TYPE_SOURCE = "default"
ACCOUNT_TYPE        = "STOCK"
for _source_name, _source_value in _account_type_sources:
    if _source_value:
        # 列表列出账号可用的全部类型，例如港股通。第一项作默认值，其余按请求选择。
        if isinstance(_source_value, (list, tuple)):
            _source_value = next((v for v in _source_value if str(v or "").strip()), "")
            if not _source_value:
                continue
        ACCOUNT_TYPE        = str(_source_value).strip().upper()
        ACCOUNT_TYPE_SOURCE = _source_name
        break


def _report_deployment():
    """打印实际运行的版本和路径，便于识别重新运行时仍被 QMT 缓存的旧模块。"""
    try:
        # QMT 中根包的 __init__.py 可能不执行，因此从子模块读取版本。
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
VALKEY_ENABLED                            = bool(BIGQMT_CONFIG.get("valkey_enabled", VALKEY_ENABLED))
VALKEY_ENABLED_EXPLICIT                   = "valkey_enabled" in BIGQMT_CONFIG
# Keep direct runtime configuration in sync with the selected transport.
RPC_TRANSPORT                             = str(BIGQMT_CONFIG.get("transport", RPC_TRANSPORT)).lower()
RPC_SERIAL_CONFIG                         = dict(BIGQMT_CONFIG.get("serial") or {})
if BIGQMT_CONFIG.get("native_xtdata_enabled") is not None:
    RPC_NATIVE_XTDATA_ENABLED = bool(BIGQMT_CONFIG.get("native_xtdata_enabled"))
VALKEY_HOST                               = BIGQMT_CONFIG.get("host", VALKEY_HOST)
VALKEY_PORT                               = int(BIGQMT_CONFIG.get("port", VALKEY_PORT))
VALKEY_DB                                 = int(BIGQMT_CONFIG.get("db", VALKEY_DB))
VALKEY_USERNAME                           = BIGQMT_CONFIG.get("username", VALKEY_USERNAME)
VALKEY_PASSWORD                           = BIGQMT_CONFIG.get("password", VALKEY_PASSWORD)
VALKEY_URL                                = BIGQMT_CONFIG.get("url", VALKEY_URL)
VALKEY_PROTOCOL                           = BIGQMT_CONFIG.get("protocol", VALKEY_PROTOCOL)
RPC_ALLOW_ORDER_METHODS                   = bool(BIGQMT_CONFIG.get("rpc_allow_order_methods", RPC_ALLOW_ORDER_METHODS))
RPC_DEFAULT_STRATEGY_NAME                 = BIGQMT_CONFIG.get("rpc_default_strategy_name", RPC_DEFAULT_STRATEGY_NAME)
RPC_PROCESS_IN_LISTENER                   = bool(BIGQMT_CONFIG.get("rpc_process_in_listener", RPC_PROCESS_IN_LISTENER and not RPC_ALLOW_ORDER_METHODS))
RPC_BACKGROUND_THREADS                    = bool(BIGQMT_CONFIG.get("rpc_background_threads", RPC_BACKGROUND_THREADS))
# The strategy side only honors an EXPLICIT rpc_background_threads on non-valkey
# transports (absent = historical default, receiver threads on). Remember
# whether the local config named it so _apply_config can forward accordingly.
RPC_BACKGROUND_THREADS_EXPLICIT           = "rpc_background_threads" in BIGQMT_CONFIG
RPC_LISTENER_METHODS                      = tuple(BIGQMT_CONFIG.get("rpc_listener_methods", RPC_LISTENER_METHODS))
QUOTE_PUSH_CONFIG                         = dict(BIGQMT_CONFIG.get("quote_push", QUOTE_PUSH_CONFIG))
SCHEDULE_ADJUST_ENABLED                   = bool(BIGQMT_CONFIG.get("schedule_adjust", SCHEDULE_ADJUST_ENABLED))
if not RPC_BACKGROUND_THREADS:
    SCHEDULE_ADJUST_ENABLED = True
SCHEDULE_ADJUST_INTERVAL                  = str(BIGQMT_CONFIG.get("schedule_adjust_interval", SCHEDULE_ADJUST_INTERVAL))
RPC_DRAIN_BUDGET_SECONDS                  = BIGQMT_CONFIG.get("drain_budget_seconds", RPC_DRAIN_BUDGET_SECONDS)
RPC_ORDER_SETTLE_TIMEOUT_SECONDS          = BIGQMT_CONFIG.get("order_settle_timeout_seconds", RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
RPC_HEAVY_OFFLOAD                         = bool(BIGQMT_CONFIG.get("rpc_heavy_offload", RPC_HEAVY_OFFLOAD))
RPC_HEAVY_CODES_THRESHOLD                 = int(BIGQMT_CONFIG.get("rpc_heavy_codes_threshold", RPC_HEAVY_CODES_THRESHOLD))
FULL_TICK_CACHE_ENABLED                   = bool(BIGQMT_CONFIG.get("full_tick_cache_enabled", FULL_TICK_CACHE_ENABLED))
FULL_TICK_DEMAND_TTL_SECONDS              = float(BIGQMT_CONFIG.get("full_tick_demand_ttl_seconds", FULL_TICK_DEMAND_TTL_SECONDS))
FULL_TICK_CACHE_TTL_SECONDS               = float(BIGQMT_CONFIG.get("full_tick_cache_ttl_seconds", FULL_TICK_CACHE_TTL_SECONDS))
FULL_TICK_REFRESH_INTERVAL_SECONDS        = float(BIGQMT_CONFIG.get("full_tick_refresh_interval_seconds", FULL_TICK_REFRESH_INTERVAL_SECONDS))
FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = float(BIGQMT_CONFIG.get("full_tick_market_refresh_interval_seconds", FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS))
FULL_TICK_REFRESH_MAX_WALL_SECONDS        = float(BIGQMT_CONFIG.get("full_tick_refresh_max_wall_seconds", FULL_TICK_REFRESH_MAX_WALL_SECONDS))
FULL_TICK_MAX_REQUESTS                    = int(BIGQMT_CONFIG.get("full_tick_max_requests", FULL_TICK_MAX_REQUESTS))
DOWNLOAD_JOBS_ENABLED                     = bool(BIGQMT_CONFIG.get("download_jobs_enabled", DOWNLOAD_JOBS_ENABLED))
DOWNLOAD_JOB_CHUNK_SIZE                   = int(BIGQMT_CONFIG.get("download_job_chunk_size", DOWNLOAD_JOB_CHUNK_SIZE))
DOWNLOAD_JOB_MAX_WALL_SECONDS             = float(BIGQMT_CONFIG.get("download_job_max_wall_seconds", DOWNLOAD_JOB_MAX_WALL_SECONDS))
DOWNLOAD_JOB_TTL_SECONDS                  = int(BIGQMT_CONFIG.get("download_job_ttl_seconds", DOWNLOAD_JOB_TTL_SECONDS))
EXEC_EVENTS_ENABLED                       = bool(BIGQMT_CONFIG.get("exec_events_enabled", EXEC_EVENTS_ENABLED))
EXEC_EVENTS_DEBUG_RAW_FIELDS              = bool(BIGQMT_CONFIG.get("exec_events_debug_raw_fields", EXEC_EVENTS_DEBUG_RAW_FIELDS))
# issue #161: sysid-less order events are held this long waiting for their
# sysid-bearing twin; 0 publishes every event immediately (old behaviour).
EXEC_EVENTS_HOLD_PRESYSID_SECONDS         = float(BIGQMT_CONFIG.get("exec_events_hold_presysid_seconds", 0.8))


def _valkey_block():
    """返回 Valkey 配置。串口模式返回空字典，使依赖 Valkey 的可选功能跳过初始化。"""
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
    # 只转发显式配置的等待时间，避免把 None 传给处理器的 float()（#396）。
    if RPC_ORDER_SETTLE_TIMEOUT_SECONDS is not None:
        rpc_block["order_settle_timeout_seconds"] = float(RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
    # 只转发显式配置的 background_threads，保留适配器对缺省值的判断（#104）。
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
    RPC_SERIAL_CONFIG                         = dict(valkey_config.get("serial") or {})
    RPC_BACKGROUND_THREADS_EXPLICIT           = "rpc_background_threads" in valkey_config
    VALKEY_ENABLED_EXPLICIT                   = "valkey_enabled" in valkey_config
    if valkey_config.get("native_xtdata_enabled") is not None:
        RPC_NATIVE_XTDATA_ENABLED = bool(valkey_config.get("native_xtdata_enabled"))
    VALKEY_ENABLED                            = bool(valkey_config.get("valkey_enabled", VALKEY_ENABLED))
    VALKEY_HOST                               = valkey_config.get("host", VALKEY_HOST)
    VALKEY_PORT                               = int(valkey_config.get("port", VALKEY_PORT))
    VALKEY_DB                                 = int(valkey_config.get("db", VALKEY_DB))
    VALKEY_USERNAME                           = valkey_config.get("username", VALKEY_USERNAME)
    VALKEY_PASSWORD                           = valkey_config.get("password", VALKEY_PASSWORD)
    VALKEY_URL                                = valkey_config.get("url", VALKEY_URL)
    VALKEY_PROTOCOL                           = valkey_config.get("protocol", VALKEY_PROTOCOL)
    RPC_ALLOW_ORDER_METHODS                   = bool(valkey_config.get("rpc_allow_order_methods", RPC_ALLOW_ORDER_METHODS))
    RPC_DEFAULT_STRATEGY_NAME                 = valkey_config.get("rpc_default_strategy_name", RPC_DEFAULT_STRATEGY_NAME)
    RPC_PROCESS_IN_LISTENER                   = bool(valkey_config.get("rpc_process_in_listener", RPC_PROCESS_IN_LISTENER and not RPC_ALLOW_ORDER_METHODS))
    RPC_BACKGROUND_THREADS                    = bool(valkey_config.get("rpc_background_threads", RPC_BACKGROUND_THREADS))
    RPC_LISTENER_METHODS                      = tuple(valkey_config.get("rpc_listener_methods", RPC_LISTENER_METHODS))
    RPC_TRANSPORT                             = str(valkey_config.get("transport", RPC_TRANSPORT)).lower()
    QUOTE_PUSH_CONFIG                         = dict(valkey_config.get("quote_push", QUOTE_PUSH_CONFIG))
    # run_time 为 adjust 限频。未注册定时器时，QMT 可能高频调用 adjust，
    # 占用 GIL，导致后台线程饥饿和 RPC 超时。非后台模式强制启用调度。
    SCHEDULE_ADJUST_ENABLED                   = bool(valkey_config.get("schedule_adjust", SCHEDULE_ADJUST_ENABLED))
    if not RPC_BACKGROUND_THREADS:
        SCHEDULE_ADJUST_ENABLED = True
    SCHEDULE_ADJUST_INTERVAL                  = str(valkey_config.get("schedule_adjust_interval", SCHEDULE_ADJUST_INTERVAL))
    RPC_DRAIN_BUDGET_SECONDS                  = valkey_config.get("drain_budget_seconds", RPC_DRAIN_BUDGET_SECONDS)
    RPC_ORDER_SETTLE_TIMEOUT_SECONDS          = valkey_config.get("order_settle_timeout_seconds", RPC_ORDER_SETTLE_TIMEOUT_SECONDS)
    RPC_HEAVY_OFFLOAD                         = bool(valkey_config.get("rpc_heavy_offload", RPC_HEAVY_OFFLOAD))
    RPC_HEAVY_CODES_THRESHOLD                 = int(valkey_config.get("rpc_heavy_codes_threshold", RPC_HEAVY_CODES_THRESHOLD))
    FULL_TICK_CACHE_ENABLED                   = bool(valkey_config.get("full_tick_cache_enabled", FULL_TICK_CACHE_ENABLED))
    FULL_TICK_DEMAND_TTL_SECONDS              = float(valkey_config.get("full_tick_demand_ttl_seconds", FULL_TICK_DEMAND_TTL_SECONDS))
    FULL_TICK_CACHE_TTL_SECONDS               = float(valkey_config.get("full_tick_cache_ttl_seconds", FULL_TICK_CACHE_TTL_SECONDS))
    FULL_TICK_REFRESH_INTERVAL_SECONDS        = float(valkey_config.get("full_tick_refresh_interval_seconds", FULL_TICK_REFRESH_INTERVAL_SECONDS))
    FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS = float(valkey_config.get("full_tick_market_refresh_interval_seconds", FULL_TICK_MARKET_REFRESH_INTERVAL_SECONDS))
    FULL_TICK_REFRESH_MAX_WALL_SECONDS        = float(valkey_config.get("full_tick_refresh_max_wall_seconds", FULL_TICK_REFRESH_MAX_WALL_SECONDS))
    FULL_TICK_MAX_REQUESTS                    = int(valkey_config.get("full_tick_max_requests", FULL_TICK_MAX_REQUESTS))
    DOWNLOAD_JOBS_ENABLED                     = bool(valkey_config.get("download_jobs_enabled", DOWNLOAD_JOBS_ENABLED))
    DOWNLOAD_JOB_CHUNK_SIZE                   = int(valkey_config.get("download_job_chunk_size", DOWNLOAD_JOB_CHUNK_SIZE))
    DOWNLOAD_JOB_MAX_WALL_SECONDS             = float(valkey_config.get("download_job_max_wall_seconds", DOWNLOAD_JOB_MAX_WALL_SECONDS))
    DOWNLOAD_JOB_TTL_SECONDS                  = int(valkey_config.get("download_job_ttl_seconds", DOWNLOAD_JOB_TTL_SECONDS))
    EXEC_EVENTS_ENABLED                       = bool(valkey_config.get("exec_events_enabled", EXEC_EVENTS_ENABLED))
    EXEC_EVENTS_DEBUG_RAW_FIELDS              = bool(valkey_config.get("exec_events_debug_raw_fields", EXEC_EVENTS_DEBUG_RAW_FIELDS))
    EXEC_EVENTS_HOLD_PRESYSID_SECONDS         = float(valkey_config.get("exec_events_hold_presysid_seconds", EXEC_EVENTS_HOLD_PRESYSID_SECONDS))
    _apply_config(ACCOUNT_ID)


def bind_runtime_api(passorder_func=None, cancel_func=None, get_trade_detail_data_func=None, extra_funcs=None):
    bind_qmt_api(
        passorder_func=passorder_func,
        cancel_func=cancel_func,
        get_trade_detail_data_func=get_trade_detail_data_func,
        extra_funcs=extra_funcs,
    )


_apply_config(ACCOUNT_ID)
