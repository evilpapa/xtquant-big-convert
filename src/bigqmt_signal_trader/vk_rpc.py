"""大 QMT 的 RPC 请求分发和应答。

服务可在监听线程直接处理指定请求。依赖策略上下文的 QMT API 通过
内存队列交给策略线程，由 drain_pending 执行。
"""

import base64
import collections
import datetime as _dt
import json
import math
import queue
import threading
import time
import traceback
import uuid

from .adapters.vk_common import decode_text
from .code_utils import normalize_stock_code
from . import exec_events as _exec_events
from .models import AccountSnapshot, OrderRef, OrderRequest


# 用单调时钟计算确认期限，避免系统校时影响。
_monotonic = time.monotonic


RPC_REVISION = "20260715-execution-snapshot-v1"


READ_METHODS = {
    "ping",
    "get_deployment_info",
    "get_request_outcome",
    "probe_capabilities",
    "probe_order_identity",
    "get_ticks",
    "get_instrument",
    "get_instrument_type",
    "get_market_data",
    "get_market_data_ex",
    "get_local_data",
    "get_stock_list_in_sector",
    "get_sector_list",
    "get_sector_info",
    "get_markets",
    "get_market_last_trade_date",
    "get_divid_factors",
    "download_history_data",
    "download_history_data2",
    "get_trading_dates",
    "get_holidays",
    "download_holiday_data",
    "download_his_st_data",
    "get_ipo_info",
    "get_etf_info",
    "download_etf_info",
    "get_option_list",
    "get_his_option_list",
    "get_his_option_list_batch",
    "get_financial_data",
    "download_financial_data",
    "download_financial_data2",
    "call_formula",
    "subscribe_formula",
    "unsubscribe_formula",
    "get_formula_result",
    "gen_factor_index",
    "get_positions",
    "get_position_statistics",
    "get_asset",
    "query_orders",
    "query_trades",
    "query_execution_snapshot",
    "describe_trade_detail_fields",
    "reload_deployment",
    "reload_status",
    "query_stock_position",
    "sync_positions",
    "submit_download_history_data",
    "submit_download_history_data2",
    "get_download_status",
    "wait_download",
    # 账户 / 融资融券 / 交易扩展查询（官方全局函数 + detail types）
    "query_account_infos",
    "query_account_status",
    "query_credit_detail",
    "query_credit_account",
    "query_stk_compacts",
    "query_credit_subjects",
    "query_credit_slo_code",
    "query_credit_assure",
    "query_appointment_info",
    "query_smt_secu_info",
    "query_smt_secu_rate",
    "smt_appointment",
    # 官方交易查询函数（直接暴露，运行时注入的全局函数）
    "get_value_by_order_id",
    "get_last_order_id",
    "get_ipo_data",
    "get_new_purchase_limit",
    "quote_subscription_status",
    "get_history_trade_detail_data",
    "get_assure_contract",
    "get_enable_short_contract",
    "get_unclosed_compacts",
    "get_closed_compacts",
    "get_debt_contract",
    "get_option_subject_position",
    "get_comb_option",
    "get_hkt_exchange_rate",
}

ORDER_METHODS = {
    "submit_order",
    "submit_orders_batch",
    "cancel_order",
    "cancel_orders_batch",
    # 直接透传原生 passorder，受 allow_order_methods 控制，由 adjust 执行。
    "passorder",
}

# 全推订阅控制方法交给 QuoteSubscriptionManager 按引用计数管理。
# 行情数据通过独立推送通道发送。
QUOTE_SUBSCRIPTION_METHODS = {
    "subscribe_whole_quote",
    "unsubscribe_whole_quote",
    "unsubscribe_whole_quote_batch",
    "quote_keepalive",
    "quote_unsubscribe_all",
}

LISTENER_DEFERRED_METHODS = {
    "sync_positions",
    # get_trade_detail_data 在后台线程返回空结果，资产和其他交易查询
    # 都交给主策略线程的 adjust 执行。
    "get_asset",
    "get_positions",
    "get_position_statistics",
    "query_stock_position",
    "query_orders",
    "query_trades",
    "query_execution_snapshot",
    "describe_trade_detail_fields",
    "reload_deployment",
    "query_account_infos",
    "query_account_status",
    "query_credit_detail",
    "query_credit_account",
    "query_stk_compacts",
    "query_credit_subjects",
    "query_credit_slo_code",
    "query_credit_assure",
    "query_appointment_info",
    "get_ipo_data",   # 8-28: 交易类查询, 需主线程上下文 (后台线程返回空)
    # get_ipo_data 上面那条注释适用于所有走 QMT 交易类全局函数的查询，当时只
    # 修了它一个。真两融账户的体检报告把漏网的挑了出来：同一次运行里
    # query_stk_compacts -> 52 行、query_credit_subjects -> 71002 行、
    # query_credit_slo_code -> 50 行，而调同一个 QMT 函数的直连 get_* 全是 0 行。
    # 两边 handler 逐字节相同、账号相同，唯一差别就是 query_* 在这张表里、
    # 直连的不在，于是后者跑在后台 listener 线程上 —— 正是 get_ipo_data 当年
    # 踩的那个坑。孪生方法必须待在同一条线程路径上（#204）。
    "get_assure_contract",
    "get_enable_short_contract",
    "get_unclosed_compacts",
    "get_closed_compacts",
    "get_debt_contract",
    "get_option_subject_position",
    "get_comb_option",
    "get_new_purchase_limit",
    "get_hkt_exchange_rate",
    "query_smt_secu_info",
    "query_smt_secu_rate",
    "get_value_by_order_id",
    "get_last_order_id",
    "get_history_trade_detail_data",
}

# READ_METHODS 中的行情查询可以在监听线程执行。仅将上列依赖交易上下文的
# 查询交给策略线程，以免缺少队列轮询的传输方式无法处理普通查询。


METHOD_ALIASES = {
    "get_full_tick": "get_ticks",
    "get_instrument_detail": "get_instrument",
    "get_instrumentdetail": "get_instrument",
    "getDividFactors": "get_divid_factors",
    "query_stock_asset": "get_asset",
    "query_stock_positions": "get_positions",
    "query_position_statistics": "get_position_statistics",
    "query_stock_orders": "query_orders",
    "query_stock_trades": "query_trades",
    "order_stock": "submit_order",
    "order_stock_async": "submit_order",
    "order_stock_batch": "submit_orders_batch",
    "cancel_order_stock": "cancel_order",
    "cancel_order_stock_sysid": "cancel_order",
    "cancel_order_stock_batch": "cancel_orders_batch",
}

BUY_ORDER_TYPES = {"23", "STOCK_BUY", "BUY", "B"}
SELL_ORDER_TYPES = {"24", "STOCK_SELL", "SELL", "S"}
CANCELABLE_ORDER_STATUSES = {"50", "55"}
CANCELED_ORDER_STATUSES = {"53", "54"}

# 默认投资备注显示在 QMT 委托列表的「报单来源」列（#154）。
# 可通过单次调用的 strategy_name 或配置 rpc_default_strategy_name 覆盖，空字符串留空。
DEFAULT_ORDER_STRATEGY_NAME = "bigqmt_rpc"
# query_credit_account 是查柜台的，官方参考 6.13 原话：「本函数只能有一个查询，
# 如果前面的查询正在进行中，后面的查询将会提前返回。本函数从服务器查询数据，
# 建议平均查询时间间隔 30s 一次，不可频繁调用。」所以桥这边自己限流，客户端
# 想怎么轮询都不会把柜台打爆（#202）。
# hollow 告警的最小间隔。跑错线程时每一次查询都会 hollow，不节流日志会被刷爆。
HOLLOW_WARNING_INTERVAL_SECONDS = 60.0
CREDIT_ACCOUNT_MIN_INTERVAL_SECONDS = 30.0
# 缓存超过这个年龄就不再交出去。
#
# 这条通道的缓存**从不自己刷新** —— 没人调就永远不刷，age_seconds 可以是 44
# 秒，也可以是三小时。真正的风险不是「30 秒 vs 0 秒」，而是一个只读 rows、
# 不看 age_seconds 的调用方，拿三小时前的**维持担保比例**去做决策还毫无察觉。
# 维持担保比例是强平线，读错方向是真损失。
#
# 120 秒：官方最小间隔是 30 秒，留够几次调用的余量，又不至于让陈数据在盘中
# 蒙混过关。要实时的维持担保比例请走 query_credit_detail（同步、读终端本地
# 缓存、无此问题）。传 0 表示不限制，那是显式放弃这层保护。
CREDIT_ACCOUNT_MAX_AGE_SECONDS = 120.0
# 一次 RPC 里最多等回调多久。**默认 0：不等。**
#
# handler 跑在 adjust 主线程上（它在 LISTENER_DEFERRED_METHODS 里，必须如此，
# 否则拿不到交易上下文）。原来这里 sleep 轮询等回调，实盘上（真两融账户、
# 已确认在大 QMT 里回调能触发）表现是：query_issued=true、等满 8 秒、
# callbacks_seen=0、无任何报错。
#
# 最可能的解释：QMT 把 credit_account_callback 投递到主线程上，而我正用等待
# 堵着这条线程 —— 用等待堵死了唯一能送达的那条路，回调只可能在 handler 返回
# 之后才进来。order_callback 走 C++ 线程，这个不一定同理，不能照搬。
#
# 所以默认改成「发出去就返回」：这一次拿到的是缓存（或空），回调随后自己落下，
# 下一次调用就能取到。在两种假设下都安全 —— 如果回调其实走的是 C++ 线程，
# 代价也只是多一次调用。想等的人显式传 wait_seconds 即可。
CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS = 0.0
CREDIT_ACCOUNT_MAX_WAIT_SECONDS = 10.0
TERMINAL_NON_CANCEL_ORDER_STATUSES = {"56", "57"}
# 51 已报待撤 / 52 部成待撤：交易所已 **接受** 撤单、正在途中。既不是已撤也
# 不是失败——继续等待，到截止时间时报“已受理”，而不是报“仍为状态 51”的失败
# （问题 #151；#148 假阴性的窄窗口孪生）。
CANCEL_IN_FLIGHT_STATUSES = {"51", "52"}
SAFE_B64_PREFIX = "b64s:"
SAFE_B64_DIGIT_ENCODE = str.maketrans("0123456789", "!#$%&()*~?")
SAFE_B64_DIGIT_DECODE = str.maketrans("!#$%&()*~?", "0123456789")
MARKET_DATA_METHODS = {
    "get_instrument_type",
    "get_market_data",
    "get_market_data_ex",
    "get_local_data",
    "get_stock_list_in_sector",
    "get_sector_list",
    "get_sector_info",
    "get_markets",
    "get_market_last_trade_date",
    "get_divid_factors",
    "download_history_data",
    "download_history_data2",
    "get_trading_dates",
    "get_holidays",
    "download_holiday_data",
    "download_his_st_data",
    "get_ipo_info",
    "get_etf_info",
    "download_etf_info",
    "get_option_list",
    "get_his_option_list",
    "get_his_option_list_batch",
    "get_financial_data",
    "download_financial_data",
    "download_financial_data2",
    "call_formula",
    "subscribe_formula",
    "unsubscribe_formula",
    "get_formula_result",
    "gen_factor_index",
    # 龙虎榜 / 股东 / 换手率 / 行业 / 收盘价
    "get_longhubang",
    "get_top10_share_holder",
    "get_holder_num",
    "get_turnover_rate",
    "get_industry",
    "get_close_price",
    # 期权定价 / 隐含波动率
    "bsm_price",
    "bsm_iv",
    "get_option_iv",
    "get_option_detail_data",
    "get_option_detail_data_batch",
    "get_option_undl_data",
    "get_option_undl",
    # 财务扩展 / 因子
    "get_raw_financial_data",
    "get_factor_data",
    # 历史 ST / 指数权重
    "get_his_st_data",
    "get_his_index_data",
    # 期货 / 合约
    "get_main_contract",
    "get_his_contract_list",
    "get_date_location",
    "get_ETF_list",
    # 北向资金 / 港股通
    "get_north_finance_change",
    "get_hkt_statistics",
    "get_hkt_details",
    # 自定义板块（写）。issue #143：这一族以前只有 create_sector，而它在大 QMT
    # 上是静默空操作；其余几个在白名单里有名字却没实现，调用报「未实现」。
    "create_sector",
    "create_sector_folder",
    "add_stock_to_sector",
    "remove_stock_from_sector",
    "reset_sector_stock_list",
    # 基础查询辅助
    "get_stock_name",
    "get_stock_type",
    "get_last_close",
    "get_last_volume",
    "get_open_date",
    "get_contract_expire_date",
    "get_contract_multiplier",
    "get_float_caps",
    "get_total_share",
    "get_turn_over_rate",
    "get_weight_in_index",
    "get_svol",
    "get_bvol",
    "get_risk_free_rate",
    # L2 行情（需 L2 权限 + 原生 xtdata SDK 行情服务）
    "get_l2_quote",
    "get_l2_order",
    "get_l2_transaction",
    "subscribe_l2thousand",
    # 指数权重 / 交易日历 / 交易时段 / 可转债 / 品种判断
    "get_index_weight",
    "get_trading_calendar",
    "get_trade_times",
    "get_cb_info",
    "is_stock_type",
    # 板块增删
    "add_sector",
    "remove_sector",
    # 数据下载扩展
    "download_cb_data",
    "download_history_contracts",
    "download_index_weight",
    "download_sector_data",
    # 时间戳转换（纯计算，服务端本地）
    "datetime_to_timetag",
    "timetag_to_datetime",
}

# 保持 READ_METHODS 与 MARKET_DATA_METHODS 同步：每个转发给适配器的行情方法
# 也都能通过 RPC 调用。（create_sector 是写操作——创建/更新自定义板块——但暴露
# 它无害；交易类写操作仍受 ORDER_METHODS + allow_order_methods 约束。）
READ_METHODS |= MARKET_DATA_METHODS
READ_METHODS |= QUOTE_SUBSCRIPTION_METHODS

# 无论参数如何都很重的读取（#351）：财务读取热态 0.8s、冷态数分钟（#104），
# 公式求值和因子生成会跑到结束。在 adjust 排空（rpc_background_threads False）
# 时，这些每一个都在 **策略线程** 上跑，而一次 1.8s 的 get_market_data_ex 就是
# 策略自己的 tick_app 停 1.8s——这正是 #321 把 LPOP 移出 adjust 线程的原因。
# 重读取改为交给一个工作线程：它们的回复要付跨线程 GIL 交接成本（每次获取约
# 一 tick）。
#
# 工作线程能带来多少收益，取决于 QMT 在这次调用中有多少时间释放了 GIL。
# 实盘实测（2026-09-22，100ms tick，每次读取都反复打回工作线程，「adjust
# 节奏」为 10s 窗口内的均值 / 最大值）：
#
#     get_full_tick ["SH","SZ"]      读取 ~200ms      节奏 0.100 / 0.25-0.33s
#     get_market_data_ex 3 x 4mo 1m   ~500ms          节奏 0.100 / 0.27-0.29s
#     get_financial_data 10 x 3 表    ~2.1s           节奏 0.100 / ~0.5s
#     download_history_data2 5 代码   ~1.2s           节奏 0.56-0.63 / 1.3-1.5s
#
# 前三个在读取的大部分时间里释放 GIL：tick 保持其均值，最坏情况从整次读取
# 缩短到其中一部分。下载在全程持有 GIL——工作线程对 tick 没有任何改善，还要给
# 回复增加一两个 tick——所以 download_* 保持就地执行。交易上下文方法
# （LISTENER_DEFERRED_METHODS）永远不在这个集合里：get_trade_detail_data 在
# 非主线程上答空。
HEAVY_READ_METHODS = frozenset([
    "get_financial_data",
    "get_raw_financial_data",
    "get_factor_data",
    "gen_factor_index",
    "call_formula",
    "get_formula_result",
    "get_his_option_list_batch",
    "get_option_detail_data_batch",
    "get_his_index_data",
    "get_longhubang",
])

# 因 **规模** 而重的读取：一个代码是毫秒级，市场标记或长名单是秒级
# （get_full_tick：每标的 0.29ms，"SH" 要 1.1s，
# 全部读要 7.4s；get_market_data_ex 每个代码 tick 周期 3.9s，1m K 线的日期
# 窗口 0.3-1.4s）。由 _heavy_by_size 按请求判定。
HEAVY_READ_CODE_METHODS = frozenset([
    "get_ticks",            # get_full_tick
    "get_market_data",
    "get_market_data_ex",
    "get_local_data",
])
HEAVY_READ_CODES_THRESHOLD = 20


def _maybe_scalar(value):
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            return value
    return value


def _is_vk_timeout(exc):
    name = exc.__class__.__name__.lower()
    module = getattr(exc.__class__, "__module__", "")
    text = str(exc).lower()
    return (module.startswith("valkey.") and "timeout" in name
            or "timeout reading from socket" in text)


def to_jsonable(value):
    # 行情载荷里占主导的几种形状的快路径。全市场快照有 190 万个节点，过去每个
    # 都要走下面完整的类型探测链——从对每个普通 float 都落空的
    # getattr(value, "item") 开始。在 51285 个标的上实测：628.7ms -> 328.0ms，
    # 输出字节完全相同。
    #
    # 有意按精确类型而不是 isinstance 判断：numpy 的 float64 是 float 的子类，
    # 用身份检查能让它落到完整路径、由 _maybe_scalar 解包。这里的提速不能改变
    # 任何东西序列化后的结果。
    kind = type(value)
    if kind is float:
        return None if (math.isnan(value) or math.isinf(value)) else value
    if kind is int or kind is str or kind is bool or value is None:
        return value
    if kind is dict:
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if kind is list:
        return [to_jsonable(item) for item in value]

    value = _maybe_scalar(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "isoformat") and value.__class__.__module__.startswith("pandas"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if hasattr(value, "to_dict") and hasattr(value, "columns") and hasattr(value, "index"):
        try:
            columns = value.columns
            if getattr(columns, "nlevels", 1) > 1:
                # Python 3.12 的 QMT 终端对 get_financial_data /
                # get_top10_share_holder / get_market_data 的多股票形式用
                # MultiIndex DataFrame 作答，而那里过去是 pandas.Panel（pandas
                # 1.0 移除了 Panel；3.12 运行时自带 3.0）。下面的
                # reset_index()/to_dict 会把元组列标签压平成字符串——
                # "('000002.SZ', 'close')"——客户端就会重建一个无法按代码索引的
                # frame。改为按第一层切片并复用 Panel 信封，它的客户端重建结果是
                # {code: DataFrame}：两代 pandas 都提供的同一个 df[code] 子表形状。
                codes = list(columns.get_level_values(0).unique())
                return {
                    "__bigqmt_type__": "Panel",
                    "items": [str(code) for code in codes],
                    "major_axis": [str(item) for item in list(value.index)],
                    "minor_axis": [
                        str(item) for item in list(columns.get_level_values(1).unique())
                    ],
                    "data": {str(code): to_jsonable(value[code]) for code in codes},
                }
            frame = value.reset_index()
            return {
                "__bigqmt_type__": "DataFrame",
                "columns": [str(col) for col in frame.columns],
                "records": to_jsonable(frame.to_dict("records")),
            }
        except Exception:
            return str(value)
    if hasattr(value, "to_dict") and hasattr(value, "index") and not isinstance(value, dict):
        try:
            return {
                "__bigqmt_type__": "Series",
                "data": to_jsonable(value.to_dict()),
            }
        except Exception:
            return str(value)
    # pandas Panel（三维）。QMT 自带 pandas 0.22，其中 get_financial_data 在多
    # 股票且多日期时仍返回一个。Panel 没有 .columns 也没有 .index，于是它越过
    # DataFrame 和 Series 分支一路落到 __dict__ 兜底——而 vars(panel) 是
    # {'_data': ..., 'is_copy': None}，经下划线过滤后只剩 {'is_copy': None}。
    # 这正是调用方当时拿到的（问题 #115）：不是错误，只是该对象恰好有的那个
    # 公开属性。
    # pandas 1.0 移除了 Panel，所以即使把轴发过去客户端也重建不出来。改为每项
    # 发一个 DataFrame；它到达时是 dict。
    if (hasattr(value, "major_axis") and hasattr(value, "minor_axis")
            and hasattr(value, "items") and not isinstance(value, dict)):
        try:
            labels = [str(item) for item in value.items]
            return {
                "__bigqmt_type__": "Panel",
                "items": labels,
                "major_axis": [str(item) for item in value.major_axis],
                "minor_axis": [str(item) for item in value.minor_axis],
                "data": {str(item): to_jsonable(value[item]) for item in value.items},
            }
        except Exception:
            return str(value)
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return to_jsonable(value.tolist())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, (str, int, float, bool)):
        return enum_value
    if hasattr(value, "__dict__"):
        return {
            key: to_jsonable(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


def _rows_for_this_submit(orders, remark, stock_code, action, submitted_at):
    """可能是本次提交的委托行，最早的在前。

    备注匹配是必要条件而非充分条件（#299）：调用方可能复用了它。一行还必须是
    同一只股票，且当该行给出方向时方向也要相同；当该行带插入时间时，它不能早于
    本次提交所在的秒。随后按插入时间升序排列，使同一股票上稍后对同一备注的复用
    （网格的下一档，在本笔还在结算时下的）不会遮蔽本笔。没有插入时间的行保留
    终端顺序，并排在有时间的行之后。
    """
    want_remark = str(remark or "").strip()
    stock_code = normalize_stock_code(stock_code)
    action = str(action or "").upper()
    not_before = int(submitted_at) if submitted_at else 0
    matched = []
    for index, row in enumerate(orders or []):
        if str(getattr(row, "user_order_id", "") or "").strip() != want_remark:
            continue
        if normalize_stock_code(getattr(row, "stock_code", "")) != stock_code:
            continue
        row_action = str(getattr(row, "action", "") or "").upper()
        if row_action and action and row_action != action:
            continue
        order_time = 0
        try:
            order_time = int(getattr(row, "order_time", 0) or 0)
        except (TypeError, ValueError):
            pass
        if order_time > 0 and not_before and order_time < not_before:
            continue
        matched.append((order_time <= 0, order_time, index, row))
    matched.sort(key=lambda item: item[:3])
    return [item[3] for item in matched]


class OrderSettlement(object):
    """一笔等待 order_sys_id 的委托。

    为什么不能简单地放到后台线程：get_trade_detail_data 在非主策略线程上返回空
    （见 LISTENER_DEFERRED_METHODS），所以后台轮询什么也找不到，并把每笔委托都
    报成静默拒单。重试必须发生在 adjust 线程上——只是不要占着它。

    server_error 放在这里，而不是 handlers._last_server_error：那个槽位属于当时
    在途的任意请求，而结算写它的时候本请求早已离开处理器（问题 #43）。
    """

    __slots__ = ("order_request", "result", "deadline", "attempts", "server_error",
                 "request", "response", "submitted_at", "shadow")

    def __init__(self, order_request, result, deadline, submitted_at=None, shadow=False):
        self.order_request = order_request
        self.result = result
        self.deadline = deadline
        self.attempts = 0
        self.server_error = ""
        self.request = None
        self.response = None
        # 影子结算（#345）属于回复已经发出去的那种委托（order_stock_async，
        # wait_settlement=False）。它不会被作答；它只盯住这笔委托是否出现，并在
        # 截止时间已过仍无踪影时推一条 order_error，使调用方的 on_order_error 因
        # 终端在创建任何委托记录之前就做的拒单而触发（资金不足：QMT 屏幕上弹窗、
        # 无回调、链路上什么都没有）。
        self.shadow = bool(shadow)
        # 在 passorder 执行 **之前** 取的墙钟时刻（#299）。查找发现的、记录时间
        # 早于此刻的任何东西——盯盘表条目、委托行——都属于恰好带同一备注的上一笔
        # 委托，绝不属于本笔。
        self.submitted_at = time.time() if submitted_at is None else float(submitted_at)


class CancelSettlement(object):
    """一份等待委托实际状态的原生撤单结果。

    大 QMT 注入的 ``cancel`` 返回值在不同终端构建之间都不可靠——两个方向都是。
    在国金 2.1.19.0 上，即使券商已确认请求、委托在 67ms 内进入状态 54，它也返回
    假（问题 #148）；而对根本不存在的委托却返回真（问题 #151）。与订单号结算
    一样，状态回读必须留在 adjust 线程上，因为 get_trade_detail_data 在工作线程
    上为空。
    """

    __slots__ = ("order_ref", "account_id", "result", "deadline", "attempts",
                 "request", "response", "account_type")

    def __init__(self, order_ref, account_id, result, deadline, account_type=None):
        self.order_ref = order_ref
        # 撤单请求声明的类型（港股通），在该类型下回读。
        self.account_type = account_type
        self.account_id = account_id
        self.result = result
        self.deadline = deadline
        self.attempts = 0
        self.request = None
        self.response = None


class BigQmtRpcHandlers:
    """白名单 RPC 方法处理器，由可替换的适配器支撑。"""

    def __init__(
        self,
        account_id,
        market_data,
        position_provider,
        order_gateway=None,
        position_sync_sink=None,
        allow_order_methods=False,
        allowed_methods=None,
        qmt_api=None,
        settle_orders_inline=False,
        order_settle_timeout_seconds=3.0,
        quote_subscription_manager=None,
        default_strategy_name=None,
    ):
        # 调用方没有指定策略时，委托携带的内容。它不是内部的：QMT 把它显示在
        # 委托列表的「报单来源」列里，所以每笔委托都在向看那个界面的人宣告
        # "bigqmt_rpc"，有位报告者要求去掉它（问题 #154）。逐次调用的
        # strategy_name= 一直优先；只是没有办法一次性设定默认值。这里空字符串是
        # 一个真实答案——它让该列留空，就像手工下的单一样。
        self.default_strategy_name = (
            DEFAULT_ORDER_STRATEGY_NAME if default_strategy_name is None
            else str(default_strategy_name))
        self.account_id = str(account_id or "")
        self.market_data = market_data
        self.position_provider = position_provider
        self.order_gateway = order_gateway
        self.position_sync_sink = position_sync_sink
        self.allow_order_methods = bool(allow_order_methods)
        self.quote_subscription_manager = quote_subscription_manager
        # QMT 运行时注入的全局函数（passorder/get_trade_detail_data/
        # 融资融券查询等)。由 strategy._build_config 解析注入。
        self.qmt_api = dict(qmt_api or {})
        self._submit_journal = {}
        # 进程内订单身份日志：remark -> strategy_name，提交时写入、查询时回读。
        # VK 身份库覆盖重启和其它进程；这个覆盖完全没有 VK 的部署（zmq
        # 单文件），那里归属过去会永远读到 ""（问题 #156，接 #133）。运行时会在
        # 开始服务请求之前，为无 VK 的部署启用磁盘日志。有界 FIFO，与 VK
        # 存储相同的七天 TTL。
        self._order_identity_local = collections.OrderedDict()
        # 已结算 id 日志（#299 后续）：remark -> 已发出的 order_sys_ids 的
        # OrderedDict，按插入顺序。股票/方向/not_before 守卫比较的是 **到达**
        # 时间——回调事件的、委托行的——而到达不等于归属：2026-09-14 实盘测得，
        # 委托 1 通过轮询结算完成，而它的 order_callback 晚几毫秒才到，即晚于
        # 委托 2（同备注、同股票、串行提交）开始之后；新鲜的到达时间通过了委托 2
        # 的 not_before 守卫，而委托 1 已返回的 id 却去回答了委托 2 的结算。桥接层
        # 为某个备注返回的 id，永远属于那笔委托，绝不能再次提供给后来同备注的
        # 结算。
        self._settled_sysids_by_remark = collections.OrderedDict()
        # 委托结算。默认异步：在这里阻塞会占住 QMT 主策略线程，把其它所有请求
        # 串行排在后面，并把吞吐上限压到约 2 笔/秒（问题 #44）。
        self._pending_settlement = None
        self.settle_orders_inline = bool(settle_orders_inline)
        self.order_settle_timeout_seconds = float(order_settle_timeout_seconds)
        # 对静默失败的服务端诊断（例如 passorder 已提交但委托不在系统里）。通过
        # server_error 送到客户端。
        self._last_server_error = ""
        # 上一次 hollow 告警的时间戳。节流是必须的：一个跑错线程的部署会让
        # **每一次**交易类查询都 hollow，不节流的话日志会被刷爆（#139 就是
        # 一行写了 16 次那种教训）。
        self._last_hollow_warning_at = 0.0
        # 信用账户的「查柜台」通道（#202）。query_credit_account 是异步的：结果
        # 只从 credit_account_callback 出来，所以这里存最后一次回调的结果，由
        # note_credit_account 填。官方 6.13 明说「只能有一个查询」「建议 30s 一
        # 次，不可频繁调用」，所以单飞 + 最小间隔，不能每个请求都透传下去。
        self._credit_account_lock = threading.Lock()
        self._credit_account_rows = []
        self._credit_account_seq = 0
        self._credit_account_stamp = 0.0      # 回调落地的时间
        self._credit_account_asked = 0.0      # 上一次真的发出查询的时间
        self._credit_account_inflight = False
        self._credit_account_error = ""
        # QMT 实际回调了多少次。0 = QMT 没回调；>0 但没数据 = 回调来了但结果
        # 没落进来。两者的排查方向完全不同，从外面看却一样（#202）。
        self._credit_account_callbacks = 0
        # 回调实际落在哪条线程上。adjust 线程 = 等待会把它堵死；C++ 回调线程
        # = 等待是安全的。这是个事实问题，记下来就不用猜（#202）。
        self._credit_account_callback_thread = ""
        self.credit_account_min_interval_seconds = CREDIT_ACCOUNT_MIN_INTERVAL_SECONDS
        self.credit_account_max_wait_seconds = CREDIT_ACCOUNT_MAX_WAIT_SECONDS
        self.credit_account_max_age_seconds = CREDIT_ACCOUNT_MAX_AGE_SECONDS
        if allowed_methods is None:
            allowed = set(READ_METHODS)
            if self.allow_order_methods:
                allowed.update(ORDER_METHODS)
            self.allowed_methods = allowed
        else:
            self.allowed_methods = {str(method) for method in allowed_methods}

    def _request_account_id(self, params):
        params = params or {}
        account = params.get("account")
        if isinstance(account, dict):
            account = account.get("account_id") or account.get("accountID") or account.get("id")
        account_id = str(params.get("account_id") or account or self.account_id or "")
        if not account_id:
            raise ValueError("account_id is required")
        return account_id

    def _canonical_method(self, method):
        return METHOD_ALIASES.get(method, method)

    def handle(self, method, params=None):
        requested_method = str(method or "").strip()
        method = self._canonical_method(requested_method)
        params = dict(params or {})
        # 每个请求清空诊断槽位。它是被 **每个** 响应读取的实例状态（见
        # _build_response），所以没有这一句，一次失败的 submit_order 会把它的
        # server_error 盖到之后每次 ping/查询上，直到下一笔委托运行为止——在成功
        # 的请求上报告过期的失败（问题 #43）。
        self._last_server_error = ""
        self._pending_settlement = None
        if not requested_method:
            raise ValueError("method is required")
        if method not in self.allowed_methods:
            raise ValueError("rpc method is not allowed: %s" % requested_method)
        if method in ORDER_METHODS and not self.allow_order_methods:
            raise PermissionError("order rpc methods are disabled")
        # query_stock_positions 在 MiniQMT 里是列表形状。它在权限和 adjust 线程
        # 路由上仍是 get_positions 的别名，但需要自己的处理器，以免同合约的多空
        # 行被 provider 的旧映射契约合并。
        if requested_method == "query_stock_positions":
            handler = self._handle_query_stock_positions
        else:
            handler = getattr(self, "_handle_%s" % method, None)
        # 调用方 StockAccount 声明的账户类型，限定在本线程的本次请求上：股票账户
        # id 上的港股通查询读的是 HUGANGTONG 行而不是 STOCK 行（account_type_map）。
        from .account_type_map import request_account_type

        with request_account_type(params.get("account_type")):
            if handler is None and method in MARKET_DATA_METHODS:
                return self._handle_market_data_method(method, params)
            elif handler is None:
                raise ValueError("rpc method is not implemented: %s" % requested_method)
            return handler(params)

    def _handle_ping(self, params):
        return {
            "pong": True,
            "account_id": self.account_id,
            "allow_order_methods": bool(self.allow_order_methods),
            "rpc_revision": RPC_REVISION,
            "version": _deployed_version(),
            "account_type": self._reported_account_type(
                params.get("account_id") or self.account_id),
            # 本账户可能被当作的所有类型（股票账户上的港股通：["STOCK",
            # "HUGANGTONG", "SHENGANGTONG"]）；客户端的不匹配告警会拿它的
            # StockAccount 类型对照这个列表。
            "account_types": self._reported_account_types(
                params.get("account_id") or self.account_id),
            "server_time": _dt.datetime.now(),
        }

    def _reported_account_types(self, account_id=None):
        try:
            from .account_type_map import account_types_for

            gateway = self.order_gateway
            default = getattr(gateway, "account_type", "") if gateway is not None else ""
            return list(account_types_for(account_id or self.account_id, default or "STOCK"))
        except Exception:
            return []

    def _reported_account_type(self, account_id=None):
        """这个部署实际会以什么类型交易。

        客户端的 StockAccount(..., "CREDIT") 永远不会到达服务端——类型来自 QMT 侧
        配置的 BIGQMT_ACCOUNT_TYPE——所以在一个 STOCK 部署上声明 CREDIT 的调用方
        无从看到不一致。它呈现为一行为全零的两融资产（问题 #92）。没有网关可问
        时为空。

        给了 account_id 且网关支持按请求解析时，返回按映射解析后的类型（多账户
        部署下 FUTURE 账户看到 "STOCK" 是同一类 #92 bug）。
        """
        gateway = self.order_gateway
        if gateway is None:
            return ""
        if account_id is not None and hasattr(gateway, "_resolve_account_type"):
            return str(gateway._resolve_account_type(account_id) or "").strip().upper()
        return str(getattr(gateway, "account_type", "") or "").strip().upper()

    def _handle_get_deployment_info(self, params):
        """这个桥接层从哪儿运行，以及是哪个构建。

        部署到 QMT 只是拷文件，而 QMT 在策略重跑之间把模块留在 sys.modules 里——
        所以“拷贝根本没发生”和“拷贝落地了但没被加载”从客户端看无法区分。这让
        客户端可以去问，而不是猜测，也给同步工具提供了拷贝目标，而无需交易进程
        重写自己的代码。
        """
        import os
        import sys as _sys

        info = {
            "version": "",
            "package_dir": "",
            "qmt_python_dir": "",
            "strategy_dir": "",
            "python_version": "",
            "rpc_revision": RPC_REVISION,
        }
        try:
            info["python_version"] = ".".join(
                str(part) for part in _sys.version_info[:3])
        except Exception:
            pass
        try:
            # 子模块：QMT 沙箱从不执行根包。
            from bigqmt_signal_trader.version import deployment_report

            version, package_dir = deployment_report()
            info["version"] = version
            info["package_dir"] = package_dir
            # QMT 的 python 目录是包的父目录：同步工具往那里写，顶层模块也住在
            # 那里。
            info["qmt_python_dir"] = os.path.dirname(package_dir)
        except Exception as exc:
            info["error"] = "%s: %s" % (exc.__class__.__name__, exc)
        try:
            strategy = _sys.modules.get("bigqmt_signal_trader_strategy")
            path = getattr(strategy, "__file__", "")
            if path:
                info["strategy_dir"] = os.path.dirname(os.path.abspath(path))
        except Exception:
            pass
        return info

    # probe 时检查的关键 QMT 运行时全局函数（存在与否决定对应能力可用性）。
    _PROBE_QMT_GLOBALS = (
        "passorder", "cancel", "get_trade_detail_data",
        "download_history_data", "download_history_data2", "down_history_data",
        "get_history_trade_detail_data", "get_value_by_order_id", "get_last_order_id",
        "get_ipo_data", "get_new_purchase_limit",
        "get_assure_contract", "get_enable_short_contract",
        "get_unclosed_compacts", "get_closed_compacts", "get_debt_contract",
        "get_option_subject_position", "get_comb_option", "get_hkt_exchange_rate",
        # 公式族（#374）：全局优先修复后，这里回答「这台终端到底注没注入」。
        "call_formula", "subscribe_formula", "unsubscribe_formula",
        "get_formula_result", "gen_factor_index",
    )

    # probe 时抽查的 ContextInfo 方法。
    # 板块写入那几个是为 issue #142 加的：读取（get_sector_list /
    # get_stock_list_in_sector）确认可用，而写入走的是哪条通道一直没验过 ——
    # 官方文档把 create_sector(parent_node, sector_name, overwrite) 记为 QMT
    # 全局函数，本仓库却按 ContextInfo.create_sector(sectorname, stocklist) 调。
    # 只探测存在性，不调用：create_sector 是写操作。
    #
    # get_market_data_ex_ori 是 #237 补进来的：适配层「有原始接口就只走它」，
    # 所以两台终端跑的可能是两条完全不同的代码路径 —— 而名单里没有这个名字时，
    # probe 永远不报它，报告人和维护者都拿它当「两边一样」的证据，白白多走了两
    # 轮。名单本身就是判据，缺一个名字等于把这条差异藏起来。
    _PROBE_CONTEXT_METHODS = (
        "get_full_tick", "get_market_data_ex", "get_market_data_ex_ori",
        "get_market_data", "get_local_data",
        "subscribe_quote", "subscribe_whole_quote", "unsubscribe_quote",
        "get_trading_dates", "get_financial_data", "get_stock_list_in_sector",
        "do_back_test", "get_trade_detail_data",
        "get_sector_list", "create_sector", "create_sector_folder",
        "add_sector", "remove_sector", "remove_stock_from_sector", "reset_sector",
    )

    def _handle_probe_capabilities(self, params):
        """只读能力探测：当前部署暴露了哪些 QMT callable。

        部署后跑一次就能回答「这台券商 QMT 缺什么」——只读调用，不触发任何
        委托。返回三部分：运行时全局函数绑定情况、ContextInfo 方法存在性、
        信用接口只读探测（调用一次看是否报错/返回行数）。
        """
        info = {
            "account_id": self.account_id,
            "allow_order_methods": self.allow_order_methods,
            "settle_orders_inline": self.settle_orders_inline,
            "order_settle_timeout_seconds": self.order_settle_timeout_seconds,
            "qmt_globals": {},
            "contextinfo_methods": {},
            "credit_probe": {},
            "server_time": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        for name in self._PROBE_QMT_GLOBALS:
            info["qmt_globals"][name] = callable(self.qmt_api.get(name))
        context_info = getattr(self.market_data, "context_info", None)
        for name in self._PROBE_CONTEXT_METHODS:
            info["contextinfo_methods"][name] = callable(getattr(context_info, name, None))
        # 板块写入的全局函数通道（issue #142）。注意 False 的含义有限：QMT 把全局
        # 只注入 *被挂载的那个文件* 的命名空间（PR #134 修的就是这件事），而这里
        # 看到的是本模块的 globals + builtins + 策略捕获过的 qmt_api。所以
        # True 说明确实拿得到，False 只说明「这条路径上没有」，不等于终端没有。
        info["global_namespace"] = {}
        for name in ("create_sector", "create_sector_folder", "add_sector",
                     "remove_sector", "remove_stock_from_sector", "reset_sector",
                     # query_credit_account 也走这条查：它在 qmt_api 里没有时，
                     # 要能分清「这台终端根本没有这个函数」和「有但没绑上」。
                     # 策略侧 _resolve_runtime_name 的兜底正是 builtins，所以
                     # builtins 里没有就等于这台终端不提供（#202）。
                     "query_credit_account"):
            found = self.qmt_api.get(name)
            if not callable(found):
                try:
                    import builtins
                    found = globals().get(name) or getattr(builtins, name, None)
                except Exception:
                    found = None
            info["global_namespace"][name] = callable(found)
        # 信用接口只读探测：不存在的全局直接标 unavailable；存在的真调一次，
        # 记录是否报错和返回行数（担保品/融券标的可能很多行，只计数）。
        # get_unclosed_compacts / get_closed_compacts 是两参数签名（accountID,
        # accountType），单参数调用会撞 boost::python 的 ArgumentError —— 探测
        # 自己少传一个参数，把一个好用的接口报成 ok:False（#201）。
        _two_arg = ("get_unclosed_compacts", "get_closed_compacts")
        for name in ("get_assure_contract", "get_enable_short_contract",
                     "get_unclosed_compacts", "get_debt_contract"):
            func = self.qmt_api.get(name)
            if not callable(func):
                info["credit_probe"][name] = {"available": False}
                continue
            try:
                if name in _two_arg:
                    rows = func(self.account_id,
                                self._configured_account_type(self.account_id)) or []
                else:
                    rows = func(self.account_id) or []
                report = {"available": True, "ok": True}
                report.update(self._describe_probe_rows(rows))
                info["credit_probe"][name] = report
            except Exception as exc:
                info["credit_probe"][name] = {
                    "available": True, "ok": False,
                    "error": "%s: %s" % (exc.__class__.__name__, exc),
                }
        # 信用账户对象本身（query_credit_detail 的数据源）。空列表有两种成因 ——
        # 这本账户不是信用账户，还是这台终端读不到 —— m_nBrokerType 能分开：
        # 3 = 信用。#201 的报告只看到「空列表」，无从判断是哪一种。
        info["credit_probe"]["get_trade_detail_data(CREDIT,ACCOUNT)"] = \
            self._probe_credit_account_object()
        info["credit_callback"] = {
            "note_credit_account_available": hasattr(self, "note_credit_account"),
            "callbacks_seen": getattr(self, "_credit_account_callbacks", 0),
            "cached_rows": len(getattr(self, "_credit_account_rows", []) or []),
            "last_callback_thread": getattr(self, "_credit_account_callback_thread", ""),
        }
        info["thread_routing"] = self._probe_thread_routing()
        info["sector_probe"] = self._probe_sector_channels()
        info["download_probe"] = self._probe_download_channels(params)
        info["order_watch"] = self._probe_order_watch()
        info["reply_residency"] = self._probe_reply_residency()
        return info

    # ------------------------------------------------------------------
    # 信用账户「查柜台」通道（#202）
    # ------------------------------------------------------------------

    def note_credit_account(self, seq, result):
        """credit_account_callback 的落点。

        QMT 从 C++ 回调线程调进来，所以这里只做「归一化 + 存下来」，绝不碰
        get_trade_detail_data 之类需要主线程上下文的东西，也绝不抛出去 ——
        回调里抛异常在 QMT 那边表现为无堆栈的 SystemError。
        """
        try:
            rows = _normalize_detail_rows([result] if result is not None else [])
        except Exception as exc:
            with self._credit_account_lock:
                self._credit_account_callbacks += 1
                self._credit_account_inflight = False
                self._credit_account_error = "%s: %s" % (exc.__class__.__name__, exc)
            return False
        try:
            thread_name = threading.current_thread().name
        except Exception:
            thread_name = "?"
        with self._credit_account_lock:
            self._credit_account_callbacks += 1
            self._credit_account_callback_thread = thread_name
            self._credit_account_rows = rows
            self._credit_account_stamp = time.time()
            self._credit_account_inflight = False
            self._credit_account_error = ""
            try:
                self._credit_account_seq = int(seq)
            except Exception:
                pass
        return True

    def _issue_credit_account_query(self, account_id):
        """触发一次 query_credit_account，遵守柜台的频率限制。

        返回 (issued, reason)。不发起是正常结果而非失败：缓存答案仍会照常提供。
        """
        func = self.qmt_api.get("query_credit_account")
        if not callable(func):
            return False, "query_credit_account is not bound in this deployment"
        context_info = getattr(self.market_data, "context_info", None)
        if context_info is None:
            return False, "no ContextInfo available"
        now = time.time()
        with self._credit_account_lock:
            if self._credit_account_inflight:
                # 兜底：inflight 卡住超过一个最小间隔就当它丢了，重新放行。
                # 官方说「前一个查询还在进行中，后面的会提前返回」，所以重问
                # 最坏也就是提前返回；而卡死是永久失去这条通道。
                stuck_for = now - self._credit_account_asked
                if stuck_for < self.credit_account_min_interval_seconds:
                    return False, ("a query is already in flight (%.1fs)"
                                   % stuck_for)
                self._credit_account_inflight = False
            since = now - self._credit_account_asked
            if self._credit_account_asked and since < self.credit_account_min_interval_seconds:
                return False, ("rate limited: %.1fs since the last query, minimum %.1fs"
                               % (since, self.credit_account_min_interval_seconds))
            self._credit_account_inflight = True
            self._credit_account_asked = now
            seq = int(now)
        try:
            func(account_id, seq, context_info)
        except Exception as exc:
            with self._credit_account_lock:
                self._credit_account_inflight = False
                self._credit_account_error = "%s: %s" % (exc.__class__.__name__, exc)
            return False, self._credit_account_error
        return True, ""

    def _handle_query_credit_account(self, params):
        """信用账户明细，查柜台的那条路（官方参考 6.13）。

        query_credit_account(accId, seq, ContextInfo) 是异步的，结果只从
        credit_account_callback 出来，所以这个 handler 做的是：发一次查询（受
        30s 最小间隔约束），有界地等回调落地，然后连同「数据是什么时候的、这次
        有没有真发出去」一起返回。等不到就返回上一次的缓存 —— 但响应里会说清楚
        stale=true，不会把陈数据冒充新的。
        """
        account_id = self._request_account_id(params)
        wait_seconds = params.get("wait_seconds")
        if wait_seconds is None:
            wait_seconds = CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS
        wait_seconds = max(0.0, min(self.credit_account_max_wait_seconds,
                                    float(wait_seconds)))
        max_age_seconds = params.get("max_age_seconds")
        if max_age_seconds is None:
            max_age_seconds = self.credit_account_max_age_seconds
        max_age_seconds = max(0.0, float(max_age_seconds))
        with self._credit_account_lock:
            stamp_before = self._credit_account_stamp
            known_callback_thread = self._credit_account_callback_thread
        # 回调如果投递在**本线程**上，等待就永远等不到 —— 我们正占着它。实盘
        # 实测（真两融账户）：callback_thread="MainThread"，而 handler 也在
        # MainThread，等满 8 秒 callbacks_seen 不动，那几次回调全是在两次调用
        # 之间、handler 没占着线程的时候落下的。所以一旦观测到这种情况就不再
        # 等，直接返回缓存 —— 白等只是把 adjust 主线程按住几秒（#202）。
        wait_pointless = False
        if wait_seconds > 0 and known_callback_thread:
            try:
                wait_pointless = (known_callback_thread
                                  == threading.current_thread().name)
            except Exception:
                wait_pointless = False
            if wait_pointless:
                wait_seconds = 0.0
        issued, reason = self._issue_credit_account_query(account_id)
        deadline = time.time() + wait_seconds
        while issued and time.time() < deadline:
            with self._credit_account_lock:
                if self._credit_account_stamp > stamp_before:
                    break
                if not self._credit_account_inflight:
                    break
            # sleep 让出 GIL —— 但如果回调本来就要走这条线程，让出 GIL 也没用，
            # 见 CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS 上面的说明。
            time.sleep(0.02)
        waited = round(time.time() - (deadline - wait_seconds), 3)
        with self._credit_account_lock:
            # 等不到就把 inflight 放掉。原来只有「回调到达」和「func 抛异常」
            # 两条路会清它 —— 超时路径没人清，于是**一次回调丢失就把这条通道
            # 永久卡死**：之后每次调用都撞 "a query is already in flight"，再也
            # 发不出去。放掉的代价只是可能重复问一次柜台，而 30s 最小间隔还在。
            timed_out = (self._credit_account_inflight
                         and not (self._credit_account_stamp > stamp_before))
            if timed_out:
                self._credit_account_inflight = False
            rows = list(self._credit_account_rows)
            stamp = self._credit_account_stamp
            error = self._credit_account_error
            seq = self._credit_account_seq
            callbacks = self._credit_account_callbacks
        # 太陈的数据宁可不给。给出去而不被察觉，比给不出更坏。
        age = round(time.time() - stamp, 3) if stamp else None
        dropped_stale = False
        stale_reason = ""
        if rows and max_age_seconds > 0 and age is not None and age > max_age_seconds:
            dropped_stale = True
            stale_reason = (
                "缓存已 %.1f 秒未更新，超过 max_age_seconds=%.0f，不再交出 —— "
                "这条通道的缓存不会自己刷新。查询已发出，稍后再调一次即可拿到"
                "新的；要实时数据请用 query_credit_detail。" % (age, max_age_seconds))
            rows = []
        return {
            "rows": rows,
            "count": len(rows),
            "query_issued": issued,
            "not_issued_reason": "" if issued else reason,
            "fresh": bool(stamp and stamp > stamp_before),
            "stale": bool(rows and not (stamp and stamp > stamp_before)),
            "age_seconds": age,
            "max_age_seconds": max_age_seconds,
            # 有数据但太陈，已经扣下不给 —— 必须说出来，否则和「没数据」没区别
            "dropped_stale": dropped_stale,
            "dropped_stale_reason": stale_reason,
            "seq": seq,
            "error": error,
            "callback_bound": callable(self.qmt_api.get("query_credit_account")),
            # 这两个才是能定位问题的：callbacks_seen=0 说明 QMT 压根没回调
            # （等太短？账户不支持？回调没挂上？），>0 说明回调通了、问题在别处。
            "callbacks_seen": callbacks,
            "waited_seconds": waited,
            # 回调实际落在哪条线程上。这一条能证伪上面那个「回调走主线程、
            # 被我的等待堵住」的假设 —— 不用再猜。
            "callback_thread": self._credit_account_callback_thread,
            # 这次是不是等超时并把 inflight 放掉了
            "inflight_released": bool(timed_out),
            # 请求要等、但我们已经知道等不到，得说出来，别让人以为等过了
            "wait_skipped_same_thread": bool(wait_pointless),
            "note": ("回调投递在本线程（%s）上，等待永远等不到 —— 已跳过等待。"
                     "查询已发出，结果随后落进缓存，下一次调用即可取到"
                     % known_callback_thread) if wait_pointless else
                    ("默认不等回调：查询已发出，结果随后落进缓存，下一次调用即可"
                     "取到（handler 跑在 adjust 主线程上，等待可能把回调堵在门外）"
                     if wait_seconds == 0 else ""),
        }

    def _describe_probe_rows(self, rows):
        """行数不等于有数据 —— 这条探测自己踩过这个坑。

        QMT 的交易类查询跑在主策略线程之外时，返回的不是空列表，而是**行数
        对、字段全空**的对象（本机实测：把 get_asset 移出 defer 名单，它照样
        返回 5 个键，但 cash / total_asset / frozen_cash / market_value 全是
        None）。原来这里只报 len(rows)，于是在一份真两融账户的报告里给出了
        `ok: true, rows: 71002`，而同一次运行里走 handler 的同一个函数是 0 行
        —— 探测反过来把人带偏了（#204）。

        所以这里过一遍 handler 用的同一个归一化，再看**字段里到底有没有东西**。
        CLAUDE.md 那句「Verify the meaning, not the shape」，说的就是这个。
        """
        report = {"rows": len(rows)}
        try:
            normalized = _normalize_detail_rows(rows)
        except Exception as exc:
            report["normalize_error"] = "%s: %s" % (exc.__class__.__name__, exc)
            return report
        report["normalized_rows"] = len(normalized)
        first = normalized[0] if normalized else None
        if not isinstance(first, dict):
            return report
        report["fields"] = len(first)
        # None / "" 才算「没拿到」。0 是合法值（没有负债就是 0），不能当空看。
        present = [key for key, value in first.items()
                   if value is not None and value != ""]
        report["populated_fields"] = len(present)
        # first 必须真的有字段。零字段是「没数据」，不是「空行」—— 判错的话
        # 一个返回 {} 的接口会被报成跑错线程（本机实跑第一次就误报了）。
        if first and not present:
            # 行在、字段全空 —— 这就是跑错线程的签名，必须说出来，不能让
            # 一个 rows=71002 看着像成功。
            report["hollow"] = True
            report["note"] = ("行数对但字段全空 —— QMT 交易类查询在主策略线程"
                              "之外就是这个样子，查 thread_routing")
        return report

    def _probe_thread_routing(self):
        """哪些方法跑在后台 listener 线程上，哪些被推回 adjust 主线程。

        QMT 的交易类查询在主策略线程之外返回空 —— 这是本项目最重要的一条约束，
        也是 #204 的症结：同一个 QMT 函数，走 adjust 线程有 71002 行，走 listener
        线程 0 行。而远端报告里看不到路由配置，只能靠猜。这一项把它摊开。

        `sample` 里给的是几个有代表性的方法，不是全集：全集有一百多个，塞进
        报告没人看得完。
        """
        service = getattr(self, "rpc_service", None)
        if service is None:
            return {"available": False,
                    "note": "handlers has no rpc_service backref"}
        report = {
            "available": True,
            "process_in_listener": bool(getattr(service, "process_in_listener", False)),
        }
        listener_methods = getattr(service, "listener_methods", None)
        if listener_methods is not None:
            try:
                report["listener_method_count"] = len(listener_methods)
            except Exception:
                pass
        decide = getattr(service, "_should_process_in_listener", None)
        if callable(decide):
            sample = {}
            for name in ("ping", "get_ticks", "get_market_data_ex",
                         "query_credit_detail", "query_stk_compacts",
                         "query_credit_subjects", "get_unclosed_compacts",
                         "get_assure_contract", "get_enable_short_contract",
                         "get_ipo_data", "get_new_purchase_limit",
                         "probe_capabilities", "get_asset", "get_positions"):
                try:
                    sample[name] = ("listener_thread" if decide({"method": name})
                                    else "adjust_thread")
                except Exception as exc:
                    sample[name] = "unknown: %s" % exc.__class__.__name__
            report["sample"] = sample
        # #351：在 adjust 排空时，重读取交给工作线程。
        heavy_thread = getattr(service, "_heavy_thread", None)
        report["background_threads"] = bool(getattr(service, "background_threads", True))
        report["heavy_offload"] = bool(getattr(service, "heavy_offload", False))
        report["heavy_worker_alive"] = bool(heavy_thread is not None and heavy_thread.is_alive())
        is_heavy = getattr(service, "is_heavy_read", None)
        if callable(is_heavy):
            heavy_sample = {}
            for name, params in (
                    ("get_full_tick", {"codes": ["600000.SH"]}),
                    ("get_full_tick[SH]", {"codes": ["SH"]}),
                    ("get_market_data_ex[count=5]", {"stock_list": ["600000.SH"], "count": 5}),
                    ("get_market_data_ex[tick]", {"stock_list": ["600000.SH"], "period": "tick"}),
                    ("get_financial_data", {"stock_list": ["600000.SH"]}),
                    ("download_history_data2", {"stock_list": ["600000.SH"]}),
                    ("get_positions", {})):
                try:
                    heavy_sample[name] = ("heavy_worker" if is_heavy(name.split("[")[0], params)
                                          else "inline")
                except Exception as exc:
                    heavy_sample[name] = "unknown: %s" % exc.__class__.__name__
            report["heavy_sample"] = heavy_sample
        return report

    def _probe_credit_account_object(self):
        """对 query_credit_detail 背后两融账户对象的只读探测。

        报告行数，以及有行时报告 m_nBrokerType（3 = 信用）和两融专属字段是否
        真的带回来了——这样才能把空答案和非两融账户区分开，而报告者看到的裸
        [] 无法区分（#201）。
        """
        # getattr 而不是属性直取：不是每种 gateway 都有这个属性（dry-run 的就
        # 没有），而 probe_capabilities 是诊断接口，它自己崩掉是最坏的结果。
        gateway = self.order_gateway
        get_detail = getattr(gateway, "get_trade_detail_data", None)
        if gateway is None or not callable(get_detail):
            return {"available": False}
        try:
            raw = get_detail(self.account_id, "CREDIT", "ACCOUNT", "") or []
        except Exception as exc:
            return {"available": True, "ok": False,
                    "error": "%s: %s" % (exc.__class__.__name__, exc)}
        report = {"available": True, "ok": True}
        report.update(self._describe_probe_rows(raw))
        rows = _normalize_detail_rows(raw) or []
        if rows and isinstance(rows[0], dict):
            report["broker_type"] = rows[0].get("m_nBrokerType")
            # 键在不等于值在。跑错线程时字段名一个不少、值全是 None，用
            # `key in row` 判断会报 has_credit_fields=True，而调用方拿到的
            # 是一行 null —— 和 rows=71002 那个误导是同一个错误（#204）。
            report["has_credit_fields"] = any(
                rows[0].get(key) is not None for key in
                ("m_dPerAssurescaleValue", "m_dFinMaxQuota", "m_dSloMaxQuota"))
        return report

    def _probe_reply_residency(self):
        """已完成的回复等 transport 发送它们要等多久。

        处理器在 adjust 线程上运行，所以在 zmq 上每个回复都排给 router 线程，
        只在该循环顶部才会离开。这能说明往返时间是否花在那种等待上（#104）。
        """
        transport = getattr(self, "rpc_transport", None)
        stats = getattr(transport, "reply_residency_stats", None)
        if not callable(stats):
            return {"available": False}
        try:
            report = dict(stats())
        except Exception as exc:
            return {"available": False,
                    "error": "%s: %s" % (exc.__class__.__name__, exc)}
        report["available"] = True
        return report

    def _probe_order_watch(self):
        """回调驱动的结算表在这里是活的吗（问题 #164）？

        这张表由 bigqmt_signal_trader_strategy 挂到 handlers 上，而那是 QMT 执行
        的顶层文件——reload_deployment 刷新不了它。于是一个部署可以带着 #164 的
        每一行代码，却仍在跑旧的轮询循环，直到有人重启策略，而没有任何东西能把
        两者区分开。这里就是告诉你跑的是哪一个。

        只报计数：备注和订单号会暴露具体委托。
        """
        table = getattr(self, "order_watch_table", None)
        if table is None:
            return {
                "wired": False,
                "note": ("settlement is polling; the watch table is wired in "
                         "the strategy file, which needs a strategy RESTART "
                         "(reload_deployment cannot refresh it)"),
            }
        report = {"wired": True}
        try:
            report.update(table.stats())
        except Exception as exc:
            report["stats_error"] = "%s: %s" % (exc.__class__.__name__, exc)
        return report

    # 板块通道探测（issue #143）。写入板块有三条可能的通道，名字还各不相同：
    #   * ContextInfo.create_sector           大 QMT 内置 Python
    #   * 原生 xtdata.add_sector/remove_sector MiniQMT SDK（要行情服务）
    #   * QMT 注入的全局函数                   文档 §4.7 那一族
    # 哪条能用只有终端自己知道，所以枚举而不是查固定表 —— 上面那两个 block
    # 就是查表，于是 add_stock_to_sector / reset_sector_stock_list 根本没被
    # 看见过。全部只读：不建板块、不改成分股。
    _SECTOR_WRITE_NAMES = (
        "create_sector", "create_sector_folder", "add_stock_to_sector",
        "remove_stock_from_sector", "reset_sector_stock_list",
        "add_sector", "remove_sector", "reset_sector",
    )

    @staticmethod
    def _enumerate_sector_names(target):
        if target is None:
            return []
        try:
            return sorted(n for n in dir(target)
                          if "sector" in n.lower() and callable(getattr(target, n, None)))
        except Exception:
            return []

    def _probe_sector_channels(self):
        report = {}
        context_info = getattr(self.market_data, "context_info", None)
        report["contextinfo_sector_names"] = self._enumerate_sector_names(context_info)
        report["qmt_global_sector_names"] = sorted(
            name for name, func in (self.qmt_api or {}).items()
            if "sector" in name.lower() and callable(func))
        report["write_names_found"] = {
            name: {
                "contextinfo": callable(getattr(context_info, name, None)),
                "qmt_global": callable((self.qmt_api or {}).get(name)),
            }
            for name in self._SECTOR_WRITE_NAMES
        }

        native = None
        # 显式禁用时跳过原生 SDK 探测。
        if getattr(self.market_data, "native_xtdata_enabled", True) is False:
            report["native_xtdata_loaded"] = False
            report["native_xtdata_skipped"] = "native_xtdata_enabled=False"
        else:
            try:
                from .adapters.market_bigqmt import _load_native_xtdata

                native = _load_native_xtdata()
            except Exception as exc:
                report["native_xtdata_error"] = "%s: %s" % (exc.__class__.__name__, exc)
            report["native_xtdata_loaded"] = native is not None
        report["native_sector_names"] = self._enumerate_sector_names(native)
        for name in self._SECTOR_WRITE_NAMES:
            if name in report["write_names_found"]:
                report["write_names_found"][name]["native_xtdata"] = callable(
                    getattr(native, name, None))

        # 唯一真调的一次，而且是读：它回答「这台终端到底能不能列出真实板块」,
        # 也就是 get_sector_list 现在是不是在拿硬编码兜底冒充真数据。
        if native is not None and callable(getattr(native, "get_sector_list", None)):
            try:
                listing = native.get_sector_list() or []
                report["native_get_sector_list"] = {
                    "ok": True, "count": len(listing),
                    "sample": [str(x) for x in list(listing)[:8]],
                }
            except Exception as exc:
                report["native_get_sector_list"] = {
                    "ok": False, "error": "%s: %s" % (exc.__class__.__name__, exc)}
        else:
            report["native_get_sector_list"] = {"ok": False, "error": "not available"}
        try:
            reported = self.market_data.get_sector_list() or []
            fallback = list(getattr(self.market_data, "_FALLBACK_SECTORS", ()) or ())
            report["get_sector_list_now"] = {
                "count": len(reported),
                "is_the_hardcoded_fallback": list(reported) == fallback,
                "sample": [str(x) for x in list(reported)[:8]],
            }
        except Exception as exc:
            report["get_sector_list_now"] = {
                "error": "%s: %s" % (exc.__class__.__name__, exc)}
        return report

    def _probe_download_channels(self, params):
        """财务下载通道探测（#277）：「接口暴露」和「独立更新可用」分开报。

        大 QMT 终端里 miniQMT（58610 行情服务）不在时，`download_financial_data`
        照样 callable、已有财务行照样读得到，真下载却报 `无法连接行情服务`。
        只看函数存在性的能力表把这种终端报成「可用」，正是 #277 的报告人遇到
        的：财务库读得到、更新不了、能力表说没问题。

        这里真发一次小范围下载（一只代码、一张表、30 天窗口）看结果。传
        `download_probe=false` 可以跳过那次拨号（服务不在时它要付 2~3 秒的
        连接超时），此时只报暴露情况，verdict 是 `exposed_untested`。
        """
        flag = (params or {}).get("download_probe", True)
        dial = (getattr(getattr(self, "rpc_transport", None), "name", None) not in ("serial", "pipe", "shm", "copydata")
                and str(flag).strip().lower() not in ("0", "false", "no", "off"))
        try:
            return self.market_data.probe_download_channels(dial=dial)
        except Exception as exc:
            return {"error": "%s: %s" % (exc.__class__.__name__, exc)}

    # ------------------------------------------------------------------
    # 全推行情订阅控制（引用计数共享 ContextInfo.subscribe_whole_quote）。
    # 数据本身走推送通道；这里只负责订阅生命周期 + 心跳。
    # ------------------------------------------------------------------

    def _require_quote_manager(self):
        manager = self.quote_subscription_manager
        if manager is None:
            raise RuntimeError("本服务端没有配置全推推送订阅")
        return manager

    @staticmethod
    def _quote_params(params, require_codes=False):
        params = params or {}
        client_id = str(params.get("client_id") or "").strip()
        sub_id = str(params.get("sub_id") or "").strip()
        if not client_id:
            raise ValueError("client_id 是必需的")
        if not sub_id:
            raise ValueError("sub_id 是必需的")
        codes = [str(c) for c in (params.get("codes") or []) if str(c or "").strip()]
        if require_codes and not codes:
            raise ValueError("codes 是必需的")
        return client_id, sub_id, codes

    def _handle_subscribe_whole_quote(self, params):
        manager = self._require_quote_manager()
        client_id, sub_id, codes = self._quote_params(params, require_codes=True)
        return manager.subscribe(client_id, sub_id, codes)

    def _handle_unsubscribe_whole_quote(self, params):
        manager = self._require_quote_manager()
        client_id, sub_id, _codes = self._quote_params(params)
        manager.unsubscribe(client_id, sub_id)
        return {}

    def _handle_unsubscribe_whole_quote_batch(self, params):
        """用一次 RPC 释放多个 sub_id（#418）：逐项释放的代价是 N 次 RPC，
        外加客户端 N 次推送订阅者重建。"""
        manager = self._require_quote_manager()
        client_id = str(params.get("client_id") or "")
        sub_ids = params.get("sub_ids") or params.get("sub_id") or []
        if isinstance(sub_ids, (str, int)):
            sub_ids = [sub_ids]
        return manager.unsubscribe_batch(client_id, [str(s) for s in sub_ids])

    def _handle_quote_keepalive(self, params):
        manager = self._require_quote_manager()
        client_id, sub_id, _codes = self._quote_params(params)
        manager.keepalive(client_id, sub_id)
        return {}

    def _handle_quote_subscription_status(self, params):
        """只读：订阅了什么、有多少客户端、陈旧程度如何。

        “我丢了 seq 但还有东西在推”的答案：一直新鲜的组合有 LIVE keepalive 在
        喂它（是遗留的客户端进程，不是泄漏）；超过心跳超时后静默的即将被回收。
        """
        return self._require_quote_manager().status()

    def _handle_quote_unsubscribe_all(self, params):
        """运维急停：不需要 seq 就能拆掉所有组合。

        keepalive 对未知 sub_id 是空操作，所以被强制清掉的组合会一直静默，直到
        有人重新订阅。
        """
        manager = self._require_quote_manager()
        return {"unsubscribed": manager.unsubscribe_all()}

    def _identity_vk(self):
        """返回订单身份库使用的 VK 客户端，或 None。

        有意用独立属性，而不是复用下载任务的客户端。只有 VK **传输** 会构造
        那个，所以在 zmq 部署上它是 None——于是委托在提交时从没被记住，查询时
        也就永远无法归属（问题 #133）。策略无论什么传输都按 VK 配置把这个
        接好，并对早于它的部署回退到下载任务客户端。这里的一切都把 None 当作
        “无归属”，而不是错误：给委托起名字是锦上添花，委托本身不是。
        """
        return (getattr(self, "order_identity_vk_client", None)
                or getattr(self, "download_job_vk_client", None))

    def _download_job_vk(self):
        vk_client = getattr(self, "download_job_vk_client", None)
        if vk_client is None:
            raise RuntimeError("下载任务需要 VK 客户端")
        return vk_client

    def _require_download_worker(self):
        """拒绝把没人会取的工作排进队列。

        工作线程是 adjust tick 上的 _pump_download_jobs，而它在大 QMT 上默认关闭，
        原因见运行时的记录：终端内嵌的 xtdata SDK 没有可达的数据服务，所以下载
        本来就会抛「无法连接行情服务」。与 #130 里 download_* 方法同一根因。

        把任务接受进一个无人排空的队列，看起来像成功却永不完成——比它替代的拒绝
        更糟。所以直接说不，并说明哪个开关能打开它。
        """
        if not getattr(self, "download_jobs_enabled", False):
            raise RuntimeError(
                "本部署已禁用异步下载任务，提交的任务会永远留在队列里：没有东西"
                "去执行它。大 QMT 内嵌的 xtdata SDK 没有可达的数据服务可供下载。"
                "请在终端的 数据管理/补充数据 界面补充历史数据，再用 "
                "get_market_data_ex / get_local_data 读回。只有在 MiniQMT/xtdata "
                "数据服务可达的环境下，才在本地配置里设 download_jobs_enabled=True。")

    def _handle_submit_download_history_data2(self, params):
        self._require_download_worker()
        from .download_jobs import submit_download_job

        stock_list = params.get("stock_list") or params.get("stock_code") or []
        if isinstance(stock_list, str):
            stock_list = [stock_list]
        return submit_download_job(
            self._download_job_vk(),
            self.account_id,
            stock_list,
            params.get("period"),
            method="download_history_data2",
            start_time=params.get("start_time", ""),
            end_time=params.get("end_time", ""),
            incrementally=params.get("incrementally"),
            chunk_size=int(params.get("chunk_size") or getattr(self, "download_job_chunk_size", 10)),
            job_ttl_seconds=int(params.get("job_ttl_seconds") or getattr(self, "download_job_ttl_seconds", 3600)),
        )

    def _handle_submit_download_history_data(self, params):
        stock_code = params.get("stock_code") or params.get("code")
        next_params = dict(params or {})
        next_params["stock_list"] = [stock_code] if stock_code else []
        return self._handle_submit_download_history_data2(next_params)

    def _handle_get_download_status(self, params):
        from .download_jobs import read_download_status

        job_id = params.get("job_id")
        if not job_id:
            raise ValueError("job_id 是必需的")
        status = read_download_status(self._download_job_vk(), self.account_id, job_id)
        if status is None:
            raise KeyError("下载任务不存在或已过期: %s" % job_id)
        return status

    def _handle_wait_download(self, params):
        from .download_jobs import wait_download_job

        job_id = params.get("job_id")
        if not job_id:
            raise ValueError("job_id 是必需的")
        return wait_download_job(
            self._download_job_vk(),
            self.account_id,
            job_id,
            wait_seconds=float(params.get("wait_seconds", 600.0)),
            poll_interval_seconds=float(params.get("poll_interval_seconds", 0.5)),
        )


    def _handle_get_ticks(self, params):
        codes = params.get("codes")
        if isinstance(codes, str):
            codes = [codes]
        if not codes:
            code = params.get("code")
            codes = [code] if code else []
        if not codes:
            raise ValueError("codes 或 code 是必需的")
        types = params.get("types")
        if isinstance(types, str):
            types = [types]
        if not types:
            # 什么都没要求时只传一个参数，使 get_ticks 只接受 `codes` 的行情
            # provider 仍能工作。
            return self.market_data.get_ticks(codes)
        return self.market_data.get_ticks(codes, types=list(types))

    def _handle_get_instrument(self, params):
        code = params.get("code")
        if not code:
            raise ValueError("code 是必需的")
        return self.market_data.get_instrument(code)

    def _handle_market_data_method(self, method, params):
        handler = getattr(self.market_data, method, None)
        if handler is None:
            raise NotImplementedError("行情方法不可用: %s" % method)
        return handler(**dict(params or {}))

    def _handle_get_positions(self, params):
        return self.position_provider.get_positions(self._request_account_id(params))

    def _handle_query_stock_positions(self, params):
        account_id = self._request_account_id(params)
        list_positions = getattr(self.position_provider, "list_positions", None)
        if callable(list_positions):
            return list_positions(account_id)
        positions = self.position_provider.get_positions(account_id)
        if isinstance(positions, dict):
            return list(positions.values())
        return list(positions or [])

    def _handle_get_position_statistics(self, params):
        return self.position_provider.get_position_statistics(self._request_account_id(params))

    def _handle_query_stock_position(self, params):
        stock_code = str(params.get("stock_code") or params.get("code") or "").strip()
        if not stock_code:
            raise ValueError("stock_code 是必需的")
        normalized_code = normalize_stock_code(stock_code)
        positions = self.position_provider.get_positions(self._request_account_id(params))
        return positions.get(normalized_code)

    def _handle_get_asset(self, params):
        return self.position_provider.get_asset(self._request_account_id(params))

    def _handle_query_orders(self, params):
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        # strategy_name 按 passorder 里用的名字过滤委托。空字符串返回该账户的
        # **全部** 委托（诊断确认过：st="" -> 9 笔，st="bigqmt_signal_trader" ->
        # 0 笔）。默认 ""，使调用方看到每一笔委托，除非显式过滤。
        orders = self.order_gateway.query_orders(
            self._request_account_id(params),
            str(params.get("strategy_name") or ""),
        )
        orders = self._recover_order_remarks(self._request_account_id(params), orders)
        if _bool_value(params.get("cancelable_only"), False):
            orders = [
                order
                for order in orders
                if str(getattr(order, "status", "") or "") in CANCELABLE_ORDER_STATUSES
            ]
        return self._attribute_to_strategies(
            self._request_account_id(params), orders)

    def _handle_query_trades(self, params):
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        # 空 strategy_name 返回该账户的 **全部** 成交（见 query_orders 的说明）。
        # 默认 ""，使调用方看到每笔成交，除非显式过滤。
        strategy_name = params.get("strategy_name")
        if strategy_name is None:
            strategy_name = ""
        account_id = self._request_account_id(params)
        return self._attribute_to_strategies(
            account_id,
            self._recover_order_remarks(
                account_id, self.order_gateway.query_trades(account_id, str(strategy_name))),
        )

    def _recover_order_remarks(self, account_id, snapshots):
        return _exec_events.recover_order_remarks(
            getattr(self, "order_remark_vk_client", None), account_id,
            self._configured_account_type(account_id), snapshots,
            _exec_events.ORDER_IDENTITY_TTL_SECONDS)

    def _attribute_to_strategies(self, account_id, snapshots):
        """把调用方的策略名写到本桥接层提交的行上。

        get_trade_detail_data 返回的 ORDER 和 DEAL 行都不带 m_strStrategyName
        ——在实盘终端上列举每一个属性验证过。行确实带的（m_strSource）是 QMT 侧
        策略的注册名——本桥接进程的名字，例如 BIGQMT_VK_DRYRUN——而不是调用
        方下单时传的 strategy_name（#216：以 'DaBanStrategy' 下单，回报成
        '大QMT桥接器'）。#174 把那个字段误读为“passorder 的 strategyName 回来了”。

        本桥接层提交的委托在提交时就被记住，以作为备注发出去的 user_order_id 为
        键，而那条记录是权威的——只要它有名字，就 **胜过** 行上的字段。没有备注
        的行（手工下单）或没有身份记录的行（别的进程下的单）保留行上的值。
        """
        self._load_order_identity_journal(account_id)
        rows = list(snapshots or [])
        candidates = [row for row in rows
                      if str(getattr(row, "user_order_id", "") or "").strip()]
        if not candidates:
            return rows
        vk_client = self._identity_vk()
        if vk_client is not None:
            try:
                from .exec_events import order_identity_map

                identities = order_identity_map(
                    vk_client, account_id,
                    [getattr(row, "user_order_id", "") for row in candidates])
                for row in candidates:
                    identity = identities.get(
                        str(getattr(row, "user_order_id", "") or "").strip())
                    if identity and identity.get("strategy_name"):
                        row.strategy_name = str(identity.get("strategy_name") or "")
            except Exception:
                pass
        # 无 VK 的部署仍能命名 **本** 进程提交的东西：提交时写入的进程内
        # 日志（问题 #156）。它在 VK 存储之后运行，因此本进程自己的记录在
        # 冲突时胜出。
        journal = getattr(self, "_order_identity_local", None)
        if journal:
            now = time.time()
            for row in candidates:
                key = (str(account_id or ""),
                       str(getattr(row, "user_order_id", "") or "").strip())
                entry = journal.get(key)
                if not entry:
                    continue
                ts, name = entry
                if name and now - ts <= self._ORDER_IDENTITY_LOCAL_TTL_SECONDS:
                    row.strategy_name = name
        return rows

    _ORDER_IDENTITY_LOCAL_LIMIT = 5000
    # 7 天，与 exec_events.ORDER_IDENTITY_TTL_SECONDS 一致：委托要跨日可查，
    # 而过去次日重读会以桥接进程名返回（#393）。
    _ORDER_IDENTITY_LOCAL_TTL_SECONDS = 7 * 86400.0

    def enable_order_identity_persistence(self, directory=None, account_types=None):
        """为有文件系统访问权限的独立调用方恢复归属。"""
        from .account_type_map import get_account_type_map, primary_account_type

        self._order_identity_journals = {}
        self._order_identity_journal_directory = directory
        if account_types is None:
            account_types = dict(get_account_type_map())
        account_types = dict(account_types)
        account_types.setdefault(self.account_id, self._configured_account_type(self.account_id))
        for account_id in account_types:
            self._load_order_identity_journal(
                account_id, primary_account_type(account_types[account_id]))

    def _load_order_identity_journal(self, account_id, account_type=None):
        stores = getattr(self, "_order_identity_journals", None)
        if stores is None:
            return None
        account_id = str(account_id or "")
        if account_id in stores:
            return stores[account_id]
        from .order_identity_journal import OrderIdentityJournal

        store = OrderIdentityJournal(
            account_id,
            self._configured_account_type(account_id) if account_type is None else account_type,
            directory=self._order_identity_journal_directory,
            ttl_seconds=self._ORDER_IDENTITY_LOCAL_TTL_SECONDS,
            limit=self._ORDER_IDENTITY_LOCAL_LIMIT)
        stores[account_id] = store
        journal = self._order_identity_local
        for key, entry in store.load().items():
            existing = journal.get(key)
            if existing is None or (entry[1] and not existing[1]):
                journal[key] = entry
        while len(journal) > self._ORDER_IDENTITY_LOCAL_LIMIT:
            journal.popitem(last=False)
        return store

    def _remember_order_identity_local(self, account_id, remark, strategy_name):
        remark = str(remark or "").strip()
        if not remark:
            return
        key = (str(account_id or ""), remark)
        try:
            store = self._load_order_identity_journal(account_id)
            journal = getattr(self, "_order_identity_local", None)
            if journal is None:
                # 测试（以及 QMT 沙箱）用 __new__ 构造 handlers 并跳过 __init__
                # ——首次使用时再创建。
                journal = self._order_identity_local = collections.OrderedDict()
            name = str(strategy_name or "")
            existing = journal.get(key)
            now = time.time()
            if existing is not None and now - existing[0] <= self._ORDER_IDENTITY_LOCAL_TTL_SECONDS:
                existing_name = existing[1]
                if existing_name and existing_name != name:
                    # 第一条 **具名** 记录胜出（#393）：被复用的备注不能给已经
                    # 上报过的行改名——无条件覆盖会把它们翻成最近一次同备注提交
                    # 的名字，或者当那次提交传了 "" 时彻底抹掉，让行退回 QMT 进程名。
                    if name:
                        print("[bigqmt_rpc] 委托备注 %r 已被多个策略复用："
                              "保留 %r，忽略 %r（#393）。"
                              "请为每笔委托使用唯一备注。"
                              % (remark, existing_name, name))
                    journal.move_to_end(key)
                    return
            journal[key] = (now, name)
            journal.move_to_end(key)
            while len(journal) > self._ORDER_IDENTITY_LOCAL_LIMIT:
                journal.popitem(last=False)
            if store is not None:
                store.remember(account_id, remark, now, name)
        except Exception:
            pass

    def _handle_describe_trade_detail_fields(self, params):
        """报告 QMT 的 ORDER / DEAL 行携带哪些属性。只有名字。

        无需再来一轮“部署加重启”就能回答“字段 X 为什么是空的/缺失”——见
        BigQmtOrderGateway.describe_detail_fields。与其它交易上下文查询一样延后到
        主线程：get_trade_detail_data 在其它线程上返回空。
        """
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        describe = getattr(self.order_gateway, "describe_detail_fields", None)
        if describe is None:
            raise RuntimeError(
                "本部署早于 describe_trade_detail_fields；请同步并重启策略")
        detail_types = params.get("detail_types") or params.get("detail_type")
        if isinstance(detail_types, str):
            detail_types = [detail_types]
        shape_fields = params.get("shape_fields") or params.get("shape_field")
        if isinstance(shape_fields, str):
            shape_fields = [shape_fields]
        if shape_fields:
            return describe(self._request_account_id(params), detail_types,
                            shape_fields=shape_fields)
        return describe(self._request_account_id(params), detail_types)

    def _handle_reload_deployment(self, params):
        """重新导入包并重跑 init，无需重启策略。

        只是排期：重载会调用 reset_app()，它会停掉正在应答本请求的 RPC 服务，所以
        回复必须先发出去。它在下一个 adjust tick 执行——请轮询 reload_status。

        会刷新 bigqmt_signal_trader/ 下的所有内容。无法刷新策略文件或入口脚本：
        QMT 执行那些，而模块无法重载自己正在其中运行的那一个。那些仍需重启。
        """
        hook = getattr(self, "reload_hook", None)
        if hook is None:
            raise RuntimeError(
                "本部署无法自我重载（早于 reload_deployment）；请同步并重启一次"
                "策略，之后重载就不需要重启了")
        return hook(str((params or {}).get("reason") or ""))

    def _handle_reload_status(self, params):
        status_hook = getattr(self, "reload_status_hook", None)
        if status_hook is None:
            raise RuntimeError("本部署早于 reload_deployment")
        return status_hook()

    def _handle_query_execution_snapshot(self, params):
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        account_id = self._request_account_id(params)
        order_name = params.get("order_strategy_name")
        if order_name is None:
            order_name = params.get("strategy_name", self.default_strategy_name)
        trade_name = params.get("trade_strategy_name")
        if trade_name is None:
            trade_name = ""
        orders = self.order_gateway.query_orders(account_id, str(order_name))
        trades = self.order_gateway.query_trades(account_id, str(trade_name))
        # 在填补任何空白之前，先观察两套原生数据。一笔 DEAL 可能揭示一个缺失的
        # 关联，也可能作废某个更早的 ORDER 会使用的那条关联。
        rows = self._recover_order_remarks(account_id, list(orders) + list(trades))
        return {
            "account_id": account_id,
            "server_time": _dt.datetime.now(),
            "rpc_revision": RPC_REVISION,
            "orders": rows[:len(orders)],
            "trades": rows[len(orders):],
        }

    def _handle_sync_positions(self, params):
        account_id = self._request_account_id(params)
        snapshot = AccountSnapshot(
            account_id=account_id,
            asset=self.position_provider.get_asset(account_id),
            positions=self.position_provider.get_positions(account_id),
            reason=str(params.get("reason") or "rpc"),
            updated_at=_dt.datetime.now(),
        )
        if self.position_sync_sink is not None:
            self.position_sync_sink.publish(snapshot)
        return snapshot

    # ------------------------------------------------------------------
    # 账户 / 融资融券 / 交易扩展查询
    # 这些是 Big QMT 运行时注入的全局函数（同 passorder），不在 ContextInfo 桩里。
    # 函数名严格按官方文档（trading_function.html），通过 self.qmt_api 调用。
    # 无该权限/函数未注入时降级为空列表。
    # ------------------------------------------------------------------

    def _warn_if_hollow(self, source, rows):
        """交易类响应「行数对、字段全空」时大声报出来，别让它静默过去。

        QMT 的交易类查询跑在主策略线程之外时，返回的不是空列表，而是行数对、
        字段全是 None 的对象（本机实测：get_asset 移出 defer 名单后照样回 5 个
        键，值全空）。从客户端看像「这个账户没钱」，不像调用失败 —— #204 就是
        这么在线上静默了几个月。

        静态闸（tests/.../test_trade_context_deferral_guard.py）保证我们**自己**
        不写漏，但挡不住「换一家券商行为不一样」。所以运行时再看一眼：真出现了
        就记一条 warning，把函数名和当前线程名一起写出来，让「每家不一样」成为
        被观测到的事实，而不是我们猜出来的。

        只告警、不改路由：defer 实测只要 2.5-3.0ms，而猜错方向的代价是静默返回
        错数据，两边完全不对等；而且「跑错线程」和「这个账户真没数据」从返回值
        上分不开，据此自动切换只会把一次误判固化下来。
        """
        if not rows:
            return rows
        first = rows[0] if isinstance(rows, (list, tuple)) else rows
        if not isinstance(first, dict) or not first:
            return rows
        if any(value is not None and value != "" for value in first.values()):
            return rows
        now = time.time()
        if now - self._last_hollow_warning_at < HOLLOW_WARNING_INTERVAL_SECONDS:
            return rows
        self._last_hollow_warning_at = now
        try:
            thread_name = threading.current_thread().name
        except Exception:
            thread_name = "?"
        try:
            from .logging_setup import get_logger
            get_logger("rpc").warning(
                "%s 在线程 %r 上返回 %d 行，但所有字段均为空。"
                "QMT 交易上下文查询在非主策略线程上会返回这种结果；"
                "调用并未失败，只是数据不在该线程可见。"
                "请检查 probe_capabilities 的 thread_routing（#204）。",
                source, thread_name, len(rows))
        except Exception:
            pass
        return rows

    def _call_qmt_global(self, func_name, *args, **kwargs):
        """调用 QMT 运行时注入的全局函数，失败时返回 []。

        这些函数（get_assure_contract / get_unclosed_compacts / ...）由 QMT 注入
        到进程全局命名空间，与 passorder 相同。不可用时（无两融账户、函数未绑定）
        我们降级为 []，而不是让 RPC 崩掉。
        """
        func = self.qmt_api.get(func_name)
        if func is None:
            return []
        try:
            return self._warn_if_hollow(func_name,
                                        _normalize_detail_rows(func(*args, **kwargs)))
        except Exception:
            return []

    def _call_qmt_scalar(self, func_name, *args, **kwargs):
        """与 _call_qmt_global 相同，用于那些以普通标量而不是明细行作答的 QMT
        全局函数——get_last_order_id 返回字符串形式的委托号。

        行规范化器会遍历交给它的任何东西，所以传进一个字符串，出来的是“每个字符
        一个空 dict”的列表：真实委托号 '635005110' 变成一串 [{}, {}, ...]，而
        '-1'（QMT 的“未找到”）变成 [{}, {}]。传 int 会在规范化器内部抛
        TypeError 并被吞成 []。与 #96 同一形态，那里 get_ipo_data 也是这样被毁掉
        的（#207）。
        """
        func = self.qmt_api.get(func_name)
        if func is None:
            return None
        try:
            return _normalize_mapping_value(func(*args, **kwargs))
        except Exception:
            return None

    def _call_qmt_object(self, func_name, *args, **kwargs):
        """与 _call_qmt_global 相同，用于那些以一个对象（而不是对象列表）作答的
        QMT 全局函数——get_value_by_order_id 返回单个委托/成交对象。

        直接把它交给行规范化器会遍历该对象本身，抛 TypeError 并被吞成 []——调用
        看起来像“没有这笔委托”，而它实际上成功了（#207）。
        """
        func = self.qmt_api.get(func_name)
        if func is None:
            return {}
        try:
            value = func(*args, **kwargs)
        except Exception:
            return {}
        if value is None:
            return {}
        if isinstance(value, (list, tuple)):
            rows = _normalize_detail_rows(value)
            return rows[0] if rows else {}
        rows = _normalize_detail_rows([value])
        return rows[0] if rows else {}

    def _call_qmt_mapping(self, func_name, *args, **kwargs):
        """与 _call_qmt_global 相同，用于那些以映射而不是行列表作答的 QMT 全局
        函数——get_ipo_data、get_new_purchase_limit。

        行规范化器会按键遍历这种 dict 并丢掉值，所以这些保持原形状，只把值处理成
        JSON 安全。
        """
        func = self.qmt_api.get(func_name)
        if func is None:
            return {}
        try:
            data = func(*args, **kwargs)
        except Exception:
            return {}
        if isinstance(data, dict):
            return {str(key): _normalize_mapping_value(value)
                    for key, value in data.items()}
        # 有些券商连这里也回行；做规范化而不是丢掉。
        return _normalize_detail_rows(data)

    def _configured_account_type(self, account_id=None):
        """这个部署会以什么账户类型交易，且按请求感知。

        当网关支持按请求解析 account_type（即带 BIGQMT_ACCOUNT_TYPE_MAP 的
        ``_resolve_account_type``）时，返回给定 account_id 对应的映射类型。没配
        映射时，原样返回网关自己的 ``account_type``。

        *account_id* 参数是显式的（不从实例状态读），因为 zmq 部署有后台监听线程
        在调用行情方法，与调用交易方法的 adjust 线程并发——把参数存在 ``self`` 上
        会竞态（问题 #43、#164）。
        """
        gateway = self.order_gateway
        if gateway is not None and hasattr(gateway, "_resolve_account_type"):
            try:
                aid = account_id or self.account_id or ""
                if aid:
                    return str(gateway._resolve_account_type(aid)).strip().upper()
            except Exception:
                pass
        return str(
            getattr(gateway, "account_type", "CREDIT") or "CREDIT"
        ).strip().upper()

    def _query_trade_detail(self, params, detail_type, strategy_name="",
                            account_type=None):
        """用 6 种官方明细类型之一调用 get_trade_detail_data。

        官方 strDatatype 取值：ACCOUNT / POSITION / POSITION_STATISTICS /
        ORDER / DEAL / TASK。其它字符串 **不** 作为明细类型支持——两融合约查询
        请用下面专门的函数。

        *account_type* 只对本次调用覆盖部署配置的类型。那是账户类型轴，不是明细
        类型轴：两融账户对象是 `(accId, 'CREDIT', 'ACCOUNT')`，调用方无论本部署
        交易什么类型都显式这样索取（#201）。
        """
        account_id = self._request_account_id(params)
        gateway = self.order_gateway
        if gateway is None or gateway.get_trade_detail_data is None:
            return []
        if account_type is None:
            account_type = (gateway._resolve_account_type(account_id)
                            if hasattr(gateway, "_resolve_account_type")
                            else getattr(gateway, "account_type", "STOCK"))
        try:
            rows = gateway.get_trade_detail_data(account_id, account_type, detail_type, strategy_name)
            return self._warn_if_hollow(
                "get_trade_detail_data(%s,%s)" % (account_type, detail_type),
                _normalize_detail_rows(rows))
        except Exception:
            return []

    def _handle_query_account_infos(self, params):
        # 账户信息 — get_trade_detail_data(ACCOUNT)
        return self._query_trade_detail(params, "ACCOUNT")

    def _handle_query_account_status(self, params):
        # 账户状态。Big QMT 没有原生账号状态结构——官方 strDatatype 只有
        # ACCOUNT / POSITION / POSITION_STATISTICS / ORDER / DEAL / TASK，
        # TASK 是委托任务状态，占位实现期误用了它：没有进行中的委托任务时恒
        # 空，和「账号状态查不到」是两回事（issue #272）。
        #
        # 能拿到的最近真源是 ACCOUNT 行的 m_Enable：
        #   账户存在且可用   -> ACCOUNT_STATUS_OK(0)
        #   账户存在但不可用 -> ACCOUNT_STATUS_FAIL(3)
        #   没有 ACCOUNT 行  -> 空列表（账号不存在）
        # MiniQMT 更丰富的状态（WAITING_LOGIN / INITING / CORRECTING / CLOSED）
        # 在大 QMT 的接口面上观察不到，别在这里编造。
        rows = self._query_trade_detail(params, "ACCOUNT")
        account_id = self._request_account_id(params)
        account_type = self._reported_account_type()
        out = []
        for row in rows or []:
            if isinstance(row, dict):
                enabled = row.get("m_Enable")
                row_account = row.get("m_strAccountID")
            else:
                enabled = getattr(row, "m_Enable", None)
                row_account = getattr(row, "m_strAccountID", None)
            out.append({
                "account_id": str(row_account or account_id),
                "account_type": account_type,
                "status": 0 if enabled else 3,
                "status_msg": "" if enabled else "account disabled (m_Enable=False)",
            })
        return out

    def _handle_query_credit_detail(self, params):
        # 信用（两融）账户明细 — get_trade_detail_data(accId, 'CREDIT', 'ACCOUNT')
        # 返回 CCreditAccountDetail（维持担保比例 / 融资融券授信额度 / 合约金额…），
        # 与 MiniQMT query_credit_detail -> XtCreditDetail 对应，也是本仓
        # docs/MiniQMT_2_BigQMT-Skill/api_mapping.md 一直记着的映射。
        #
        # 0.3.23 之前这里调的是 get_debt_contract(accId)：那是「负债合约明细」——
        # 一张张融资融券合约，不是账户对象，而且官方参考 6.17 已标记【已弃用】
        # （改用 get_unclosed_compacts / get_closed_compacts）。两融账户查它必然
        # 拿不到账户信息，没有未了结负债时更是直接空列表（#201）。负债合约本身
        # 走 query_stk_compacts（get_unclosed_compacts）。
        return self._query_trade_detail(params, "ACCOUNT", account_type="CREDIT")

    def _handle_query_stk_compacts(self, params):
        # 未平仓合约（负债）— 官方 get_unclosed_compacts
        return self._call_qmt_global(
            "get_unclosed_compacts",
            self._request_account_id(params),
            self._configured_account_type(self._request_account_id(params)),
        )

    def _handle_query_credit_subjects(self, params):
        # 融资标的（担保品）— 官方 get_assure_contract
        return self._call_qmt_global("get_assure_contract", self._request_account_id(params))

    def _handle_query_credit_slo_code(self, params):
        # 融券标的 — 官方 get_enable_short_contract
        return self._call_qmt_global("get_enable_short_contract", self._request_account_id(params))

    def _handle_query_credit_assure(self, params):
        # 担保品合约 — 同 query_credit_subjects（get_assure_contract）
        return self._call_qmt_global("get_assure_contract", self._request_account_id(params))

    def _handle_query_appointment_info(self, params):
        # 新股数据 — 官方 get_ipo_data(type)
        # 8-28 修复: 原实现把 account_id 当第一个参数传给 get_ipo_data (期望 type),
        # 导致返回 [{}]. 改为透传 type 参数 ("STOCK"/"BOND"/缺省全部).
        return self._call_qmt_global(
            "get_ipo_data", str(params.get("type") or params.get("stock_type") or ""))

    def _handle_query_smt_secu_info(self, params):
        # 期权标的持仓 — 官方 get_option_subject_position
        return self._call_qmt_global("get_option_subject_position", self._request_account_id(params))

    def _handle_query_smt_secu_rate(self, params):
        # 组合期权 — 官方 get_comb_option
        return self._call_qmt_global("get_comb_option", self._request_account_id(params))

    def _handle_smt_appointment(self, params):
        # SMB/预约打新属于交易类，需要下单通道；当前不支持。
        raise NotImplementedError("大 QMT RPC 不支持 smt_appointment")

    # 官方交易查询函数（直接暴露）
    def _handle_get_value_by_order_id(self, params):
        # 官方签名 get_value_by_order_id(orderId, accountID, strAccountType,
        # strDatatype)，strDatatype 是 'ORDER' / 'DEAL'（6.11）。原来只传了
        # orderId 一个参数 —— 少三个，_call_qmt_global 把 TypeError 吞掉返回
        # 空，从客户端看就是「查不到这笔委托」（#207）。
        order_id = str(params.get("order_id") or params.get("order_sysid") or "")
        if not order_id:
            raise ValueError("order_id 是必需的")
        account_id = self._request_account_id(params)
        detail_type = str(params.get("detail_type")
                          or params.get("datatype") or "ORDER").strip().upper()
        # 返回的是单个委托/成交对象，不是行列表 —— 走 _call_qmt_object，
        # 否则 _normalize_detail_rows 会去迭代这个对象、抛 TypeError 被吞成
        # []，看起来像「查不到这笔委托」（#207）。
        return self._call_qmt_object(
            "get_value_by_order_id", order_id, account_id,
            self._configured_account_type(account_id), detail_type)

    def _handle_get_last_order_id(self, params):
        # 官方签名 get_last_order_id(accountID, strAccountType, strDatatype
        # [, strategyName])（6.11）。原来只传了 accountID。strategyName 只对
        # 本地下单有效，用部署的默认策略名，空则不传。
        account_id = self._request_account_id(params)
        detail_type = str(params.get("detail_type")
                          or params.get("datatype") or "ORDER").strip().upper()
        strategy_name = params.get("strategy_name")
        if strategy_name is None:
            strategy_name = self.default_strategy_name
        strategy_name = str(strategy_name or "")
        args = [account_id, self._configured_account_type(account_id), detail_type]
        if strategy_name:
            args.append(strategy_name)
        # 返回的是委托号字符串，不是行列表 —— 走 _call_qmt_scalar。实盘实测：
        # 走 _call_qmt_global 时 '635005110' 被按字符迭代成 9 个空 dict，
        # QMT 表示「没找到」的 '-1' 变成 2 个空 dict（#207）。
        return self._call_qmt_scalar("get_last_order_id", *args)

    def _handle_get_ipo_data(self, params):
        # get_ipo_data 以申购代码为键的 dict 作答，而不是明细行。把它交给
        # _normalize_detail_rows 会遍历该 dict（也就是它的键），并对每个代码
        # 字符串做属性抓取——于是 {"730001": {...}, "001234": {...}} 变成
        # [{}, {}]：代码、发行价和数量全没了。#96 修好了 `type` 参数，但响应仍
        # 在这里被毁掉。
        return self._call_qmt_mapping(
            "get_ipo_data", str(params.get("type") or params.get("stock_type") or ""))

    def _handle_get_new_purchase_limit(self, params):
        # 文档说返回 板块 -> 额度 的 dict，所以它有与 get_ipo_data 相同的形状
        # 问题（API 参考 6.10）。
        return self._call_qmt_mapping(
            "get_new_purchase_limit", self._request_account_id(params))

    def _handle_get_history_trade_detail_data(self, params):
        account_id = self._request_account_id(params)
        detail_type = str(params.get("detail_type") or params.get("datatype") or "DEAL")
        start_date = str(params.get("start_date") or params.get("start_time") or "")
        end_date = str(params.get("end_date") or params.get("end_time") or "")
        # 官方签名 get_history_trade_detail_data(accountID, strAccountType,
        # strDatatype, startDate, endDate)（6.9）。原来漏了 strAccountType，
        # 于是 detail_type 被塞进了账户类型的位置 —— 少一个参数、还错位，
        # 和 #96 里 get_ipo_data 把 account_id 当 type 传是同一个形状（#207）。
        #
        # 不用 _call_qmt_global（#395）：那个辅助函数的“不可用 -> []”和吞异常对
        # 两融查询是对的，但在这里它们让三种完全不同的情况无法区分——函数未绑定、
        # 原生抛异常、以及真正空的历史，全都答成 ok=True data=[]。历史是事实记录：
        # 不可用必须是错误，原生失败必须传播。
        func = self.qmt_api.get("get_history_trade_detail_data")
        if func is None:
            raise RuntimeError(
                "get_history_trade_detail_data 在本终端上未绑定"
                "（probe_capabilities.qmt_globals 显示为 false）。在这里答空会被"
                "读成“没有历史”，而那不是同一回事（#395）")
        raw = func(account_id, self._configured_account_type(account_id),
                   detail_type, start_date, end_date)
        return _normalize_grouped_detail_rows(raw)

    def _handle_get_assure_contract(self, params):
        return self._call_qmt_global("get_assure_contract", self._request_account_id(params))

    def _handle_get_enable_short_contract(self, params):
        return self._call_qmt_global("get_enable_short_contract", self._request_account_id(params))

    def _handle_get_unclosed_compacts(self, params):
        return self._call_qmt_global(
            "get_unclosed_compacts",
            self._request_account_id(params),
            self._configured_account_type(self._request_account_id(params)),
        )

    def _handle_get_closed_compacts(self, params):
        return self._call_qmt_global(
            "get_closed_compacts",
            self._request_account_id(params),
            self._configured_account_type(self._request_account_id(params)),
        )

    def _handle_get_debt_contract(self, params):
        return self._call_qmt_global("get_debt_contract", self._request_account_id(params))

    def _handle_get_option_subject_position(self, params):
        return self._call_qmt_global("get_option_subject_position", self._request_account_id(params))

    def _handle_get_comb_option(self, params):
        return self._call_qmt_global("get_comb_option", self._request_account_id(params))

    def _handle_get_hkt_exchange_rate(self, params):
        # 官方签名 get_hkt_exchange_rate(accountID, accountType)，accountType 须为
        # HUGANGTONG / SHENGANGTONG（6.18）。原来一个参数都没传，而 _call_qmt_global
        # 会把 TypeError 吞掉返回 {} —— 又一次静默返回空，从客户端看和「这台终端
        # 没有港股通」一模一样（#205）。
        account_type = str(params.get("account_type")
                           or params.get("accountType") or "HUGANGTONG").strip().upper()
        return self._call_qmt_mapping(
            "get_hkt_exchange_rate", self._request_account_id(params), account_type)

    def _handle_download_history_data(self, params):
        """download_history_data 是 QMT 的全局函数（问题 #32）。

        它不是 ContextInfo 方法——适配器的 _call_context 路径总是抛
        NotImplementedError。现在改走 qmt_api（注入的全局函数），失败时回退到
        适配器（先试原生 xtdata SDK，再试 ContextInfo）。
        """
        func = self.qmt_api.get("download_history_data")
        # 某些 QMT 构建只暴露 down_history_data（签名相同，4 个参数）。
        if func is None:
            func = self.qmt_api.get("down_history_data")
        if func is not None:
            try:
                stock_code = str(params.get("stock_code") or "")
                period = str(params.get("period") or "1d")
                start_time = str(params.get("start_time") or "")
                end_time = str(params.get("end_time") or "")
                result = func(stock_code, period, start_time, end_time)
                if result is not None and not result:
                    raise RuntimeError(
                        "download_history_data：终端拒绝了这个任务"
                        "（原生返回 %r）code=%s period=%s"
                        % (result, stock_code, period))
                return True
            except Exception as exc:
                raise RuntimeError("download_history_data 失败: %s" % exc)
        # 回退：适配器先试原生 xtdata SDK，再试 ContextInfo。
        # #339：完全没有下载通道必须抛异常（#47 契约：失败要响），而不是返回
        # False——静默的 False 变成了客户端虚假的 finished==total 进度，实际零行
        # 落地。
        try:
            return self._handle_market_data_method("download_history_data", params)
        except (NotImplementedError, AttributeError):
            raise RuntimeError(
                "download_history_data：本终端没有下载通道（"
                "download_history_data/down_history_data 未绑定，ContextInfo "
                "也没有下载方法）——请检查 probe_capabilities 的 qmt_globals")

    def _handle_download_history_data2(self, params):
        """download_history_data2 是 QMT 的全局函数（问题 #32）。

        原生签名包含一个可选的进度回调；QMT 的全局函数可能要求它，所以客户端
        没给时传一个空函数。

        某些 QMT 构建只暴露单股票下载的全局函数（``download_history_data`` /
        ``down_history_data``）——那就用它们逐代码循环回退。没有这一步，RPC 返回
        False 且什么都没下载，读取只能永远看到最近一天（问题 #54）。
        """
        stock_list = list(params.get("stock_list") or [])
        period = str(params.get("period") or "1d")
        start_time = str(params.get("start_time") or "")
        end_time = str(params.get("end_time") or "")
        func = self.qmt_api.get("download_history_data2")
        if func is not None:
            try:
                # 先带一个空回调试（某些 QMT 构建要求它）；若抛 TypeError 再退回
                # 4 参数调用。
                try:
                    result = func(stock_list, period, start_time, end_time, lambda data: None)
                except TypeError:
                    result = func(stock_list, period, start_time, end_time)
                if result is not None and not result:
                    raise RuntimeError(
                        "终端拒绝了这个下载任务（原生返回 %r）"
                        "codes=%d period=%s" % (result, len(stock_list), period))
                return True
            except Exception as exc:
                raise RuntimeError("download_history_data2 失败: %s" % exc)
        single = self.qmt_api.get("download_history_data") or self.qmt_api.get("down_history_data")
        if single is not None:
            rejected = []
            errors = []
            for code in stock_list:
                try:
                    result = single(code, period, start_time, end_time)
                except Exception as exc:
                    errors.append("%s: %s" % (code, exc))
                    continue
                if result is not None and not result:
                    rejected.append(code)
            if errors or rejected:
                raise RuntimeError(
                    "download_history_data2 的逐代码回退失败"
                    "（rejected=%s errors=%s）" % (",".join(rejected) or "-",
                                                 "; ".join(errors) or "-"))
            return True
        # #339：这里曾是 `return False`——没有通道时的常量 False，正是变成客户端
        # 虚假 finished==total 进度的那个形状。失败必须抛异常（#47 契约）。
        try:
            return self._handle_market_data_method("download_history_data2", params)
        except (NotImplementedError, AttributeError):
            raise RuntimeError(
                "download_history_data2：本终端没有下载通道"
                "（download_history_data2/download_history_data/"
                "down_history_data 全部未绑定；ContextInfo 也没有下载方法）——"
                "请检查 probe_capabilities 的 qmt_globals")

    def _handle_probe_order_identity(self, params):
        """逐环节诊断 strategy_name 回填链。

        #156 的报告者：身份记录在 VK 里存在、键也对得上，而查询读到的仍是
        strategy_name=""。从外面看，链上没有任何一节能说明是哪一节把它丢了——
        这里从内部回答：身份 VK 到底接上了吗、键是否在 **本** 账户下存在、
        本地日志是否覆盖了它（#156 的进程内回退）、以及原始查找是否会抛异常。
        """
        account_id = self._request_account_id(params)
        remark = str(params.get("remark") or params.get("user_order_id") or "").strip()
        vk_client = self._identity_vk()
        journal = getattr(self, "_order_identity_local", None) or {}
        out = {
            "account_id": account_id,
            "remark": remark,
            "identity_valkey_wired": vk_client is not None,
            "local_journal_size": len(journal),
        }
        store = self._load_order_identity_journal(account_id)
        out["local_persistence_enabled"] = store is not None
        if store is not None:
            out["local_persistence_directory"] = store.directory
            out["local_persistence_error"] = store.error
            journal = self._order_identity_local
            out["local_journal_size"] = len(journal)
        if not remark:
            out["note"] = "pass remark=<the order's remark> to check the key"
            return out
        from .exec_events import order_identity_key

        key = order_identity_key(account_id, remark)
        out["identity_key"] = key
        if vk_client is not None:
            try:
                identity = _exec_events._read_order_identity(vk_client, account_id, remark)
                out["valkey_hit"] = bool(identity)
                if identity:
                    out["valkey_strategy_name"] = str(identity.get("strategy_name") or "")
            except Exception as exc:
                out["valkey_hit"] = None
                out["lookup_error"] = "%s: %s" % (exc.__class__.__name__, exc)
        entry = journal.get((account_id, remark))
        out["local_hit"] = bool(entry and entry[1])
        if entry:
            out["local_strategy_name"] = entry[1]
        return out

    def _handle_download_holiday_data(self, params):
        # MiniQMT 从 xtdata 服务下载节假日表。大 QMT 的终端自己维护交易日历
        # （登录时以及由它自己的数据更新器刷新），所以没有东西可下载，也没有
        # ContextInfo 方法可调——旧的通用路径答的是 NotImplementedError（问题
        # #163）。改为明确的空操作。
        return {
            "ok": True,
            "downloaded": False,
            "note": ("Big QMT maintains the trading calendar itself; nothing "
                     "to download. get_trading_dates/get_holidays read the "
                     "terminal's own data."),
        }

    def _handle_download_his_st_data(self, params):
        # 与 download_holiday_data 同一形态：ST 历史在大 QMT 上就在终端自己的
        # 数据里（问题 #163）。
        return {
            "ok": True,
            "downloaded": False,
            "note": ("Big QMT maintains ST history in the terminal's own data; "
                     "nothing to download. get_his_st_data reads it directly."),
        }

    def _order_action_from_params(self, params):
        action = str(params.get("action") or "").upper()
        if action:
            return action
        raw = params.get("order_type")
        order_type = str(raw or "").upper()
        if order_type in BUY_ORDER_TYPES:
            return "BUY"
        if order_type in SELL_ORDER_TYPES:
            return "SELL"
        # 两融操作的买卖方向编码在类型本身里（问题 #103）。直接还款 (32/45)
        # 移动的是资金而不是证券，没有方向。过去要求显式 action——但 MiniQMT 的
        # order_stock 没有 action 参数，所以兼容层调用方永远给不出来，归还融资
        # 就完全不可用（#314）。方向只是记账用：passorder 拿到的是原始 opType，
        # 而结算查找不会对无方向的类型按方向过滤。
        credit = _credit_action_of(raw)
        if credit:
            return credit
        if _credit_optype_of(raw) is not None:
            return _sideless_default_action()
        # 期货 (0-15) 和 ETF 期权 (50-59) 的 opType 把方向编码在类型里。
        # 行权/锁定 (56-59) 没有；与直接还款同样处理。
        passthrough = _passthrough_action_of(raw)
        if passthrough:
            return passthrough
        if _passthrough_optype_of(raw) is not None:
            return _sideless_default_action()
        # 可转债 转股 / 回售 (80-83): no side either.
        if _convertible_optype_of(raw) is not None:
            return _sideless_default_action()
        if raw in (None, ""):
            raise ValueError("action 或 order_type 是必需的")
        # 传了 order_type 却没被识别。在这里说“必需”，会让报告者盯着自己的调用
        # 看二十分钟（问题 #92）：真正的答案几乎总是部署在 QMT 里的包早于他所用的
        # 类型，而客户端 pip 升级修不了这一点——这段代码运行在 QMT 里。
        raise ValueError(
            # 仅限 ASCII：这段文本要写进 QMT 自己的日志，而它会丢掉非 ASCII
            # 字符（一个中文安装路径回来时就是乱码）。
            "order_type %r is not recognised by the package deployed in QMT "
            "(%s). Credit order types (27-32, and 40-45 special) need 0.3.1 "
            "or newer HERE, in the QMT python directory -- upgrading the "
            "client with pip does not change this file. Run "
            "xt_trader.sync_deployment(), restart the strategy, then check "
            "xtdata.get_deployment_info()." % (raw, self._deployed_version()))

    @staticmethod
    def _deployed_version():
        """从不抛异常：它只在构造错误消息时运行。"""
        try:
            from bigqmt_signal_trader.version import __version__

            return __version__
        except Exception:
            return "unknown version"

    def _forwarded_order_type(self, params):
        """原样转发给 passorder 的 order_type，或 None。

        两融 (27-32/40-45/70-75) 和期货/期权 (0-15/50-59) 的 opType 都编码了
        比方向更多的信息，所以 submit() 需要原始值。直接从 params 读取——调用
        之间不携带状态，所以这不依赖 OrderRequest 的关键字参数恰好以什么顺序
        求值。
        """
        raw = params.get("order_type")
        if _credit_optype_of(raw) is not None:
            return raw
        if _passthrough_optype_of(raw) is not None:
            return raw
        if _convertible_optype_of(raw) is not None:
            return raw
        return None

    # 旧名保留：外部调用方和既有测试还在用
    _credit_order_type_from_params = _forwarded_order_type

    def _resolve_strategy_name(self, *candidates):
        """第一个给出的候选胜出；"" 是真实答案，不是“未设置”。

        过去 ``or`` 链会把显式的空字符串吞回 ``default_strategy_name``。空白正是
        调用方要求「报单来源」列为空的方式，就像手工下的单（#154），所以只有
        缺失值（``None``）才可以落到默认值。
        """
        for candidate in candidates:
            if candidate is not None:
                return str(candidate)
        return str(self.default_strategy_name)

    def _handle_submit_order(self, params):
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        price = params.get("price")
        signal_id = str(params.get("signal_id") or "rpc-%s" % uuid.uuid4().hex)
        order_tag = str(params.get("remark") or params.get("order_remark") or "").strip()
        if not order_tag:
            order_tag = "bqrpc:%s" % signal_id
        request = OrderRequest(
            signal_id=signal_id,
            account_id=self._request_account_id(params),
            action=self._order_action_from_params(params),
            stock_code=str(params.get("stock_code") or ""),
            volume=int(params.get("volume") or params.get("order_volume") or 0),
            price=float(price if price not in (None, "") else 0),
            price_type=params.get("price_type") or "LIMIT",
            strategy_name=self._resolve_strategy_name(
                params.get("strategy_name")),
            remark=order_tag,
            order_type=self._forwarded_order_type(params),
            account_type=self._configured_account_type(self._request_account_id(params)),
        )
        if request.action not in ("BUY", "SELL"):
            raise ValueError("action 必须是 BUY 或 SELL")
        if not request.stock_code:
            raise ValueError("stock_code 是必需的")
        if request.volume <= 0:
            if _is_cash_repay_order_type(request.order_type):
                # #330：调用方把金额放进了 price。大 QMT 的 passorder 把直接还款
                # 的金额放在 VOLUME 槽位（整数元）并忽略 price；MiniQMT 的签名也
                # 没有其它槽位可以放它。
                raise ValueError(
                    "直接还款 (order_type %s)：还款金额放在 order_volume 里，"
                    "以整数元计，price 被忽略——收到 order_volume=%r price=%r。"
                    "示例：order_stock(acc, code, CREDIT_DIRECT_CASH_REPAY, "
                    "10000, FIX_PRICE, 0, ...)"
                    % (request.order_type, params.get("volume", params.get("order_volume")),
                       params.get("price")))
            raise ValueError("volume 必须为正")
        if _is_cash_repay_order_type(request.order_type) and request.price > 0:
            print("%s 直接还款：金额=%d（取自 order_volume）；passorder 会忽略价格=%r"
                  "（#330）" % ("[bigqmt_rpc]", request.volume, request.price))

        identity = None
        identity_vk = self._identity_vk()
        try:
            identity = _exec_events.remember_order_identity(
                identity_vk,
                request.account_id,
                request.remark,
                strategy_name=request.strategy_name,
                stock_code=request.stock_code,
            )
            if identity is None and identity_vk is not None:
                # 重复的备注复用了既有记录。新进程必须缓存 **那个** 名字，否则
                # 它新的本地日志会覆盖 VK 并再次把旧委托改名。
                identity = _exec_events._read_order_identity(
                    identity_vk, request.account_id, request.remark)
        except Exception as exc:
            _exec_events._warn_order_identity(exc)
        # 始终也写本地日志——成本低，而且是无 VK 部署唯一的归属来源
        # （问题 #156）。
        self._remember_order_identity_local(
            request.account_id, request.remark,
            (identity or {}).get("strategy_name") or request.strategy_name)

        # 在 passorder 之前取，使这笔委托产生的每个回调/行都在它之后；来自更早
        # 同备注委托的条目则严格早于它（#299）。
        submitted_at = time.time()
        result = self.order_gateway.submit(request)

        # 委托后校验：确认委托是否真的进了系统。passorder 调用成功但委托没进
        # 系统时（静默失败），记录 server_error 让客户端知道。匹配严格按
        # user_order_id(remark) 精确比对，不做 stock_code+action 的模糊兜底。
        # QMT 的委托号是异步分配的（passorder 无返回值），这里按唯一
        # user_order_id(remark) 精确匹配并回填 order_sys_id，避免客户端把
        # 「已提交但暂无委托号」误判为下单失败（issue #38）。
        self._last_server_error = ""

        # 异步调用方选择不等待订单号。MiniQMT 的 order_stock_async 立刻返回 seq
        # 并通过 order_callback 交付 id，所以把回复压到结算才发，正是异步 API 存在
        # 的意义所要避免的延迟（问题 #50）。order_callback 推送本来就带
        # order_sys_id，所以什么都不丢——只是跳过提交后的“落地了吗”检查，而静默
        # 拒单会表现为那条推送的缺失，而不是 server_error。
        if not _bool_value(params.get("wait_settlement"), True):
            # 立即回复，但继续盯着（#345）：在任何委托记录存在之前的拒单，否则
            # 对异步调用方不可见。它与单一回复槽位分开存放，使批量（#181）可以给
            # 每一项都挂上影子，而回复从不被压住。
            if not self.settle_orders_inline:
                shadows = getattr(self, "_pending_shadow_settlements", None)
                if shadows is None:
                    shadows = self._pending_shadow_settlements = []
                shadows.append(OrderSettlement(
                    request, result, _monotonic() + self.order_settle_timeout_seconds,
                    submitted_at=submitted_at, shadow=True))
            return result

        if self.settle_orders_inline:
            # 选择退出：像过去那样在这里阻塞。只保留给没有 adjust 排空可重试的
            # 运行时。
            try:
                import time as _time
                _time.sleep(self.order_settle_timeout_seconds)
                self._apply_order_lookup(
                    OrderSettlement(request, result, 0.0, submitted_at=submitted_at),
                    final=True, inline=True)
            except Exception:
                pass
            return result
        # 把结算交给调用方，而不是抛异常：对直接驱动 handlers 的人，handle() 仍
        # 是一个普通函数，只有 service 会延后它的回复。
        self._pending_settlement = OrderSettlement(
            request, result, _monotonic() + self.order_settle_timeout_seconds,
            submitted_at=submitted_at,
        )
        return result

    def _exec_event_sink(self):
        """本处理器优先向 VK 发布执行事件，其次通过行情推送通道发布，
        最后 None。"""
        transport = getattr(self, "rpc_transport", None)
        if getattr(transport, "name", None) in ("serial", "pipe", "copydata"):
            return transport if getattr(self, "exec_events_enabled", True) else None
        vk_client = self._identity_vk()
        if vk_client is not None:
            return vk_client
        manager = getattr(self, "quote_subscription_manager", None)
        publisher = getattr(manager, "_on_push_publisher", None)
        if callable(publisher):
            class _PushSink(object):
                def publish(self_, topic, data):
                    return publisher(topic, data)
            return _PushSink()
        return None

    def publish_order_error(self, request, error_msg):
        """对在任何记录之前就被拒的委托推送 on_order_error（#345）。"""
        sink = self._exec_event_sink()
        if sink is None:
            return False
        from .exec_events import publish_exec_event

        event = {
            "event_type": "order_error",
            "account_id": str(request.account_id or ""),
            "stock_code": str(request.stock_code or ""),
            "order_sys_id": "",
            "error_id": -1,
            "status": 57,
            "order_remark": str(request.remark or ""),
            "user_order_id": str(request.remark or ""),
            "strategy_name": str(request.strategy_name or ""),
            "error_msg": str(error_msg or ""),
            "source": "settlement",
            "created_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_at_ts": time.time(),
        }
        publish_exec_event(sink, event["account_id"], event)
        return True

    def take_shadow_settlements(self):
        """弹出上一次请求登记的影子结算（#345）。"""
        shadows = getattr(self, "_pending_shadow_settlements", None) or []
        self._pending_shadow_settlements = []
        return shadows

    def take_pending_settlement(self):
        """弹出上一次 submit_order 登记的结算，若有。"""
        settlement = self._pending_settlement
        self._pending_settlement = None
        return settlement

    # 与盯盘表一样有界：一个备注被整天复用（网格的各档）时，每结算一笔委托就
    # 追加一条。
    _SETTLED_MAX_REMARKS = 2000
    _SETTLED_MAX_PER_REMARK = 1000

    def _remember_settled_sysid(self, remark, sysid):
        """为 ``remark`` 发出的 id 已用掉；绝不再提供一次。"""
        remark = str(remark or "").strip()
        sysid = str(sysid or "").strip()
        if not remark or not sysid:
            return
        ids = self._settled_sysids_by_remark.get(remark)
        if ids is None:
            ids = collections.OrderedDict()
            self._settled_sysids_by_remark[remark] = ids
        else:
            self._settled_sysids_by_remark.move_to_end(remark)
        ids[sysid] = True
        while len(ids) > self._SETTLED_MAX_PER_REMARK:
            ids.popitem(last=False)
        while len(self._settled_sysids_by_remark) > self._SETTLED_MAX_REMARKS:
            self._settled_sysids_by_remark.popitem(last=False)

    def _sysid_already_settled(self, remark, sysid):
        remark = str(remark or "").strip()
        sysid = str(sysid or "").strip()
        if not remark or not sysid:
            return False
        ids = self._settled_sysids_by_remark.get(remark)
        return bool(ids) and sysid in ids

    def _apply_order_lookup(self, settlement, final=False, inline=False, orders_cache=None):
        """按备注查找委托。已结算返回 True，需重试返回 False。

        必须在主策略线程上运行——get_trade_detail_data 在其它任何地方都返回空。

        ``orders_cache``（(account_id, account_type) -> rows）让一次结算遍历在
        所有待结算委托之间共用同一份 ORDER 快照（#303）。最终查找总是重新读取：
        它的结论是“不在系统里”，而那不能建立在为别人取的列表上。

        按下单时的类型运行：股票账户上的港股通委托在 HUGANGTONG 列表里，不在
        STOCK 里。
        """
        from .account_type_map import request_account_type

        with request_account_type(getattr(settlement.order_request, "account_type", None)):
            return self._apply_order_lookup_as_requested(
                settlement, final=final, inline=inline, orders_cache=orders_cache)

    def _apply_order_lookup_as_requested(self, settlement, final=False, inline=False,
                                         orders_cache=None):
        request = settlement.order_request
        settlement.attempts += 1
        # 快路径：QMT 的 order_callback 已经把答案推过来了（问题 #164）。未命中
        # 不算什么——继续走轮询。备注并不唯一。自动生成的 "bqrpc:<uuid>" 唯一，
        # 但调用方给的是可以随意复用的——网格策略、串行脚本（「串行买入600股」跨
        # 三只股票两批，见 #299）。只按备注匹配会返回最近一笔携带它的 **别的**
        # 委托，调用方随后用那个错 id 过滤掉每个真实回调，把三笔成交当成没下单。
        # 所以每个候选还必须是本请求的股票和方向、不能早于本次提交，**而且** 不能
        # 是本桥接层已经为该备注结算过的 id——回调的到达时间并不能说明它属于哪笔
        # 委托，上一笔委托的回调可以晚于本次提交开始才落地，并携带一个已用掉的
        # id（2026-09-14 工行实盘复现，#300 上线几秒后）。
        want_code = normalize_stock_code(request.stock_code)
        want_action = str(request.action or "").upper()
        if _is_sideless_order_type(getattr(request, "order_type", None)):
            # 直接还款 / 行权 / 锁定 没有方向（#314）：记录下来的 action 只是记账
            # 默认值，终端行上报的任何方向都不应使候选被排除。
            want_action = ""
        watch = getattr(self, "order_watch_table", None)
        if watch is not None:
            try:
                watched_sysid = watch.sysid_for_remark(
                    request.remark, stock_code=want_code, action=want_action,
                    not_before=settlement.submitted_at)
            except Exception:
                watched_sysid = None
            if watched_sysid and self._sysid_already_settled(
                    request.remark, watched_sysid):
                watched_sysid = None  # 已用掉的 id——继续走轮询
            if watched_sysid:
                try:
                    settlement.result.order_sys_id = watched_sysid
                except Exception:
                    pass
                self._remember_settled_sysid(request.remark, watched_sysid)
                return True
        try:
            orders = None
            cache_key = (request.account_id, getattr(request, "account_type", None) or "")
            if orders_cache is not None and not final:
                orders = orders_cache.get(cache_key)
            if orders is None:
                orders = self.order_gateway.query_orders(request.account_id, "") or []
                if orders_cache is not None:
                    orders_cache[cache_key] = orders
            by_remark = _rows_for_this_submit(
                orders, request.remark, want_code, want_action, settlement.submitted_at)
            by_remark = [
                row for row in by_remark
                if not self._sysid_already_settled(
                    request.remark, str(getattr(row, "order_sys_id", "") or ""))]
            if by_remark:
                sysid = str(getattr(by_remark[0], "order_sys_id", "") or "")
                if sysid:
                    try:
                        settlement.result.order_sys_id = sysid
                    except Exception:
                        pass
                    self._remember_settled_sysid(request.remark, sysid)
                    return True
                # 行在，但 m_strOrderSysID 还没填。此时结算会发布
                # order_sys_id=None，客户端把它转成 -1，而一笔 **活着的** 委托就
                # 被报成 ORDER_REJECTED——在拒单上重试的调用方会重复下单。国金
                # 2.1.19.0 实测：紧接着 -1 之后手工回读时 id 已经在了（问题
                # #152）。所以继续等；行本身已证明委托到了券商。
                if not final:
                    return False
                message = (
                    "ORDER IS LIVE -- DO NOT RESUBMIT. passorder reached the "
                    "broker and the order row exists (stock=%s action=%s "
                    "price=%s volume=%d), but QMT had still not assigned "
                    "order_sys_id after %d lookup(s), so this reply carries no "
                    "id. Find it by remark %r, or in the 委托 list; it is not a "
                    "rejection (issue #152)."
                    % (request.stock_code, request.action, request.price,
                       request.volume, settlement.attempts, request.remark)
                )
                settlement.server_error = message
                if inline:
                    self._last_server_error = message
                return True
            if not final:
                # 还没出现。QMT 异步分配 id，所以早期未命中是正常的——只有到
                # 截止时间的未命中才是真未命中。
                return False
            if _is_cash_repay_order_type(getattr(request, "order_type", None)):
                # 直接还款（#330）：还款已经过账，而带此备注的行从未出现在
                # ORDER 列表里（实盘测得一次部分还款：1000 还了 100，随后在调用方
                # 抛出“not found in system”）。现金还款不是证券委托；终端不保留
                # 桥接层可用于结算的委托记录。没有 id、没有错误：order_stock 返回
                # -1 且不抛，调用方用 query_credit_detail 核对。
                try:
                    settlement.result.message = (
                        "直接还款已提交；终端不为现金还款保留委托行，所以没有 "
                        "order_sys_id —— 请用 query_credit_detail 核对")
                except Exception:
                    pass
                return True
            # 到截止时间仍无备注匹配。**不要** 退回到匹配 stock_code+action
            # （问题 #41）。但截止未命中也不等于拒单：在实盘终端上 ORDER 行 /
            # order_callback 可以在任意固定窗口之后数秒才到（盘中实测 +4.1s，
            # 问题 #360——委托活着并已成交，而调用方被告知“未找到”）。所以同步
            # 结论是“未确认”，不是“失败”：答 ok 但不带 id，并在延长后的截止时间
            # 上作为影子结算继续盯。只有延长窗口 **结束** 时的未命中才报为拒单。
            # （inline 模式没有排空循环可继续盯，所以保留旧的即时结论。）
            if not settlement.shadow and not inline:
                settlement.shadow = True
                settlement.deadline = _monotonic() + max(
                    4.0 * float(getattr(self, "order_settle_timeout_seconds", 3.0)), 10.0)
                try:
                    settlement.result.message = (
                        "submitted but UNCONFIRMED after %d lookup(s): no order "
                        "row yet. This can be plain callback latency (observed "
                        ">4s on a live terminal during market hours, #360) -- "
                        "DO NOT RESUBMIT. Watch order_callback / the 委托 list "
                        "for the id; if the extended watch also misses, an "
                        "order_error push follows (#345)." % settlement.attempts)
                except Exception:
                    pass
                return True
            # 延长窗口（或 inline）未命中 -> 现在才算拒单。首先要查的仍是策略的
            # 运行模式（#122），但迟到回调这个成因也真实存在（#360）。
            message = (
                "passorder submitted but order not found in system "
                "(stock=%s action=%s price=%s volume=%d, %d lookup(s) including "
                "an extended watch). FIRST check the strategy's run mode: in "
                "QMT's 模型交易 list the 运行模式 column defaults to 模拟, where "
                "passorder matches internally and never reaches the broker -- "
                "switch it to 实盘 (a simulated account stays simulated; the "
                "editor window and backtest/signal modes place no real order "
                "either). Second: the terminal refused the order BEFORE "
                "creating a record -- insufficient funds/position, price range, "
                "permissions: it shows a dialog on the QMT screen and fires no "
                "callback (#345). Third: the ORDER row / order_callback was "
                "simply late -- observed >4s on a live terminal during market "
                "hours (#360). Check the 委托 list before resubmitting."
                % (request.stock_code, request.action, request.price,
                   request.volume, settlement.attempts)
            )
            settlement.server_error = message
            if inline:
                self._last_server_error = message
            return True
        except Exception:
            # 查找失败不能丢掉这笔委托——它已经提交了。
            return True

    def _handle_submit_orders_batch(self, params):
        orders = params.get("orders") or []
        if not isinstance(orders, list) or not orders:
            raise ValueError("orders 必须是非空列表")
        if len(orders) > 500:
            raise ValueError("orders 超过批量上限 500")
        batch_started = time.time()
        batch_id = str(params.get("batch_id") or uuid.uuid4().hex)
        account_id = self._request_account_id(params)
        strategy_name = self._resolve_strategy_name(
            params.get("strategy_name"),
            (orders[0] or {}).get("strategy_name"),
        )
        # order_stock_async 把已排队的积压路由到这里（#181），但它从未承诺
        # order_stock_batch 的幂等契约，而继承那个契约会吞掉委托却答成功（#190）：
        # 对异步来说没有备注是正常的（单笔路径会自造 "bqrpc:<uuid>"），重复一个
        # 备注在那里也合法。在这个批量端点下，前者意味着 ORDER_TAG_REQUIRED 且
        # 什么也没下，后者意味着“已提交过”并只下出 N 笔中的一笔。
        # 默认 True，使有意调用 order_stock_batch 的调用方——他们提交标签正是为了
        # 防止重试重复下单——完全保持今天的行为。只有显式退出的调用方才得到单笔
        # 路径的语义。
        idempotent = params.get("idempotent")
        idempotent = True if idempotent is None else bool(idempotent)
        existing_by_tag = {}
        lookup_ok = True
        requires_lookup = idempotent and any(
            bool((item or {}).get("require_idempotency_check")) for item in orders)
        if requires_lookup:
            try:
                identity_query = getattr(self.order_gateway, "query_submission_identities_strict", None)
                if callable(identity_query):
                    existing, trades = identity_query(account_id, strategy_name)
                else:
                    query = getattr(self.order_gateway, "query_orders_strict", None)
                    existing = query(account_id, strategy_name) if callable(query) else self.order_gateway.query_orders(account_id, strategy_name)
                    trades = []
                existing_by_tag = {
                    str(getattr(order, "user_order_id", "") or ""): order
                    for order in existing or []
                    if str(getattr(order, "user_order_id", "") or "")
                }
                for trade in trades or []:
                    tag = str(getattr(trade, "user_order_id", "") or "")
                    if tag and tag not in existing_by_tag:
                        existing_by_tag[tag] = trade
            except Exception:
                lookup_ok = False
        results = []
        for index, item in enumerate(orders):
            item = dict(item or {})
            # 没有逐项结算，也不能按项协商（#181）。
            #
            # _handle_submit_order 把 wait_settlement 默认成 True，并把结算寄存在
            # **唯一** 一个槽位里；service 每个请求只取一次该槽位。于是一个请求里
            # 的 N 笔委托会互相覆盖，只有最后一笔留下——而它带着整批的请求/响应，
            # 所以整个批量的回复会被压住，直到那一笔结算完成或超时，并且那一笔的
            # 诊断（“order not found in system”，只点名一只股票、一个价格、一个
            # 数量）被挂到覆盖全部委托的回复上。它回填的 order_sys_id 没人读：每个
            # 结果 dict 都在任何结算运行之前就已由提交结果构造好了。
            #
            # 跳过它不损失任何东西。批量本来就按项作答，而 order_sys_id 与
            # order_stock_async 一样通过 order_callback 推送到达（#50）。要按项尊重
            # 显式的 wait_settlement，需要一条对应单个回复的结算队列，那正是 #181
            # 里“把单槽改成队列”的那一半。
            item["wait_settlement"] = False
            order_tag = str(item.get("order_remark") or item.get("remark") or item.get("signal_id") or "")
            if not order_tag and not idempotent:
                # 与 _handle_submit_order 相同的一套自造逻辑，使没有备注的异步委托
                # 能到达券商而不是被拒。只设 signal_id：_handle_submit_order 会由它
                # 推导出 "bqrpc:rpc-<hex>" 形式的备注，而在这里预先造备注会让前缀
                # 翻倍，并改变 QMT 在「备注」里显示的字符串（#152 的结算查找正是按
                # 它匹配）。
                item.setdefault("signal_id", "rpc-%s" % uuid.uuid4().hex)
                order_tag = "bqrpc:%s" % item["signal_id"]
            if not order_tag:
                results.append({
                    "index": index,
                    "batch_id": batch_id,
                    "success": False,
                    "accepted": False,
                    "explicit_failure": True,
                    "code": -3,
                    "error": "ORDER_TAG_REQUIRED",
                    "user_order_id": "",
                })
                continue
            known = existing_by_tag.get(order_tag) if idempotent else None
            journal_key = (account_id, strategy_name, order_tag)
            journal = self._submit_journal.get(journal_key) if idempotent else None
            if known is not None or journal is not None:
                results.append({
                    "index": index,
                    "batch_id": batch_id,
                    "success": True,
                    "accepted": True,
                    "confirmed": known is not None,
                    "idempotent": True,
                    "code": 0,
                    "order_sys_id": str(getattr(known, "order_sys_id", "") or (journal or {}).get("order_sys_id") or ""),
                    "user_order_id": order_tag,
                })
                continue
            if bool(item.get("require_idempotency_check")) and not lookup_ok:
                results.append({
                    "index": index,
                    "batch_id": batch_id,
                    "success": False,
                    "accepted": False,
                    "explicit_failure": False,
                    "code": -2,
                    "error": "IDEMPOTENCY_CHECK_UNAVAILABLE",
                    "user_order_id": order_tag,
                })
                continue
            try:
                result = self._handle_submit_order(item)
                response = {
                    "index": index,
                    "batch_id": batch_id,
                    "success": True,
                    "accepted": True,
                    "confirmed": False,
                    "idempotent": False,
                    "code": 0,
                    "order_sys_id": str(getattr(result, "order_sys_id", None) or ""),
                    "user_order_id": str(getattr(result, "user_order_id", None) or ""),
                }
                # 调用方选择退出时不写日志：那些标签不是身份（异步可能为真正不同的
                # 委托重复同一备注），记录它们会让一条记录压掉后来复用该字符串的
                # 有意 order_stock_batch 项。
                if order_tag and idempotent:
                    self._submit_journal[journal_key] = dict(response)
                results.append(response)
            except Exception as exc:
                results.append({
                    "index": index,
                    "batch_id": batch_id,
                    "success": False,
                    "accepted": False,
                    "explicit_failure": True,
                    "code": -1,
                    "error": "%s: %s" % (exc.__class__.__name__, exc),
                    "user_order_id": order_tag,
                })
        # 每个批量无条件打一行：超出客户端超时的批量会在这里继续跑到完成，没有
        # 这一行，随后翻倍的委托在服务端就毫无痕迹。
        try:
            from .logging_setup import get_logger
            get_logger("rpc").info(
                "批量提交 account=%s items=%d 已提交=%d 失败=%d %.0fms",
                account_id, len(orders),
                sum(1 for r in results if r.get("success") and not r.get("idempotent")),
                sum(1 for r in results if not r.get("success")),
                (time.time() - batch_started) * 1000.0)
        except Exception:
            pass
        return results

    def _handle_passorder(self, params):
        """原生 passorder 直通（路线 2）。

        把 11 个参数的 passorder 签名原样转发给 QMT，ContextInfo 由服务端提供。
        不做 opType 校验、不做代码归一、不做结算回读——原生契约由调用方负责。见
        BigQmtOrderGateway.passorder_passthrough。

        同时接受原生参数名（op_type/order_type/price_type/prType...）和 passorder
        自己的拼写，这样调用方可以粘贴他们熟悉的签名。account 默认取请求/网关
        账户。
        """
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        passthrough = getattr(self.order_gateway, "passorder_passthrough", None)
        if not callable(passthrough):
            raise RuntimeError("本部署早于 passorder 直通")

        def pick(*names, **kw):
            for name in names:
                if name in params and params[name] is not None:
                    return params[name]
            return kw.get("default")

        op_type = pick("op_type", "opType", "optype")
        order_type = pick("order_type", "orderType", "ordertype",
                          default=self.order_gateway.combo_type)
        account_id = self._request_account_id(params)
        order_code = pick("order_code", "orderCode", "stock_code", "code")
        price_type = pick("price_type", "prType", "prtype",
                          default=self.order_gateway.price_type)
        price = pick("price", default=-1)
        volume = pick("volume", "order_volume", "vol")
        strategy_name = pick("strategy_name", "strategyName",
                             default=self.default_strategy_name)
        quick_trade = pick("quick_trade", "quickTrade",
                           default=self.order_gateway.quick_trade)
        user_order_id = pick("user_order_id", "userOrderId", "order_remark",
                             "remark", default="")
        if op_type is None:
            raise ValueError("op_type 是必需的")
        if not order_code:
            raise ValueError("order_code 是必需的")
        if volume is None:
            raise ValueError("volume 是必需的")
        return passthrough(
            op_type=op_type,
            order_type=order_type,
            account_id=account_id,
            order_code=order_code,
            price_type=price_type,
            price=price,
            volume=volume,
            strategy_name=strategy_name,
            quick_trade=quick_trade,
            user_order_id=user_order_id,
            dry_run=bool(pick("dry_run", "dryRun", default=False)),
        )

    def _handle_cancel_order(self, params):
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        account_id = self._request_account_id(params)
        order_sys_id = str(params.get("order_sys_id") or params.get("order_sysid") or params.get("order_id") or "")
        if not order_sys_id:
            raise ValueError("order_sys_id 或 order_id 是必需的")
        order_ref = OrderRef(
            order_sys_id=order_sys_id,
            user_order_id=str(params.get("user_order_id") or ""),
        )
        result = self.order_gateway.cancel(order_ref, account_id=account_id)

        # 原生撤单返回值两个方向都不可信。#148：券商已接受撤单（67ms 内状态变成
        # 54）时它返回假。#151：对根本不存在的委托它返回真——这个返回值描述的是
        # “请求已发出”，而不是“委托已撤销”。所以现在两个方向都按委托快照结算。
        # 返回真时先做一次即时查找：常见情形（委托存在、已经是 53/54）无需额外
        # 往返就能确认，快路径仍然快；只有未确认的真值才付结算等待的代价。
        settlement = CancelSettlement(
            order_ref,
            account_id,
            result,
            _monotonic() + self.order_settle_timeout_seconds,
            account_type=self._configured_account_type(account_id),
        )
        if getattr(result, "success", None) is not False:
            try:
                if self._apply_cancel_lookup(settlement):
                    return result
            except Exception:
                pass  # 继续走下面的排队 / inline 等待
        if self.settle_orders_inline:
            try:
                import time as _time
                _time.sleep(self.order_settle_timeout_seconds)
                self._apply_cancel_lookup(settlement, final=True)
            except Exception:
                pass
        else:
            self._pending_settlement = settlement
        return result

    def _handle_cancel_orders_batch(self, params):
        """用一次 RPC 撤 N 笔委托。

        跳过结算查找（原因同 submit_orders_batch：get_trade_detail_data 在 adjust
        线程之外为空）。客户端上的撤单回调会异步确认结果。
        """
        if self.order_gateway is None:
            raise RuntimeError("order_gateway 未配置")
        items = params.get("items") or []
        if not isinstance(items, list) or not items:
            raise ValueError("items 必须是非空列表")
        if len(items) > 500:
            raise ValueError("items 超过批量上限 500")
        results = []
        for index, item in enumerate(items):
            item = dict(item or {})
            account_id = item.get("account_id") or self._request_account_id(params)
            order_sys_id = str(
                item.get("order_sysid")
                or item.get("order_sys_id")
                or item.get("order_id")
                or ""
            )
            market = str(item.get("market") or "")
            # accepted/confirmed 沿用提交批量的说法：原生撤单返回值回答的是“请求
            # 已发出”，而不是“委托已撤销”——而且它在实盘中两个方向都撒谎（#148：
            # 返回假而 67ms 后委托被撤销；#151：对不存在的委托返回真）。单笔撤单
            # 路径按委托快照结算；这个批量路径跳过它（那正是批量存在要省的延迟），
            # 所以这里任何地方都不能声称“已撤销”。确认来自委托状态推送（54）或一次
            # 查询回读。
            entry = {"index": index, "success": False,
                     "accepted": False, "confirmed": False}
            if not order_sys_id:
                entry["error"] = "order_sysid is required"
                results.append(entry)
                continue
            order_ref = OrderRef(
                order_sys_id=order_sys_id,
                user_order_id=str(item.get("user_order_id") or ""),
            )
            try:
                result = self.order_gateway.cancel(order_ref, account_id=account_id)
                entry["success"] = bool(getattr(result, "success", result))
                entry["accepted"] = True
                entry["message"] = str(getattr(result, "message", "") or "")
            except Exception as exc:
                entry["error"] = "%s: %s" % (type(exc).__name__, exc)
            results.append(entry)
        return results

    def _settle_cancel_from_status(self, settlement, status, order_sys_id, final):
        """一个状态答案，来自盯盘表或快照行。"""
        if status in CANCELED_ORDER_STATUSES:
            settlement.result.success = True
            settlement.result.message = ""
            return True
        if status in TERMINAL_NON_CANCEL_ORDER_STATUSES:
            return self._fail_cancel_settlement(
                settlement,
                "cancel was not confirmed: order %s reached status %s"
                % (order_sys_id, status))
        if status in CANCEL_IN_FLIGHT_STATUSES:
            if not final:
                return False
            # 交易所已接受撤单、正在途中——51/52 通常几毫秒内就转 54，临近收盘或
            # 拥堵时更慢。那不是撤单失败，把它报成失败就是 #148 假阴性从一个更窄
            # 的窗口再现（问题 #151）。
            settlement.result.success = True
            settlement.result.message = (
                "cancel accepted by exchange, still in flight: order %s is status %s"
                % (order_sys_id, status)
            )
            return True
        if not final:
            return False
        return self._fail_cancel_settlement(
            settlement,
            "cancel was not confirmed after %d lookup(s): order %s is still status %s"
            % (settlement.attempts, order_sys_id, status or "unknown"))

    def _fail_cancel_settlement(self, settlement, message):
        """把一次撤单结算为失败，并推一条 cancel_error 事件（#389）。

        QMT 会记录被拒的撤单（已成不可撤等），但不会为它触发任何回调：监听
        on_cancel_error 的策略永远不知道，于是不停去撤一笔已死的委托。桥接层自己
        的结算路径是唯一能得出失败结论的地方（按委托快照查状态），所以推送在这里
        合成，并与原生 order/trade 事件走同一通道。
        """
        settlement.result.success = False
        settlement.result.message = message
        self._publish_cancel_error(settlement, message)
        return True

    def _publish_cancel_error(self, settlement, message):
        hook = getattr(self, "exec_event_publisher", None)
        if not callable(hook):
            # 早于策略侧钩子的测试和部署没有通道；RPC 回复仍会带上结论。
            return
        try:
            order_ref = settlement.order_ref
            order_sys_id = str(getattr(order_ref, "order_sys_id", "") or "")
            remark = str(getattr(order_ref, "user_order_id", "") or "")
            event = {
                "event_type": "cancel_error",
                "account_id": str(getattr(settlement, "account_id", "") or ""),
                "stock_code": str(getattr(order_ref, "stock_code", "") or ""),
                "order_sys_id": order_sys_id,
                "order_id": order_sys_id,
                "error_id": None,
                "order_remark": remark,
                "user_order_id": remark,
                "error_msg": str(message or ""),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at_ts": time.time(),
            }
            hook(str(getattr(settlement, "account_id", "") or ""), event)
        except Exception:
            pass

    def _apply_cancel_lookup(self, settlement, final=False):
        """按委托快照解析含义不明的原生撤单返回值。"""
        from .account_type_map import request_account_type

        with request_account_type(getattr(settlement, "account_type", None)):
            return self._apply_cancel_lookup_as_requested(settlement, final=final)

    def _apply_cancel_lookup_as_requested(self, settlement, final=False):
        settlement.attempts += 1
        order_sys_id = str(settlement.order_ref.order_sys_id or "")
        # 快路径：这笔委托自己的状态变化已由 QMT 的 order_callback 推给我们
        # （问题 #164）。表未命中就走轮询。
        watch = getattr(self, "order_watch_table", None)
        if watch is not None:
            try:
                watched = watch.status_for_sysid(order_sys_id)
            except Exception:
                watched = None
            if watched is not None:
                return self._settle_cancel_from_status(
                    settlement, str(watched), order_sys_id, final)
        try:
            strict_query = getattr(self.order_gateway, "query_orders_strict", None)
            if callable(strict_query):
                orders = strict_query(settlement.account_id, "") or []
            else:
                orders = self.order_gateway.query_orders(settlement.account_id, "") or []
        except Exception as exc:
            if not final:
                return False
            return self._fail_cancel_settlement(
                settlement,
                "cancel status lookup failed after %d attempt(s): %s: %s"
                % (settlement.attempts, exc.__class__.__name__, exc))

        matches = [
            order for order in orders
            if str(getattr(order, "order_sys_id", "") or "") == order_sys_id
        ]
        if not matches:
            if not final:
                return False
            return self._fail_cancel_settlement(
                settlement,
                "cancel was not confirmed: order %s was not found after %d lookup(s)"
                % (order_sys_id, settlement.attempts))

        status = str(getattr(matches[0], "status", "") or "")
        return self._settle_cancel_from_status(
            settlement, status, order_sys_id, final)


def _bool_value(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _credit_action_of(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import credit_action_of

        return credit_action_of(order_type)
    except Exception:
        return None


def _credit_optype_of(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import credit_optype_of

        return credit_optype_of(order_type)
    except Exception:
        return None


def _convertible_optype_of(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import convertible_optype_of

        return convertible_optype_of(order_type)
    except Exception:
        return None


def _is_cash_repay_order_type(order_type):
    """直接还款 (MiniQMT 32 / 45)：金额放在 volume 槽位里。"""
    try:
        return int(order_type) in (32, 45)
    except (TypeError, ValueError):
        return False


def _is_sideless_order_type(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import is_sideless_order_type

        return is_sideless_order_type(order_type)
    except Exception:
        return False


def _sideless_default_action():
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import SIDELESS_DEFAULT_ACTION

        return SIDELESS_DEFAULT_ACTION
    except Exception:
        return "SELL"


def _passthrough_optype_of(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import passthrough_optype_of

        return passthrough_optype_of(order_type)
    except Exception:
        return None


def _passthrough_action_of(order_type):
    try:
        from bigqmt_signal_trader.adapters.order_bigqmt import passthrough_action_of

        return passthrough_action_of(order_type)
    except Exception:
        return None


def _deployed_version():
    """此处实际运行的桥接层版本，未知时返回 ""。"""
    try:
        from bigqmt_signal_trader.version import __version__

        return __version__
    except Exception:
        return ""


def _normalize_mapping_value(value):
    """把单个映射值处理成 JSON 安全，同时不压平它的形状。"""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _normalize_mapping_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_mapping_value(v) for v in value]
    rows = _normalize_detail_rows([value])
    return rows[0] if rows else {}


def _normalize_detail_rows(rows):
    """把 get_trade_detail_data 的行对象转成可 JSON 序列化的 dict。

    QMT 返回带 m_strXxx / m_nXxx / m_dXxx 属性的对象。我们把每个映射到它的
    公开属性，使结果能通过 JSON 编码。
    """
    if not rows:
        return []
    result = []
    for row in rows:
        if isinstance(row, dict):
            result.append(row)
            continue
        item = {}
        for name in dir(row):
            if name.startswith("_"):
                continue
            # QMT 的委托对象上除了 60 个 m_* 字段，还挂着 60 个枚举常量，
            # 名字是 '0' / '-1' / '101' 这种数字串（值就是常量本身）。dir()
            # 一并刮进来会让载荷翻倍、还夹着看不懂的键。非标识符的名字一律
            # 丢掉 —— 真字段（m_xxx / 普通属性名）都是合法标识符，不会误伤。
            if not name.isidentifier():
                continue
            try:
                value = getattr(row, name)
            except Exception:
                continue
            if callable(value):
                continue
            item[name] = value
        result.append(item)
    return result


def _jsonable_scalar(value):
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _normalize_grouped_detail_rows(rows):
    """序列化 get_history_trade_detail_data 文档化的分组答案。

    官方契约是 ``[(timetag, [detail, ...]), ...]``——示例里用
    ``for time, data in obj_list`` 迭代，然后再遍历 ``data`` 里的明细。只用
    _normalize_detail_rows 会把每个外层元组当成一个明细对象：元组没有数据属性，
    于是一份填满数据的历史被规范化成 [{}]，分组和里面的每笔成交都丢了（#395）。
    分组条目变成 ``{"timetag": t, "details": [...]}``；不符合文档形状的一律走
    扁平规范化器，使以扁平行作答的构建仍能作答。
    """
    if not rows:
        return []
    result = []
    for entry in rows:
        if (isinstance(entry, (list, tuple)) and len(entry) == 2
                and isinstance(entry[1], (list, tuple))):
            timetag, details = entry
            result.append({
                "timetag": _jsonable_scalar(timetag),
                "details": _normalize_detail_rows(details),
            })
        else:
            result.extend(_normalize_detail_rows([entry]))
    return result


# 只在响应含 DataFrame、Series 或 Panel 类型标记时递归还原。
# 对 51285 个标的的全市场快照，扫描原文比遍历结构快（实测 345.9 毫秒降至 3.7 毫秒）。
TYPED_PAYLOAD_MARKER = '"__bigqmt_type__"'
TYPED_PAYLOAD_FLAG = "__bigqmt_typed__"


def loads_rpc_response(raw):
    """解析应答并标记其中是否包含需还原的类型。"""
    text = decode_text(raw)
    response = json.loads(text)
    if isinstance(response, dict):
        response[TYPED_PAYLOAD_FLAG] = TYPED_PAYLOAD_MARKER in text
    return response


def encode_rpc_request_payload(request):
    """按 RPC 载荷格式编码请求 JSON。"""

    raw = json.dumps(request, ensure_ascii=False).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii").translate(SAFE_B64_DIGIT_ENCODE)
    return SAFE_B64_PREFIX + encoded


def decode_rpc_request_payload(text):
    text = str(text)
    if not text.startswith(SAFE_B64_PREFIX):
        return text
    encoded = text[len(SAFE_B64_PREFIX):].translate(SAFE_B64_DIGIT_DECODE)
    return base64.b64decode(encoded.encode("ascii")).decode("utf-8")


class VkPubSubRpcService:
    """通过配置的传输层接收 RPC 请求和发送应答。"""

    def __init__(
        self,
        vk_client,
        handlers,
        account_id="",
        response_vk_client=None,
        request_channel_template="bigqmt:rpc:req:{account_id}",
        request_queue_template="bigqmt:rpc:queue:{account_id}",
        response_channel_template="bigqmt:rpc:resp:{account_id}:{request_id}",
        response_list_template="bigqmt:rpc:respq:{account_id}:{request_id}",
        response_key_template="bigqmt:rpc:resp:{account_id}:{request_id}",
        response_ttl_seconds=60,
        max_queue_size=200,
        process_in_listener=False,
        listener_methods=None,
        background_threads=True,
        queue_poll_interval_seconds=0.02,
        debug_log_limit=0,
        print_prefix="[bigqmt_rpc]",
        transport=None,
        expire_margin_seconds=1.0,
        settle_interval_seconds=0.25,
        slow_request_seconds=1.0,
        heavy_offload=True,
        heavy_codes_threshold=HEAVY_READ_CODES_THRESHOLD,
    ):
        self.listen_vk = vk_client
        # adjust 处理队列时，大查询可交给一个工作线程（#351）。
        # 启用后台接收线程时，这些查询直接在该线程执行。
        self.heavy_offload = bool(heavy_offload)
        self.heavy_codes_threshold = max(1, int(heavy_codes_threshold))
        self._heavy_queue = queue.Queue()
        # 工作线程的结果在下一轮 adjust 发送。
        self._heavy_replies = queue.Queue()
        self._heavy_thread = None
        self._heavy_count = 0
        # 超过此阈值时记录具体方法和线程，定位拖慢 adjust 的请求（#342）。
        self.slow_request_seconds = float(slow_request_seconds)
        # 收到请求后计时，按客户端传入的等待时长判断是否过期（#303）。
        # 留出传输和一次 passorder 的余量，避免执行调用方已超时的委托。
        self.expire_margin_seconds = max(0.0, float(expire_margin_seconds))
        # 批量处理时定期暂停队列，先确认并应答已提交的委托。
        self.settle_interval_seconds = max(0.0, float(settle_interval_seconds))
        self._expired_request_count = 0
        self.vk = response_vk_client or vk_client
        self.handlers = handlers
        self.account_id = str(account_id or "")
        self.request_channel_template = request_channel_template
        self.request_queue_template = request_queue_template
        self.response_channel_template = response_channel_template
        self.response_list_template = response_list_template
        self.response_key_template = response_key_template
        self.response_ttl_seconds = int(response_ttl_seconds)
        self.process_in_listener = bool(process_in_listener)
        self.background_threads = bool(background_threads)
        if listener_methods is None:
            listener_methods = ("ping",)
        self.listener_methods = self._expand_listener_methods(listener_methods)
        self.queue_poll_interval_seconds = max(0.001, float(queue_poll_interval_seconds))
        self.debug_log_limit = int(debug_log_limit)
        self._received_count = 0
        self._processed_count = 0
        # 按 (account_id, request_id) 缓存写请求应答，防止 RPUSH 重试重复下单（#245）。
        self._order_requests = collections.OrderedDict()
        self._duplicate_order_requests = 0
        self._published_count = 0
        self._deferred_count = 0
        self.print_prefix = print_prefix
        self.pending = queue.Queue(maxsize=int(max_queue_size))
        # 等待主线程委托快照的下单和撤单请求。队列不设上限，确保待确认委托有应答。
        self._pending_settlements = queue.Queue()
        # 单独跟踪已应答的异步委托，识别终端落单前的拒绝（#345）。
        self._shadow_settlements = queue.Queue()
        self._running = threading.Event()
        self._thread = None
        self._queue_thread = None
        self._pubsub = None
        # 通信由传输层负责。未指定 transport 时沿用传入的客户端和模板构造 VK 传输。
        if transport is None:
            from .transports.vk_transport import VkTransport

            transport = VkTransport(
                vk_client,
                account_id=self.account_id,
                response_vk_client=response_vk_client,
                request_channel_template=request_channel_template,
                request_queue_template=request_queue_template,
                response_channel_template=response_channel_template,
                response_list_template=response_list_template,
                response_key_template=response_key_template,
                response_ttl_seconds=response_ttl_seconds,
                queue_poll_interval_seconds=queue_poll_interval_seconds,
                debug_log_limit=debug_log_limit,
                print_prefix=print_prefix,
            )
        self._transport = transport
        # 供处理器读取传输层的诊断信息，如应答在队列中的停留时间（#104）。
        try:
            self.handlers.rpc_transport = transport
        except Exception:
            pass
        # 同理，让 handlers 能报出「这个方法会跑在哪条线程上」。#204 查了半天
        # 才发现症结是线程路由，而远端报告里恰恰看不到这个 —— 一个只能远程
        # 诊断的部署，把决定行为的开关藏起来就等于让人猜。
        try:
            self.handlers.rpc_service = self
        except Exception:
            pass
        # 原始载荷交给 RPC 服务，统一判断直接执行还是排队。
        self._transport.on_raw_payload = self._handle_received_payload

    @property
    def request_channel(self):
        return self.request_channel_template.format(account_id=self.account_id)

    @property
    def request_queue(self):
        return self.request_queue_template.format(account_id=self.account_id)

    def start(self):
        self._running.set()
        # 传输层管理接收线程。enqueue_payload 分发已解码的请求并负责应答，
        # 返回 None 以避免传输层重复发送。VK 的接收循环也可上交原始载荷。
        self._transport.start_receiving(
            self.enqueue_payload,
            background_threads=self.background_threads,
        )
        # 记录传输线程，供停止操作及诊断使用。
        self._thread = getattr(self._transport, "_thread", None)
        self._queue_thread = getattr(self._transport, "_queue_thread", None)
        if not self.background_threads:
            self._start_heavy_worker()
            print("%s 已启动 queue=%s background_threads=False heavy_offload=%s"
                  % (self.print_prefix, self.request_queue,
                     self._heavy_thread is not None))
            return
        print("%s 已启动 channel=%s queue=%s" % (self.print_prefix, self.request_channel, self.request_queue))

    def stop(self):
        self._running.clear()
        try:
            self._transport.stop()
        except Exception:
            pass
        heavy = self._heavy_thread
        self._heavy_thread = None
        if heavy is not None and heavy.is_alive():
            heavy.join(1.0)
        # 线程由传输层管理，保留属性以兼容已有调用。
        self._thread = None
        self._queue_thread = None
        self._pubsub = None

    # -- 把重读取移出 adjust 线程（#351） ------------------------------
    def _start_heavy_worker(self):
        if not self.heavy_offload or self._heavy_thread is not None:
            return
        thread = threading.Thread(
            target=self._heavy_loop, name="bigqmt-rpc-heavy", daemon=True)
        thread.start()
        self._heavy_thread = thread

    def _heavy_loop(self):
        while self._running.is_set():
            try:
                request = self._heavy_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.process_request(request)
            except Exception:
                print("%s 重任务工作线程失败：\n%s"
                      % (self.print_prefix, traceback.format_exc()))

    def _on_heavy_worker(self):
        thread = self._heavy_thread
        return thread is not None and threading.current_thread() is thread

    def _should_offload(self, payload):
        """重读取，且有工作线程可以承接它。"""
        thread = self._heavy_thread
        if thread is None or not thread.is_alive():
            return False
        method = str((payload or {}).get("method") or "")
        return self.is_heavy_read(method, (payload or {}).get("params"))

    def is_heavy_read(self, method, params=None):
        canonical = self._canonical(method)
        if canonical in LISTENER_DEFERRED_METHODS or canonical in ORDER_METHODS:
            return False
        if canonical in HEAVY_READ_METHODS:
            return True
        if canonical in HEAVY_READ_CODE_METHODS:
            return self._heavy_by_size(canonical, params)
        return False

    def _heavy_by_size(self, canonical, params):
        params = params if isinstance(params, dict) else {}
        codes = params.get("stock_list")
        if codes is None:
            codes = params.get("codes")
        if codes is None:
            codes = params.get("stock_code", params.get("code"))
        if isinstance(codes, str):
            codes = [codes]
        try:
            codes = list(codes or [])
        except TypeError:
            codes = []
        if len(codes) > self.heavy_codes_threshold:
            return True
        # 市场标记（"SH"、"SZ"、"SHO" 等）代表数千个标的。
        for code in codes:
            if isinstance(code, str) and code and "." not in code:
                return True
        if canonical == "get_ticks":
            # types= 只在带标记时有意义；传它的调用方要的就是整个市场。
            return bool(params.get("types"))
        period = str(params.get("period") or "").lower()
        if period in ("tick", "l2quote", "l2order", "l2transaction"):
            return True
        # 用日期窗口（count 为 -1 且带 start），而不是最近 N 根 K 线。
        try:
            count = int(params.get("count", -1))
        except (TypeError, ValueError):
            count = -1
        if count < 0 and (params.get("start_time") or params.get("end_time")):
            return True
        return False

    def heavy_queue_depth(self):
        return self._heavy_queue.qsize()

    def flush_heavy_replies(self, max_items=100):
        """发送工作线程已完成的回复。adjust 线程。"""
        sent = 0
        for _ in range(int(max_items)):
            try:
                request, response = self._heavy_replies.get_nowait()
            except queue.Empty:
                break
            try:
                self._publish_response(request, response)
            except Exception:
                try:
                    from .logging_setup import get_logger
                    get_logger("rpc").error(
                        "发布重任务应答失败 method=%s：\n%s",
                        (request or {}).get("method"), traceback.format_exc())
                except Exception:
                    pass
            sent += 1
        return sent

    def _listen_loop(self):
        while self._running.is_set():
            try:
                pubsub = self.listen_vk.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                pubsub.subscribe(self.request_channel)
                if self.debug_log_limit > 0:
                    print("%s 已订阅 channel=%s"
                          % (self.print_prefix, self.request_channel))
                while self._running.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if not self._running.is_set():
                        break
                    if not message or message.get("type") != "message":
                        continue
                    self._handle_received_payload(message.get("data"), "pubsub")
            except Exception:
                print("%s 监听器失败：\n%s"
                      % (self.print_prefix, traceback.format_exc()))
                time.sleep(1.0)
            finally:
                try:
                    if self._pubsub is not None:
                        self._pubsub.close()
                except Exception:
                    pass
                self._pubsub = None

    def _queue_loop(self):
        while self._running.is_set():
            try:
                if self.debug_log_limit > 0:
                    print("%s 队列轮询 key=%s"
                          % (self.print_prefix, self.request_queue))
                while self._running.is_set():
                    # brpop 在服务端阻塞直到有元素到达（或短超时触发），所以请求
                    # 被推入后约 1ms 内就被取走，而不是等满
                    # queue_poll_interval_seconds。1s 的上限让我们能重新检查
                    # _running 以干净退出。
                    item = self.listen_vk.brpop(self.request_queue, timeout=1)
                    if not self._running.is_set():
                        break
                    if not item:
                        continue
                    raw = item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item
                    self._handle_received_payload(raw, "queue")
            except Exception:
                print("%s 队列监听器失败：\n%s"
                      % (self.print_prefix, traceback.format_exc()))
                time.sleep(1.0)

    def _handle_received_payload(self, raw_payload, source):
        self._received_count += 1
        if self._received_count <= self.debug_log_limit:
            try:
                preview = self._loads(raw_payload)
                method = str(preview.get("method") or "")
                print(
                    "%s 已收到请求 source=%s method=%s inline=%s"
                    % (self.print_prefix, source, method, self._should_process_in_listener(preview))
                )
                self.enqueue_payload(preview)
                return
            except Exception:
                print("%s 请求预览失败：\n%s"
                      % (self.print_prefix, traceback.format_exc()))
        self.enqueue_payload(raw_payload)

    def enqueue_payload(self, raw_payload):
        payload = self._loads(raw_payload)
        # 服务端时钟，在收到时取。请求最终被 adjust 线程取走时的积压年龄从
        # 这里起算。
        payload.setdefault("_received_at", time.time())
        if self._should_offload(payload):
            self._heavy_count += 1
            if self._heavy_count <= self.debug_log_limit:
                print("%s 重读取已转交工作线程 method=%s queued=%s"
                      % (self.print_prefix, payload.get("method"), self._heavy_queue.qsize()))
            self._heavy_queue.put(payload)
            return
        if self._should_process_in_listener(payload):
            self.process_request(payload)
            return
        self._deferred_count += 1
        if self._deferred_count <= self.debug_log_limit:
            print(
                "%s 已延迟处理 method=%s pending_before=%s"
                % (self.print_prefix, payload.get("method"), self.pending.qsize())
            )
        try:
            self.pending.put_nowait(payload)
        except queue.Full:
            # 待处理队列满（客户端轮询风暴）时不能把异常抛进 adjust 线程——QMT 在
            # 回调抛异常时会停掉策略。丢掉最旧的请求并保留最新的，而不是崩掉。
            try:
                self.pending.get_nowait()
            except Exception:
                pass
            try:
                self.pending.put_nowait(payload)
            except Exception:
                pass

    def _should_process_in_listener(self, payload):
        if not self.process_in_listener:
            return False
        method = str((payload or {}).get("method") or "")
        canonical = getattr(self.handlers, "_canonical_method", lambda value: value)(method)
        # 第二道闸，在按请求的路径上而不是配置路径上：交易上下文方法绝不在接收
        # 线程上运行，无论 listener_methods 里恰好有什么（#252）。下面的展开是
        # 构造时一次性的；这里对每个请求都跑，所以没人想到的某种拼写也无法把
        # get_trade_detail_data 路由到主策略线程之外——在那里它答出的行数是对的、
        # 每个字段都是 None，而客户端会把那读成“没钱”，不是“调用失败”。
        if method in LISTENER_DEFERRED_METHODS or canonical in LISTENER_DEFERRED_METHODS:
            return False
        if method in self.listener_methods:
            return True
        return canonical in self.listener_methods

    def _expand_listener_methods(self, listener_methods):
        """哪些方法可以在接收线程上就地运行。

        延后集合是 **最后** 减去的，作用于整个结果，而不只在 ``"*"`` 分支里。过去
        显式点名一个方法会完全跳过这次减法，于是
        ``rpc_listener_methods=("get_asset",)`` 把交易上下文方法又放回了接收线程。
        当接收线程 **就是** adjust 线程时（rpc_background_threads False）这无害，
        而一旦不是就静默出错：在主策略线程之外，它们答出的行数是对的、每个字段都
        是 None——读起来像“这个账户没钱”，而不是调用失败。

        让它依赖两个不相关的键相互一致，正是过去把 rpc_background_threads 一律钉
        在 False 的原因。改为在这里强制：任何配置都无法把交易上下文方法路由到
        后台线程，于是那个开关可以自由地按延迟来选。
        """
        resolve = getattr(self.handlers, "_canonical_method", lambda value: value)
        methods = set()
        for method in listener_methods or ():
            method = str(method)
            if method in ("*", "all", "read", "readonly"):
                methods.update(READ_METHODS)
            else:
                methods.add(method)
                methods.add(resolve(method))
        # 按 **规范名** 而不是字面拼写做减法。上面的循环把调用方的别名与规范名
        # 并存，所以朴素的 `methods - LISTENER_DEFERRED_METHODS` 会移掉
        # `get_positions` 却把 `query_stock_positions` 留在旁边——而
        # `_should_process_in_listener` 先匹配原始方法名，于是别名胜出（#252）。
        # `("get_positions",)` 和 `("*",)` 一直是正确的，这正是它读起来像“配置里
        # 的拼写改变了答案”而不是分发 bug 的原因。
        return set(m for m in methods if resolve(m) not in LISTENER_DEFERRED_METHODS)

    def _loads(self, raw_payload):
        if isinstance(raw_payload, dict):
            return dict(raw_payload)
        text = decode_text(raw_payload)
        text = decode_rpc_request_payload(text)
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("rpc 载荷必须是 JSON 对象")
        return payload

    def settle_pending_orders(self, max_items=100):
        """在 adjust 线程上重试已排队的提交/撤单查找。

        用队列而不是列表，因为 rpc_listener_methods 是可配置的：如果 submit_order
        被放进去，生产者就变成监听线程，而消费者仍在 adjust 上。

        未结算的条目会回到队列，所以每笔委托每个 adjust tick 花一次查找，直到结清
        或过了截止时间。

        像请求那样计时（#342 的慢请求日志）：一次错过回调快路径的遍历会扫描终端的
        ORDER 列表，每趟一次，而 drain_pending 每个 tick 最多跑三趟。#345 之后每笔
        order_stock_async 也要盯 order_settle_timeout_seconds，所以在有数百笔委托
        的日子里，这是唯一增长了的 adjust 线程成本，而日志就是它显现的地方。
        """
        pending = self._pending_settlements.qsize()
        shadow = self._shadow_settlements.qsize()
        if not pending and not shadow:
            return 0
        _t0 = time.perf_counter()
        try:
            return self._settle_pending_orders(max_items)
        finally:
            self._note_slow_request(
                "settle_pending_orders[pending=%d shadow=%d]" % (pending, shadow),
                time.perf_counter() - _t0)

    def _settle_pending_orders(self, max_items=100):
        settled = 0
        # 先快照队列大小。未结算条目会回到同一个队列，所以“排到空”会不断重新取
        # 到它们，把一个 adjust tick 变成每笔委托的许多次查找。
        batch = min(int(max_items), self._pending_settlements.qsize())
        # 每个账户每趟一份 ORDER 快照，由第一个错过回调快路径的结算去取、其余的
        # 复用。过去是每个待结算委托每个 tick 一次 get_trade_detail_data：在一天有
        # 数百行的日子里，一次 20 笔的突发每 100ms 就要完整转换 20 次当天的列表
        # （#303）。
        orders_cache = {}
        for _ in range(batch):
            try:
                settlement = self._pending_settlements.get_nowait()
            except queue.Empty:
                break
            expired = _monotonic() >= settlement.deadline
            try:
                if isinstance(settlement, CancelSettlement):
                    done = self.handlers._apply_cancel_lookup(settlement, final=expired)
                else:
                    done = self.handlers._apply_order_lookup(
                        settlement, final=expired, orders_cache=orders_cache)
            except Exception:
                done = True  # 绝不把已提交的委托搁在队列里
            if not done:
                self._pending_settlements.put(settlement)
                continue
            response = settlement.response
            response["data"] = to_jsonable(settlement.result)
            response["ok"] = True
            if getattr(settlement, "server_error", ""):
                response["server_error"] = settlement.server_error
            response["handled_at"] = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                request = settlement.request or {}
                self._remember_order_response(
                    (str(request.get("account_id") or self.account_id or ""),
                     str(request.get("request_id") or "")),
                    response, state="settled")
            except Exception:
                pass
            try:
                self._publish_response(settlement.request, response)
            except Exception:
                pass
            settled += 1
            if (getattr(settlement, "shadow", False)
                    and not isinstance(settlement, CancelSettlement)):
                # 在它的截止时间转换（#360）：回复当时答的是“未确认”；继续在影子
                # 队列上盯着，使迟到的回调能静默结清它，只有真正的拒单才会在延长
                # 后的截止时间推 order_error。
                self._shadow_settlements.put(settlement)
        settled += self._settle_shadow_orders(orders_cache, max_items)
        return settled

    def _settle_shadow_orders(self, orders_cache, max_items=100):
        """盯着已回复过的异步委托；被拒时推 order_error（#345）。"""
        settled = 0
        batch = min(int(max_items), self._shadow_settlements.qsize())
        for _ in range(batch):
            try:
                settlement = self._shadow_settlements.get_nowait()
            except queue.Empty:
                break
            expired = _monotonic() >= settlement.deadline
            try:
                done = self.handlers._apply_order_lookup(
                    settlement, final=expired, orders_cache=orders_cache)
            except Exception:
                done = True
            if not done:
                self._shadow_settlements.put(settlement)
                continue
            if getattr(settlement, "server_error", ""):
                self._push_shadow_order_error(settlement)
            settled += 1
        return settled

    def shadow_settlement_count(self):
        return self._shadow_settlements.qsize()

    def pending_settlement_count(self):
        return self._pending_settlements.qsize()

    def _push_shadow_order_error(self, settlement):
        """终端从未记录过的异步委托的 order_error（#345）。

        与废单回调路径相同的通道：有 VK 客户端时走 VK 执行事件通道，否则
        走行情推送通道的 exec:* 主题。两个通道都没有的部署无法承载它——客户端在
        那里退回到同步结算。
        """
        request = settlement.order_request
        publish = getattr(self.handlers, "publish_order_error", None)
        if not callable(publish):
            return
        try:
            publish(request, str(settlement.server_error or ""))
        except Exception as exc:
            print("%s 影子 order_error 推送失败：%s" % (self.print_prefix, exc))

    def drain_pending(self, max_items=20, budget_seconds=None):
        """在 adjust 线程上运行已排队的请求，然后结算。

        ``budget_seconds`` 限定这个 tick 占住策略线程多久（#303）。20 笔各约
        200ms 的委托把它占了 4s：QMT 自己的回调落不下来，于是每次结算都退到慢
        查询，而第一笔 0.2s 就该发的回复跟着第二十笔一起走。放不下的继续排队等
        下一个 tick；至少总会运行一个请求。None 保留旧的无限批量。每干
        ``settle_interval_seconds`` 的活，批量就暂停去结算并回复，使早的委托在
        后面的仍在跑时就已经作答。
        """
        # 自上个 tick 以来重读取工作线程已完成的回复先发出去：它们单是读取本身
        # 就已经晚了。
        self.flush_heavy_replies()
        # 先结算上一个 tick 遗留的条目，再接新活。
        self.settle_pending_orders()
        processed = 0
        started = _monotonic()
        last_settle = started
        budget = None if budget_seconds is None else max(0.0, float(budget_seconds))
        for _ in range(int(max_items)):
            if budget is not None and processed and _monotonic() - started >= budget:
                break
            try:
                request = self.pending.get_nowait()
            except queue.Empty:
                break
            if self._processed_count < self.debug_log_limit:
                print(
                    "%s 正在 drain method=%s pending_after=%s"
                    % (self.print_prefix, request.get("method"), self.pending.qsize())
                )
            self.process_request(request)
            processed += 1
            if (self.settle_interval_seconds
                    and self._pending_settlements.qsize()
                    and _monotonic() - last_settle >= self.settle_interval_seconds):
                self.settle_pending_orders()
                last_settle = _monotonic()
        # 再次结算，使 **本次** 排空中提交的委托仍能在本 tick 回复。一次查找、不
        # 睡眠——QMT 还没分配 id 就下一个 tick 重试，而不是占住线程（问题 #44）。
        self.settle_pending_orders()
        return processed

    def drain_request_queue(self, max_items=20):
        # 由 transport 直接拥有链路时交给它；对 VK 来说，transport 的排空驱动
        # _handle_received_payload（它遵守就地/延后的分叉），与原始语义一致。
        transport_drain = getattr(self._transport, "drain_request_queue", None)
        if transport_drain is not None and not isinstance(self._transport, type(None)):
            return transport_drain(max_items=max_items)
        processed = 0
        for _ in range(int(max_items)):
            item = self.listen_vk.lpop(self.request_queue)
            if not item:
                break
            self.process_request(self._loads(item))
            processed += 1
        return processed

    ORDER_DEDUP_MAX = 512
    ORDER_DEDUP_TTL_SECONDS = 600

    def _canonical(self, method):
        resolve = getattr(self.handlers, "_canonical_method", None)
        return resolve(method) if callable(resolve) else method

    def _claim_order_request(self, key):
        """为一次写操作认领 (account_id, request_id)。None == 首次。

        该 id 已见过时返回既有条目，使调用方无需再派发一次原生委托就能作答。
        """
        store = getattr(self, "_order_requests", None)
        if store is None:
            store = self._order_requests = collections.OrderedDict()
        now = time.time()
        # 先按年龄、再按数量裁剪。两者都有意有界：这在 adjust 线程上运行，不能
        # 随运行时长增长。
        for stale in [k for k, v in store.items()
                      if now - v.get("at", now) > self.ORDER_DEDUP_TTL_SECONDS]:
            store.pop(stale, None)
        while len(store) > self.ORDER_DEDUP_MAX:
            store.popitem(last=False)
        existing = store.get(key)
        if existing is not None:
            return existing
        store[key] = {"at": now, "response": None, "state": "dispatching"}
        return None

    def _remember_order_response(self, key, response, state=None):
        store = getattr(self, "_order_requests", None)
        if store is not None and key in store:
            store[key]["response"] = response
            if state:
                store[key]["state"] = state

    # 撤单无论多晚到达都执行：迟到的撤单无害，而且正是已放弃它的调用方仍想要的。
    # 迟到的提交相反——调用方已经认定它没有发生。
    _EXPIRY_EXEMPT_METHODS = frozenset(["cancel_order", "cancel_orders_batch"])

    def _client_deadline(self, request):
        """本请求的调用方何时不再等待，按服务端时钟。

        信封没有说明时（早于 #303 的客户端）为 None——这样的请求永不被拒，与以前
        完全一致。
        """
        try:
            timeout = float(request.get("timeout_seconds") or 0)
            received_at = float(request.get("_received_at") or 0)
        except (TypeError, ValueError):
            return None
        if timeout <= 0 or received_at <= 0:
            return None
        margin = min(self.expire_margin_seconds, timeout * 0.25)
        return received_at + timeout - margin

    def _expired_before_dispatch(self, request, method):
        # 同机单调时钟把 adjust 与后续排队的时间都计入客户端期限。
        deadline = request.get("_shm_deadline")
        if deadline is not None and getattr(getattr(self, "_transport", None), "name", None) == "shm":
            if _monotonic() >= deadline:
                return max(0.0, time.time() - request["_received_at"])
        if self._canonical(method) in self._EXPIRY_EXEMPT_METHODS:
            return None
        deadline = self._client_deadline(request)
        if deadline is None:
            return None
        now = time.time()
        if now < deadline:
            return None
        return now - float(request.get("_received_at") or now)

    def _refuse_expired(self, request, response, method, waited, order_key):
        """不派发，直接回复 RequestExpired（#303）。

        委托在 QMT 策略线程上一笔一笔执行，每笔约 200ms。32 个并发 order_stock 配
        6s 超时：前 20 个填满第一次排空，其余等下一个 tick，在 6s 超时——然后仍然
        被下出去了，在调用方记下失败之后几分钟。调用方的超时必须说到做到：超时
        之后，它所请求的任何事情都不该发生。
        """
        self._expired_request_count += 1
        response["error"] = (
            "RequestExpired: queued %.1fs on the server, past the client's "
            "%.1fs timeout; NOT dispatched (no order was placed). Orders run "
            "serially on the QMT strategy thread -- lower the concurrency "
            "or raise timeout_seconds." % (waited, float(request.get("timeout_seconds") or 0)))
        response["_t_reply"] = time.time()
        if order_key is not None:
            self._remember_order_response(order_key, response, state="refused")
        if self._expired_request_count <= 5 or self._expired_request_count % 100 == 0:
            print("%s 已拒绝过期请求 method=%s request_id=%s waited=%.1fs（累计 %d）"
                  % (self.print_prefix, method, response.get("request_id"), waited,
                     self._expired_request_count))
        try:
            self._publish_response(request, response)
        except Exception:
            pass
        return response

    def _request_outcome(self, params, account_id=None):
        """某笔委托请求的结果，按 request_id 查询（#303）。

        给 order_stock 超时的调用方用。在监听线程上读取，所以即使 adjust 线程正深
        陷某个批量也能作答。

            unknown      从未被取出（仍在队列——取出时因已过期会被拒——或被丢弃）
            dispatching  passorder 正在运行
            dispatched   passorder 已返回；正在查订单号
            settled      已回复；``response`` 就是调用方错过的那条
            refused      分派前已过期；什么都没下
        """
        request_id = str((params or {}).get("request_id") or "").strip()
        if not request_id:
            raise ValueError("request_id 是必需的")
        account_id = str(account_id or (params or {}).get("account_id") or self.account_id or "")
        store = getattr(self, "_order_requests", None) or {}
        entry = store.get((account_id, request_id))
        if entry is None:
            return {"request_id": request_id, "state": "unknown", "response": None}
        return {"request_id": request_id, "state": entry.get("state") or "dispatching",
                "response": entry.get("response")}

    def process_request(self, request):
        request = dict(request or {})
        request_id = str(request.get("request_id") or request.get("id") or uuid.uuid4().hex)
        account_id = str(request.get("account_id") or self.account_id or "")
        method = str(request.get("method") or "")

        # 写操作的至多一次（#245）。客户端的 valkey-py 连接在
        # ConnectionError/TimeoutError 上会透明重试，而重试会重发整条命令——
        # conn.retry 包住 send 和 parse，所以服务器已接受的 RPUSH 在回复丢失时会
        # 被再发一次。下游没有任何去重：order_stock / order_stock_async 都别名到
        # submit_order，而 _handle_submit_order 没有幂等检查（只有
        # submit_orders_batch 有）。于是一次 client.call 可能派发两次原生 passorder。
        #
        # 重试会重发完全相同的载荷（含 request_id），这正是可以在这里修的原因：
        # 第二份是可识别的。用第一份的回复作答，而不是再派发一次——两份指向同一个
        # 回复键，所以它会落在客户端已经等待的地方。
        order_key = None
        if request_id and self._canonical(method) in ORDER_METHODS:
            order_key = (account_id, request_id)
            seen = self._claim_order_request(order_key)
            if seen is not None:
                self._duplicate_order_requests += 1
                remembered = seen.get("response")
                print("%s 已抑制重复委托请求 method=%s request_id=%s"
                      % (self.print_prefix, method, request_id))
                if remembered is None:
                    # 仍在途：第一份结算时会发布到同一个回复键。除了不派发，无事
                    # 可做。
                    return None
                try:
                    self._publish_response(request, remembered)
                except Exception:
                    pass
                return remembered
        response = {
            "schema_version": 1,
            "request_id": request_id,
            "account_id": account_id,
            "method": method,
            "ok": False,
            "data": None,
            "error": "",
            # server_error 携带 QMT 侧诊断（例如 passorder 已提交但委托不在系统里、
            # get_trade_detail_data 返回空），它们不抛异常但表明出了问题。让客户端
            # 能看到操作为何静默失败。
            "server_error": "",
            "handled_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # 墙钟而不是 perf_counter：客户端是同机上的独立进程，所以这些时间可以
            # 直接与它自己的 time.time() 比较，并把往返分解成 排队等待 / 处理 / 返回，
            # 无需猜测（#104）。
            "_t_recv": time.time(),
        }
        waited = self._expired_before_dispatch(request, method)
        if waited is not None:
            return self._refuse_expired(request, response, method, waited, order_key)
        _t0 = _t1 = _t_json1 = None
        try:
            if self.account_id and account_id and account_id != self.account_id:
                raise PermissionError("account_id 不匹配")
            _t0 = time.perf_counter()
            try:
                if method == "get_request_outcome":
                    result = self._request_outcome(request.get("params") or {}, account_id)
                else:
                    result = self.handlers.handle(method, request.get("params") or {})
            finally:
                _t1 = time.perf_counter()
                self._note_slow_request(method, _t1 - _t0)
            response["data"] = to_jsonable(result)
            # to_jsonable 是大答案烧时间的地方（#386：12.7MB 的 5m K 线在服务侧
            # 这里花了约 11s，而处理器本身很快，且没有任何东西记录——慢请求记录只
            # 覆盖处理器那一段）。
            _t_json1 = time.perf_counter()
            response["ok"] = True
            # 处理器记录了服务端诊断时把它暴露出来。
            server_error = getattr(self.handlers, "_last_server_error", None)
            if server_error:
                response["server_error"] = str(server_error)
            # passorder 可能仍在等它异步分配的 id；返回假的原生撤单可能仍在等一个
            # 可靠的终端状态（#148）。把任一回复挂起，而不是在这个线程上睡；之后的
            # adjust tick 会结算并发布它（#44）。
            take_shadows = getattr(self.handlers, "take_shadow_settlements", None)
            for shadow in (take_shadows() if callable(take_shadows) else []):
                shadow.request = request
                self._shadow_settlements.put(shadow)
            take = getattr(self.handlers, "take_pending_settlement", None)
            settlement = take() if callable(take) else None
            if settlement is not None:
                settlement.request = request
                settlement.response = response
                self._pending_settlements.put(settlement)
                self._deferred_count += 1
                if order_key is not None:
                    self._remember_order_response(order_key, response, state="dispatched")
                return response
        except Exception as exc:
            response["error"] = "%s: %s" % (exc.__class__.__name__, exc)
        response["_t_reply"] = time.time()
        if order_key is not None:
            self._remember_order_response(order_key, response, state="settled")
        _t_pub0 = time.perf_counter()
        try:
            self._publish_response(request, response)
        except Exception:
            # 响应发布失败（例如 VK 故障）不能传播到 adjust 线程——QMT 在回调抛
            # 异常时会停掉策略。请求已经执行；客户端只会看到超时。
            try:
                from .logging_setup import get_logger
                get_logger("rpc").exception("发布应答失败 method=%s", method)
            except Exception:
                pass
        _t_pub1 = time.perf_counter()
        self._note_slow_response(method, _t0, _t1, _t_json1, _t_pub0, _t_pub1)
        if method == "ping":
            # 在发布 **之后** 记录：这条 print 会写到 QMT 输出面板，在服务线程上要花
            # 约 1 个 adjust tick 的 GIL 等待——夹在处理器与发送之间时，它把 ping 的
            # 往返放大约 100ms，并让分解数据撒谎（#104）。
            try:
                from .logging_setup import get_logger
                get_logger("rpc").info(
                    "ping 耗时明细 handle=%.1fms jsonable=%.1fms publish=%.1fms",
                    (_t1 - _t0) * 1000.0,
                    (_t_pub0 - _t1) * 1000.0,
                    (time.perf_counter() - _t_pub0) * 1000.0)
            except Exception:
                pass
        self._processed_count += 1
        if self._processed_count <= self.debug_log_limit:
            print("%s 已应答 method=%s ok=%s"
                  % (self.print_prefix, method, response["ok"]))
        return response

    def _note_slow_request(self, method, seconds):
        """点名一个占住线程 ``slow_request_seconds`` 的处理器。

        在处理器返回（或抛异常）之后记录，所以它不会给本来很快的请求增加 GIL 等待。
        从不抛异常：这也在 adjust 线程上运行，而那里抛异常会停掉策略。
        """
        try:
            threshold = float(self.slow_request_seconds)
        except Exception:
            threshold = 1.0
        if threshold <= 0 or seconds < threshold:
            return
        try:
            thread_name = threading.current_thread().name
        except Exception:
            thread_name = "?"
        try:
            from .logging_setup import get_logger
            get_logger("rpc").warning(
                "请求处理缓慢 method=%s 耗时 %.1fs thread=%s（#342："
                "同规模的 [adjust_phase] drain 就运行在这段时间内）",
                method, seconds, thread_name)
        except Exception:
            pass

    def _note_slow_response(self, method, t_handle0, t_handle1, t_json_done,
                            t_pub0, t_pub1):
        """针对处理器 **之外** 时间（#386）的分段慢日志。

        上面的处理器级记录只覆盖 ``handlers.handle``；一个大答案随后把真正的耗时
        花在 ``to_jsonable`` 和发布上，而没有任何日志点名它们——一次 60 个代码、
        40 天、5m 的读取在服务端花了约 11.5s，却没有一行慢日志。只有当 **总耗时**
        超过 ``slow_request_seconds`` 而处理器段单独没有超过时才触发（否则处理器
        记录已经点名了）。从不抛异常：这也在 adjust 线程上运行。
        """
        if t_handle0 is None or t_handle1 is None:
            return
        try:
            threshold = float(self.slow_request_seconds)
        except Exception:
            threshold = 1.0
        if threshold <= 0:
            return
        handle = t_handle1 - t_handle0
        total = t_pub1 - t_handle0
        if total < threshold or handle >= threshold:
            return
        jsonable = (t_json_done - t_handle1) if t_json_done is not None else -1.0
        publish = t_pub1 - t_pub0
        try:
            thread_name = threading.current_thread().name
        except Exception:
            thread_name = "?"
        try:
            from .logging_setup import get_logger
            get_logger("rpc").warning(
                "应答发送缓慢 method=%s total=%.1fs handle=%.1fs "
                "to_jsonable=%.1fs publish=%.1fs thread=%s（#386："
                "瓶颈在应答转换和发送，不在处理函数；"
                "可考虑调整 chunk_size 或缩小查询窗口）",
                method, total, handle, jsonable, publish, thread_name)
        except Exception:
            pass

    def _format_response_target(self, template, account_id, request_id):
        if not template:
            return ""
        return template.format(account_id=account_id, request_id=request_id)

    def _publish_response(self, request, response):
        if self._on_heavy_worker():
            # 在工作线程上构建，由 adjust 线程发送（见 _heavy_replies）。工作线程
            # 到达的每次发布——正常回复、RequestExpired 拒绝——都挂在这里。
            self._heavy_replies.put((request, response))
            return
        # 交给 transport（VkTransport 分发到 key/list/channel；ZMQ/MySQL
        # transport 走它们自己的回复路径）。
        self._transport.send_response(request, response)
        self._published_count = getattr(self._transport, "_published_count", self._published_count)

    def _response_clients(self):
        clients = [self.vk]
        if self.listen_vk is not self.vk:
            clients.append(self.listen_vk)
        return clients

    def _write_response_key(self, response_key, ttl_seconds, payload):
        first_error = None
        wrote = 0
        for client in self._response_clients():
            try:
                if ttl_seconds > 0:
                    client.setex(response_key, ttl_seconds, payload)
                else:
                    client.set(response_key, payload)
                wrote += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if wrote <= 0 and first_error is not None:
            raise first_error
        return wrote

    def _push_response_list(self, response_list, ttl_seconds, payload):
        first_error = None
        pushed = 0
        for client in self._response_clients():
            try:
                client.rpush(response_list, payload)
                if ttl_seconds > 0:
                    client.expire(response_list, ttl_seconds)
                pushed += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if pushed <= 0 and first_error is not None:
            raise first_error
        return pushed

    def _publish_response_channel(self, response_channel, payload):
        first_error = None
        receivers = 0
        published = 0
        for client in self._response_clients():
            try:
                receivers += int(client.publish(response_channel, payload) or 0)
                published += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if published <= 0 and first_error is not None:
            raise first_error
        self._published_count += 1
        if self._published_count <= self.debug_log_limit:
            print("%s 已发布应答 receivers=%s" % (self.print_prefix, receivers))
        return receivers


def call_vk_rpc(
    vk_client,
    account_id,
    method,
    params=None,
    request_channel_template="bigqmt:rpc:req:{account_id}",
    request_queue_template="bigqmt:rpc:queue:{account_id}",
    response_channel_template="bigqmt:rpc:resp:{account_id}:{request_id}",
    response_list_template="bigqmt:rpc:respq:{account_id}:{request_id}",
    response_key_template="bigqmt:rpc:resp:{account_id}:{request_id}",
    timeout_seconds=3.0,
    ttl_seconds=60,
    transport="queue",
    request_id=None,
):
    """供测试和管理脚本使用的小型外部客户端助手。"""

    request_id = str(request_id or uuid.uuid4().hex)
    request_channel = request_channel_template.format(account_id=account_id)
    request_queue = request_queue_template.format(account_id=account_id)
    response_channel = response_channel_template.format(account_id=account_id, request_id=request_id)
    response_list = response_list_template.format(account_id=account_id, request_id=request_id)
    response_key = response_key_template.format(account_id=account_id, request_id=request_id)
    request = {
        "schema_version": 1,
        "request_id": request_id,
        "account_id": account_id,
        "method": method,
        "params": params or {},
        "reply_channel": response_channel,
        "reply_list": response_list,
        "reply_key": response_key,
        "ttl_seconds": ttl_seconds,
        # 本调用方等多久。超过这个时间之后才轮到执行的委托，服务端会直接拒绝而
        # 不是执行它（#303）。
        "timeout_seconds": float(timeout_seconds),
        "_sent_at": time.time(),
    }
    payload = encode_rpc_request_payload(request)
    if str(transport or "queue").lower() in ("queue", "list", "blpop"):
        vk_client.rpush(request_queue, payload)
        vk_client.expire(request_queue, max(60, int(ttl_seconds)))
        deadline = time.time() + float(timeout_seconds)
        while True:
            raw_response = vk_client.get(response_key)
            if raw_response:
                return loads_rpc_response(raw_response)
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            wait_timeout = max(1, int(min(remaining, 1.0) + 0.999))
            try:
                item = vk_client.blpop(response_list, timeout=wait_timeout)
            except Exception as exc:
                if _is_vk_timeout(exc):
                    continue
                raise
            if item:
                raw_response = item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item
                try:
                    vk_client.delete(response_list)
                except Exception:
                    pass
                return loads_rpc_response(raw_response)
        raw_response = vk_client.get(response_key)
        if raw_response:
            return loads_rpc_response(raw_response)
        raise TimeoutError(
            "VK rpc 超时: %s account_id=%s request_queue=%s" % (method, account_id, request_queue)
        )

    pubsub = vk_client.pubsub(ignore_subscribe_messages=True)
    try:
        pubsub.subscribe(response_channel)
        vk_client.publish(request_channel, payload)
        deadline = time.time() + float(timeout_seconds)
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            message = pubsub.get_message(timeout=remaining)
            if not message or message.get("type") != "message":
                continue
            response = loads_rpc_response(message.get("data"))
            if response.get("request_id") == request_id:
                return response
        raw_response = vk_client.get(response_key)
        if raw_response:
            return loads_rpc_response(raw_response)
        raise TimeoutError("VK rpc 超时: %s" % method)
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
