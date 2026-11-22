# coding: utf-8
"""QMT 端的私有配置示例。

复制为 bigqmt_signal_trader_qmt_config.py，放到 QMT 的 python 目录。
真实配置可能含账号和 Valkey 凭据，请勿提交到版本库。
"""

BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"

# STOCK(股票) / CREDIT(信用两融) / FUTURE(期货) / STOCK_OPTION(股票期权) ...
# 信用账户务必设为 CREDIT：按 STOCK 查询不会报错，get_trade_detail_data 会返回
# 一行全 0 的资产，表现为「信用账户资产全是 0」而日志里毫无线索（issue #92）。
# 也可以写在下面 BIGQMT_CONFIG 的 "account_type" 里，两处都认；解析结果
# 会在启动时打印，冲突也会指出来。
#
# 同一个账号有几种类型（港股通）就写成列表，第一个是默认：
#     BIGQMT_ACCOUNT_TYPE = ["STOCK", "HUGANGTONG", "SHENGANGTONG"]
# 客户端 StockAccount(id, "HUGANGTONG") 的类型会随每个交易类请求传来，在列表里
# 就按它查（港股通的持仓/委托/成交在终端里记在 HUGANGTONG / SHENGANGTONG 下，
# 不在 STOCK 下）；不在列表里仍按默认答，并记一次日志。下单不用改：passorder
# 的 23/24 对 .HK 代码就是港股通买卖。
BIGQMT_ACCOUNT_TYPE = "STOCK"

BIGQMT_CONFIG = {
    # 这台机器上有没有 valkey 可用。默认 True，仅对非 valkey 传输生效。
    #
    # 设 False 时整个 valkey 块不再下发给策略，于是委托身份库、异步下载任务、
    # 全推快照缓存、exec 事件推送都不会去连 valkey —— 一次都不试。用在券商 QMT
    # 的 import 白名单不含 RESP 客户端的场景
    # （issue #145 / #147）。下面的 host/port 有默认值，所以不设这一项时
    # 「配了 valkey」和「什么都没写」是分辨不出来的。
    #
    # 代价：查询里的 strategy_name 回填失效（那份记录只存在 valkey 里，见 #133）；
    # 异步下载任务和全推快照缓存不可用（这两个在大 QMT 上本来就默认关闭）。
    # 委托/成交回调不受影响
    # "valkey_enabled": False,
    "host":     "127.0.0.1",
    "port":            6379,
    "db":                 5,
    "username": "",
    "password": "",
    # 默认关闭远程下单和撤单。
    "rpc_allow_order_methods": False,
    # 未指定 strategy_name 的委托带的 投资备注，默认 "bigqmt_rpc"。
    # 注意这不是内部字段：QMT 把它显示在 委托 列表的「报单来源」列里
    # （issue #154），任何看那个界面的人都能看到。设成自己的名字，或者设成
    # "" 让该列留空 —— 和手动下单一样。单次调用传 strategy_name= 始终优先。
    # "rpc_default_strategy_name": "",
    # get_trade_detail_data 在主策略线程外返回空结果，依赖交易上下文的方法
    # （LISTENER_DEFERRED_METHODS）始终由 QMT 的 adjust 回调执行（#244）。
    #
    # 各传输方式默认由 adjust 处理队列，最多等一个调度周期。
    # 后台线程每次获取 GIL 都需等待，一次请求可能等多次（#343）。
    # 仅在传输方式无法由 adjust 处理队列时启用后台线程。
    "rpc_process_in_listener":  True,
    "rpc_listener_methods":     ("*",),
    "rpc_background_threads":   False,
    "schedule_adjust":          True,
    "schedule_adjust_interval": "100nMilliSecond",

    # 单次 adjust 处理 RPC 队列的时间上限（#303）。默认一个调度周期且至少 0.5 秒。
    # 超出预算的请求留到下一轮，设为 0 取消上限。
    # "drain_budget_seconds": 0.5,
    # 大查询可交给一个工作线程（#351），包括财务、公式、全市场行情和
    # get_market_data_ex 的多标的、逐笔或日期窗口查询。
    # QMT 在多数这类查询期间释放 GIL，减少 adjust 卡顿，应答可能多等一轮。
    # 下载、轻量查询和交易查询仍在 adjust 线程执行。
    # "rpc_heavy_offload": True,
    # "rpc_heavy_codes_threshold": 20,
    # get_full_tick 默认走 RPC。全市场数据过大时可开启按需快照缓存。
    # 客户端查询会续期需求，默认有效 10 秒。代码列表按常规间隔刷新，
    # 全市场（SH/SZ/BJ/HK）使用较慢的间隔。
    "full_tick_cache_enabled":                   False,
    "full_tick_demand_ttl_seconds":                 10,
    "full_tick_cache_ttl_seconds":                  10,
    "full_tick_refresh_interval_seconds":          0.5,
    "full_tick_market_refresh_interval_seconds":     3,

    # 每轮刷新的耗时预算，已开始的单项查询会执行完毕。
    "full_tick_refresh_max_wall_seconds": 0.3,
    "full_tick_max_requests":               8,

    # 异步下载任务由策略线程分批处理。每批标的数和每轮耗时由下列参数控制。
    # 仅在可连接 MiniQMT/xtdata 数据服务时启用。大 QMT 可在终端的
    # 「数据管理 / 补充数据」下载历史数据，再通过 RPC 读取。
    "download_jobs_enabled":         False,
    "download_job_chunk_size":          10,
    "download_job_max_wall_seconds":   0.5,
    "download_job_ttl_seconds":       3600,

    # 通过 Valkey 推送委托和成交回报，供客户端触发 on_stock_order / on_stock_trade。
    "exec_events_enabled": True,
    # 诊断时可开启，在策略面板打印每次回调的原始字段，并随 raw_fields 发布。
    # 用于核对 m_nDirection / m_nOffsetFlag 的实际值，当前买卖映射按 48/49 处理。
    "exec_events_debug_raw_fields": False,
}
