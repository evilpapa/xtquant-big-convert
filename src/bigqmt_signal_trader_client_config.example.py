# coding: utf-8
"""MiniQMT 兼容客户端的私有配置示例。

复制为 bigqmt_signal_trader_client_config.py，放在客户端脚本旁或 Python
模块搜索路径中。可用 BIGQMT_CLIENT_CONFIG_MODULE 显式指定模块名。
找不到客户端配置时，回退到 bigqmt_signal_trader_qmt_config。
真实配置可能含账号和 Valkey 凭据，请勿提交到版本库。
"""

BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
BIGQMT_RPC_TIMEOUT_SECONDS = 30.0
BIGQMT_DOWNLOAD_WAIT_SECONDS = 1800
BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS = 0.5

BIGQMT_CONFIG = {
    # 账户查询（持仓/资产）失败时，是否用 valkey 里的上一份快照作答。
    # 默认 False —— 查询失败就抛给调用方。打开它等于接受
    # 「拿旧持仓当当前持仓」，策略据此算仓位是会出事的（#243）。
    # 打开后仍要求快照带 updated_at 且在时限内，否则照样抛。
    # "account_cache_fallback": False,
    # "account_cache_max_age_seconds": 30,

    "host": "YOUR_VALKEY_HOST",
    "port": 6379,
    "db": 5,
    "username": "",
    "password": "",
    # 传输方式须与 QMT 端配置一致。
    "transport": "valkey",
}

# get_full_tick 默认走 RPC。开启后，客户端读取按需刷新的 Valkey 快照。
BIGQMT_FULL_TICK_CACHE_CONFIG = {
    "enabled": False,
    "demand_ttl_seconds": 10,
    "cache_ttl_seconds": 10,
    "wait_seconds": 3.5,
    "poll_interval_seconds": 0.2,
}

# get_market_data_ex 将 K 线写入 dir，get_local_data 优先读本地缓存。
# download_history_data 系列接口向大 QMT 提交下载任务。
# fallback_rpc 默认在缓存未命中时通过 RPC 补齐，兼容下载后按不同复权方式读取。
# 仅离线读取时设为 False。
BIGQMT_LOCAL_CACHE_CONFIG = {
    "enabled": True,
    "dir": None,            # 默认目录为 ~/.bigqmt_cache
    "fallback_rpc": True,
    # 可选 "parquet"、"pkl" 或 "auto"。自动模式有 pyarrow 时用 Parquet，否则用 pickle。
    # 按周期、复权类型和代码分别存文件，切换格式时自动迁移。
    "format": "auto",
}

# FormulaServer（默认 58600 端口）提供参考数据和历史数据直读。
# 无法直接读取时自动回退到 RPC。账户、持仓、委托、成交和五档行情走 RPC。
BIGQMT_FORMULA_SERVER_CONFIG = {
    "enabled": True,        # 可用环境变量 BIGQMT_FORMULA_ENABLED=0 关闭
    # "host": "127.0.0.1",  # FormulaServer 监听 0.0.0.0，防火墙放行后可跨机器访问
    # "port": 58600,        # 未设置时读 qmt_root 下的 formulaserver.ini，缺省用 58600
    # "qmt_root": r"D:\国金证券QMT交易端",
    # "timeout_seconds": 3.0,
    # "methods": [...],     # 限定直读的方法，默认使用所有已映射的方法
    # "failure_cooldown_seconds": 30.0,  # 失败后暂停直读的秒数
}

# 全推订阅 xtdata.subscribe_whole_quote 通过 RPC 管理订阅，通过独立的 Valkey
# 发布订阅通道推送增量行情。装有 msgpack 时用 MsgPack，否则用 JSON。
# 服务端按 (client_id, sub_id) 计数，所有客户端退订或超时后释放共享订阅。
# 默认在 ~/.cache/bigqmt/quote_client_id 保存订阅者标识，供重启后恢复。
# 客户端默认每 3 秒发送 quote_keepalive，服务端默认超时为 30 秒。
BIGQMT_QUOTE_CLIENT_ID = None  # 可指定如 "my-strategy-1" 的固定标识
# BIGQMT_QUOTE_HEARTBEAT_SECONDS = 3.0  # 也可通过环境变量设置，须小于服务端超时

