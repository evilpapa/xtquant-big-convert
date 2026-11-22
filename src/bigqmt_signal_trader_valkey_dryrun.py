# coding: utf-8
"""Big QMT Valkey 模拟运行策略入口。

此入口从 Valkey db5 测试信号读取数据并将状态写入 Valkey，但订单只会使用
DryRunOrderGateway，不会提交真实的 QMT 订单。
"""

from bigqmt_signal_trader_strategy import (  # noqa: E402
    adjust,
    configure,
    credit_account_callback,
    deal_callback,
    handlebar,
    init,
    order_callback,
    set_account_id,
    sync_positions,
)
from bigqmt_signal_trader.adapters.valkey_common import local_valkey_config


ACCOUNT_ID      = "bigqmt_probe"
VALKEY_HOST     = "127.0.0.1"
VALKEY_PORT     = 6379
VALKEY_DB       = 5
VALKEY_USERNAME = ""
VALKEY_PASSWORD = ""

try:
    import bigqmt_signal_trader_qmt_config as _local_config

    BIGQMT_CONFIG = local_valkey_config(_local_config)
except Exception:
    BIGQMT_CONFIG = {}

VALKEY_HOST     = BIGQMT_CONFIG.get("host", VALKEY_HOST)
VALKEY_PORT     = int(BIGQMT_CONFIG.get("port", VALKEY_PORT))
VALKEY_DB       = int(BIGQMT_CONFIG.get("db", VALKEY_DB))
VALKEY_USERNAME = BIGQMT_CONFIG.get("username", VALKEY_USERNAME)
VALKEY_PASSWORD = BIGQMT_CONFIG.get("password", VALKEY_PASSWORD)


if ACCOUNT_ID:
    set_account_id(ACCOUNT_ID)

configure(
    mode="dryrun",
    account_id=ACCOUNT_ID,
    signal_source_type="valkey",
    state_store_type="valkey",
    position_sync_type="valkey",
    valkey={
        "host": VALKEY_HOST,
        "port": VALKEY_PORT,
        "db": VALKEY_DB,
        "username": VALKEY_USERNAME,
        "password": VALKEY_PASSWORD,
        "stream_key_template": "bigqmt:signals:{account_id}",
        "group_name": "bigqmt-signal-trader",
        "consumer_name": "bigqmt-probe",
        "block_ms": 0,
        "claim_key_template": "bigqmt:signal_claim:{account_id}:{signal_id}",
        "status_key_template": "bigqmt:signal_status:{account_id}:{signal_id}",
        "position_key_template": "bigqmt:positions:{account_id}",
        "position_event_stream_template": "bigqmt:position_events:{account_id}",
    },
)
