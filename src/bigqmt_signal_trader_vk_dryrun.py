# coding: utf-8
"""Big QMT VK 模拟运行策略入口。

此入口从 VK db5 读取测试信号并写回状态，但订单只会使用
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
from bigqmt_signal_trader.adapters.vk_common import local_vk_config


ACCOUNT_ID      = "bigqmt_probe"
VK_HOST     = "127.0.0.1"
VK_PORT     = 6379
VK_DB       = 5
VK_USERNAME = ""
VK_PASSWORD = ""

try:
    import bigqmt_signal_trader_qmt_config as _local_config

    BIGQMT_CONFIG = local_vk_config(_local_config)
except Exception:
    BIGQMT_CONFIG = {}

VK_HOST     = BIGQMT_CONFIG.get("host", VK_HOST)
VK_PORT     = int(BIGQMT_CONFIG.get("port", VK_PORT))
VK_DB       = int(BIGQMT_CONFIG.get("db", VK_DB))
VK_USERNAME = BIGQMT_CONFIG.get("username", VK_USERNAME)
VK_PASSWORD = BIGQMT_CONFIG.get("password", VK_PASSWORD)


if ACCOUNT_ID:
    set_account_id(ACCOUNT_ID)

configure(
    mode="dryrun",
    account_id=ACCOUNT_ID,
    signal_source_type="valkey",
    state_store_type="valkey",
    position_sync_type="valkey",
    valkey={
        "host": VK_HOST,
        "port": VK_PORT,
        "db": VK_DB,
        "username": VK_USERNAME,
        "password": VK_PASSWORD,
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
