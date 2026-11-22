# coding: utf-8
"""Big QMT 信号交易器干运行入口。

请将此文件放入 QMT 的 Python 策略目录，并从 QMT 中加载它。
当前默认使用空信号源和 DryRunOrderGateway，因此不会提交真实订单。
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


# 填写此项后再进行真实账户测试。干运行加载测试时请留空。
ACCOUNT_ID = ""


if ACCOUNT_ID:
    set_account_id(ACCOUNT_ID)

configure(mode="dryrun", account_id=ACCOUNT_ID or "dryrun")
