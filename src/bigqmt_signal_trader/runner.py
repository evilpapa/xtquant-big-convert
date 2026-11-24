"""大 QMT 运行文件可复用的转发入口。"""

import datetime as _dt
import traceback

from .logging_setup import get_logger

_log = get_logger("runner")

_APP = None


def reset_app():
    global _APP
    _APP = None


def get_app():
    return _APP


def init_app(context_info, app_factory):
    global _APP
    _APP = app_factory(context_info)
    if hasattr(_APP, "on_init"):
        _APP.on_init(context_info)
    return _APP


def tick_app(context_info, now=None):
    if _APP is None:
        return None
    now = now or _dt.datetime.now()
    try:
        return _APP.tick(now)
    except Exception:
        _log.error("策略周期任务 tick_app 执行失败：\n%s", traceback.format_exc())
        return None


def forward_order_event(event):
    # Unguarded events reach QMT's order_callback, which stops the strategy on
    # raise. Guard like tick_app so a bad event (e.g. VK outage during the
    # position-sync publish) never stops the strategy.
    if _APP is None:
        return None
    try:
        return _APP.on_order_event(event)
    except Exception:
        _log.error("转发委托回报失败：\n%s", traceback.format_exc())
        return None


def forward_trade_event(event):
    if _APP is None:
        return None
    try:
        return _APP.on_trade_event(event)
    except Exception:
        _log.error("转发成交回报失败：\n%s", traceback.format_exc())
        return None


def sync_positions_app(reason="manual"):
    if _APP is None:
        return None
    try:
        return _APP.sync_positions(reason)
    except Exception:
        _log.error("同步持仓失败：\n%s", traceback.format_exc())
        return None
