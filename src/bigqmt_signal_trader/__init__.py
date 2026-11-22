"""可替换的大 QMT 信号下单包核心模块。"""

import sys

if sys.version_info < (3, 12):
    raise RuntimeError("xtquant-big-convert requires Python 3.12 or newer.")

from .version import __version__, deployment_report

from .app import SignalTradingApp
from .models import (
    AccountSnapshot,
    AssetSnapshot,
    OrderRequest,
    OrderSubmitResult,
    PositionSnapshot,
    SignalAction,
    SignalStatus,
    TradeSignal,
)
from .option_analytics import (
    black_scholes_price,
    calculate_option_analytics,
    implied_volatility,
    option_greeks,
)

def __getattr__(name):
    if name in ("BigQmtRpcClient", "BigQmtXtData", "BigQmtXtTrader"):
        from . import xtquant_compat

        return getattr(xtquant_compat, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "AccountSnapshot",
    "AssetSnapshot",
    "BigQmtRpcClient",
    "BigQmtXtData",
    "BigQmtXtTrader",
    "OrderRequest",
    "OrderSubmitResult",
    "PositionSnapshot",
    "SignalAction",
    "SignalStatus",
    "SignalTradingApp",
    "TradeSignal",
    "__version__",
    "black_scholes_price",
    "calculate_option_analytics",
    "implied_volatility",
    "option_greeks",
]
