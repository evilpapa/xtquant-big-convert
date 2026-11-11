"""Isolated QMT-native and standalone backtest components."""

from .data_feed import CsvBarFeed, InMemoryBarFeed
from .engine import BacktestConfig, BacktestEngine
from .protocol import BacktestBridgeProtocol


__all__ = [
    "BacktestBridgeProtocol",
    "BacktestConfig",
    "BacktestEngine",
    "CsvBarFeed",
    "InMemoryBarFeed",
]

__version__ = "1.0.0"
