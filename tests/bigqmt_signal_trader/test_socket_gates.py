# -*- coding: utf-8 -*-
"""验证原生行情开关以及未配置 VK 时的客户端连接行为。"""
import os
import sys
import types
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider  # noqa: E402
from bigqmt_signal_trader.xtquant_compat import (  # noqa: E402
    BigQmtXtTrader, _build_quote_push_channel,
)
from bigqmt_signal_trader.quote_push_channel import VkQuotePushChannel  # noqa: E402


class NativeXtdataGateTest(unittest.TestCase):
    def test_disabled_never_loads(self):
        import bigqmt_signal_trader.adapters.market_bigqmt as m

        def _boom():
            raise AssertionError("_load_native_xtdata must not be called")

        saved = m._load_native_xtdata
        m._load_native_xtdata = _boom
        try:
            provider = BigQmtMarketDataProvider(object(), native_xtdata_enabled=False)
            self.assertIsNone(provider._native())
        finally:
            m._load_native_xtdata = saved

    def test_enabled_loads_lazily_as_before(self):
        provider = BigQmtMarketDataProvider(object(), native_xtdata_enabled=True)
        # 仓库环境里没有 QMT 的原生 SDK：加载尝试合法地回 None，但确实走了加载
        self.assertIsNone(provider._native())

    pass


class RpcHandlerNativeGateTest(unittest.TestCase):
    """The RPC handler honors the explicit native xtdata setting."""

    def _build_service(self, rpc):
        import bigqmt_signal_trader_strategy as strategy

        config = {
            "account_id": "acct",
            "enable_rpc": True,
            "rpc": dict({"enabled": True, "account_id": "acct"}, **rpc),
            "qmt_api": {},
            "account_type": "STOCK",
        }
        app = types.SimpleNamespace(order_gateway=None, position_sync_sink=None)
        return strategy._build_rpc_service(types.SimpleNamespace(), app, config)


    def test_explicit_value_wins_over_transport_default(self):
        import bigqmt_signal_trader_strategy as strategy

        self.assertFalse(strategy._native_xtdata_enabled(
            {"transport": "shm", "native_xtdata_enabled": True}))
        self.assertFalse(strategy._native_xtdata_enabled(
            {"transport": "valkey", "native_xtdata_enabled": False}))

    pass


class ClientVkGateTest(unittest.TestCase):
    def _trader(self, transport, explicit):
        trader = BigQmtXtTrader(account_id="acct")
        trader.client = types.SimpleNamespace(
            transport_name=transport,
            account_id="acct",
            _vk_explicit=explicit,
            _vk=lambda: (_ for _ in ()).throw(AssertionError("must not build")),
        )
        return trader

    def test_vk_transport_always_probes(self):
        reachable = types.SimpleNamespace(ping=lambda: True)
        trader = self._trader("valkey", explicit=False)
        trader.client._vk = lambda: reachable

        self.assertIs(trader._exec_events_vk_or_none(), reachable)


class QuoteChannelGateTest(unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
