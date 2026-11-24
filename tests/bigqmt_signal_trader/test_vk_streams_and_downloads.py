# coding: utf-8
"""Unavailable streams and terminal-maintained download tables (#163).

When an endpoint reports `unknown command 'XADD'`, skip further stream writes
until the process restarts. Pub/sub callbacks remain available.

And MiniQMT's download_holiday_data / download_his_st_data raised
NotImplementedError on big QMT, whose terminal maintains those tables itself.
They now answer with an explicit no-op note.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import exec_events
from bigqmt_signal_trader.adapters import vk_common
from bigqmt_signal_trader.adapters.position_sync_vk import VkPositionSyncSink


class ResponseError(Exception):
    pass


class FakeVk(object):
    def __init__(self, xadd_error=None):
        self.xadd_error = xadd_error
        self.xadd_calls = 0
        self.published = []
        self.setex_calls = []

    def xadd(self, *args, **kwargs):
        self.xadd_calls += 1
        if self.xadd_error is not None:
            raise self.xadd_error
        return b"1-0"

    def publish(self, channel, raw):
        self.published.append((channel, raw))
        return 1

    def setex(self, key, ttl, payload):
        self.setex_calls.append(key)

    def set(self, key, payload):
        self.setex_calls.append(key)


class _Snapshot(object):
    def __init__(self):
        self.account_id = "acct"
        self.reason = "tick"
        self.updated_at = None
        self.asset = type("A", (), {"cash": 1.0, "total_asset": 2.0})()
        self.positions = {}


class _StreamsState(unittest.TestCase):
    def setUp(self):
        self._dead = vk_common._STREAMS_DEAD
        vk_common._STREAMS_DEAD = False

    def tearDown(self):
        vk_common._STREAMS_DEAD = self._dead


class StreamFallbackTest(_StreamsState):
    def test_unknown_command_disables_xadd_after_one_warning(self):
        r = FakeVk(ResponseError("unknown command 'XADD'"))
        warnings = []

        class Log(object):
            def warning(self, msg, *a):
                warnings.append(msg)

        exec_events._publish(r, "ch", {"e": 1})
        exec_events._publish(r, "ch", {"e": 2})

        self.assertEqual(len(r.published), 2, "pub/sub must keep working")
        self.assertEqual(r.xadd_calls, 1, "the dead command must not be retried")
        self.assertTrue(vk_common.streams_dead())

    def test_transient_failure_does_not_disable(self):
        r = FakeVk(ConnectionError("valkey went away"))
        exec_events._publish(r, "ch", {"e": 1})
        exec_events._publish(r, "ch", {"e": 2})

        self.assertEqual(r.xadd_calls, 2, "transient failures stay retried")
        self.assertFalse(vk_common.streams_dead())

    def test_position_sink_skips_xadd_once_dead(self):
        vk_common._STREAMS_DEAD = True
        r = FakeVk(ResponseError("unknown command 'XADD'"))
        sink = VkPositionSyncSink(r)

        sink.publish(_Snapshot())

        self.assertEqual(len(r.setex_calls), 1, "the snapshot key must still be written")
        self.assertEqual(r.xadd_calls, 0)


class DownloadNoopTest(unittest.TestCase):
    def _handlers(self):
        from test_vk_rpc import FakeMarketData, FakePositionProvider
        from bigqmt_signal_trader.vk_rpc import BigQmtRpcHandlers
        return BigQmtRpcHandlers(
            account_id="acct", market_data=FakeMarketData(),
            position_provider=FakePositionProvider())

    def test_download_holiday_data_is_an_honest_noop(self):
        result = self._handlers().handle("download_holiday_data", {})
        self.assertTrue(result["ok"])
        self.assertFalse(result["downloaded"])
        self.assertIn("calendar", result["note"])

    def test_download_his_st_data_is_an_honest_noop(self):
        result = self._handlers().handle("download_his_st_data", {})
        self.assertTrue(result["ok"])
        self.assertFalse(result["downloaded"])

    def test_both_methods_whitelisted_and_on_the_client(self):
        from bigqmt_signal_trader.vk_rpc import READ_METHODS, MARKET_DATA_METHODS
        from bigqmt_signal_trader import xtquant_compat
        for method in ("download_holiday_data", "download_his_st_data"):
            self.assertIn(method, READ_METHODS)
            self.assertIn(method, MARKET_DATA_METHODS)
            self.assertTrue(
                hasattr(xtquant_compat.BigQmtXtData, method),
                "client missing %s" % method)


if __name__ == "__main__":
    unittest.main()
