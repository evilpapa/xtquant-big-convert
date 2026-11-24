"""Adjust-phase helpers must reuse one VK client, not build one per tick.

Found in a live QMT panel log: 31 occurrences in one session of

    Exception ignored in: <object repr() failed>
      File "...valkey/client.py", line 899, in __del__     self.close()
      File "...valkey/client.py", line 902, in close       conn = self.connection
    AttributeError: 'Valkey' object has no attribute 'connection'

That AttributeError is the symptom, not the cause: valkey-py's __del__ runs on a
client whose construction never finished, so `connection` was never assigned.
The cause is a fresh client per adjust tick -- at a 100ms interval, ten per
second, each with its own connection pool.

It is easy to miss because Python swallows the traceback ("Exception ignored
in") and it never reaches a log this package writes. Only the QMT panel shows
it, which is also why it survived so long.

_exec_event_vk already caches for exactly this reason. _pump_download_jobs
was missed. These tests pin both, and would catch a third helper repeating it.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader.adapters import vk_common


class _FakeVk(object):
    """Answers anything; these tests count constructions, not calls."""

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class VkClientReuseTest(unittest.TestCase):
    def setUp(self):
        self._real_build = vk_common.build_vk_client
        self.built = []

        def spy(config=None):
            self.built.append(dict(config or {}))
            return _FakeVk()

        vk_common.build_vk_client = spy
        # No RPC service -> the zmq-transport case, where these helpers have to
        # build their own client. With VK transport they borrow the service's.
        self._real_service = strategy._rpc_service
        strategy._rpc_service = None
        strategy._exec_event_vk_client = None

    def tearDown(self):
        vk_common.build_vk_client = self._real_build
        strategy._rpc_service = self._real_service
        strategy._exec_event_vk_client = None

    def _config(self):
        return {
            "account_id": "acct",
            "valkey": {"host": "127.0.0.1", "port": 6379, "db": 5},
            "download_jobs": {"enabled": True},
        }

    def test_download_pump_builds_one_client_across_many_ticks(self):
        config = self._config()
        for _ in range(20):
            try:
                strategy._pump_download_jobs(None, config)
            except Exception:
                pass  # the job pump itself may fail; only construction count matters

        self.assertEqual(len(self.built), 1,
                         "built %d clients over 20 ticks" % len(self.built))

    def test_exec_event_helper_still_caches(self):
        config = self._config()
        for _ in range(20):
            strategy._exec_event_vk(config)

        self.assertEqual(len(self.built), 1)

    def test_both_helpers_share_the_same_client(self):
        """Two cached clients would still be one pool too many."""
        config = self._config()
        strategy._pump_download_jobs(None, config)
        strategy._exec_event_vk(config)

        self.assertEqual(len(self.built), 1)

    def test_the_client_uses_the_configured_vk_block(self):
        config = self._config()
        config["valkey"] = {"host": "10.0.0.5", "port": 6380, "db": 7}
        strategy._pump_download_jobs(None, config)

        self.assertEqual(self.built[0]["host"], "10.0.0.5")
        self.assertEqual(self.built[0]["port"], 6380)

    def test_missing_vk_config_skips_without_building(self):
        result = strategy._pump_download_jobs(
            None, {"account_id": "acct", "download_jobs": {"enabled": True}})

        self.assertIsNone(result)
        self.assertEqual(self.built, [])


    def test_service_client_is_preferred_over_building_one(self):
        """With the VK transport the service already holds a client."""
        class _Service(object):
            vk = _FakeVk()
            handlers = None

        strategy._rpc_service = _Service()
        try:
            self.assertIs(strategy._exec_event_vk(self._config()), _Service.vk)
            for _ in range(5):
                try:
                    strategy._pump_download_jobs(None, self._config())
                except Exception:
                    pass
        finally:
            strategy._rpc_service = None

        self.assertEqual(self.built, [], "built a client while the service had one")


if __name__ == "__main__":
    unittest.main()
