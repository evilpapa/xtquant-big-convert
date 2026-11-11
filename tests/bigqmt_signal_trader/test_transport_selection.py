import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader_strategy import _is_redis_transport, _resolve_background_threads
from bigqmt_signal_trader.transports.factory import build_transport


class RemovedTransportTest(unittest.TestCase):
    def test_removed_transport_fails_with_clear_error(self):
        with self.assertRaisesRegex(ValueError, "unknown rpc transport"):
            build_transport("zmq")


class BackgroundThreadResolutionTest(unittest.TestCase):
    def test_redis_transport_keeps_configured_value(self):
        # Redis (default) honors the configured flag — adjust-drain path when off.
        self.assertFalse(_resolve_background_threads("redis", False))
        self.assertTrue(_resolve_background_threads("redis", True))
        self.assertFalse(_resolve_background_threads("", False))
        self.assertFalse(_resolve_background_threads("default", False))
        self.assertFalse(_resolve_background_threads(None, False))



    def test_mysql_explicit_false_opts_into_adjust_drain(self):
        # mysql also implements drain_request_queue, so the override stands.
        self.assertFalse(_resolve_background_threads("mysql", None))
        self.assertFalse(_resolve_background_threads("mysql", False))
        self.assertTrue(_resolve_background_threads("mysql", True))

    def test_drainless_transports_ignore_the_override(self):
        # shm has no drain_request_queue: without the receiver thread requests
        # would never be picked up, so even an explicit False cannot turn it
        # off here.
        self.assertTrue(_resolve_background_threads("shm", None))
        self.assertTrue(_resolve_background_threads("shm", False))
        self.assertTrue(_resolve_background_threads("shm", True))



if __name__ == "__main__":
    unittest.main()
