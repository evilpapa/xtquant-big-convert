import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader_strategy import _is_vk_transport, _resolve_background_threads
from bigqmt_signal_trader.transports.factory import build_transport, transport_supports_drain


class RemovedTransportTest(unittest.TestCase):
    def test_removed_transport_fails_with_clear_error(self):
        with self.assertRaisesRegex(ValueError, "unknown rpc transport"):
            build_transport("zmq")

    def test_pipe_is_removed_and_has_no_drain(self):
        with self.assertRaisesRegex(ValueError, "unknown rpc transport"):
            build_transport("pipe")
        self.assertFalse(transport_supports_drain("pipe"))


class BackgroundThreadResolutionTest(unittest.TestCase):
    def test_vk_transport_keeps_configured_value(self):
        # VK (default) honors the configured flag — adjust-drain path when off.
        self.assertFalse(_resolve_background_threads("valkey", False))
        self.assertTrue(_resolve_background_threads("valkey", True))
        self.assertFalse(_resolve_background_threads("", False))
        self.assertFalse(_resolve_background_threads("default", False))
        self.assertFalse(_resolve_background_threads(None, False))


    def test_drainless_transports_ignore_the_override(self):
        # shm has no drain_request_queue: without the receiver thread requests
        # would never be picked up, so even an explicit False cannot turn it
        # off here.
        self.assertTrue(_resolve_background_threads("shm", None))
        self.assertTrue(_resolve_background_threads("shm", False))
        self.assertTrue(_resolve_background_threads("shm", True))


if __name__ == "__main__":
    unittest.main()
