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

    def test_pipe_factory_is_lazy_and_drains_on_adjust(self):
        transport = build_transport("pipe", {"pipe": {"name": "test_pipe"}})
        self.assertEqual(transport.port, r"\\.\pipe\test_pipe")
        self.assertTrue(transport_supports_drain("pipe"))
        self.assertFalse(_resolve_background_threads("pipe", True))

    def test_copydata_factory_is_lazy_and_drains_on_adjust(self):
        transport = build_transport("copydata", {"copydata": {"window_name": "test_window"}})
        self.assertEqual(transport.port, "BigQmtCopyData:test_window")
        self.assertTrue(transport_supports_drain("copydata"))
        self.assertFalse(_resolve_background_threads("copydata", True))


class BackgroundThreadResolutionTest(unittest.TestCase):
    def test_vk_transport_keeps_configured_value(self):
        # VK (default) honors the configured flag — adjust-drain path when off.
        self.assertFalse(_resolve_background_threads("valkey", False))
        self.assertTrue(_resolve_background_threads("valkey", True))
        self.assertFalse(_resolve_background_threads("", False))
        self.assertFalse(_resolve_background_threads("default", False))
        self.assertFalse(_resolve_background_threads(None, False))


    def test_shm_always_drains_on_adjust(self):
        self.assertTrue(transport_supports_drain("shm"))
        for configured in (None, False, True):
            with self.subTest(configured=configured):
                self.assertFalse(_resolve_background_threads("shm", configured))


if __name__ == "__main__":
    unittest.main()
