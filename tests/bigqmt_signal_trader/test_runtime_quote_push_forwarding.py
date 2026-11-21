# coding: utf-8
"""The QMT runtime entry forwards explicit quote-push configuration."""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_valkey_rpc_runtime as runtime


class RuntimeQuotePushForwardingTest(unittest.TestCase):
    def setUp(self):
        self._had_quote_push_config = hasattr(runtime, "QUOTE_PUSH_CONFIG")
        self._saved_quote_push_config = getattr(runtime, "QUOTE_PUSH_CONFIG", None)
        self._saved_background_threads_explicit = runtime.RPC_BACKGROUND_THREADS_EXPLICIT
        self._saved_configure = runtime.configure
        self._saved_set_account_id = runtime.set_account_id
        self._captured = {}
        runtime.QUOTE_PUSH_CONFIG = {}
        runtime.configure = lambda **kwargs: self._captured.update(kwargs)
        runtime.set_account_id = lambda account_id: None

    def tearDown(self):
        if self._had_quote_push_config:
            runtime.QUOTE_PUSH_CONFIG = self._saved_quote_push_config
        else:
            del runtime.QUOTE_PUSH_CONFIG
        runtime.RPC_BACKGROUND_THREADS_EXPLICIT = self._saved_background_threads_explicit
        runtime.configure = self._saved_configure
        runtime.set_account_id = self._saved_set_account_id




if __name__ == "__main__":
    unittest.main()
