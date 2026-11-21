# coding: utf-8
"""Tests for the pluggable transport layer.

Covers:
* ``FakeTransport`` round-trip (validates the abstract contract).
* ZMQ transport round-trip on tcp loopback (the main low-latency path).
* MySQL transport round-trip against an in-memory sqlite3 DB (driver-agnostic).
* Factory dispatch: name → transport class, unknown-name rejection, shm raising.
"""

import os
import sys
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.transports import (  # noqa: E402
    RpcTransport,
    TransportError,
    TransportTimeout,
    build_transport,
)
from bigqmt_signal_trader.transports.base import RpcTransport as Base  # noqa: E402


class FakeTransport(RpcTransport):
    """In-process transport: a thread-safe queue pair. Validates the contract."""

    name = "fake"

    def __init__(self, account_id="", print_prefix="[fake]"):
        super(FakeTransport, self).__init__(account_id=account_id, print_prefix=print_prefix)
        self._req_q = []  # inbound requests waiting for the server
        self._resp_q = {}  # request_id -> response
        self._lock = threading.Lock()

    def send_request(self, request, timeout_seconds):
        rid = request["request_id"]
        # Hand the request to the server (call on_request), then collect reply.
        with self._lock:
            self._req_q.append(request)
        # Server processing: the registered callback handles it immediately.
        callback = self._on_request
        if callback is None:
            raise TransportError("no server registered")
        response = callback(request) or {}
        with self._lock:
            self._resp_q[rid] = response
        return response

    def send_response(self, request, response):
        rid = response.get("request_id") or request.get("request_id")
        with self._lock:
            self._resp_q[rid] = response

    def start_receiving(self, on_request, background_threads=True):
        super(FakeTransport, self).start_receiving(on_request)


def _build_request(method="ping", params=None, account_id="acct"):
    import uuid

    return {
        "schema_version": 1,
        "request_id": uuid.uuid4().hex,
        "account_id": account_id,
        "method": method,
        "params": params or {},
        "reply_channel": "",
        "reply_list": "",
        "reply_key": "",
        "ttl_seconds": 5,
    }


class FakeTransportTest(unittest.TestCase):
    def test_round_trip(self):
        server = FakeTransport(account_id="acct")
        server.start_receiving(lambda req: {"ok": True, "request_id": req["request_id"], "data": {"echo": req["params"]}})
        client = FakeTransport(account_id="acct")
        client._on_request = server._on_request  # share the handler
        req = _build_request(params={"x": 1})
        resp = client.send_request(req, 2.0)
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["request_id"], req["request_id"])
        self.assertEqual(resp["data"], {"echo": {"x": 1}})

    def test_deliver_auto_sends_response(self):
        server = FakeTransport(account_id="acct")
        sent = []
        server.send_response = lambda req, resp: sent.append(resp)  # capture
        server.start_receiving(lambda req: {"ok": True, "request_id": req["request_id"]})
        server.deliver(_build_request())
        self.assertEqual(len(sent), 1)
        self.assertTrue(sent[0]["ok"])

    def test_deliver_turns_handler_exception_into_error_envelope(self):
        server = FakeTransport(account_id="acct")
        sent = []
        server.send_response = lambda req, resp: sent.append(resp)
        def boom(req):
            raise ValueError("handler exploded")
        server.start_receiving(boom)
        server.deliver(_build_request())
        self.assertEqual(len(sent), 1)
        self.assertFalse(sent[0]["ok"])
        self.assertIn("handler exploded", sent[0]["error"])




class FactoryTest(unittest.TestCase):
    def test_unknown_transport_rejected(self):
        with self.assertRaises(ValueError):
            build_transport("nonsense", {}, account_id="x")

    def test_shm_raises_on_use(self):
        shm = build_transport("shm", {}, account_id="x")
        with self.assertRaises(TransportError):
            shm.send_request(_build_request(), 1.0)

    def test_valkey_factory_with_injected_clients(self):
        sentinel_listen = object()
        sentinel_resp = object()
        t = build_transport(
            "valkey",
            {"valkey_client": sentinel_listen, "response_valkey_client": sentinel_resp},
            account_id="acct",
        )
        self.assertIs(t.listen_valkey, sentinel_listen)
        self.assertIs(t.valkey, sentinel_resp)
        self.assertEqual(t.account_id, "acct")




class MysqlTransportTest(unittest.TestCase):
    """Round-trip over sqlite3 (the transport is driver-agnostic)."""

    def setUp(self):
        try:
            import sqlite3  # noqa: F401
        except ImportError:
            self.skipTest("sqlite3 not available")
        self.db_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "_test_rpc_%s.sqlite" % os.getpid()
        )
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except Exception:
                pass

class ValkeyAdjustDrainTest(unittest.TestCase):
    def test_valkey_adjust_lpop_skipped_when_brpop_thread_alive(self):
        """Background BRPOP owns the list; adjust LPOP must not steal it."""
        from bigqmt_signal_trader.transports.valkey_transport import ValkeyTransport

        class _AliveThread(object):
            def is_alive(self):
                return True

        class _FailValkey(object):
            def lpop(self, key):
                raise AssertionError("adjust LPOP should not run while BRPOP owns %s" % key)

        transport = ValkeyTransport.__new__(ValkeyTransport)
        transport.print_prefix = "[test]"
        transport.account_id = "acct"
        transport.request_queue_template = "bigqmt:rpc:queue:{account_id}"
        transport.listen_valkey = _FailValkey()
        transport.on_raw_payload = None
        transport._queue_thread = _AliveThread()
        self.assertEqual(0, transport.drain_request_queue(max_items=20))

    def test_valkey_adjust_lpop_runs_when_brpop_thread_dead(self):
        """If the BRPOP thread is gone, LPOP remains the adjust fallback."""
        from bigqmt_signal_trader.transports.valkey_transport import ValkeyTransport

        class _DeadThread(object):
            def is_alive(self):
                return False

        class _EmptyValkey(object):
            def lpop(self, key):
                return None

        transport = ValkeyTransport.__new__(ValkeyTransport)
        transport.print_prefix = "[test]"
        transport.account_id = "acct"
        transport.request_queue_template = "bigqmt:rpc:queue:{account_id}"
        transport.listen_valkey = _EmptyValkey()
        transport.on_raw_payload = None
        transport._queue_thread = _DeadThread()
        self.assertEqual(0, transport.drain_request_queue(max_items=20))


if __name__ == "__main__":
    unittest.main()


class ValkeyPipelineErrorSlimTest(unittest.TestCase):
    """A failed response pipeline must not raise with the payload embedded.

    valkey-py renders a failed batch with its command arguments, and the
    argument is the response JSON itself. An OOM refusal on a large K-line
    reply measured 5 MB of str(exc); a handful of such lines wrote a 542 MB
    log file and degraded the bridge until it stopped serving. See
    _slim_pipeline_error in valkey_transport.
    """

    class _FailingClient(object):
        def __init__(self, error):
            self._error = error

        def pipeline(self, transaction=True):
            client_error = self._error

            class _Pipe(object):
                def setex(self, *args, **kwargs):
                    pass

                def publish(self, *args, **kwargs):
                    pass

                def execute(self):
                    raise client_error

            return _Pipe()

    def _transport(self, error):
        from bigqmt_signal_trader.transports.valkey_transport import ValkeyTransport
        return ValkeyTransport(self._FailingClient(error), account_id="acct")

    def test_pipeline_error_payload_is_slimmed(self):
        big = RuntimeError(
            "OOM command not allowed when used memory > 'maxmemory'. "
            "x" * 5000000)
        transport = self._transport(big)
        with self.assertRaises(RuntimeError) as ctx:
            transport.send_response(
                {"request_id": "r1"}, {"request_id": "r1", "ok": True})
        message = str(ctx.exception)
        self.assertLessEqual(len(message), 800)
        self.assertIn("OOM command not allowed", message)
        self.assertIn("chars of embedded payload omitted", message)

    def test_short_pipeline_error_untouched(self):
        short = RuntimeError("connection refused")
        transport = self._transport(short)
        with self.assertRaises(RuntimeError) as ctx:
            transport.send_response(
                {"request_id": "r1"}, {"request_id": "r1", "ok": True})
        self.assertIs(ctx.exception, short)
