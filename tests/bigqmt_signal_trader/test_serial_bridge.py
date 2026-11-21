import importlib
import json
import logging
import os
import sys
import threading
import time
import types
from collections import defaultdict, deque
from types import SimpleNamespace

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import order_identity_journal, serial_valkey_bridge
from bigqmt_signal_trader.exec_events import order_channel, publish_exec_event
from bigqmt_signal_trader.quote_push_channel import decode_push_payload
from bigqmt_signal_trader.valkey_rpc import call_valkey_rpc, encode_rpc_request_payload
from bigqmt_signal_trader.serial_valkey_bridge import SerialValkeyBridge
from bigqmt_signal_trader.transports import serial_transport
from bigqmt_signal_trader.transports.factory import build_transport, transport_supports_drain


ACCOUNT = "serial-account"
REQUEST_QUEUE = "bigqmt:rpc:queue:" + ACCOUNT
REPLY_PREFIX = "bigqmt:rpc:resp:" + ACCOUNT + ":"


class Port:
    def __init__(self):
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.closed = False
        self.peer = None

    @property
    def in_waiting(self):
        with self.lock:
            return len(self.buffer)

    def read(self, size):
        with self.lock:
            data = bytes(self.buffer[:size])
            del self.buffer[:size]
        return data

    def write(self, data):
        count = min(37, len(data))
        with self.peer.lock:
            self.peer.buffer.extend(data[:count])
        return count

    def close(self):
        self.closed = True


class MemoryValkey:
    def __init__(self):
        self.lists = defaultdict(deque)
        self.keys = {}
        self.published = defaultdict(list)
        self.streams = defaultdict(list)
        self.ttls = {}
        self.pump = lambda: None

    def lpop(self, key):
        if self.lists[key]:
            return self.lists[key].popleft()

    def rpush(self, key, value):
        self.lists[key].append(value)

    def blpop(self, key, timeout):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            self.pump()
            value = self.lpop(key)
            if value is not None:
                return key, value
            time.sleep(0.001)

    def expire(self, key, ttl):
        self.ttls[key] = ttl

    def setex(self, key, ttl, value):
        self.keys[key] = value
        self.expire(key, ttl)

    def get(self, key):
        return self.keys.get(key)

    def publish(self, channel, value):
        self.published[channel].append(value)
        return 1

    def xadd(self, key, data, **kwargs):
        self.streams[key].append(data)

    def delete(self, key):
        self.lists.pop(key, None)


def request(request_id, method="ping", sent_at=None, **extra):
    return encode_rpc_request_payload(dict(extra, request_id=request_id, account_id=ACCOUNT,
                                           method=method, timeout_seconds=3,
                                           _sent_at=time.time() if sent_at is None else sent_at))


def pump_until(pump, ready, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end and not ready():
        pump()
        time.sleep(0.001)
    assert ready()


@pytest.fixture
def duplex():
    qmt, bridge = Port(), Port()
    qmt.peer, bridge.peer = bridge, qmt
    return qmt, bridge


def test_framing_checks_integrity_and_recovers_after_oversized_or_partial_lines(duplex):
    outgoing, incoming = duplex
    writer = serial_transport.SerialFrames(outgoing)
    reader = serial_transport.SerialFrames(incoming, max_frame_bytes=1024)
    outgoing.write(b"bad\n")
    incoming.buffer.extend(b"x" * 2048)
    assert reader.read_frames(5) == []
    assert len(reader._line) == 0
    incoming.buffer.extend(b"\n")
    writer.begin({"text": "中文\n换行", "long": "z" * 100})
    writer.write_step()
    assert reader.read_frames(5) == []
    while writer.writing:
        writer.write_step()
    writer.begin({"text": "second"})
    while writer.writing:
        writer.write_step()
    assert reader.read_frames(1)[0]["text"] == "中文\n换行"
    assert reader.read_frames(1) == [{"text": "second"}]


def test_bridge_waits_for_qmt_and_refuses_stale_or_legacy_orders(duplex):
    qmt, windows = duplex
    valkey = MemoryValkey()
    bridge = SerialValkeyBridge(valkey, windows, ACCOUNT)
    valkey.rpush(REQUEST_QUEUE, request("old-order", "order_stock", time.time() - 10))
    valkey.rpush(REQUEST_QUEUE, encode_rpc_request_payload({
        "request_id": "legacy-order", "method": "order_stock", "account_id": ACCOUNT,
    }))
    assert not bridge.step()
    assert len(valkey.lists[REQUEST_QUEUE]) == 2
    assert qmt.in_waiting == 0

    wire = serial_transport.SerialFrames(qmt)
    wire.begin({"type": "hello", "account_id": ACCOUNT, "session": "current"})
    while wire.writing:
        wire.write_step()
    bridge.step()
    bridge.step()
    assert "NOT dispatched" in json.loads(valkey.get(REPLY_PREFIX + "old-order"))["error"]
    assert "timestamp" in json.loads(valkey.get(REPLY_PREFIX + "legacy-order"))["error"]
    assert qmt.in_waiting == 0


def test_serial_session_change_never_replays_an_unanswered_order(duplex):
    qmt, windows = duplex
    valkey = MemoryValkey()
    bridge = SerialValkeyBridge(valkey, windows, ACCOUNT)
    wire = serial_transport.SerialFrames(qmt)

    def hello(session):
        wire.begin({"type": "hello", "account_id": ACCOUNT, "session": session})
        while wire.writing:
            wire.write_step()

    hello("before")
    valkey.rpush(REQUEST_QUEUE, request("uncertain", "order_stock"))
    pump_until(bridge.step, lambda: "uncertain" in bridge._inflight and not bridge.wire.writing)
    assert wire.read_frames(1)[0]["session"] == "before"

    hello("after")
    valkey.rpush(REQUEST_QUEUE, request("outcome", "get_request_outcome", params={"request_id": "uncertain"}))
    pump_until(bridge.step, lambda: valkey.get(REPLY_PREFIX + "outcome") is not None)
    assert "query orders and trades" in json.loads(valkey.get(REPLY_PREFIX + "outcome"))["error"]
    assert wire.read_frames(1) == []


def test_quote_backpressure_does_not_raise_into_qmt_or_displace_order_events():
    transport = build_transport("serial", account_id=ACCOUNT)
    transport._running = True
    for _ in range(transport._quote_events.maxsize + 1):
        transport.publish("quote-test", {})
    transport.publish("exec:order", {"account_id": ACCOUNT})
    assert transport._drops == 1
    assert transport._next_outbound()["topic"] == "exec:order"


def test_runtime_forwards_the_serial_port_without_a_valkey_connection(monkeypatch):
    import bigqmt_signal_trader_valkey_rpc_runtime as runtime

    config = {}
    monkeypatch.setattr(runtime, "configure", lambda **settings: config.update(settings))
    monkeypatch.setattr(runtime, "set_account_id", lambda account_id: None)
    monkeypatch.setattr(runtime, "RPC_TRANSPORT", "serial")
    monkeypatch.setattr(runtime, "RPC_SERIAL_CONFIG", {"port": "COM20", "baudrate": 115200})
    monkeypatch.setattr(runtime, "VALKEY_ENABLED", True)
    monkeypatch.setattr(runtime, "SCHEDULE_ADJUST_ENABLED", False)
    runtime._apply_config(ACCOUNT)
    assert config["rpc"]["serial"] == {"port": "COM20", "baudrate": 115200}
    assert config["valkey"] == {}
    assert config["schedule_adjust"] is True


@pytest.mark.parametrize("allow_order_methods", [False, True])
def test_existing_valkey_client_round_trip_and_execution_gate(
        duplex, monkeypatch, allow_order_methods):
    qmt, windows = duplex
    valkey = MemoryValkey()
    bridge = SerialValkeyBridge(valkey, windows, ACCOUNT)
    monkeypatch.setattr(serial_transport, "open_serial", lambda port, baudrate: qmt)
    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_events", None)
    monkeypatch.setattr(strategy, "_exec_event_valkey_client", object())
    seen = []

    def forbidden_journal(*args, **kwargs):
        pytest.fail("QMT serial RPC attempted to open a filesystem identity journal")

    monkeypatch.setattr(order_identity_journal, "OrderIdentityJournal", forbidden_journal)

    def submit(order):
        seen.append((threading.current_thread(), order.remark))
        return SimpleNamespace(order_sys_id="simulated")

    config = {"account_id": ACCOUNT, "valkey": {"host": "must-not-connect"}, "rpc": {
        "enabled": True, "transport": "serial", "allow_order_methods": allow_order_methods,
        "order_identity_dir": "must-not-open", "background_threads": True,
        "native_xtdata_enabled": True, "heavy_offload": False,
    }}
    context = SimpleNamespace()
    gateway = SimpleNamespace(submit=submit)
    service = strategy._build_rpc_service(context, SimpleNamespace(order_gateway=gateway), config)
    assert service.valkey is None and service.listen_valkey is None
    assert not service.background_threads and not service.handlers.market_data.native_xtdata_enabled
    assert transport_supports_drain("serial")
    assert build_transport("serial", {"serial": {"port": "COM100"}}).port == "COM100"

    def pump():
        bridge.step()
        service.drain_request_queue()
        service.drain_pending()

    valkey.pump = pump
    service.start()
    try:
        ping = call_valkey_rpc(valkey, ACCOUNT, "ping", request_id="ping-1")
        assert ping["data"]["pong"] is True
        assert valkey.ttls[REPLY_PREFIX + "ping-1"] == 60
        assert valkey.published[REPLY_PREFIX + "ping-1"]

        params = {"stock_code": "000001.SZ", "action": "BUY", "volume": 100,
                  "remark": "simulated", "strategy_name": "serial-test",
                  "wait_settlement": False}
        response = call_valkey_rpc(valkey, ACCOUNT, "order_stock", params, request_id="order-1")
        assert response["ok"] == allow_order_methods
        if allow_order_methods:
            assert response["data"]["order_sys_id"] == "simulated"
            assert call_valkey_rpc(valkey, ACCOUNT, "order_stock", params,
                                  request_id="order-1")["ok"] is True
            pump_until(pump, lambda: service._duplicate_order_requests == 1)
            assert seen == [(threading.current_thread(), "simulated")]
        else:
            assert "not allowed" in response["error"]
            assert seen == []

        probe = call_valkey_rpc(valkey, ACCOUNT, "probe_order_identity",
                                {"remark": "simulated"}, request_id="identity-1")
        assert probe["ok"] is True
        assert probe["data"]["local_persistence_enabled"] is False
        assert probe["data"]["local_hit"] is allow_order_methods
        if allow_order_methods:
            assert probe["data"]["local_strategy_name"] == "serial-test"

        event = {"account_id": ACCOUNT, "event_type": "order", "order_sys_id": "simulated"}
        publish_exec_event(service.handlers._exec_event_sink(), ACCOUNT, event)
        service.handlers.quote_subscription_manager._on_push_publisher(
            "quote-test", {"000001.SZ": {"lastPrice": 10}})
        exec_channel = order_channel(ACCOUNT)
        quote_channel = "bigqmt:quote_push:" + ACCOUNT + ":quote-test"
        pump_until(pump, lambda: valkey.published[exec_channel] and valkey.published[quote_channel])
        assert json.loads(valkey.published[exec_channel][0]) == event
        assert decode_push_payload(valkey.published[quote_channel][0])["data"]["000001.SZ"]["lastPrice"] == 10
    finally:
        service.stop()
    assert qmt.closed


def test_bridge_log_helpers_hide_credentials_and_follow_causes():
    assert serial_valkey_bridge._valkey_endpoint(
        "valkey://user:secret@10.0.0.1:6380/9") == "10.0.0.1:6380/9"
    assert serial_valkey_bridge._valkey_endpoint("valkey://h") == "h:6379/0"
    assert serial_valkey_bridge._valkey_endpoint("not a url") == "the configured Valkey URL"
    assert serial_valkey_bridge._valkey_endpoint(
        "valkey://h:bad/1") == "the configured Valkey URL"

    try:
        try:
            raise ValueError("inner")
        except ValueError as exc:
            raise TimeoutError("outer") from exc
    except TimeoutError as exc:
        text = serial_valkey_bridge._describe(exc)
    assert "TimeoutError: outer" in text
    assert "ValueError: inner" in text


def test_bridge_warns_and_stops_without_an_account(monkeypatch, caplog):
    monkeypatch.setenv("BIGQMT_ACCOUNT_ID", "  ")
    monkeypatch.setitem(sys.modules, "valkey", None)

    with caplog.at_level(logging.WARNING):
        assert serial_valkey_bridge.main([]) == 2

    assert "BIGQMT_ACCOUNT_ID" in caplog.text


@pytest.mark.parametrize("scheme", ["valkey", "redis"])
def test_bridge_startup_names_the_side_that_failed(monkeypatch, caplog, scheme):
    class FakeValkey:
        @classmethod
        def from_url(cls, url, **kwargs):
            assert url == "valkey://user:secret@127.0.0.1:6399/5"
            return cls()

        def ping(self):
            raise TimeoutError("timed out")

        def close(self):
            pass

    package = types.ModuleType("valkey")
    package.Valkey = FakeValkey
    backoff = types.ModuleType("valkey.backoff")
    backoff.NoBackoff = lambda: None
    retry = types.ModuleType("valkey.retry")
    retry.Retry = lambda *args, **kwargs: None
    package.backoff = backoff
    package.retry = retry
    monkeypatch.setitem(sys.modules, "valkey", package)
    monkeypatch.setitem(sys.modules, "valkey.backoff", backoff)
    monkeypatch.setitem(sys.modules, "valkey.retry", retry)
    monkeypatch.setenv("BIGQMT_ACCOUNT_ID", " acct ")

    with caplog.at_level(logging.ERROR):
        code = serial_valkey_bridge.main([
            "--valkey-url", scheme + "://user:secret@127.0.0.1:6399/5",
        ])

    assert code == 1
    assert "Cannot reach Valkey at 127.0.0.1:6399/5" in caplog.text
    assert "secret" not in caplog.text
