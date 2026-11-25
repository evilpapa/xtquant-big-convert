import builtins
import importlib
import io
import json
import logging
import os
import socket
import sys
import threading
import time
import types
from collections import defaultdict, deque
from types import SimpleNamespace
from unittest.mock import patch

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import order_identity_journal, serial_bridge_features, serial_vk_bridge
from bigqmt_signal_trader.adapters import market_bigqmt, vk_common
from bigqmt_signal_trader.download_jobs import read_download_status
from bigqmt_signal_trader.exec_events import order_channel, publish_exec_event
from bigqmt_signal_trader.full_tick_cache import read_full_tick_cache, request_full_tick_cache
from bigqmt_signal_trader.quote_push_channel import decode_push_payload
from bigqmt_signal_trader.vk_rpc import call_vk_rpc, encode_rpc_request_payload
from bigqmt_signal_trader.serial_vk_bridge import SerialVkBridge
from bigqmt_signal_trader.transports import serial_transport
from bigqmt_signal_trader.transports.factory import build_transport, transport_supports_drain
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


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


class MemoryVk:
    def __init__(self):
        self.lists = defaultdict(deque)
        self.hashes = defaultdict(dict)
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

    def lindex(self, key, index):
        return self.lists[key][index] if self.lists[key] else None

    def lmove(self, source, destination, src, dest):
        assert (src, dest) == ("LEFT", "RIGHT")
        value = self.lpop(source)
        if value is not None:
            self.rpush(destination, value)
        return value

    def hset(self, key, name, value):
        self.hashes[key][name] = value

    def hgetall(self, key):
        return self.hashes[key].copy()

    def hdel(self, key, name):
        self.hashes[key].pop(name, None)

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

    def pipeline(self, transaction=True):
        calls = []

        class Pipeline:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                pass

            def __getattr__(self, name):
                return lambda *args: calls.append((name, args))

            def execute(pipe):
                return [getattr(self, name)(*args) for name, args in calls]

        return Pipeline()


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
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    vk.rpush(REQUEST_QUEUE, request("old-order", "order_stock", time.time() - 10))
    vk.rpush(REQUEST_QUEUE, encode_rpc_request_payload({
        "request_id": "legacy-order", "method": "order_stock", "account_id": ACCOUNT,
    }))
    assert not bridge.step()
    assert len(vk.lists[REQUEST_QUEUE]) == 2
    assert qmt.in_waiting == 0

    wire = serial_transport.SerialFrames(qmt)
    wire.begin({"type": "hello", "account_id": ACCOUNT, "session": "current"})
    while wire.writing:
        wire.write_step()
    bridge.step()
    bridge.step()
    assert "NOT dispatched" in json.loads(vk.get(REPLY_PREFIX + "old-order"))["error"]
    assert "timestamp" in json.loads(vk.get(REPLY_PREFIX + "legacy-order"))["error"]
    assert qmt.in_waiting == 0


def test_bridge_logs_each_received_rpc_request(duplex, caplog):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    wire = serial_transport.SerialFrames(qmt)
    wire.begin({"type": "hello", "account_id": ACCOUNT, "session": "logging"})
    while wire.writing:
        wire.write_step()
    vk.rpush(REQUEST_QUEUE, request("logged-ping", "ping", params={"probe": "x"}))
    vk.rpush(REQUEST_QUEUE, request("logged-stale", "order_stock", sent_at=time.time() - 10))

    with caplog.at_level(logging.INFO, logger=serial_vk_bridge._log.name):
        pump_until(bridge.step,
                   lambda: "logged-ping" in caplog.text and "logged-stale" in caplog.text)

    assert "method=ping params={'probe': 'x'}" in caplog.text
    assert "logged-stale" in caplog.text and "method=order_stock" in caplog.text


def test_bridge_logs_invalid_payload_preview_without_flooding(duplex, caplog):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    wire = serial_transport.SerialFrames(qmt)
    wire.begin({"type": "hello", "account_id": ACCOUNT, "session": "invalid"})
    while wire.writing:
        wire.write_step()
    vk.rpush(REQUEST_QUEUE, "not-json-" + "z" * 1000)

    with caplog.at_level(logging.WARNING, logger=serial_vk_bridge._log.name):
        pump_until(bridge.step, lambda: "已丢弃格式无效" in caplog.text)

    assert "not-json-" in caplog.text
    assert "...(truncated)" in caplog.text
    assert "z" * 500 not in caplog.text


def test_serial_session_change_never_replays_an_unanswered_order(duplex):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    wire = serial_transport.SerialFrames(qmt)

    def hello(session):
        wire.begin({"type": "hello", "account_id": ACCOUNT, "session": session})
        while wire.writing:
            wire.write_step()

    hello("before")
    vk.rpush(REQUEST_QUEUE, request("uncertain", "order_stock"))
    pump_until(bridge.step, lambda: "uncertain" in bridge._inflight and not bridge.wire.writing)
    assert wire.read_frames(1)[0]["session"] == "before"

    hello("after")
    vk.rpush(REQUEST_QUEUE, request("outcome", "get_request_outcome", params={"request_id": "uncertain"}))
    pump_until(bridge.step, lambda: vk.get(REPLY_PREFIX + "outcome") is not None)
    assert "query orders and trades" in json.loads(vk.get(REPLY_PREFIX + "outcome"))["error"]
    assert wire.read_frames(1) == []


def test_qmt_serial_recovers_after_a_partial_write_timeout(duplex, monkeypatch, caplog):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    transport = serial_transport.SerialTransport(account_id=ACCOUNT)
    handled = []
    io_attempts = []
    timed_out = threading.Event()
    original_write = qmt.write

    class SerialTimeoutException(OSError):
        pass

    def write(data):
        if b'"request_id":"lost-response"' in bytes(data):
            original_write(data)
            timed_out.set()
            raise SerialTimeoutException("Write timeout")
        return original_write(data)

    monkeypatch.setitem(sys.modules, "serial", SimpleNamespace(
        Serial=lambda **options: qmt, SerialTimeoutException=SerialTimeoutException))
    monkeypatch.setattr(qmt, "write", write)

    def respond(incoming):
        handled.append(incoming["request_id"])
        transport.send_response(incoming, {
            "request_id": incoming["request_id"], "account_id": ACCOUNT,
            "method": incoming["method"], "ok": True, "data": {"pong": True},
        })

    def pump():
        bridge.step()
        transport.drain_request_queue()

    def forbidden_io(*args, **kwargs):
        io_attempts.append(args)
        raise OSError("unexpected external I/O during serial recovery")

    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", forbidden_io)
        guard.setattr(io, "open", forbidden_io)
        guard.setattr(os, "open", forbidden_io)
        guard.setattr(socket, "socket", forbidden_io)
        transport.start_receiving(respond)
        try:
            vk.rpush(REQUEST_QUEUE, request("lost-response", "get_market_data_ex"))
            pump_until(pump, timed_out.is_set)
            session = bridge._session
            bridge._last_hello -= 4
            vk.rpush(REQUEST_QUEUE, request("next-ping"))
            pump_until(pump, lambda: vk.get(REPLY_PREFIX + "next-ping") is not None)
            assert not qmt.closed
        finally:
            transport.stop()

    assert bridge._session == session
    assert json.loads(vk.get(REPLY_PREFIX + "next-ping"))["data"] == {"pong": True}
    assert vk.get(REPLY_PREFIX + "lost-response") is None
    assert handled == ["lost-response", "next-ping"]
    assert "写超时" in caplog.text
    assert not io_attempts


def test_quote_backpressure_does_not_raise_into_qmt_or_displace_order_events():
    transport = build_transport("serial", account_id=ACCOUNT)
    transport._running = True
    for _ in range(transport._quote_events.maxsize + 1):
        transport.publish("quote-test", {})
    transport.publish("exec:order", {"account_id": ACCOUNT})
    assert transport._drops == 1
    assert transport._next_outbound()["topic"] == "exec:order"


def test_runtime_forwards_the_serial_port_without_a_vk_connection(monkeypatch):
    import bigqmt_signal_trader_vk_rpc_runtime as runtime

    config = {}
    monkeypatch.setattr(runtime, "configure", lambda **settings: config.update(settings))
    monkeypatch.setattr(runtime, "set_account_id", lambda account_id: None)
    monkeypatch.setattr(runtime, "RPC_TRANSPORT", "serial")
    monkeypatch.setattr(runtime, "RPC_SERIAL_CONFIG", {"port": "COM20", "baudrate": 115200})
    monkeypatch.setattr(runtime, "VK_ENABLED", True)
    monkeypatch.setattr(runtime, "SCHEDULE_ADJUST_ENABLED", False)
    runtime._apply_config(ACCOUNT)
    assert config["rpc"]["serial"] == {"port": "COM20", "baudrate": 115200}
    assert config["valkey"] == {}
    assert config["schedule_adjust"] is True


@pytest.mark.parametrize(("settings", "interval"), [
    ({}, None),
    ({"adjust_log_enabled": False, "adjust_log_interval_seconds": 2.5}, None),
    ({"adjust_log_enabled": True, "adjust_log_interval_seconds": 2.5}, 2.5),
    ({"adjust_log_enabled": True}, 10.0),
    ({"adjust_log_enabled": True, "adjust_log_interval_seconds": "nan"}, 10.0),
])
def test_qmt_adjust_cadence_log_settings(settings, interval, monkeypatch, capsys):
    import bigqmt_signal_trader_vk_rpc_runtime as runtime

    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(strategy, "time", SimpleNamespace(
        monotonic=lambda: clock.now, perf_counter=lambda: clock.now))
    monkeypatch.setattr(strategy, "_config", {})
    monkeypatch.setattr(strategy, "_account_id", "")
    monkeypatch.setattr(strategy, "_rpc_service", None)
    monkeypatch.setattr(strategy, "_adjust_logged", False)
    monkeypatch.setattr(strategy, "_reload_request", {"pending": False})
    monkeypatch.setattr(strategy, "_held_presysid_orders", {})
    monkeypatch.setattr(strategy, "_adjust_tick_stats", dict(strategy._ADJUST_TICK_STATS_DEFAULT))
    monkeypatch.setattr(strategy, "_tick_app_hist", [0] * len(strategy._tick_app_hist))
    monkeypatch.setattr(strategy, "_tick_app_max_ms", [0.0])
    monkeypatch.setattr(strategy, "tick_app", lambda *_: "tick")
    with patch.dict(vars(runtime)):
        runtime.configure_runtime_vk(dict(settings, transport="serial"))

    names = ("adjust 节拍", "[adjust_source]", "[tick_app_hist]")
    context = SimpleNamespace()
    assert strategy.adjust(context) == "tick"
    clock.now = (interval or 60.0) - 0.25
    assert strategy.handlebar(context) == "tick"
    output = capsys.readouterr().out
    assert all(name not in output for name in names)

    clock.now = interval or 60.0
    assert strategy.adjust(context) == "tick"
    output = capsys.readouterr().out
    if interval is None:
        assert all(name not in output for name in names)
        assert sum(strategy._tick_app_hist) == 0
    else:
        assert all(output.count(name) == 1 for name in names)
        assert "handlebar=1 timer=2" in output
        assert "over %.3gs" % interval in output
        clock.now += 0.25
        strategy.adjust(context)
        output = capsys.readouterr().out
        assert all(name not in output for name in names)


@pytest.mark.parametrize("allow_order_methods", [False, True])
def test_existing_vk_client_round_trip_and_execution_gate(
        duplex, monkeypatch, allow_order_methods):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    monkeypatch.setattr(serial_transport, "open_serial", lambda port, baudrate: qmt)
    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_events", None)
    monkeypatch.setattr(strategy, "_exec_event_vk_client", object())
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
    assert service.vk is None and service.listen_vk is None
    assert not service.background_threads and not service.handlers.market_data.native_xtdata_enabled
    assert transport_supports_drain("serial")
    assert build_transport("serial", {"serial": {"port": "COM100"}}).port == "COM100"

    def pump():
        bridge.step()
        service.drain_request_queue()
        service.drain_pending()

    vk.pump = pump
    service.start()
    try:
        ping = call_vk_rpc(vk, ACCOUNT, "ping", request_id="ping-1")
        assert ping["data"]["pong"] is True
        assert vk.ttls[REPLY_PREFIX + "ping-1"] == 60
        assert vk.published[REPLY_PREFIX + "ping-1"]

        params = {"stock_code": "000001.SZ", "action": "BUY", "volume": 100,
                  "remark": "simulated", "strategy_name": "serial-test",
                  "wait_settlement": False}
        response = call_vk_rpc(vk, ACCOUNT, "order_stock", params, request_id="order-1")
        assert response["ok"] == allow_order_methods
        if allow_order_methods:
            assert response["data"]["order_sys_id"] == "simulated"
            assert call_vk_rpc(vk, ACCOUNT, "order_stock", params,
                                  request_id="order-1")["ok"] is True
            pump_until(pump, lambda: service._duplicate_order_requests == 1)
            assert seen == [(threading.current_thread(), "simulated")]
        else:
            assert "not allowed" in response["error"]
            assert seen == []

        probe = call_vk_rpc(vk, ACCOUNT, "probe_order_identity",
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
        pump_until(pump, lambda: vk.published[exec_channel] and vk.published[quote_channel])
        assert json.loads(vk.published[exec_channel][0]) == event
        assert decode_push_payload(vk.published[quote_channel][0])["data"]["000001.SZ"]["lastPrice"] == 10
    finally:
        service.stop()
    assert qmt.closed


def test_serial_bridge_refreshes_cache_and_verifies_downloads_through_qmt(duplex, monkeypatch):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    monkeypatch.setattr(serial_transport, "open_serial", lambda port, baudrate: qmt)
    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_events", None)
    calls = []
    requested, readable = set(), set()
    fail_io = lambda *args, **kwargs: pytest.fail("QMT attempted forbidden external I/O")
    monkeypatch.setattr(vk_common, "build_vk_client", fail_io)
    monkeypatch.setattr(market_bigqmt, "_load_native_xtdata", fail_io)

    def download(codes, period, start, end, callback):
        calls.append(("download", codes, threading.current_thread()))
        requested.update(codes)
        return True

    def local_data(stock_code, **kwargs):
        calls.append(("read", stock_code, threading.current_thread()))
        return {code: [{"stime": "20261120150000" if code in readable else "19700101000000", "close": 10}]
                for code in stock_code}

    def ticks(codes):
        calls.append(("ticks", codes, threading.current_thread()))
        return {code: {"lastPrice": 10} for code in codes}

    config = {"account_id": ACCOUNT, "valkey": {"host": "must-not-connect"}, "rpc": {
        "enabled": True, "transport": "serial", "heavy_offload": False,
    }, "qmt_api": {"download_history_data2": download},
        "download_jobs": {"enabled": True}, "full_tick_cache": {"enabled": True}}
    context = SimpleNamespace(get_full_tick=ticks, get_local_data=local_data, is_last_bar=lambda: False)
    monkeypatch.setattr(strategy, "_config", config)
    monkeypatch.setattr(strategy, "_account_id", ACCOUNT)

    def qmt_only(action):
        with monkeypatch.context() as guard:
            guard.setattr(builtins, "open", fail_io)
            guard.setattr(io, "open", fail_io)
            guard.setattr(os, "mkdir", fail_io)
            guard.setattr(socket, "socket", fail_io)
            return action()

    qmt_config = qmt_only(strategy._build_config)
    assert qmt_config["valkey"] == {}
    assert not qmt_config["download_jobs"]["enabled"] and not qmt_config["full_tick_cache"]["enabled"]
    service = qmt_only(lambda: strategy._build_rpc_service(context, None, qmt_config))
    monkeypatch.setattr(strategy, "_rpc_service", service)

    def pump():
        bridge.step()
        qmt_only(lambda: strategy.adjust(context))

    vk.pump = pump
    qmt_only(service.start)
    try:
        codes = ["000001.SZ"]
        request_full_tick_cache(vk, ACCOUNT, codes)
        request_full_tick_cache(vk, ACCOUNT, codes)
        pump_until(pump, lambda: read_full_tick_cache(vk, ACCOUNT, codes))
        assert read_full_tick_cache(vk, ACCOUNT, codes) == {
            "000001.SZ": {"lastPrice": 10},
        }
        assert len([entry for entry in calls if entry[0] == "ticks"]) == 1

        def call(method, params):
            response = call_vk_rpc(vk, ACCOUNT, method, params)
            assert response["ok"], response
            return response["data"]

        client = BigQmtXtData(SimpleNamespace(call=call, download_wait_seconds=3,
                                              download_poll_interval_seconds=0.05))
        job = call("submit_download_history_data2", {
            "stock_list": ["000001.SZ", "600000.SH"], "period": "1d",
            "start_time": "20261120", "end_time": "20261120", "chunk_size": 1,
        })
        assert job["state"] == "pending"
        job_id = job["job_id"]
        pump_until(pump, lambda: any(entry[0] == "read" for entry in calls))
        status = client.get_download_status(job_id)
        assert (status["state"], status["accepted"], status["done"]) == ("running", 1, 0)
        assert client.wait_download(job_id, timeout=0)["done"] == 0

        readable.update(["000001.SZ", "600000.SH"])
        waited = call("wait_download", {"job_id": job_id, "wait_seconds": 2,
                                        "poll_interval_seconds": 0.05})
        assert (waited["state"], waited["done"]) == ("done", 2)
        statuses = []
        finished = client.wait_download(job_id, callback=statuses.append)
        assert (finished["state"], finished["accepted"], finished["done"]) == ("done", 2, 2)
        assert statuses[-1] == finished
        assert requested == readable
        assert [(name, codes) for name, codes, _ in calls if name == "download"] == [
            ("download", ["000001.SZ"]), ("download", ["600000.SH"]),
        ]
        assert all(thread is threading.current_thread() for _, _, thread in calls)

        monkeypatch.setattr(serial_bridge_features, "_VERIFY_WINDOW", 0.15)
        missing = client.submit_download_history_data("000001.SZ", "1d", "20261119", "20261119")
        failed = call("wait_download", {"job_id": missing["job_id"], "wait_seconds": 2,
                                        "poll_interval_seconds": 0.05})
        assert (failed["state"], failed["accepted"], failed["done"]) == ("failed", 1, 0)
        assert "history was not readable" in failed["error"]
    finally:
        service.stop()


def test_serial_download_reports_lost_submission_and_never_replays_it(duplex):
    qmt, windows = duplex
    vk = MemoryVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT)
    wire = serial_transport.SerialFrames(qmt)

    def hello(session):
        wire.begin({"type": "hello", "account_id": ACCOUNT, "session": session})
        while wire.writing:
            wire.write_step()

    hello("before")
    raw = request("download", "submit_download_history_data", params={
        "stock_code": "000001.SZ", "period": "1d",
    })
    vk.rpush(REQUEST_QUEUE, raw)
    pump_until(bridge.step, lambda: vk.get(REPLY_PREFIX + "download"))
    job_id = json.loads(vk.get(REPLY_PREFIX + "download"))["data"]["job_id"]
    vk.rpush(REQUEST_QUEUE, raw)
    frames = []

    def pump():
        bridge.step()
        frames.extend(wire.read_frames(10))

    pump_until(pump, lambda: frames)
    assert [frame["request"]["method"] for frame in frames] == ["download_history_data2"]
    hello("after")
    bridge.step()
    status = read_download_status(vk, ACCOUNT, job_id)
    assert status["state"] == "failed"
    assert "outcome unknown" in status["error"]
    assert not bridge.vk.lists[bridge.features.active_key]
    assert not bridge._inflight


def test_bridge_log_helpers_hide_credentials_and_follow_causes():
    assert serial_vk_bridge._vk_endpoint(
        "valkey://user:secret@10.0.0.1:6380/9") == "10.0.0.1:6380/9"
    assert serial_vk_bridge._vk_endpoint("valkey://h") == "h:6379/0"
    assert serial_vk_bridge._vk_endpoint("not a url") == "the configured VK URL"
    assert serial_vk_bridge._vk_endpoint(
        "valkey://h:bad/1") == "the configured VK URL"

    try:
        try:
            raise ValueError("inner")
        except ValueError as exc:
            raise TimeoutError("outer") from exc
    except TimeoutError as exc:
        text = serial_vk_bridge._describe(exc)
    assert "TimeoutError: outer" in text
    assert "ValueError: inner" in text


def test_bridge_warns_and_stops_without_an_account(tmp_path, monkeypatch, caplog):
    # 空目录避免读到仓库本地的 .env，否则该用例依赖开发机的账号配置。
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BIGQMT_ACCOUNT_ID", "  ")
    monkeypatch.setitem(sys.modules, "valkey", None)

    with caplog.at_level(logging.WARNING):
        assert serial_vk_bridge.main([]) == 2

    assert "BIGQMT_ACCOUNT_ID" in caplog.text


@pytest.mark.parametrize("source, scheme, timeout", [
    ("argument", "valkey", None),
    ("argument", "valkey", 7.5),
    ("dotenv", "redis", None),
])
def test_bridge_startup_names_the_side_that_failed(
        tmp_path, monkeypatch, caplog, source, scheme, timeout):
    query = "?socket_timeout=3&db=5"
    expected_query = "?db=5" if timeout is not None else query

    class FakeVk:
        @classmethod
        def from_url(cls, url, **kwargs):
            assert url == f"valkey://user:secret@127.0.0.1:6399/5{expected_query}"
            assert kwargs["socket_timeout"] == (5.0 if timeout is None else timeout)
            return cls()

        def ping(self):
            raise TimeoutError("timed out")

        def close(self):
            pass

    package = types.ModuleType("valkey")
    package.Valkey = FakeVk
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
    monkeypatch.chdir(tmp_path)
    dotenv_url = f"{scheme}://user:secret@127.0.0.1:6399/5{query}"
    argv = ["--vk-url", dotenv_url]
    if timeout is not None:
        argv.extend(["--vk-timeout", str(timeout)])
    if source == "dotenv":
        monkeypatch.setenv("BIGQMT_SERIAL_VK_URL", "valkey://wrong-host:6379/5")
        monkeypatch.delenv("BIGQMT_ACCOUNT_ID")
        (tmp_path / ".env").write_text(
            f"BIGQMT_ACCOUNT_ID=acct\nBIGQMT_SERIAL_VK_URL={dotenv_url}\n",
            encoding="utf-8",
        )
        argv = []

    with caplog.at_level(logging.ERROR):
        code = serial_vk_bridge.main(argv)

    assert code == 1
    assert "无法连接 VK 127.0.0.1:6399/5" in caplog.text
    assert "secret" not in caplog.text


class VkTimeoutError(Exception):
    """模拟 valkey.TimeoutError；_is_vk_error 按模块名识别。"""


VkTimeoutError.__module__ = "valkey.exceptions"


def test_bridge_survives_vk_timeouts_and_keeps_the_serial_link(duplex, caplog):
    qmt, windows = duplex

    class FlakyVk(MemoryVk):
        def __init__(self):
            super().__init__()
            self.failures = 2

        def lpop(self, key):
            if self.failures:
                self.failures -= 1
                raise VkTimeoutError("Timeout reading from socket")
            return super().lpop(key)

    vk = FlakyVk()
    bridge = SerialVkBridge(vk, windows, ACCOUNT, vk_endpoint="192.168.2.112:6379/5")
    wire = serial_transport.SerialFrames(qmt)
    wire.begin({"type": "hello", "account_id": ACCOUNT, "session": "session-1"})
    while wire.writing:
        wire.write_step()
    bridge.step()
    vk.rpush(REQUEST_QUEUE, request("after-timeout"))

    with caplog.at_level(logging.INFO, logger=serial_vk_bridge._log.name):
        bridge.step()
    assert "VK 192.168.2.112:6379/5 暂时不可用" in caplog.text
    assert "Timeout reading from socket" in caplog.text
    assert bridge._session == "session-1"
    assert bridge._vk_cooldown_until > time.monotonic()

    # 冷却期仍然读串口；VK 发布失败只丢这一条，不抛异常
    wire.begin({"type": "event", "session": "session-1",
                "topic": "bigqmt:quote_push:%s:test" % ACCOUNT, "data": {"000001.SZ": {}}})
    while wire.writing:
        wire.write_step()
    bridge.step()
    assert "放弃发布行情推送" in caplog.text

    # 退避结束后继续取请求（默认端口每次只写出 37 字节，需要多轮 write_step）
    with caplog.at_level(logging.INFO, logger=serial_vk_bridge._log.name):
        for _ in range(500):
            bridge._vk_cooldown_until = 0
            bridge.step()
            if bridge._inflight:
                break
    assert "after-timeout" in bridge._inflight
    assert not vk.lists[REQUEST_QUEUE]
    assert "已恢复" in caplog.text


def test_qmt_serial_reports_lost_peer_after_consecutive_write_timeouts(caplog):
    transport = serial_transport.SerialTransport(account_id=ACCOUNT)
    with caplog.at_level(logging.WARNING, logger=serial_transport._log.name):
        for _ in range(serial_transport._STALL_WRITE_TIMEOUTS - 1):
            transport._write_timeout()
        assert "连续写超时" not in caplog.text
        transport._write_timeout()
    assert "连续写超时" in caplog.text
    assert "桥接进程可能已停止" in caplog.text
    transport._link_alive()
    assert transport._write_timeouts == 0
