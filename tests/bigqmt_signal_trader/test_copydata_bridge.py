"""Exercise real Windows WM_COPYDATA hidden windows with a fake QMT and in-memory VK."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from test_serial_bridge import ACCOUNT, REPLY_PREFIX, REQUEST_QUEUE, MemoryVk, pump_until, request
from bigqmt_signal_trader.copydata_vk_bridge import CopyDataVkBridge
from bigqmt_signal_trader.transports.base import LocalIpcChannel, TransportError
from bigqmt_signal_trader.transports.copydata_transport import (
    CopyDataTransport,
    local_window_name,
    local_window_title,
    open_copydata_client,
)
from bigqmt_signal_trader.transports.serial_transport import SerialFrames
from bigqmt_signal_trader.vk_rpc import encode_rpc_request_payload


ROOT = Path(__file__).resolve().parents[2]
windows = pytest.mark.skipif(sys.platform != "win32", reason="uses real Windows WM_COPYDATA windows")


@pytest.mark.parametrize("name", [r"\\remote\window", r"C:\data.txt", "foo/bar", "bad name", "..", ".", "", 123])
def test_copydata_rejects_invalid_window_names(name):
    with pytest.raises(ValueError, match="copydata window name"):
        CopyDataTransport(window_name=name)


def test_copydata_window_name_normalization_and_alias():
    assert local_window_name("qmt_bridge") == "qmt_bridge"
    assert local_window_name("BigQmtCopyData:qmt_bridge") == "qmt_bridge"
    assert local_window_title("qmt_bridge") == "BigQmtCopyData:qmt_bridge"
    assert local_window_title("BigQmtCopyData:qmt_bridge") == "BigQmtCopyData:qmt_bridge"
    transport = CopyDataTransport(name="alt_bridge")
    assert transport.port == "BigQmtCopyData:alt_bridge"
    with pytest.raises(ValueError, match="either window_name or name"):
        CopyDataTransport(window_name="one", name="two")


@windows
def test_only_one_listener_accepts_poll_send_chunks_and_can_rearm():
    name = "test-" + uuid4().hex
    assert local_window_title(name) == f"BigQmtCopyData:{name}"
    listener = CopyDataVkBridge.open_listener(name)
    try:
        assert isinstance(listener, LocalIpcChannel)
        assert listener.accept() is False
        assert listener.poll() == b""
        with pytest.raises(OSError):
            CopyDataVkBridge.open_listener(name)
        for _ in range(2):
            client = open_copydata_client(name)
            try:
                assert isinstance(client, LocalIpcChannel)
                assert listener.accept() is True
                # Second client must not hijack an active session.
                with pytest.raises(TransportError, match="busy"):
                    open_copydata_client(name)
                assert client.send(b"\0ping") == 5
                assert listener.in_waiting == 5
                assert listener.poll(5) == b"\0ping"
                assert listener.poll() == b""
                assert listener.send(b"pong\0") == 5
                assert client.in_waiting == 5
                assert client.poll(5) == b"pong\0"
                # Multi-chunk transfer (>64 KiB single WM_COPYDATA chunk cap).
                blob = bytes(range(256)) * 400  # 102,400 bytes
                assert client.send(blob) == len(blob)
                received = bytearray()
                while len(received) < len(blob):
                    chunk = listener.poll(32768)
                    assert chunk
                    received.extend(chunk)
                assert bytes(received) == blob
            finally:
                client.close()
            listener.disconnect()
            assert listener.accept() is False
    finally:
        listener.close()
    with pytest.raises(TransportError, match="start the Windows bridge first"):
        open_copydata_client(name)


@windows
def test_bridge_drops_inflight_write_on_disconnect_and_keeps_listening():
    name = "test-" + uuid4().hex
    memory = MemoryVk()
    listener = CopyDataVkBridge.open_listener(name)
    peer = None
    try:
        bridge = CopyDataVkBridge(memory, listener, ACCOUNT)

        def connect(session):
            port = open_copydata_client(name)
            wire = SerialFrames(port)
            wire.begin({"type": "hello", "account_id": ACCOUNT, "session": session})
            while wire.writing:
                wire.write_step()
            pump_until(bridge.step, lambda: bridge._session == session)
            return port, wire

        peer, wire = connect("first")
        memory.rpush(REQUEST_QUEUE, request("wrong-account", "cancel_order_stock_batch", params={
            "items": [{"account_id": "another-account", "order_id": "simulated"}],
        }))
        pump_until(bridge.step, lambda: memory.get(REPLY_PREFIX + "wrong-account") is not None)
        assert "different account" in json.loads(memory.get(REPLY_PREFIX + "wrong-account"))["error"]
        assert wire.read_frames(1) == []
        memory.rpush(REQUEST_QUEUE, request("lost-write", "order_stock"))
        frames = []

        def step():
            bridge.step()
            frames.extend(wire.read_frames(1))

        pump_until(step, lambda: bool(frames))
        assert frames[0]["request"]["request_id"] == "lost-write"
        peer.close()
        pump_until(bridge.step, lambda: bridge._session is None)
        assert not bridge._inflight and "lost-write" in bridge._uncertain_writes
        assert bridge.step() is False  # rearm the same hidden window

        peer, wire = connect("second")
        memory.rpush(REQUEST_QUEUE, request("lost-write", "order_stock"))
        pump_until(bridge.step, lambda: memory.get(REPLY_PREFIX + "lost-write") is not None)
        error = json.loads(memory.get(REPLY_PREFIX + "lost-write"))
        assert error["ok"] is False and "query orders and trades" in error["error"]
        assert wire.read_frames(1) == []
    finally:
        if peer is not None:
            peer.close()
        listener.close()


def _build_copydata_strategy(tmp_path, name):
    artifact = tmp_path / "copydata_strategy.py"
    env = {key: value for key, value in os.environ.items() if not key.startswith("BIGQMT_")}
    env.update(BIGQMT_BUILD_OUT=str(artifact), BIGQMT_ACCOUNT_ID="qmt-simulator",
               BIGQMT_VK_PORT="unused-bad-port", PYTHONIOENCODING="utf-8")
    subprocess.run([sys.executable, str(ROOT / "tools/build_single_file.py")],
                   cwd=tmp_path, env=env, check=True, capture_output=True, timeout=20)
    source = artifact.read_text(encoding="ascii")
    source = source.replace(
        '"transport": "serial",',
        f'"transport": "copydata",\n    "copydata": {{"window_name": {name!r}}},',
        1,
    )
    artifact.write_text(source, encoding="ascii")
    return artifact, env


_SIMULATOR = """
import sys, time
from pathlib import Path
from types import SimpleNamespace
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
from simulate_qmt import SimulatedContext, StrategyIOGuard, compile_strategy
context = SimulatedContext()
base_get_tick = context.get_full_tick

def get_full_tick(codes):
    result = base_get_tick(codes)
    if result:
        result[next(iter(result))]['test_blob'] = 'x' * (256 * 1024)
    return result

context.get_full_tick = get_full_tick
code = compile_strategy(Path(sys.argv[2]))
ns = dict(__name__='copydata_test', get_trade_detail_data=context.get_trade_detail_data,
          passorder=context.reject_trade, cancel=context.reject_trade)
with StrategyIOGuard():
    exec(code, ns)
    strategy = ns['_strategy']
    assert ns['init'](context) is not None
    assert 'adjust' in context.timers
    try:
        service = strategy._rpc_service
        if sys.argv[3] == 'missing':
            assert service is None
            ns['adjust'](context)
        else:
            assert service is not None and service.vk is None and not service.background_threads
            assert not service.handlers.market_data.native_xtdata_enabled
            assert not service.handlers.allow_order_methods
            order = SimpleNamespace(m_strAccountID=context.account, m_strInstrumentID='000001',
                                    m_strExchangeID='SZ', m_strOrderSysID='simulated', m_nOrderStatus=50)
            ns['order_callback'](context, order)
            ns['deal_callback'](context, order)
            end = time.monotonic() + 12
            while time.monotonic() < end and not service._transport._failed:
                ns['adjust'](context)
                time.sleep(0.002)
            assert service._transport._failed, 'bridge stop did not break the QMT link'
    finally:
        ns['stop'](context)
print('QMT copydata simulation passed')
"""


def _sim_args(artifact, mode):
    return [sys.executable, "-c", _SIMULATOR, str(ROOT / "tools"), str(artifact), mode]


@windows
def test_real_cross_process_rpc_and_reports_under_qmt_io_guard(tmp_path):
    name = "test-" + uuid4().hex
    artifact, env = _build_copydata_strategy(tmp_path, name)
    listener = CopyDataVkBridge.open_listener(name)
    memory = MemoryVk()
    account = "qmt-simulator"
    bridge = CopyDataVkBridge(memory, listener, account)
    queue = "bigqmt:rpc:queue:" + account
    response_prefix = "bigqmt:rpc:resp:" + account + ":"
    with subprocess.Popen(_sim_args(artifact, "bridge"), cwd=tmp_path, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, encoding="utf-8") as process:
        try:
            methods = ("ping", "get_asset", "order_stock", "reload_deployment", "get_full_tick")
            for method in methods:
                params = {"codes": ["000001.SZ"]} if method == "get_full_tick" else {}
                memory.rpush(queue, encode_rpc_request_payload({
                    "schema_version": 1, "request_id": method, "account_id": account,
                    "method": method, "params": params, "timeout_seconds": 9, "_sent_at": time.time(),
                }))
            pump_until(bridge.step, lambda: all(memory.get(response_prefix + name) for name in methods)
                       or process.poll() is not None, timeout=10)
            listener.close()
            output = process.communicate(timeout=4)[0]
            assert process.returncode == 0, output
            assert "QMT copydata simulation passed" in output
            assert "通信失败" in output
            respond = lambda name: json.loads(memory.get(response_prefix + name))
            assert respond("ping")["data"]["pong"] is True
            assert respond("get_asset")["data"]["cash"] == 9000
            assert "not allowed" in respond("order_stock")["error"]
            assert respond("reload_deployment")["ok"] is False
            assert respond("get_full_tick")["data"]["000001.SZ"]["test_blob"] == "x" * (256 * 1024)
            assert any(topic.startswith("bigqmt:order_events:") for topic in memory.published)
            assert any(topic.startswith("bigqmt:trade_events:") for topic in memory.published)
        finally:
            listener.close()
            if process.poll() is None:
                process.kill()


@windows
def test_missing_window_does_not_fall_back_under_qmt_io_guard(tmp_path):
    artifact, env = _build_copydata_strategy(tmp_path, "missing-" + uuid4().hex)
    result = subprocess.run(_sim_args(artifact, "missing"), cwd=tmp_path, env=env,
                            encoding="utf-8", capture_output=True, timeout=16)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "start the Windows bridge first" in result.stdout
    assert "QMT copydata simulation passed" in result.stdout
