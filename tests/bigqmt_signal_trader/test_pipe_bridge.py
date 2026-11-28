"""Exercise real Windows named pipes with a fake QMT and in-memory VK."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from test_serial_bridge import ACCOUNT, REPLY_PREFIX, REQUEST_QUEUE, MemoryVk, pump_until, request
from bigqmt_signal_trader.pipe_vk_bridge import PipeVkBridge
from bigqmt_signal_trader.transports.base import TransportError
from bigqmt_signal_trader.transports.pipe_transport import PipeTransport, local_pipe_name, open_pipe_client
from bigqmt_signal_trader.transports.serial_transport import SerialFrames
from bigqmt_signal_trader.vk_rpc import encode_rpc_request_payload


ROOT = Path(__file__).resolve().parents[2]
windows = pytest.mark.skipif(sys.platform != "win32", reason="uses real Windows named pipes")


@pytest.mark.parametrize("name", [r"\\remote\pipe\qmt_bridge", r"C:\data.txt", "foo/bar", "..", ""])
def test_pipe_rejects_nonlocal_or_ambiguous_paths(name):
    with pytest.raises(ValueError, match="local pipe name"):
        PipeTransport(name=name)


@windows
def test_only_one_listener_accepts_and_can_rearm():
    name = "test-" + uuid4().hex
    assert local_pipe_name(name) == rf"\\.\pipe\{name}"
    listener = PipeVkBridge.open_listener(name)
    try:
        assert listener.accept() is False
        with pytest.raises(OSError):
            PipeVkBridge.open_listener(name)
        for _ in range(2):
            client = open_pipe_client(name)
            try:
                assert listener.accept() is True
                assert client.write(b"\0ping") == 5
                assert listener.in_waiting == 5
                assert listener.read(5) == b"\0ping"
                assert listener.write(b"pong\0") == 5
                assert client.in_waiting == 5
                assert client.read(5) == b"pong\0"
            finally:
                client.close()
            listener.disconnect()
            assert listener.accept() is False
    finally:
        listener.close()
    with pytest.raises(TransportError, match="start the Windows bridge first"):
        open_pipe_client(name)


@windows
def test_bridge_drops_inflight_write_on_disconnect_and_keeps_listening():
    name = "test-" + uuid4().hex
    memory = MemoryVk()
    listener = PipeVkBridge.open_listener(name)
    peer = None
    try:
        bridge = PipeVkBridge(memory, listener, ACCOUNT)

        def connect(session):
            port = open_pipe_client(name)
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
        assert bridge.step() is False  # rearm the same listener

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


def _build_pipe_strategy(tmp_path, name):
    artifact = tmp_path / "pipe_strategy.py"
    env = {key: value for key, value in os.environ.items() if not key.startswith("BIGQMT_")}
    env.update(BIGQMT_BUILD_OUT=str(artifact), BIGQMT_ACCOUNT_ID="qmt-simulator",
               BIGQMT_VK_PORT="unused-bad-port", PYTHONIOENCODING="utf-8")
    subprocess.run([sys.executable, str(ROOT / "tools/build_single_file.py")],
                   cwd=tmp_path, env=env, check=True, capture_output=True, timeout=20)
    source = artifact.read_text(encoding="ascii")
    source = source.replace('"transport": "serial",',
                            f'"transport": "pipe",\n    "pipe": {{"name": {name!r}}},', 1)
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
ns = dict(__name__='pipe_test', get_trade_detail_data=context.get_trade_detail_data,
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
print('QMT pipe simulation passed')
"""


def _sim_args(artifact, mode):
    return [sys.executable, "-c", _SIMULATOR, str(ROOT / "tools"), str(artifact), mode]


@windows
def test_real_cross_process_rpc_and_reports_under_qmt_io_guard(tmp_path):
    name = "test-" + uuid4().hex
    artifact, env = _build_pipe_strategy(tmp_path, name)
    listener = PipeVkBridge.open_listener(name)
    memory = MemoryVk()
    account = "qmt-simulator"
    bridge = PipeVkBridge(memory, listener, account)
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
            assert "QMT pipe simulation passed" in output
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
def test_missing_pipe_does_not_fall_back_under_qmt_io_guard(tmp_path):
    artifact, env = _build_pipe_strategy(tmp_path, "missing-" + uuid4().hex)
    result = subprocess.run(_sim_args(artifact, "missing"), cwd=tmp_path, env=env,
                            encoding="utf-8", capture_output=True, timeout=16)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "start the Windows bridge first" in result.stdout
    assert "QMT pipe simulation passed" in result.stdout
