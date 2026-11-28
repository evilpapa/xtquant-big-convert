import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import logging_setup, xtquant_compat
from bigqmt_signal_trader.transports import shm_transport
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout
from bigqmt_signal_trader.transports.factory import build_transport, transport_supports_drain


ACCOUNT = "simulated-shm"
windows = pytest.mark.skipif(sys.platform != "win32", reason="uses real Windows named IPC")


def request(request_id="ping", method="ping", **params):
    return dict(request_id=request_id, account_id=ACCOUNT, method=method, params=params)


def reply(incoming, data=None):
    response = {key: incoming[key] for key in ("request_id", "account_id", "method")}
    return dict(response, ok=True, data=data)


def pump_until(ready, pump, timeout=5):
    deadline = time.monotonic() + timeout
    while not ready() and time.monotonic() < deadline:
        pump()
        time.sleep(0.001)
    assert ready()


@pytest.fixture
def config():
    return {"shm": {"namespace": uuid4().hex, "max_request_bytes": 4096,
                    "max_response_bytes": 4096}}


@pytest.fixture
def pair(config):
    server = build_transport("shm", config, account_id=ACCOUNT)
    client = build_transport("shm", config, account_id=ACCOUNT)
    try:
        yield server, client
    finally:
        client.stop()
        server.stop()


def test_transport_factory_is_lazy_and_reports_unsupported_platform(pair, monkeypatch):
    assert transport_supports_drain("shm")
    monkeypatch.setattr(shm_transport.sys, "platform", "linux")
    server, client = pair
    with pytest.raises(TransportError, match="requires Windows"):
        server.start_receiving(reply)
    with pytest.raises(TransportError, match="requires Windows"):
        client.send_request(request(), 1)


def test_early_windows_wait_timeout_does_not_shorten_dispatch_deadline(pair, monkeypatch):
    _, client = pair
    now = 0.0
    waits = []

    def early_timeout(handle, millis):
        nonlocal now
        waits.append(millis)
        now += 0.007
        return shm_transport._TIMEOUT

    monkeypatch.setattr(shm_transport, "time", SimpleNamespace(monotonic=lambda: now))
    wire = SimpleNamespace(ready=object(), wait=early_timeout)
    with pytest.raises(TransportTimeout):
        client._wait_response(wire, uuid4().bytes, 0.020)
    assert now >= 0.020 and len(waits) == 3


@windows
def test_real_cross_process_frames(config):
    config["shm"]["max_response_bytes"] = 2 * 1024 * 1024
    server = build_transport("shm", config, account_id=ACCOUNT)
    seen = []

    def echo(incoming):
        seen.append(threading.get_ident())
        data = incoming["params"]
        if data["i"] == 19:
            data = dict(data, blob="x" * (1024 * 1024))
        return reply(incoming, data)

    server.start_receiving(echo)
    script = """
import json, sys
from bigqmt_signal_trader.transports.factory import build_transport
a, config = sys.argv[1:]
client = build_transport('shm', json.loads(config), account_id=a)
for i in range(20):
    r = client.send_request(dict(request_id=str(i), account_id=a, method='echo', params={'i': i}), 4)
    assert r['data']['i'] == i
    if i == 19:
        assert r['data']['blob'] == 'x' * (1024 * 1024)
print('child read 20 responses')
"""
    try:
        with subprocess.Popen([sys.executable, "-c", script, ACCOUNT, json.dumps(config)],
                              env=dict(os.environ, PYTHONPATH=str(ROOT / "src")),
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
            try:
                pump_until(lambda: proc.poll() is not None, server.drain_request_queue, timeout=10)
                output = proc.communicate(timeout=1)[0].decode("utf-8", "replace")
                assert proc.returncode == 0, output
                assert "child read 20 responses" in output
            finally:
                if proc.poll() is None:
                    proc.kill()
    finally:
        server.stop()
    assert seen == [threading.get_ident()] * 20


@windows
def test_lifecycle_and_slot_sizes(pair, config):
    server, client = pair
    with pytest.raises(TransportError, match="server is not running"):
        client.send_request(request(), 1)
    with pytest.raises(TransportError, match="not dispatched"):
        client.send_request(request(text="x" * 5000), 1)
    with pytest.raises(ValueError, match="positive"):
        client.send_request(request(), float("nan"))
    with pytest.raises(TransportError, match="account_id"):
        client.send_request(dict(request(), account_id="other"), 1)

    server.start_receiving(lambda incoming: reply(incoming, "x" * 5000))
    duplicate = build_transport("shm", config, account_id=ACCOUNT)
    with pytest.raises(TransportError, match="already exists"):
        duplicate.start_receiving(reply)
    mismatched = build_transport("shm", {"shm": dict(config["shm"], max_response_bytes=8192)},
                                 account_id=ACCOUNT)
    with pytest.raises(TransportError, match="sizes"):
        mismatched.send_request(request(), 1)
    with ThreadPoolExecutor(1) as pool:
        call = pool.submit(client.send_request, request(), 3)
        pump_until(call.done, server.drain_request_queue)
        response = call.result()
        assert response["ok"] is False
        assert "max_response_bytes" in response["error"]

    server.stop()
    server.start_receiving(lambda incoming: reply(incoming, "重启成功"))
    with ThreadPoolExecutor(1) as pool:
        call = pool.submit(client.send_request, request(), 3)
        pump_until(call.done, server.drain_request_queue)
        assert call.result()["data"] == "重启成功"


@windows
def test_invalid_frame_length_stops_server_before_dispatch(pair, capsys):
    server, client = pair
    received = []
    server.start_receiving(received.append)
    with client._open(create=False) as wire, wire.locked(wire.guard, 1000) as state:
        assert state == 0
        server._write_header(wire, uuid4().bytes, time.monotonic() + 1,
                             server.max_request_bytes + 1, 0)
    assert server.drain_request_queue() == 0
    assert server._server is None and not received
    assert "Invalid SHM header" in capsys.readouterr().out
    server.start_receiving(received.append)


@windows
def test_expired_requests_and_late_reply_are_never_reused(pair):
    server, client = pair
    received = []
    server.start_receiving(received.append)
    with pytest.raises(TransportTimeout, match="outcome may be unknown"):
        client.send_request(request(), 0.02)
    assert server.drain_request_queue() == 0
    assert not received

    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(client.send_request, request(), 0.5)
        pump_until(lambda: len(received) == 1, server.drain_request_queue)
        with pytest.raises(TransportTimeout, match="outcome may be unknown"):
            first.result(2)
        # Same RPC id, different wire ticket. Late replies cannot be mistaken
        # for a second attempt even when the user reuses an id deliberately.
        second = pool.submit(client.send_request, request(), 3)
        pump_until(lambda: len(received) == 2, server.drain_request_queue)
        server.send_response(received[0], reply(received[0], "late"))
        server.send_response(received[1], reply(received[1], "current"))
        assert second.result(2)["data"] == "current"


@windows
def test_client_mutex_times_out_without_replacing_active_request(pair):
    server, client = pair
    received = []
    server.start_receiving(received.append)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(client.send_request, request("first"), 3)
        pump_until(lambda: received, server.drain_request_queue)
        with pytest.raises(TransportTimeout, match="not dispatched"):
            client.send_request(request("second"), 0.02)
        server.send_response(received[0], reply(received[0], True))
        assert first.result(2)["data"] is True
    assert not server.drain_request_queue()
    assert [row["request_id"] for row in received] == ["first"]


@windows
def test_stop_wakes_pending_client_without_replaying(pair):
    server, client = pair
    received = []
    server.start_receiving(received.append)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(client.send_request, request(), 3)
        pump_until(lambda: received, server.drain_request_queue)
        server.stop()
        with pytest.raises(TransportError, match="server stopped"):
            future.result(1)
    server.start_receiving(received.append)
    assert not server.drain_request_queue()
    assert len(received) == 1


@windows
def test_adjust_does_not_wait_for_client_critical_section(pair):
    server, client = pair
    server.start_receiving(reply)
    held, release = threading.Event(), threading.Event()

    def hold_guard():
        with client._open(create=False) as wire, wire.locked(wire.guard, 1000) as result:
            assert result == 0
            held.set()
            assert release.wait(3)

    with ThreadPoolExecutor(2) as pool:
        holder = pool.submit(hold_guard)
        try:
            assert held.wait(1)
            assert pool.submit(server.drain_request_queue).result(0.5) == 0
        finally:
            release.set()
        holder.result(1)


@windows
def test_strategy_uses_adjust_and_preserves_default_order_gate(config, monkeypatch):
    monkeypatch.setattr(logging_setup, "_qmt_stdout_only", False)
    monkeypatch.setattr(xtquant_compat, "_client_local_env", lambda: {})
    client_config = {"vk_config": {"transport": "shm", "shm": config["shm"],
                                   "port": "unused invalid VK port"}}
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda *_: client_config)
    config = {"account_id": ACCOUNT, "valkey": {"host": "must-not-connect"}, "rpc": {
        "enabled": True, "transport": "shm", "background_threads": True,
        "native_xtdata_enabled": True, **config,
    }}
    monkeypatch.setattr(strategy, "_config", config)
    monkeypatch.setattr(strategy, "_account_id", ACCOUNT)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_events", strategy._exec_events)
    ctx = SimpleNamespace(get_full_tick=lambda codes: {codes[0]: {"lastPrice": 10}},
                          is_last_bar=lambda: False)
    per_call_threads = []

    def get_details(*args):
        per_call_threads.append(threading.get_ident())
        return [SimpleNamespace(m_dAvailable=9000, m_dBalance=10000)]

    config["qmt_api"] = {"get_trade_detail_data": get_details}
    service = strategy._build_rpc_service(ctx, None, strategy._build_config())
    monkeypatch.setattr(strategy, "_rpc_service", service)
    client = xtquant_compat.BigQmtRpcClient(account_id=ACCOUNT)
    monkeypatch.setattr(client, "_formula_router", lambda: pytest.fail("SHM used a network fallback"))
    assert service.vk is None and not service.background_threads and not service.heavy_offload
    assert not service.handlers.market_data.native_xtdata_enabled
    service.start()
    try:
        with ThreadPoolExecutor(1) as pool:
            def call(method, params=None):
                future = pool.submit(client.call, method, params, timeout_seconds=3)
                pump_until(future.done, lambda: strategy.adjust(ctx))
                return future.result()

            assert call("ping")["allow_order_methods"] is False
            assert call("get_asset")["cash"] == 9000
            assert call("get_full_tick", {"codes": ["000001.SZ"]}) == {"000001.SZ": {"lastPrice": 10}}
            for method in ("order_stock", "cancel_order_stock"):
                with pytest.raises(xtquant_compat.RpcServerRepliedError, match="not allowed"):
                    call(method)
            with pytest.raises(xtquant_compat.RpcServerRepliedError, match="reload_deployment"):
                call("reload_deployment")
            assert not strategy._reload_request["pending"]
    finally:
        service.stop()
    assert per_call_threads == [threading.get_ident()]


@windows
@pytest.mark.parametrize("startup_failure", [False, True])
def test_generated_strategy_attempts_no_file_or_socket_io(tmp_path, startup_failure):
    artifact = tmp_path / "strategy.py"
    env = {key: value for key, value in os.environ.items() if not key.startswith("BIGQMT_")}
    env.update(BIGQMT_BUILD_OUT=str(artifact), BIGQMT_ACCOUNT_ID="qmt-simulator",
               BIGQMT_VK_PORT="unused-invalid-port", PYTHONIOENCODING="utf-8")
    subprocess.run([sys.executable, str(ROOT / "tools/build_single_file.py")], cwd=tmp_path,
                   env=env, check=True, capture_output=True, timeout=15)
    source = artifact.read_text(encoding="ascii")
    source = source.replace('"transport": "serial",', '"transport": "shm",', 1)
    config = {"namespace": uuid4().hex, "max_response_bytes": 4096}
    source = source.replace('    "rpc_allow_order_methods":',
                            f'    "shm": {config!r},\n    "rpc_allow_order_methods":', 1)
    artifact.write_text(source, encoding="ascii")
    script = """
import sys, threading, time
from pathlib import Path
from types import SimpleNamespace
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
from simulate_qmt import ACCOUNT, SimulatedContext, StrategyIOGuard, compile_strategy
from concurrent.futures import ThreadPoolExecutor
import ctypes
context = SimulatedContext()
code = compile_strategy(Path(sys.argv[2]))
ns = dict(__name__='shm_test', get_trade_detail_data=context.get_trade_detail_data)
with StrategyIOGuard():
    exec(code, ns)
    strategy = ns['_strategy']
    cls = ns['_load_local_module']('bigqmt_signal_trader.transports.shm_transport').SharedMemoryTransport
    client = cls(account_id=ACCOUNT, **ns['BIGQMT_CONFIG']['shm'])
    if sys.argv[3] == 'True':
        def fail(*a, **kw):
            raise OSError('simulated kernel mapping failure')
        ctypes.WinDLL = fail
    try:
        assert ns['init'](context) is not None
        assert 'adjust' in context.timers
        if sys.argv[3] == 'True':
            assert strategy._rpc_service is None
            ns['adjust'](context)
        else:
            assert strategy._rpc_service is not None
            def call():
                return client.send_request(dict(account_id=ACCOUNT, request_id='probe', method='get_asset'), 3)
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(call)
                while not future.done():
                    ns['adjust'](context)
                    time.sleep(0.001)
                response = future.result()
                assert response['ok'] and response['data']['cash'] == 9000, response
            order = SimpleNamespace(m_strAccountID=ACCOUNT, m_strInstrumentID='000001',
                                    m_strExchangeID='SZ', m_strOrderSysID='simulated')
            ns['order_callback'](context, order)
            ns['deal_callback'](context, order)
            def failing_callback(*args):
                raise ValueError('simulated callback error')
            assert strategy._adjust_phase('test', failing_callback) is None
    finally:
        ns['stop'](context)
print('SHM simulation passed')
"""
    result = subprocess.run([sys.executable, "-c", script, str(ROOT / "tools"),
                             str(artifact), str(startup_failure)],
                            cwd=tmp_path, env=env, encoding="utf-8", capture_output=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SHM simulation passed" in result.stdout
    if startup_failure:
        assert "simulated kernel mapping failure" in result.stdout
