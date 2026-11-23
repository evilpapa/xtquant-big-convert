"""在本地解释器中运行大 QMT 的串口单文件策略。"""

import argparse
import base64
import importlib.util
import json
import linecache
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
from uuid import uuid4


ACCOUNT = "qmt-simulator"
STOCK = "000001.SZ"
BUILDER = Path(__file__).with_name("build_single_file.py").resolve()


class MemoryPort:
    def __init__(self):
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.peer = None
        self.closed = False

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
        with self.peer.lock:
            self.peer.buffer.extend(data)
        return len(data)

    def close(self):
        self.closed = True


class SimulatedContext:
    stockcode = STOCK
    period = "1m"
    do_back_test = False

    def __init__(self):
        self.account = None
        self.timers = {}
        self.strategy_thread = threading.get_ident()
        self.last_bar = False

    def set_account(self, account):
        self.account = account

    def run_time(self, name, interval, start_time):
        self.timers[name] = (interval, start_time)

    def is_last_bar(self):
        return self.last_bar

    def get_full_tick(self, codes):
        return {STOCK: {"lastPrice": 10.0, "volume": 1000}} if STOCK in codes else {}

    def get_financial_data(self, *args, **kwargs):
        return {}

    def get_trade_detail_data(self, account, account_type, detail_type, strategy_name=""):
        if threading.get_ident() != self.strategy_thread:
            raise RuntimeError("模拟账户数据须在策略线程读取")
        if account != self.account or detail_type.upper() not in ("ACCOUNT", "ASSET"):
            return []
        return [SimpleNamespace(m_dAvailable=9_000.0, m_dBalance=10_000.0,
                                m_dMarketValue=1_000.0, m_dFrozenCash=0.0)]

    def reject_trade(self, *args, **kwargs):
        raise RuntimeError("模拟器禁止调用下单和撤单")


class StrategyIOGuard:
    FILE_EVENTS = frozenset((
        "open", "os.listdir", "os.scandir", "os.mkdir", "os.remove", "os.rename",
        "os.rmdir", "os.chmod", "os.link", "os.symlink", "os.truncate",
        "os.utime", "os.system",
    ))

    def __init__(self):
        self.active = False
        self.attempts = set()
        self.stat = os.stat
        self.lstat = os.lstat
        sys.addaudithook(self.audit)

    def block(self, event, caller):
        self.attempts.add((event, caller.f_code.co_filename, caller.f_lineno))
        raise PermissionError(f"策略尝试外部 I/O ({event})")

    def audit(self, event, args):
        if not self.active or (
            event not in self.FILE_EVENTS and not event.startswith(("socket.", "subprocess."))
        ):
            return
        caller = sys._getframe(1)
        importing = caller.f_code.co_filename == "<frozen importlib._bootstrap_external>"
        if importing and event == "open":
            flags = args[2]
            if not flags & (os.O_ACCMODE | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
                return
        if importing and event == "os.listdir":
            return
        self.block(event, caller)

    def check_stat(self, func, path, *args, **kwargs):
        caller = sys._getframe(2)
        if caller.f_code.co_filename != "<frozen importlib._bootstrap_external>":
            self.block(f"os.{func.__name__}", caller)
        return func(path, *args, **kwargs)

    def __enter__(self):
        self.active = True
        os.stat = lambda *args, **kwargs: self.check_stat(self.stat, *args, **kwargs)
        os.lstat = lambda *args, **kwargs: self.check_stat(self.lstat, *args, **kwargs)
        return self

    def __exit__(self, exc_type, exc, traceback):
        os.stat, os.lstat = self.stat, self.lstat
        self.active = False
        if self.attempts:
            details = "\n".join(f"  {event}  {file}:{line}"
                                for event, file, line in sorted(self.attempts))
            raise RuntimeError(f"检测到策略尝试外部 I/O\n{details}") from exc


class SerialSession:
    def __init__(self, wire, tick, timeout):
        self.wire = wire
        self.tick = tick
        self.timeout = timeout
        self.session = self.wait_for(lambda frame: frame["type"] == "hello")["session"]

    def wait_for(self, match):
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            for frame in self.wire.read_frames(1):
                if match(frame):
                    return frame
            self.tick()
            time.sleep(0.002)
        raise RuntimeError("等待模拟串口消息超时，请查看上方的策略日志")

    def request(self, method, params=None):
        request_id = uuid4().hex
        request = dict(schema_version=1, request_id=request_id, account_id=ACCOUNT,
                       method=method, params=params or {}, timeout_seconds=self.timeout,
                       _sent_at=time.time())
        self.wire.begin(dict(type="request", session=self.session, request=request))
        while self.wire.writing:
            self.wire.write_step()
        return self.wait_for(
            lambda frame: frame["type"] == "response"
            and frame["response"]["request_id"] == request_id
        )["response"]


def run_demo(session, namespace, context):
    for method, params, matches in (
        ("ping", {}, lambda data: data["pong"] is True),
        ("get_asset", {}, lambda data: data["cash"] == 9_000.0),
        ("get_full_tick", {"codes": [STOCK]}, lambda data: data[STOCK]["lastPrice"] == 10.0),
    ):
        response = session.request(method, params)
        if not response["ok"] or not matches(response["data"]):
            raise RuntimeError(f"{method} 的模拟结果异常 {response}")
        print(f"[simulate_qmt] {method} OK")

    event_fields = dict(m_strAccountID=ACCOUNT, m_strInstrumentID="000001",
                        m_strExchangeID="SZ", m_strOrderSysID="sim-1")
    for callback, event_type, fields in (
        ("order_callback", "order", dict(m_nOrderStatus=50, m_nVolumeTotalOriginal=100,
                                        m_dLimitPrice=10.0)),
        ("deal_callback", "trade", dict(m_strTradeID="sim-deal-1", m_nVolume=100,
                                       m_dPrice=10.0)),
    ):
        namespace[callback](context, SimpleNamespace(**event_fields, **fields))
        frame = session.wait_for(lambda frame: frame["type"] == "event"
                                 and frame["data"].get("event_type") == event_type)
        if frame["data"].get("order_sys_id") != "sim-1":
            raise RuntimeError(f"{callback} 的模拟回报异常 {frame}")
        print(f"[simulate_qmt] {callback} OK")
    return True


def cache_source(path, source):
    entry = (len(source), None, source.splitlines(keepends=True), path)
    linecache.cache[path] = entry
    linecache.cache[os.path.normcase(os.path.abspath(path))] = entry


def compile_strategy(path):
    source = path.read_bytes()
    code = compile(source, str(path), "exec")
    cache_source(str(path), importlib.util.decode_source(source))
    return code


def run_strategy(code, method, params, timeout):
    context = SimulatedContext()
    strategy_port, client_port = MemoryPort(), MemoryPort()
    strategy_port.peer, client_port.peer = client_port, strategy_port
    serial = ModuleType("serial")
    serial.Serial = lambda **options: strategy_port
    sys.modules["serial"] = serial
    namespace = {
        "__name__": "__qmt_strategy__",
        "get_trade_detail_data": context.get_trade_detail_data,
        "passorder": context.reject_trade,
        "cancel": context.reject_trade,
    }

    with StrategyIOGuard():
        try:
            exec(code, namespace)
            config = namespace["BIGQMT_CONFIG"]
            if str(config.get("transport", "")).lower() != "serial":
                raise RuntimeError("模拟器仅支持 transport='serial' 的单文件策略")
            if namespace["BIGQMT_ACCOUNT_ID"] != ACCOUNT:
                raise RuntimeError("单文件策略须从 BIGQMT_ACCOUNT_ID 读取模拟账号")
            for path, encoded in namespace["_EMBEDDED_SOURCES"].items():
                cache_source(path, importlib.util.decode_source(base64.b64decode(encoded)))
            if namespace["init"](context) is None or context.account != ACCOUNT:
                raise RuntimeError("init 未成功初始化模拟账户")
            if "adjust" not in context.timers:
                raise RuntimeError("策略未通过 run_time 注册 adjust")
            namespace["handlebar"](context)
            context.last_bar = True
            namespace["handlebar"](context)

            def tick():
                for callback in context.timers:
                    namespace[callback](context)

            load = namespace["_load_local_module"]
            frames = load("bigqmt_signal_trader.transports.serial_transport").SerialFrames
            session = SerialSession(frames(client_port), tick, timeout)
            if method:
                response = session.request(method, params)
                print(json.dumps(response, ensure_ascii=False, indent=2))
                return response["ok"]
            return run_demo(session, namespace, context)
        finally:
            stop = namespace.get("stop")
            if stop is not None:
                stop(context)
                if "adjust" in context.timers and not strategy_port.closed and sys.exc_info()[0] is None:
                    raise RuntimeError("stop 未关闭模拟串口")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("strategy", nargs="?", type=Path, help="已有单文件，省略则从当前源码临时构建")
    parser.add_argument("--rpc", metavar="METHOD", help="运行一个指定的 RPC，省略则执行内置检查")
    parser.add_argument("--params", type=json.loads, default={}, help="RPC 参数，接受 JSON 对象")
    parser.add_argument("--timeout", type=float, default=3.0, help="每条串口消息的等待秒数")
    args = parser.parse_args()
    if not isinstance(args.params, dict) or (args.params and not args.rpc):
        parser.error("--params 必须为 JSON 对象，并与 --rpc 一起使用")
    if not 0 < args.timeout < float("inf"):
        parser.error("--timeout 必须是有限的正数")

    for name in tuple(os.environ):
        if name.startswith("BIGQMT_"):
            del os.environ[name]
    os.environ["BIGQMT_ACCOUNT_ID"] = ACCOUNT
    sys.dont_write_bytecode = True
    try:
        if args.strategy is None:
            with tempfile.NamedTemporaryFile(suffix=".py", delete_on_close=False) as output:
                output.close()
                subprocess.run([sys.executable, str(BUILDER)], check=True,
                               env=dict(os.environ, BIGQMT_BUILD_OUT=output.name))
                code = compile_strategy(Path(output.name))
        else:
            code = compile_strategy(args.strategy)
        script = Path(__file__).resolve()
        cache_source(str(script), script.read_text(encoding="utf-8"))
        passed = run_strategy(code, args.rpc, args.params, args.timeout)
    except (OSError, RuntimeError, SyntaxError, subprocess.CalledProcessError) as exc:
        print(f"[simulate_qmt] 失败 {exc}", file=sys.stderr)
        return 1
    if passed:
        print("[simulate_qmt] 检查通过，策略已停止")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
