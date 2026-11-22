"""在 QMT 进程外运行，通过串口转发已有的 Valkey RPC 消息。"""

import argparse
import logging
import math
import os
import re
import time
from collections import OrderedDict
from urllib.parse import urlsplit

from .adapters.valkey_common import normalize_valkey_url
from .exec_events import EXEC_TOPICS, publish_exec_event
from .local_env import local_serial_settings
from .quote_push_channel import ValkeyQuotePushChannel
from .valkey_rpc import METHOD_ALIASES, ORDER_METHODS
from .transports.valkey_transport import ValkeyTransport, _loads
from .transports.serial_transport import (
    DEFAULT_BAUDRATE, DEFAULT_MAX_FRAME_BYTES, SerialFrames, open_serial,
)


_log = logging.getLogger(__name__)
_REQUEST_ID = re.compile(r"[a-zA-Z0-9_.:-]{1,128}\Z")
_MAX_INFLIGHT = 16


def _valkey_endpoint(url):
    """返回可写入日志的连接地址 host:port/db，省略凭据。"""
    try:
        parts = urlsplit(url)
        port = parts.port or 6379
    except ValueError:
        return "the configured Valkey URL"
    if not parts.hostname:
        return "the configured Valkey URL"
    db = (parts.path or "/0").strip("/") or "0"
    return "%s:%s/%s" % (parts.hostname, port, db)


def _describe(exc):
    """返回异常类型、消息及其原因链。"""
    parts = []
    seen = set()
    while exc is not None and id(exc) not in seen and len(parts) < 4:
        seen.add(id(exc))
        try:
            text = str(exc).strip()
        except Exception:
            text = ""
        parts.append("%s: %s" % (type(exc).__name__, text) if text else type(exc).__name__)
        exc = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
    return " <- ".join(parts)


class SerialValkeyBridge:
    def __init__(self, valkey_client, port, account_id, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES):
        self.valkey = valkey_client
        self.account_id = str(account_id or "").strip()
        if not self.account_id or len(self.account_id) > 128:
            raise ValueError("a valid account_id is required")
        self.wire = SerialFrames(port, max_frame_bytes)
        self.replies = ValkeyTransport(valkey_client, account_id=self.account_id)
        self.quotes = ValkeyQuotePushChannel(valkey_client, account_id=self.account_id)
        self._inflight = {}
        self._uncertain_writes = OrderedDict()
        self._session = None
        self._last_hello = None

    def _forget_request(self, request_id, record, now):
        self._inflight.pop(request_id, None)
        if METHOD_ALIASES.get(record[1], record[1]) in ORDER_METHODS:
            self._uncertain_writes[request_id] = now + 600
            if len(self._uncertain_writes) > 512:
                self._uncertain_writes.popitem(last=False)
        _log.warning("未收到 %s 的串口应答，桥接不会重放该请求", record[1])

    def _reply_route(self, request_id, ttl):
        fields = {"account_id": self.account_id, "request_id": request_id}
        return dict(fields, ttl_seconds=ttl,
                    reply_list=self.replies.response_list_template.format(**fields))

    def _refuse(self, route, method, reason):
        self.replies.send_response(route, {
            "schema_version": 1, "request_id": route["request_id"],
            "account_id": self.account_id, "method": method, "ok": False,
            "data": None, "error": reason,
        })

    def _account_matches(self, params):
        account = params.get("account")
        if isinstance(account, dict):
            account = account.get("account_id") or account.get("accountID") or account.get("id")
        return str(params.get("account_id") or account or self.account_id) == self.account_id

    def _forward_request(self, raw):
        try:
            if len(raw) > self.wire.max_frame_bytes * 2:
                raise ValueError("request exceeds serial size limit")
            request = _loads(raw)
            request_id = request.get("request_id")
            if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
                raise ValueError("invalid request_id")
        except (ValueError, TypeError, RecursionError) as exc:
            _log.warning("已丢弃格式无效的 Valkey RPC 请求（%s）", type(exc).__name__)
            return
        if request_id in self._inflight:
            return
        try:
            ttl = max(1, min(600, int(request.get("ttl_seconds") or 60)))
        except (TypeError, ValueError, OverflowError):
            ttl = 60
        route = self._reply_route(request_id, ttl)
        method = str(request.get("method") or "").strip()
        params = request.get("params")
        if params is None:
            params = {}
        canonical = METHOD_ALIASES.get(method, method)
        if (str(request.get("account_id") or "") != self.account_id
                or not isinstance(params, dict) or not self._account_matches(params)):
            self._refuse(route, method, "Serial bridge account mismatch or invalid params")
            return
        outcome_request = params.get("request_id") if method == "get_request_outcome" else None
        uncertain_id = outcome_request if isinstance(outcome_request, str) else request_id
        if self._uncertain_writes.get(uncertain_id, 0) > time.monotonic():
            self._refuse(route, method, "Serial link lost the order response; query orders and trades before retrying")
            return
        if canonical == "submit_orders_batch" and isinstance(params.get("orders"), list):
            if any(isinstance(item, dict) and not self._account_matches(item)
                   for item in params["orders"]):
                self._refuse(route, method, "Serial bridge batch contains a different account")
                return
        now = time.time()
        try:
            timeout = float(request.get("timeout_seconds", 3))
            if not math.isfinite(timeout) or not 0 < timeout <= 600:
                raise ValueError("timeout out of range")
            sent_at = float(request.get("_sent_at", 0))
        except (TypeError, ValueError, OverflowError):
            self._refuse(route, method, "Serial bridge received invalid request timing")
            return
        valid_timestamp = math.isfinite(sent_at) and 0 < sent_at <= now + 1
        if canonical in ORDER_METHODS and not valid_timestamp:
            self._refuse(route, method, "Serial order requires a fresh Valkey RPC client timestamp; check client version and clock")
            return
        submitted_at = min(now, sent_at) if valid_timestamp else now
        if submitted_at + timeout <= now:
            self._refuse(route, method, "RequestExpired: queued in Valkey past the client timeout; NOT dispatched")
            return
        serial_request = {
            "schema_version": request.get("schema_version", 1),
            "request_id": request_id, "account_id": self.account_id,
            "method": method, "params": params, "timeout_seconds": timeout,
            "_received_at": submitted_at,
        }
        try:
            self.wire.begin({"type": "request", "request": serial_request, "session": self._session})
        except (ValueError, TypeError, RecursionError):
            self._refuse(route, method, "Request cannot be encoded within serial frame limit; NOT dispatched")
            return
        self._inflight[request_id] = (route, method, time.monotonic() + max(60, timeout + 10))

    def _receive_frame(self, frame):
        session = frame.get("session")
        if frame.get("type") == "hello":
            if frame.get("account_id") != self.account_id or not isinstance(session, str) or not session:
                return
            now = time.monotonic()
            if session != self._session:
                for request_id, record in list(self._inflight.items()):
                    self._forget_request(request_id, record, now)
                _log.info("QMT 串口会话已就绪")
            self._session = session
            self._last_hello = now
            return
        if self._session is None or session != self._session:
            return
        if frame.get("type") == "response":
            response = frame.get("response")
            if not isinstance(response, dict) or response.get("account_id") != self.account_id:
                return
            request_id = response.get("request_id")
            record = self._inflight.get(request_id) if isinstance(request_id, str) else None
            if record is None or response.get("method") != record[1]:
                return
            self.replies.send_response(record[0], response)
            self._inflight.pop(request_id, None)
        elif frame.get("type") == "event":
            topic, data = frame.get("topic"), frame.get("data")
            if not isinstance(topic, str) or not isinstance(data, dict):
                return
            if topic.startswith("exec:"):
                event_type = data.get("event_type")
                if (isinstance(event_type, str) and EXEC_TOPICS.get(event_type) == topic
                        and str(data.get("account_id") or "") == self.account_id):
                    publish_exec_event(self.valkey, self.account_id, data)
            else:
                self.quotes.publish(topic, data)

    def step(self, pubsub=None):
        frames = self.wire.read_frames(16)
        for frame in frames:
            self._receive_frame(frame)
        now = time.monotonic()
        for request_id, record in list(self._inflight.items()):
            if record[2] <= now:
                self._forget_request(request_id, record, now)
        raw = None
        if (self._last_hello is not None and now - self._last_hello < 3
                and not self.wire.writing and len(self._inflight) < _MAX_INFLIGHT):
            if pubsub is not None:
                message = pubsub.get_message(timeout=0)
                if message and message.get("type") == "message":
                    raw = message.get("data")
            if raw is None:
                raw = self.valkey.lpop(self.replies.request_queue)
            if raw is not None:
                self._forward_request(raw)
        sent = self.wire.write_step()
        return bool(frames or raw is not None or sent)

    def run(self, valkey_pubsub=False):
        pubsub = None
        try:
            if valkey_pubsub:
                pubsub = self.valkey.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.replies.request_channel)
            while True:
                if not self.step(pubsub):
                    time.sleep(0.002)
        finally:
            if pubsub is not None:
                pubsub.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", help="资金账号，默认读取本地 .env 或 BIGQMT_ACCOUNT_ID 环境变量")
    parser.add_argument("--port", default="COM11")
    parser.add_argument("--baudrate", type=int, default=DEFAULT_BAUDRATE)
    parser.add_argument("--max-frame-bytes", type=int, default=DEFAULT_MAX_FRAME_BYTES)
    parser.add_argument("--valkey-url")
    parser.add_argument("--valkey-pubsub", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    local_settings = local_serial_settings()
    args.account_id = ((args.account_id or "").strip() or local_settings["account_id"]
                       or os.environ.get("BIGQMT_ACCOUNT_ID", "").strip())
    args.valkey_url = (args.valkey_url or local_settings["valkey_url"]
                       or os.environ.get("BIGQMT_SERIAL_VALKEY_URL")
                       or os.environ.get("BIGQMT_VALKEY_URL") or "valkey://127.0.0.1:6379/5")
    if not args.account_id:
        _log.warning("未配置资金账号。请在 .env 中设置 BIGQMT_ACCOUNT_ID，或传入 --account-id。")
        return 2
    try:
        from valkey import Valkey
        from valkey.backoff import NoBackoff
        from valkey.retry import Retry
    except ImportError:
        parser.exit(1, "请在桥接进程的 Python 环境中安装 xtquant-big-convert[valkey,serial]。\n")

    client = port = None
    endpoint = _valkey_endpoint(args.valkey_url)
    try:
        url = normalize_valkey_url(args.valkey_url)
    except ValueError as exc:
        parser.error(str(exc))

    def describe(exc):
        # 从异常消息中移除 Valkey URL 可能包含的凭据。
        return _describe(exc).replace(args.valkey_url, endpoint).replace(url, endpoint)

    try:
        client = Valkey.from_url(url, socket_timeout=2, socket_connect_timeout=2,
                                retry=Retry(NoBackoff(), 0))
        try:
            client.ping()
        except Exception as exc:
            _log.error(
                "无法连接 Valkey %s（%s）。桥接从该实例读取 RPC 请求，请通过 --valkey-url "
                "或 BIGQMT_SERIAL_VALKEY_URL 指定实际发送请求的 Valkey 实例及数据库。",
                endpoint, describe(exc))
            return 1
        try:
            port = open_serial(args.port, args.baudrate)
        except Exception as exc:
            _log.error(
                "无法打开串口 %s（波特率 %s，%s）。请检查 com0com 端口对和端口名称，"
                "并确认 QMT 或其他桥接进程未占用该端口。",
                args.port, args.baudrate, describe(exc))
            return 1
        bridge = SerialValkeyBridge(client, port, args.account_id, args.max_frame_bytes)
        _log.info("Valkey 串口桥接已启动，串口 %s，波特率 %s，Valkey %s，账号 %s",
                  args.port, args.baudrate, endpoint, args.account_id)
        bridge.run(valkey_pubsub=args.valkey_pubsub)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        module = type(exc).__module__ or ""
        if module.startswith("valkey"):
            hint = ("运行期间 Valkey %s 连接中断。连接恢复后，先重启桥接，再重启 QMT 策略。" % endpoint)
        elif module.startswith("serial"):
            hint = ("运行期间串口 %s 连接中断。请关闭桥接和 QMT，检查 com0com 端口对后重启两端。" % args.port)
        else:
            hint = ""
        _log.error("桥接已停止（%s）。桥接不会重放已转发的请求，请先查询委托和成交再决定是否重试。%s",
                   describe(exc), hint)
        return 1
    finally:
        if port is not None:
            port.close()
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
