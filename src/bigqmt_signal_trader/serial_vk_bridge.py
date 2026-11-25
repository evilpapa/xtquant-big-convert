"""在 QMT 进程外运行，通过串口转发已有的 VK RPC 消息。"""

import argparse
import logging
import math
import os
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit

from .adapters.vk_common import normalize_vk_url
from .exec_events import EXEC_TOPICS, publish_exec_event
from .local_env import local_serial_settings
from .quote_push_channel import VkQuotePushChannel
from .serial_bridge_features import DOWNLOAD_METHODS, SerialBridgeFeatures
from .vk_rpc import METHOD_ALIASES, ORDER_METHODS
from .transports.vk_transport import VkTransport, _loads
from .transports.serial_transport import (
    DEFAULT_BAUDRATE, DEFAULT_MAX_FRAME_BYTES, SerialFrames, open_serial,
)


_log = logging.getLogger(__name__)
_REQUEST_ID = re.compile(r"[a-zA-Z0-9_.:-]{1,128}\Z")
_MAX_INFLIGHT = 16
_MAX_LOG_CHARS = 400

# 桥接常驻：VK 短暂超时或连接闪断不应终止进程。串口继续收发，
# 暂停从 VK 取请求并退避重试；应答发布失败时丢弃该条（客户端会超时），
# 与桥接退出时的语义一致，但不影响后续请求和串口会话。
DEFAULT_VK_TIMEOUT = 5.0
_VK_RETRY_INITIAL = 1.0
_VK_RETRY_MAX = 10.0
_VK_LINK_LOG_INTERVAL = 30.0


def _is_vk_error(exc):
    """VK/Redis 客户端层面的异常（超时、断连、协议错误）。"""
    module = (type(exc).__module__ or "").split(".", 1)[0]
    return module in ("valkey", "redis")


def _preview(value):
    """返回用于日志的简短表示，避免超长参数刷屏。"""
    try:
        text = repr(value)
    except Exception:
        return "<unprintable>"
    if len(text) > _MAX_LOG_CHARS:
        return text[:_MAX_LOG_CHARS] + "...(truncated)"
    return text


@dataclass(slots=True)
class _InFlight:
    method: str
    deadline: float
    reply: object
    background: bool = False


def _vk_endpoint(url):
    """返回可写入日志的连接地址 host:port/db，省略凭据。"""
    try:
        parts = urlsplit(url)
        port = parts.port or 6379
    except ValueError:
        return "the configured VK URL"
    if not parts.hostname:
        return "the configured VK URL"
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


class SerialVkBridge:
    def __init__(self, vk_client, port, account_id, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
                 vk_endpoint="VK"):
        self.vk = vk_client
        self.account_id = str(account_id or "").strip()
        if not self.account_id or len(self.account_id) > 128:
            raise ValueError("a valid account_id is required")
        self.vk_endpoint = vk_endpoint or "VK"
        self.wire = SerialFrames(port, max_frame_bytes)
        self.replies = VkTransport(vk_client, account_id=self.account_id)
        self.quotes = VkQuotePushChannel(vk_client, account_id=self.account_id)
        self.features = SerialBridgeFeatures(vk_client, self.account_id)
        self._inflight = {}
        self._uncertain_writes = OrderedDict()
        self._session = None
        self._last_hello = None
        self._vk_cooldown_until = 0.0
        self._vk_retry_delay = 0.0
        self._vk_down_since = None
        self._vk_last_log = 0.0
        self._vk_dropped = 0

    # ---- VK 短暂故障的退避与恢复 -------------------------------------------
    def _vk_ready(self, now):
        return now >= self._vk_cooldown_until

    def _vk_failed(self, exc, now):
        described = _describe(exc)
        if self._vk_down_since is None:
            self._vk_down_since = now
            self._vk_retry_delay = _VK_RETRY_INITIAL
            self._vk_last_log = now
            _log.warning(
                "VK %s 暂时不可用（%s）；桥接保持运行，串口继续收发，%.1f 秒后重试",
                self.vk_endpoint, described, self._vk_retry_delay)
        else:
            self._vk_retry_delay = min(_VK_RETRY_MAX,
                                       max(_VK_RETRY_INITIAL, self._vk_retry_delay * 2))
            if now - self._vk_last_log >= _VK_LINK_LOG_INTERVAL:
                self._vk_last_log = now
                _log.warning(
                    "VK %s 仍不可用（%s）；已暂停转发 RPC 请求，串口继续收发",
                    self.vk_endpoint, described)
        self._vk_cooldown_until = now + self._vk_retry_delay

    def _vk_recovered(self):
        if self._vk_down_since is not None:
            _log.info("VK %s 已恢复，继续转发 RPC 请求", self.vk_endpoint)
        self._vk_down_since = None
        self._vk_retry_delay = 0.0
        self._vk_cooldown_until = 0.0

    def _dispatch(self, action, now, what):
        """发布一条 VK 消息；遇到 VK 故障时退避并丢弃这一条，不中断串口循环。"""
        if self._vk_ready(now):
            try:
                action()
                self._vk_recovered()
                return True
            except Exception as exc:
                if not _is_vk_error(exc):
                    raise
                self._vk_failed(exc, now)
        self._vk_dropped += 1
        if self._vk_dropped <= 3 or self._vk_dropped % 100 == 0:
            _log.warning(
                "VK 不可用，放弃发布%s（累计 %d 条；相关客户端会超时，"
                "按需查询委托和成交）", what, self._vk_dropped)
        return False

    def _forget_request(self, request_id, record, now):
        self._inflight.pop(request_id, None)
        if record.background:
            self._dispatch(lambda: record.reply({"ok": False, "error": (
                f"No serial response for {record.method}; outcome unknown")}),
                now, "下载任务应答")
        if METHOD_ALIASES.get(record.method, record.method) in ORDER_METHODS:
            self._uncertain_writes[request_id] = now + 600
            if len(self._uncertain_writes) > 512:
                self._uncertain_writes.popitem(last=False)
        _log.warning("未收到 %s 的串口应答，桥接不会重放该请求", record.method)

    def _reply_route(self, request_id, ttl):
        fields = {"account_id": self.account_id, "request_id": request_id}
        return dict(fields, ttl_seconds=ttl,
                    reply_list=self.replies.response_list_template.format(**fields))

    def _reply(self, route, method, data=None, error=None):
        self.replies.send_response(route, {
            "schema_version": 1, "request_id": route["request_id"],
            "account_id": self.account_id, "method": method, "ok": error is None,
            "data": data, "error": error or "",
        })

    def _refuse(self, route, method, reason):
        self._reply(route, method, error=reason)

    def _send_request(self, request, record):
        self.wire.begin({"type": "request", "request": request, "session": self._session})
        self._inflight[request["request_id"]] = record

    def _send_background(self, method, params, timeout, reply):
        request = {
            "schema_version": 1, "request_id": uuid.uuid4().hex, "account_id": self.account_id,
            "method": method, "params": params, "timeout_seconds": timeout, "_received_at": time.time(),
        }
        record = _InFlight(method, time.monotonic() + timeout, reply, background=True)
        try:
            self._send_request(request, record)
        except (ValueError, TypeError, RecursionError):
            reply({"ok": False, "error": "Request cannot be encoded within serial frame limit; NOT dispatched"})

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
            _log.warning("已丢弃格式无效的 VK RPC 请求（%s）：%s", type(exc).__name__, _preview(raw))
            return
        method = str(request.get("method") or "").strip()
        params = request.get("params")
        if params is None:
            params = {}
        _log.info("收到 RPC 请求 %s：method=%s params=%s",
                  request_id, method or "<missing>", _preview(params))
        if request_id in self._inflight:
            _log.info("请求 %s 仍在等待串口应答，忽略重复投递", request_id)
            return
        try:
            ttl = max(1, min(600, int(request.get("ttl_seconds") or 60)))
        except (TypeError, ValueError, OverflowError):
            ttl = 60
        route = self._reply_route(request_id, ttl)
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
            self._refuse(route, method, "Serial order requires a fresh VK RPC client timestamp; check client version and clock")
            return
        submitted_at = min(now, sent_at) if valid_timestamp else now
        if submitted_at + timeout <= now:
            self._refuse(route, method, "RequestExpired: queued in VK past the client timeout; NOT dispatched")
            return
        if canonical in DOWNLOAD_METHODS:
            try:
                self.features.handle_download(
                    canonical, params, request_id, timeout - (now - submitted_at),
                    lambda data: self._reply(route, method, data=data),
                )
            except (KeyError, ValueError, TypeError, OverflowError) as exc:
                self._refuse(route, method, str(exc))
            return
        serial_request = {
            "schema_version": request.get("schema_version", 1),
            "request_id": request_id, "account_id": self.account_id,
            "method": method, "params": params, "timeout_seconds": timeout,
            "_received_at": submitted_at,
        }
        record = _InFlight(method, time.monotonic() + max(60, timeout + 10),
                           lambda response: self.replies.send_response(route, response))
        try:
            self._send_request(serial_request, record)
        except (ValueError, TypeError, RecursionError):
            self._refuse(route, method, "Request cannot be encoded within serial frame limit; NOT dispatched")

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
            if record is None or response.get("method") != record.method:
                return
            self._dispatch(lambda: record.reply(response), time.monotonic(), "串口应答")
            self._inflight.pop(request_id, None)
        elif frame.get("type") == "event":
            topic, data = frame.get("topic"), frame.get("data")
            if not isinstance(topic, str) or not isinstance(data, dict):
                return
            now = time.monotonic()
            if topic.startswith("exec:"):
                event_type = data.get("event_type")
                if (isinstance(event_type, str) and EXEC_TOPICS.get(event_type) == topic
                        and str(data.get("account_id") or "") == self.account_id):
                    self._dispatch(
                        lambda: publish_exec_event(self.vk, self.account_id, data),
                        now, "委托成交事件")
            else:
                self._dispatch(lambda: self.quotes.publish(topic, data), now, "行情推送")

    def step(self, pubsub=None):
        frames = self.wire.read_frames(16)
        for frame in frames:
            self._receive_frame(frame)
        now = time.monotonic()
        for request_id, record in list(self._inflight.items()):
            if record.deadline <= now:
                self._forget_request(request_id, record, now)
        raw = None
        background = False
        if self._vk_ready(now):
            self._dispatch(lambda: self.features.poll_waiters(now), now, "下载任务状态")
            try:
                if (self._last_hello is not None and now - self._last_hello < 3
                        and not self.wire.writing and len(self._inflight) < _MAX_INFLIGHT):
                    if pubsub is not None:
                        message = pubsub.get_message(timeout=0)
                        if message and message.get("type") == "message":
                            raw = message.get("data")
                    if raw is None:
                        raw = self.vk.lpop(self.replies.request_queue)
                    if raw is not None:
                        self._forward_request(raw)
                    elif not self._inflight:
                        background = self.features.step(now, self._send_background)
                self._vk_recovered()
            except Exception as exc:
                if not _is_vk_error(exc):
                    raise
                self._vk_failed(exc, now)
                raw = None
        sent = self.wire.write_step()
        return bool(frames or raw is not None or sent or background)

    def run(self, vk_pubsub=False):
        pubsub = None
        try:
            if vk_pubsub:
                pubsub = self.vk.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.replies.request_channel)
            while True:
                try:
                    worked = self.step(pubsub)
                except Exception as exc:
                    # 兜底：_forward_request 等路径上的 VK 故障也不退出进程。
                    if not _is_vk_error(exc):
                        raise
                    self._vk_failed(exc, time.monotonic())
                    worked = True
                if not worked:
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
    parser.add_argument("--vk-url")
    parser.add_argument("--vk-timeout", type=float,
                        help="VK 单条命令的 socket 超时（秒），默认 %s" % DEFAULT_VK_TIMEOUT)
    parser.add_argument("--vk-pubsub", action="store_true")
    args = parser.parse_args(argv)
    vk_timeout = DEFAULT_VK_TIMEOUT if args.vk_timeout is None else args.vk_timeout
    if not 0 < vk_timeout <= 60:
        parser.error("--vk-timeout 必须在 0 到 60 秒之间")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    local_settings = local_serial_settings()
    args.account_id = ((args.account_id or "").strip() or local_settings["account_id"]
                       or os.environ.get("BIGQMT_ACCOUNT_ID", "").strip())
    args.vk_url = (args.vk_url or local_settings["vk_url"]
                       or os.environ.get("BIGQMT_SERIAL_VK_URL")
                       or os.environ.get("BIGQMT_VK_URL") or "valkey://127.0.0.1:6379/5")
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
    endpoint = _vk_endpoint(args.vk_url)
    try:
        url = normalize_vk_url(args.vk_url)
        if args.vk_timeout is not None:
            parts = urlsplit(url)
            params = parse_qsl(parts.query, keep_blank_values=True)
            remaining = [(key, value) for key, value in params if key != "socket_timeout"]
            if len(remaining) != len(params):
                url = parts._replace(query=urlencode(remaining)).geturl()
    except ValueError as exc:
        parser.error(str(exc))

    def describe(exc):
        # 从异常消息中移除 VK URL 可能包含的凭据。
        return _describe(exc).replace(args.vk_url, endpoint).replace(url, endpoint)

    try:
        client = Valkey.from_url(url, socket_timeout=vk_timeout,
                                socket_connect_timeout=min(2.0, vk_timeout),
                                retry=Retry(NoBackoff(), 0))
        try:
            client.ping()
        except Exception as exc:
            _log.error(
                "无法连接 VK %s（%s）。桥接从该实例读取 RPC 请求，请通过 --vk-url "
                "或 BIGQMT_SERIAL_VK_URL 指定实际发送请求的 VK 实例及数据库。",
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
        bridge = SerialVkBridge(client, port, args.account_id, args.max_frame_bytes,
                                vk_endpoint=endpoint)
        _log.info("VK 串口桥接已启动，串口 %s，波特率 %s，VK %s，账号 %s，"
                  "VK 超时 %s 秒（断线自动重试，不退出）",
                  args.port, args.baudrate, endpoint, args.account_id, vk_timeout)
        bridge.run(vk_pubsub=args.vk_pubsub)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        module = type(exc).__module__ or ""
        if module.startswith("valkey"):
            hint = (" 运行期间 VK %s 出现无法自动恢复的错误。请检查网络/VK 服务后重启桥接；"
                    "串口会话会随之重建，无需重启 QMT 策略。" % endpoint)
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
