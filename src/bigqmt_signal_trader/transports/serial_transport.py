"""带大小限制的串口分帧与 QMT 侧 RPC 传输。"""

import json
import logging
import queue
import sys
import threading
import time
import uuid
from binascii import crc32
from collections import deque
from contextlib import suppress

from .base import RpcTransport, TransportError


DEFAULT_BAUDRATE = 921600
DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024
_WRITE_CHUNK = 4096
_READ_CHUNK = 64 * 1024
# 连续写超时到这里就明确报“对端失联”一次（有积压约 3 秒，只有心跳时约 30 秒），
# 避免日志里只剩每 100 条一次的丢帧计数，看不出桥接已经停了。
_STALL_WRITE_TIMEOUTS = 30
_STALL_REPORT_EVERY = 300
_log = logging.getLogger(__name__)


def open_serial(port, baudrate):
    try:
        import serial
    except ImportError as exc:
        raise TransportError("serial transport requires pyserial in this Python environment") from exc
    return serial.Serial(port=port, baudrate=baudrate, timeout=0, write_timeout=0.1)


class SerialFrames:
    """调用方管理串口，每帧由 CRC32、制表符、UTF-8 JSON 和换行符组成。"""

    def __init__(self, port, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES):
        self.port = port
        self.max_frame_bytes = int(max_frame_bytes)
        if self.max_frame_bytes < 1024:
            raise ValueError("max_frame_bytes must be at least 1024")
        self._line = bytearray()
        self._discard = False
        self._lines = deque()
        self._tx = memoryview(b"")
        self._needs_delimiter = True
        self._bad_frames = 0

    def _bad_frame(self, reason):
        self._bad_frames += 1
        if self._bad_frames <= 3 or self._bad_frames % 100 == 0:
            _log.warning("已丢弃无效串口帧：%s（累计 %d 帧）", reason, self._bad_frames)

    def _accept_chunk(self, chunk):
        parts = chunk.split(b"\n")
        for index, part in enumerate(parts):
            complete = index < len(parts) - 1
            if not self._discard:
                if len(self._line) + len(part) > self.max_frame_bytes:
                    self._line.clear()
                    self._discard = True
                    self._bad_frame("超过帧大小上限")
                else:
                    self._line.extend(part)
            if complete:
                if self._line and not self._discard:
                    self._lines.append(bytes(self._line))
                self._line.clear()
                self._discard = False

    def read_frames(self, limit):
        """最多读取一个固定上限的数据块，并解析至多 limit 帧。"""
        if limit <= 0:
            return []
        if not self._lines:
            waiting = self.port.in_waiting
            if waiting:
                self._accept_chunk(self.port.read(min(waiting, _READ_CHUNK)))
        frames = []
        for _ in range(min(limit, len(self._lines))):
            line = self._lines.popleft()
            try:
                check, separator, payload = line.partition(b"\t")
                if (not separator or len(check) != 8
                        or check != ("%08x" % crc32(payload)).encode("ascii")):
                    raise ValueError("checksum mismatch")
                frame = json.loads(payload.decode("utf-8"))
                if not isinstance(frame, dict):
                    raise ValueError("expected a JSON object")
                frames.append(frame)
            except (ValueError, RecursionError):
                self._bad_frame("校验和错误或 JSON 格式无效")
        return frames

    @property
    def writing(self):
        return bool(self._tx)

    def begin(self, frame):
        if self.writing:
            raise RuntimeError("previous serial frame is still being written")
        payload = json.dumps(frame, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(payload) + 9 > self.max_frame_bytes:
            raise ValueError("serial frame exceeds max_frame_bytes")
        line = ("%08x\t" % crc32(payload)).encode("ascii") + payload + b"\n"
        if self._needs_delimiter:
            line = b"\n" + line
            self._needs_delimiter = False
        self._tx = memoryview(line)

    def abort_write(self):
        self._tx = memoryview(b"")
        self._needs_delimiter = True

    def write_step(self):
        if not self.writing:
            return False
        chunk = self._tx[:_WRITE_CHUNK]
        written = self.port.write(chunk)
        if not isinstance(written, int) or not 0 <= written <= len(chunk):
            raise TransportError("invalid serial write count")
        self._tx = self._tx[written:]
        return bool(written)


class SerialTransport(RpcTransport):
    name = "serial"

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]", port="COM10",
                 baudrate=DEFAULT_BAUDRATE, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES):
        super(SerialTransport, self).__init__(account_id, print_prefix)
        self.port = str(port)
        self.baudrate = int(baudrate)
        self.max_frame_bytes = int(max_frame_bytes)
        self._session = uuid.uuid4().hex
        self._incoming = queue.Queue(maxsize=64)
        self._replies = queue.Queue(maxsize=32)
        self._exec_events = queue.Queue(maxsize=64)
        self._quote_events = queue.Queue(maxsize=8)
        self._stopped = threading.Event()
        self._io_thread = None
        self._failed = False
        self._drops = 0
        self._write_timeouts = 0

    def send_request(self, request, timeout_seconds):
        raise TransportError("serial is server-side only; use a VK client via the bridge")

    def start_receiving(self, on_request, background_threads=False):
        if background_threads:
            raise ValueError("serial RPC handlers must be drained on the strategy thread")
        if self._io_thread is not None:
            raise RuntimeError("build a new SerialTransport to reopen the port")
        port = open_serial(self.port, self.baudrate)
        try:
            wire = SerialFrames(port, self.max_frame_bytes)
            super(SerialTransport, self).start_receiving(on_request)
            self._io_thread = threading.Thread(
                target=self._io_loop, args=(wire,), name="bigqmt-serial-io", daemon=True)
            self._io_thread.start()
        except Exception:
            super(SerialTransport, self).stop()
            port.close()
            raise

    def _io_loop(self, wire):
        try:
            write_timeout = getattr(sys.modules.get("serial"), "SerialTimeoutException", ())
            next_hello = 0.0
            while not self._stopped.is_set():
                capacity = self._incoming.maxsize - self._incoming.qsize()
                frames = wire.read_frames(min(capacity, 16))
                if frames:
                    self._link_alive()
                for frame in frames:
                    request = frame.get("request")
                    if (frame.get("type") == "request" and frame.get("session") == self._session
                            and isinstance(request, dict)):
                        request.setdefault("_received_at", time.time())
                        self._incoming.put_nowait(request)
                if not wire.writing:
                    now = time.monotonic()
                    if now >= next_hello:
                        frame = {"type": "hello", "account_id": self.account_id}
                        next_hello = now + 1.0
                    else:
                        frame = self._next_outbound()
                    if frame is not None:
                        self._begin_frame(wire, frame)
                try:
                    sent = wire.write_step()
                except write_timeout:
                    wire.abort_write()
                    self._write_timeout()
                    self._note_drop("写超时，已中止当前帧的发送，继续收发")
                    sent = False
                if sent:
                    self._link_alive()
                if not frames and not sent:
                    self._stopped.wait(0.002)
        except Exception:
            self._failed = True
            _log.exception("串口 %s 通信失败，请检查端口后重启策略", self.port)
        finally:
            wire.port.close()

    def _next_outbound(self):
        for buffer in (self._replies, self._exec_events, self._quote_events):
            with suppress(queue.Empty):
                return buffer.get_nowait()
        return None

    def _begin_frame(self, wire, frame):
        try:
            wire.begin(dict(frame, session=self._session))
        except (ValueError, TypeError, RecursionError):
            if frame["type"] == "response":
                response = frame["response"]
                error = {key: response.get(key) for key in (
                    "schema_version", "request_id", "account_id", "method")}
                error.update(ok=False, data=None, error=(
                    "Response exceeds serial frame limit or failed encoding; "
                    "the RPC was already handled"))
                wire.begin({"type": "response", "response": error, "session": self._session})
            else:
                self._note_drop("事件超过帧大小上限或无法编码")

    def drain_request_queue(self, max_items=20):
        if not self._running or self._failed:
            return 0
        processed = 0
        for _ in range(int(max_items)):
            try:
                request = self._incoming.get_nowait()
            except queue.Empty:
                break
            self.deliver(request)
            processed += 1
        return processed

    def _note_drop(self, reason):
        self._drops += 1
        if self._drops <= 3 or self._drops % 100 == 0:
            _log.warning("串口 %s：%s（累计丢弃 %d 条消息）", self.port, reason, self._drops)

    def _write_timeout(self):
        self._write_timeouts += 1
        if (self._write_timeouts == _STALL_WRITE_TIMEOUTS
                or (self._write_timeouts > _STALL_WRITE_TIMEOUTS
                    and self._write_timeouts % _STALL_REPORT_EVERY == 0)):
            _log.warning(
                "串口 %s 连续写超时 %d 次且未收到对端数据：桥接进程可能已停止或未读取串口，"
                "请检查桥接日志并重启桥接；已转发的请求不会重放，重启前先查委托和成交",
                self.port, self._write_timeouts)

    def _link_alive(self):
        self._write_timeouts = 0

    def _enqueue(self, buffer, frame):
        if self._failed or not self._running:
            self._note_drop("串口连接已断开")
            raise TransportError("serial link is down")
        try:
            buffer.put_nowait(frame)
        except queue.Full:
            self._note_drop("发送队列已满")
            raise TransportError("串口发送队列已满")

    def send_response(self, request, response):
        self._enqueue(self._replies, {"type": "response", "response": response})

    def start_publisher(self):
        pass

    def publish(self, topic, data):
        buffer = self._exec_events if topic.startswith("exec:") else self._quote_events
        with suppress(TransportError):
            self._enqueue(buffer, {"type": "event", "topic": topic, "data": data})

    def stop(self):
        super(SerialTransport, self).stop()
        self._stopped.set()
        thread = self._io_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(1.0)
            if thread.is_alive():
                _log.warning("串口 %s 的收发线程仍在退出", self.port)
