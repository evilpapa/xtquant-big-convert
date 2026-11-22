"""大 QMT RPC 桥接服务的 Valkey 传输实现。

这是参考实现，也是默认传输方式。它完整保留了原有
``ValkeyPubSubRpcService`` 的通信行为：

* 客户端 ``send_request``：用 ``RPUSH`` 将经过 Base64 混淆的请求放入对应账号的请求队列，
  再用 ``BLPOP`` 从该请求专属的响应列表取回响应，必要时改用 ``GET response_key``。
  若调用方给 ``call_valkey_rpc`` 传入 ``transport="pubsub"``，
  仍可使用 ``pubsub`` 传输方式。
* 服务端接收：运行两个后台循环，一个通过 ``pubsub.subscribe`` 订阅消息，另一个通过
  ``brpop`` 读取队列。两个循环都会将收到的数据交给已注册的 ``on_request`` 回调。
* 服务端 ``send_response``：将响应分别写入 ``reply_key``（``SETEX``）和
  ``reply_list``（``RPUSH`` + ``EXPIRE``），并发布到 ``reply_channel``（``PUBLISH``）。

模块级辅助函数 :func:`call_valkey_rpc` 保留原有签名，并将实际处理交给此传输实现，
因此现有调用方和 ``bench_latency.py`` 无需修改。
"""

import threading
import time
import traceback
import uuid

# 在导入此模块的线程上立即预加载 idna 编解码器。
#
# socket.getaddrinfo(host) 使用 idna 编码主机名。编解码器按需加载，路径为
# codecs.lookup → encodings.search_function → import encodings.idna →
# stringprep → unicodedata（位于 DLLs/ 的 C 扩展）。在大 QMT 中，首次 Valkey
# 连接发生在 init 后的第一轮 tick，由 adjust 线程（C++ 定时器回调）发起。
# 沙箱中的导入器可能无法在该线程加载扩展模块（#135 记录了 importlib.reload
# 遇到的同类问题）。search_function 会吞掉 ImportError，随后 codecs.lookup
# 抛出 "LookupError: unknown encoding: idna"。这曾导致策略首次运行时只有一轮
# 队列轮询失败；下一轮重连成功后，相关模块进入 sys.modules。QMT 在策略重新运行时
# 会保留 sys.modules，因此问题没有再出现。
#
# 此处导入发生在主线程的 init() 构建传输实例时，此时尚未创建 adjust 定时器。
# 第一层：导入将整条依赖链放入 sys.modules，后续 __import__ 直接命中缓存，
# 无需调用沙箱中的模块查找器。第二层：预先查找会填充解释器的编解码器缓存，
# 让 adjust 线程中的 codecs.lookup('idna') 连 search_function 都无需调用。
# PyInstaller 也用同样的办法处理此错误。这里捕获异常，以免沙箱拒绝预加载时
# 导致整个传输模块无法导入；如有问题，实际连接时仍会报错。
try:
    import codecs as _codecs
    import encodings.idna  # noqa: F401
    _codecs.lookup("idna")
except (ImportError, LookupError):
    pass

from ..adapters.valkey_common import decode_text
from ..valkey_rpc import (
    decode_rpc_request_payload,
    encode_rpc_request_payload,
)
from .base import RpcTransport, TransportTimeout

import json  # noqa: E402  （保留在此处，由传输层统一负责通信编码）


REQUEST_CHANNEL_TEMPLATE = "bigqmt:rpc:req:{account_id}"
REQUEST_QUEUE_TEMPLATE = "bigqmt:rpc:queue:{account_id}"
RESPONSE_CHANNEL_TEMPLATE = "bigqmt:rpc:resp:{account_id}:{request_id}"
RESPONSE_LIST_TEMPLATE = "bigqmt:rpc:respq:{account_id}:{request_id}"
RESPONSE_KEY_TEMPLATE = "bigqmt:rpc:resp:{account_id}:{request_id}"


def _format(template, account_id, request_id):
    if not template:
        return ""
    return template.format(account_id=account_id, request_id=request_id)


def _loads(raw_payload):
    """将传输数据（bytes、str 或 dict）解码为请求字典。"""
    if isinstance(raw_payload, dict):
        return dict(raw_payload)
    text = decode_text(raw_payload)
    text = decode_rpc_request_payload(text)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("rpc payload must be a json object")
    return payload


def _is_valkey_timeout(exc):
    name = exc.__class__.__name__.lower()
    module = getattr(exc.__class__, "__module__", "")
    text = str(exc).lower()
    return (module.startswith("valkey.") and "timeout" in name
            or "timeout reading from socket" in text)


def _slim_pipeline_error(exc, keep_chars=500):
    """截去流水线错误消息中内嵌的响应数据。

    批量命令失败时，valkey-py 会把完整的命令参数写进错误消息，这里的参数就是
    响应 JSON。因此，当 Valkey 因内存不足拒绝写入较大的 K 线响应时，str(exc)
    可能长达数 MB（复现时，5 MB 的响应产生了约 5 MB 的错误消息；生产环境中，
    几条这样的日志就累积成了 542 MB 的日志文件，拖慢桥接服务直至无法提供服务）。
    保留异常类型和消息开头，足以看到 "OOM command not allowed"；其余内容用
    省略标记替代，避免异常堆栈和日志随响应体增大。
    """
    msg = str(exc)
    if len(msg) <= keep_chars:
        return exc
    slimmed = "%s ...[%d chars of embedded payload omitted]" % (
        msg[:keep_chars], len(msg) - keep_chars)
    try:
        return type(exc)(slimmed)
    except Exception:
        return RuntimeError(slimmed)


class ValkeyTransport(RpcTransport):
    """基于 Valkey 的传输实现，负责 rpush、blpop、brpop、publish 和 setex 操作。"""

    name = "valkey"
    # adjust 线程的 LPOP 超时后，暂停轮询；若连续超时，暂停时间逐次翻倍，
    # 直至达到上限。首次收到 LPOP 响应后重置。Valkey 主机不可用时，每次探测
    # 都会等满 socket_connect_timeout（1.5 秒）。如果不暂停，每轮 tick 都会
    # 等待一次（2026-09-17 实盘中，1.5 秒一轮的情况持续了 3.5 小时）。
    DRAIN_TIMEOUT_BACKOFF_SECONDS = 5.0
    DRAIN_TIMEOUT_BACKOFF_MAX_SECONDS = 30.0

    def __init__(
        self,
        valkey_client,
        account_id="",
        response_valkey_client=None,
        request_channel_template=REQUEST_CHANNEL_TEMPLATE,
        request_queue_template=REQUEST_QUEUE_TEMPLATE,
        response_channel_template=RESPONSE_CHANNEL_TEMPLATE,
        response_list_template=RESPONSE_LIST_TEMPLATE,
        response_key_template=RESPONSE_KEY_TEMPLATE,
        response_ttl_seconds=60,
        queue_poll_interval_seconds=0.02,
        debug_log_limit=0,
        print_prefix="[bigqmt_rpc]",
    ):
        super(ValkeyTransport, self).__init__(account_id=account_id, print_prefix=print_prefix)
        self.listen_valkey = valkey_client
        self.valkey = response_valkey_client or valkey_client
        self.request_channel_template = request_channel_template
        self.request_queue_template = request_queue_template
        self.response_channel_template = response_channel_template
        self.response_list_template = response_list_template
        self.response_key_template = response_key_template
        self.response_ttl_seconds = int(response_ttl_seconds)
        self.queue_poll_interval_seconds = max(0.001, float(queue_poll_interval_seconds))
        self.debug_log_limit = int(debug_log_limit)
        self._received_count = 0
        self._published_count = 0
        self._pubsub = None
        self._thread = None
        self._queue_thread = None
        # adjust 线程的 LPOP 超时后的退避状态（见 2026-09-17 故障）。
        # 退避截止时间使用单调时钟，每轮 tick 与 time.monotonic() 比较。
        self._drain_backoff_until = 0.0
        self._drain_backoff_seconds = 0.0
        self._drain_timeouts = 0
        # 服务可通过此钩子检查或拦截收到的原始数据，例如记录调试日志，或选择
        # 立即分发还是延后分发。为 None 时，请求直接交给 on_request 回调。
        self.on_raw_payload = None

    # 与原服务一致的属性
    @property
    def request_channel(self):
        return self.request_channel_template.format(account_id=self.account_id)

    @property
    def request_queue(self):
        return self.request_queue_template.format(account_id=self.account_id)

    def _response_clients(self):
        clients = [self.valkey]
        if self.listen_valkey is not self.valkey:
            clients.append(self.listen_valkey)
        return clients

    # 客户端
    def send_request(self, request, timeout_seconds, transport="queue"):
        """发送 ``request``，阻塞等待响应字典。

        ``transport`` 选择 Valkey 的传输方式：``"queue"``（默认，RPUSH + BLPOP）
        或 ``"pubsub"``（PUBLISH + subscribe）。保留此参数以兼容原有
        ``call_valkey_rpc`` 的签名。
        """
        return _call_valkey_rpc(
            self.listen_valkey,
            self.account_id,
            request,
            timeout_seconds=float(timeout_seconds),
            transport=transport,
            request_channel_template=self.request_channel_template,
            request_queue_template=self.request_queue_template,
            response_channel_template=self.response_channel_template,
            response_list_template=self.response_list_template,
            response_key_template=self.response_key_template,
        )

    # 服务端
    def start_receiving(self, on_request, background_threads=True):
        """启用后台线程时，启动发布订阅监听和队列接收循环。"""
        super(ValkeyTransport, self).start_receiving(on_request)
        if not background_threads:
            print(
                "%s 已启动 queue=%s background_threads=False"
                % (self.print_prefix, self.request_queue)
            )
            return
        if (
            self._thread is not None
            and self._thread.is_alive()
            and self._queue_thread is not None
            and self._queue_thread.is_alive()
        ):
            return
        self._thread = threading.Thread(
            target=self._listen_loop, name="bigqmt-valkey-rpc", daemon=True
        )
        self._queue_thread = threading.Thread(
            target=self._queue_loop, name="bigqmt-valkey-rpc-queue", daemon=True
        )
        self._thread.start()
        self._queue_thread.start()
        print(
            "%s 已启动 channel=%s queue=%s"
            % (self.print_prefix, self.request_channel, self.request_queue)
        )

    def _listen_loop(self):
        while self._running:
            try:
                pubsub = self.listen_valkey.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                pubsub.subscribe(self.request_channel)
                if self.debug_log_limit > 0:
                    print(
                        "%s 已订阅 channel=%s" % (self.print_prefix, self.request_channel)
                    )
                while self._running:
                    message = pubsub.get_message(timeout=1.0)
                    if not self._running:
                        break
                    if not message or message.get("type") != "message":
                        continue
                    self._handle_received_payload(message.get("data"), "pubsub")
            except Exception:
                if not self._running:
                    # 若此线程正在 get_message 中等待，stop() 关闭 pubsub 时
                    # 会断开套接字，导致 valkey ConnectionError 封装 WinError 10038。
                    # 这是正常关闭流程。打印完整异常堆栈会让每次重启看起来都像故障，
                    # 使人习惯性忽略日志，反而容易漏掉真正的故障（#189）。
                    break
                print(
                    "%s 监听器失败：\n%s" % (self.print_prefix, traceback.format_exc())
                )
                time.sleep(1.0)
            finally:
                try:
                    if self._pubsub is not None:
                        self._pubsub.close()
                except Exception:
                    pass
                self._pubsub = None

    def _queue_loop(self):
        while self._running:
            try:
                if self.debug_log_limit > 0:
                    print(
                        "%s 轮询队列 key=%s" % (self.print_prefix, self.request_queue)
                    )
                while self._running:
                    item = self.listen_valkey.brpop(self.request_queue, timeout=1)
                    if not self._running:
                        break
                    if not item:
                        continue
                    raw = (
                        item[1]
                        if isinstance(item, (list, tuple)) and len(item) >= 2
                        else item
                    )
                    self._handle_received_payload(raw, "queue")
            except Exception:
                if not self._running:
                    break          # 正常关闭，与发布订阅循环相同（#189）
                print(
                    "%s 队列监听器失败：\n%s"
                    % (self.print_prefix, traceback.format_exc())
                )
                time.sleep(1.0)

    def _handle_received_payload(self, raw_payload, source):
        self._received_count += 1
        if self.on_raw_payload is not None:
            # 由服务检查或拦截原始数据，例如记录调试日志并选择分发方式。
            self.on_raw_payload(raw_payload, source)
            return
        # 默认解码后直接交给已注册的回调。
        request = _loads(raw_payload)
        self.deliver(request)

    def send_response(self, request, response):
        """将响应分发到 reply_key、reply_list 和 reply_channel。"""
        request_id = response.get("request_id") or request.get("request_id") or ""
        account_id = response.get("account_id") or request.get("account_id") or self.account_id
        payload = json.dumps(response, ensure_ascii=False)
        ttl_seconds = int(request.get("ttl_seconds") or self.response_ttl_seconds)
        response_key = request.get("reply_key") or _format(
            self.response_key_template, account_id, request_id
        )
        response_channel = request.get("reply_channel") or _format(
            self.response_channel_template, account_id, request_id
        )
        response_list = request.get("reply_list")
        if self._send_response_pipelined(
                response_key, response_list, response_channel, ttl_seconds, payload):
            return
        if response_key:
            self._write_response_key(response_key, ttl_seconds, payload)
        if response_list:
            self._push_response_list(response_list, ttl_seconds, payload)
        if response_channel:
            self._publish_response_channel(response_channel, payload)

    def _send_response_pipelined(self, response_key, response_list, response_channel,
                                 ttl_seconds, payload):
        """通过一个客户端，用一次往返发送完整响应（#343）。

        后台监听线程每执行一条 Valkey 命令，都会在套接字 I/O 期间释放 GIL，
        随后再与 QMT 主线程争抢 GIL，每次约耗费一轮 adjust 的时间（#104）。
        逐条执行的路径包含 SETEX、RPUSH、EXPIRE 和 PUBLISH；原先响应客户端和
        监听客户端各执行一遍，因此每条响应要往返八次、重新获取 GIL 八次。
        实盘终端（0.3.50、valkey、rpc_background_threads=True、100nMilliSecond）
        的日志为 ``ping breakdown handle=0.0ms ... publish=500-700ms``：
        请求处理几乎不耗时，延迟主要来自发送响应时的往返。

        第二个客户端连接的是同一个 Valkey，不是另一个接收方，因此这里只把它
        用作备用，第一个客户端抛出异常时才尝试使用它。若所有客户端都不支持
        流水线（例如旧版测试替身），则返回 False，沿用逐条执行命令的路径。
        """
        clients = self._response_clients()
        first_error = None
        attempted = False
        for client in clients:
            pipeline_factory = getattr(client, "pipeline", None)
            if not callable(pipeline_factory):
                continue
            try:
                pipe = pipeline_factory(transaction=False)
            except TypeError:
                pipe = pipeline_factory()
            if not callable(getattr(pipe, "execute", None)):
                continue
            attempted = True
            try:
                publish_index = None
                queued = 0
                if response_key:
                    if ttl_seconds > 0:
                        pipe.setex(response_key, ttl_seconds, payload)
                    else:
                        pipe.set(response_key, payload)
                    queued += 1
                if response_list:
                    pipe.rpush(response_list, payload)
                    queued += 1
                    if ttl_seconds > 0:
                        pipe.expire(response_list, ttl_seconds)
                        queued += 1
                if response_channel:
                    pipe.publish(response_channel, payload)
                    publish_index = queued
                    queued += 1
                results = pipe.execute() if queued else []
            except Exception as exc:
                if first_error is None:
                    first_error = _slim_pipeline_error(exc)
                continue
            if response_channel:
                receivers = 0
                try:
                    receivers = int(results[publish_index] or 0)
                except Exception:
                    pass
                self._published_count += 1
                if self._published_count <= self.debug_log_limit:
                    print("%s 已发布应答，接收方数量=%s" % (self.print_prefix, receivers))
            return True
        if not attempted:
            return False
        raise first_error

    def _write_response_key(self, response_key, ttl_seconds, payload):
        first_error = None
        wrote = 0
        for client in self._response_clients():
            try:
                if ttl_seconds > 0:
                    client.setex(response_key, ttl_seconds, payload)
                else:
                    client.set(response_key, payload)
                wrote += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if wrote <= 0 and first_error is not None:
            raise first_error
        return wrote

    def _push_response_list(self, response_list, ttl_seconds, payload):
        first_error = None
        pushed = 0
        for client in self._response_clients():
            try:
                client.rpush(response_list, payload)
                if ttl_seconds > 0:
                    client.expire(response_list, ttl_seconds)
                pushed += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if pushed <= 0 and first_error is not None:
            raise first_error
        return pushed

    def _publish_response_channel(self, response_channel, payload):
        first_error = None
        receivers = 0
        published = 0
        for client in self._response_clients():
            try:
                receivers += int(client.publish(response_channel, payload) or 0)
                published += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if published <= 0 and first_error is not None:
            raise first_error
        self._published_count += 1
        if self._published_count <= self.debug_log_limit:
            print("%s 已发布应答，接收方数量=%s" % (self.print_prefix, receivers))
        return receivers

    # 由策略 adjust 线程调用的队列轮询辅助方法
    def drain_request_queue(self, max_items=20):
        """后台 BRPOP 循环未运行时，改用非阻塞 LPOP 轮询队列。

        当 ``rpc_background_threads=True`` 时，后台线程通过 ``_queue_loop``
        处理这个 Valkey 列表。此时在 adjust 线程执行 LPOP 会把
        ``get_market_data_ex`` 等读取型 RPC 抢到 QMT 主线程处理，打乱原本
        100 毫秒一轮的节奏（实测轮询耗时 0.6–1.8 秒）。后台线程仍在运行时
        跳过 LPOP，退出后才用 LPOP 接管。
        """
        queue_thread = self._queue_thread
        if queue_thread is not None and queue_thread.is_alive():
            if not getattr(self, "_skip_adjust_queue_drain_logged", False):
                print(
                    "%s 队列 %s 由后台 BRPOP 处理，本轮 adjust 跳过 LPOP"
                    % (self.print_prefix, self.request_queue)
                )
                self._skip_adjust_queue_drain_logged = True
            return 0
        # 使用 getattr：未执行 __init__ 的实例（如测试替身或部分重载）
        # 没有退避状态，直接跳过暂停检查。
        if time.monotonic() < getattr(self, "_drain_backoff_until", 0.0):
            return 0
        processed = 0
        for _ in range(int(max_items)):
            try:
                item = self.listen_valkey.lpop(self.request_queue)
            except Exception as exc:
                if _is_valkey_timeout(exc):
                    # 这次 LPOP 让 adjust 线程一直等到套接字超时。暂停轮询，
                    # 避免下一轮 tick 再等一次。2026-09-17，Valkey 主机故障
                    # 持续 3.5 小时，策略的运行间隔从 100 毫秒拉长至 1.5 秒；
                    # 共 1212 个统计窗口记录了 "adjust cadence: ticks=7 avg=1.509s"，
                    # 策略自身的 tick_app 也随之变慢。
                    self._note_drain_timeout()
                    break
                raise
            if getattr(self, "_drain_timeouts", 0):
                print(
                    "%s LPOP 连续超时 %d 次后已恢复，queue=%s"
                    % (self.print_prefix, self._drain_timeouts, self.request_queue)
                )
                self._drain_timeouts = 0
                self._drain_backoff_seconds = 0.0
            if not item:
                break
            if self.on_raw_payload is not None:
                self.on_raw_payload(item, "queue-drain")
            else:
                self.deliver(_loads(item))
            processed += 1
        return processed

    def _note_drain_timeout(self):
        """记录 adjust 线程的 LPOP 超时，并设置暂停时长。

        连续超时时，依次暂停 5、10、20、30、30……秒；首次收到 LPOP 响应后
        重置退避。暂停期间，drain_request_queue 立即返回 0，让 adjust 保持原有
        节奏，策略自身的任务得以按时执行。请求留在 Valkey 列表中，由首次成功的
        探测取出；若 Valkey 不可用，客户端的请求会失败。
        """
        self._drain_timeouts = getattr(self, "_drain_timeouts", 0) + 1
        previous = getattr(self, "_drain_backoff_seconds", 0.0)
        if previous <= 0:
            self._drain_backoff_seconds = float(self.DRAIN_TIMEOUT_BACKOFF_SECONDS)
        else:
            self._drain_backoff_seconds = min(
                previous * 2.0, float(self.DRAIN_TIMEOUT_BACKOFF_MAX_SECONDS))
        self._drain_backoff_until = time.monotonic() + self._drain_backoff_seconds
        print(
            "%s LPOP 读取队列 %s 超时（第 %d 次），暂停轮询 %.0f 秒，adjust 继续运行"
            % (self.print_prefix, self.request_queue, self._drain_timeouts,
               self._drain_backoff_seconds)
        )

    def stop(self):
        super(ValkeyTransport, self).stop()
        pubsub = self._pubsub
        if pubsub is not None:
            try:
                pubsub.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        queue_thread = self._queue_thread
        if queue_thread is not None and queue_thread.is_alive():
            queue_thread.join(1.0)
        self._thread = None
        self._queue_thread = None
        self._pubsub = None


def _call_valkey_rpc(
    valkey_client,
    account_id,
    request,
    timeout_seconds=3.0,
    transport="queue",
    request_channel_template=REQUEST_CHANNEL_TEMPLATE,
    request_queue_template=REQUEST_QUEUE_TEMPLATE,
    response_channel_template=RESPONSE_CHANNEL_TEMPLATE,
    response_list_template=RESPONSE_LIST_TEMPLATE,
    response_key_template=RESPONSE_KEY_TEMPLATE,
    ttl_seconds=60,
):
    """执行一次客户端 RPC 往返，接收已构建好的请求报文。"""
    request_id = request.get("request_id") or uuid.uuid4().hex
    request_channel = request_channel_template.format(account_id=account_id)
    request_queue = request_queue_template.format(account_id=account_id)
    response_channel = response_channel_template.format(
        account_id=account_id, request_id=request_id
    )
    response_list = response_list_template.format(account_id=account_id, request_id=request_id)
    response_key = response_key_template.format(account_id=account_id, request_id=request_id)
    # 补齐响应路由字段，与原辅助函数的行为一致。
    request = dict(request)
    request.setdefault("request_id", request_id)
    request.setdefault("reply_channel", response_channel)
    request.setdefault("reply_list", response_list)
    request.setdefault("reply_key", response_key)
    request.setdefault("ttl_seconds", ttl_seconds)
    request.setdefault("timeout_seconds", float(timeout_seconds))
    request["_sent_at"] = time.time()
    request["request_id"] = request_id
    payload = encode_rpc_request_payload(request)

    if str(transport or "queue").lower() in ("queue", "list", "blpop"):
        valkey_client.rpush(request_queue, payload)
        valkey_client.expire(request_queue, max(60, int(ttl_seconds)))
        wait_timeout = max(1, int(float(timeout_seconds) + 0.999))
        item = valkey_client.blpop(response_list, timeout=wait_timeout)
        if item:
            raw_response = (
                item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item
            )
            try:
                valkey_client.delete(response_list)
            except Exception:
                pass
            return json.loads(decode_text(raw_response))
        raw_response = valkey_client.get(response_key)
        if raw_response:
            return json.loads(decode_text(raw_response))
        raise TransportTimeout("valkey rpc timeout: %s" % request.get("method"))

    pubsub = valkey_client.pubsub(ignore_subscribe_messages=True)
    try:
        pubsub.subscribe(response_channel)
        valkey_client.publish(request_channel, payload)
        deadline = time.time() + float(timeout_seconds)
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            message = pubsub.get_message(timeout=remaining)
            if not message or message.get("type") != "message":
                continue
            response = json.loads(decode_text(message.get("data")))
            if response.get("request_id") == request_id:
                return response
        raw_response = valkey_client.get(response_key)
        if raw_response:
            return json.loads(decode_text(raw_response))
        raise TransportTimeout("valkey rpc timeout: %s" % request.get("method"))
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
