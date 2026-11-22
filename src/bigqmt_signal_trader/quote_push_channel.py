"""服务端向客户端推送全推行情的通道。

Valkey 按账号和订阅组合发布增量行情。载荷优先用 MsgPack 编码，
未安装 msgpack 时用 JSON。
"""

import json
import threading
import time

try:
    import msgpack

    _HAS_MSGPACK = True
except Exception:  # pragma: no cover - 取决于是否安装可选依赖
    msgpack = None
    _HAS_MSGPACK = False


def encode_push_payload(payload):
    """将推送载荷编码为字节，优先使用 MsgPack，回退到 JSON。"""
    if _HAS_MSGPACK:
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def decode_push_payload(blob):
    """解码字节或字符串，兼容收发两端安装不同依赖的情况。

JSON 的起始字节可被 MsgPack 解析为整数并留下多余字节；只有整个载荷
是合法的单个 MsgPack 对象时才使用其结果。
    """
    if blob is None:
        return None
    if isinstance(blob, str):
        blob = blob.encode("utf-8")
    if _HAS_MSGPACK:
        try:
            return msgpack.unpackb(blob, raw=False)
        except Exception:
            pass
    return json.loads(blob.decode("utf-8"))


class QuotePushChannel(object):
    """推送通道接口，实例可启动为发布端或订阅端。"""

    def start_publisher(self):
        raise NotImplementedError

    def start_subscriber(self, topics, on_msg):
        raise NotImplementedError

    def publish(self, topic, data):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class NullQuotePushChannel(QuotePushChannel):
    """无推送通道时忽略发布操作。"""

    def start_publisher(self):
        pass

    def start_subscriber(self, topics, on_msg):
        raise NotImplementedError("no push channel on this deployment")

    def publish(self, topic, data):
        pass

    def stop(self):
        pass


def _unique_accounts(values):
    seen = []
    for value in values:
        text = str(value or "")
        if text and text not in seen:
            seen.append(text)
    return seen


class ValkeyQuotePushChannel(QuotePushChannel):
    def __init__(self, valkey_client, account_id="", channel_template="bigqmt:quote_push:{account_id}:{topic}", print_prefix="[bigqmt_quote_push]", account_ids=None):
        self.valkey = valkey_client
        self.account_id = str(account_id or "")
        self.channel_template = channel_template
        self.print_prefix = print_prefix
        # 推送通道按账号区分。一个桥接服务支持多账号时，每个账号各发布一次，
        # 避免附属账号订阅成功却收不到行情。订阅端只使用自己的账号（#315）。
        self.account_ids = _unique_accounts([self.account_id] + list(account_ids or []))
        self._running = False
        self._pubsub = None
        self._thread = None
        self._subscriber_stop = threading.Event()

    def _channel(self, topic, account_id=None):
        return self.channel_template.format(
            account_id=self.account_id if account_id is None else account_id, topic=topic)

    # 发布端
    def start_publisher(self):
        # Valkey 发布无需额外初始化。
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        for account_id in self.account_ids or [self.account_id]:
            try:
                self.valkey.publish(self._channel(topic, account_id), payload)
            except Exception as exc:
                print("%s 发布 Valkey 行情失败：%s" % (self.print_prefix, exc))

    # 订阅端
    def start_subscriber(self, topics, on_msg):
        self.stop()
        self._running = True
        self._subscriber_stop = threading.Event()
        self._thread = threading.Thread(
            target=self._sub_loop, args=(list(topics or []), on_msg, self._subscriber_stop),
            name="bigqmt-quote-push-sub", daemon=True
        )
        self._thread.start()

    def _sub_loop(self, topics, on_msg, stopped):
        # 接收线程负责重连和关闭连接，使用自己的停止事件，避免新一轮启动唤醒旧线程。
        channels = [self._channel(topic) for topic in topics]
        failures = 0          # 连续重连失败次数，用于日志节流
        last_report = 0.0
        while not stopped.is_set():
            pubsub = None
            try:
                pubsub = self.valkey.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                pubsub.subscribe(*channels)
                while not stopped.is_set():
                    message = pubsub.get_message(timeout=0.2)
                    if stopped.is_set():
                        break
                    if not message or message.get("type") != "message":
                        continue
                    channel = message.get("channel")
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8", errors="ignore")
                    topic = str(channel).rsplit(":", 1)[-1]
                    data = decode_push_payload(message.get("data"))
                    payload_data = data.get("data") if isinstance(data, dict) else data
                    try:
                        on_msg(topic, payload_data)
                    except Exception as exc:
                        print("%s 行情订阅回调失败：%s" % (self.print_prefix, exc))
            except Exception as exc:
                if not stopped.is_set():
                    # 每秒重连，日志在第 1、2、4、8……次失败或距上次满一分钟时输出。
                    failures += 1
                    now = time.time()
                    if (failures & (failures - 1)) == 0 or now - last_report >= 60.0:
                        last_report = now
                        print("%s 正在重新连接 Valkey 行情订阅（第 %d 次）：%s"
                              % (self.print_prefix, failures, exc))
            else:
                failures = 0
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass
            stopped.wait(1.0)

    def stop(self):
        self._running = False
        self._subscriber_stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(1.0)
        self._thread = None
        self._pubsub = None
