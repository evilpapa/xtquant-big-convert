"""Server→client whole-quote push channel.

The RPC transport is request/response only; whole-quote data needs the opposite
direction — the server pushes each incremental tick batch to every client
subscribed to that combination. This module provides an abstract channel with
* :class:`RedisQuotePushChannel` — redis ``publish``/``subscribe`` on a
  per-account, per-combination channel, for redis deployments.

Wire encoding is msgpack when available (smaller + faster for the
``{code: {field: number}}`` payload shape), falling back to stdlib json so the
channel stays usable without the optional dependency.
"""

import json
import threading
import time

try:
    import msgpack

    _HAS_MSGPACK = True
except Exception:  # pragma: no cover - depends on optional dependency
    msgpack = None
    _HAS_MSGPACK = False


def encode_push_payload(payload):
    """Encode a push payload dict to bytes (msgpack preferred, json fallback)."""
    if _HAS_MSGPACK:
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def decode_push_payload(blob):
    """Inverse of :func:`encode_push_payload`. Accepts bytes or str.

    Encoding is not symmetric across deployments: a server without msgpack
    falls back to json while a client with msgpack installed decodes with
    msgpack — ``msgpack.unpackb`` then raises ``ExtraData`` on the json text
    (its first byte ``{`` parses as an int, leaving trailing bytes). So try
    msgpack first, and fall back to json when the bytes are not a single
    valid msgpack object.
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
    """Abstract push channel. Server side: ``start_publisher`` + ``publish``.
    Client side: ``start_subscriber(topics, on_msg)``. A single instance may act
    as publisher or subscriber depending on which start method is called."""

    def start_publisher(self):
        raise NotImplementedError

    def start_subscriber(self, topics, on_msg):
        raise NotImplementedError

    def publish(self, topic, data):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class NullQuotePushChannel(QuotePushChannel):
    """No-op push channel for deployments with no push wire (pipe, or redis
    absent). Publishing is a silent no-op -- the alternative was
    RedisQuotePushChannel(None), whose publish raised AttributeError per event.
    """

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


class RedisQuotePushChannel(QuotePushChannel):
    def __init__(self, redis_client, account_id="", channel_template="bigqmt:quote_push:{account_id}:{topic}", print_prefix="[bigqmt_quote_push]", account_ids=None):
        self.redis = redis_client
        self.account_id = str(account_id or "")
        self.channel_template = channel_template
        self.print_prefix = print_prefix
        # Every account this publisher speaks for (#315). The channel is
        # keyed by account, and a single-instance multi-account bridge
        # (BIGQMT_ACCOUNT_TYPE_MAP) serves several: a client configured with
        # the secondary account subscribes bigqmt:quote_push:<secondary>:*,
        # its subscribe RPC lands on the shared handlers and succeeds, and
        # every push went to bigqmt:quote_push:<primary>:* -- "subscribed,
        # no callbacks". Market data is not account-specific, so publish
        # once per account. The client role uses account_id alone.
        self.account_ids = _unique_accounts([self.account_id] + list(account_ids or []))
        self._running = False
        self._pubsub = None
        self._thread = None
        self._subscriber_stop = threading.Event()

    def _channel(self, topic, account_id=None):
        return self.channel_template.format(
            account_id=self.account_id if account_id is None else account_id, topic=topic)

    # -- server side ---------------------------------------------------------
    def start_publisher(self):
        # Redis publish needs no setup; present for interface symmetry.
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        for account_id in self.account_ids or [self.account_id]:
            try:
                self.redis.publish(self._channel(topic, account_id), payload)
            except Exception as exc:
                print("%s redis publish failed: %s" % (self.print_prefix, exc))

    # -- client side ---------------------------------------------------------
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
        # This receiver owns reconnect and closes its own connections. Capture
        # its stop event so a later start cannot revive an old blocked receiver.
        channels = [self._channel(topic) for topic in topics]
        failures = 0          # consecutive reconnect failures, for log throttling
        last_report = 0.0
        while not stopped.is_set():
            pubsub = None
            try:
                pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
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
                        print("%s subscriber callback failed: %s" % (self.print_prefix, exc))
            except Exception as exc:
                if not stopped.is_set():
                    # Retry every second -- quotes should come back fast -- but
                    # do NOT print every second. Redis down over a weekend is
                    # 3600 lines/hour, and this repo has been bitten by log
                    # volume before (#139 wrote one line 16 times; #144 grew a
                    # log without bound because rotation could never run). The
                    # zmq ROUTER rebuild backs its reporting off for the same
                    # reason (#240). Report attempts 1, 2, 4, 8 ... then at most
                    # once a minute: the first failure stays loud and a long
                    # outage stays readable.
                    failures += 1
                    now = time.time()
                    if (failures & (failures - 1)) == 0 or now - last_report >= 60.0:
                        last_report = now
                        print("%s redis subscriber reconnecting (attempt %d): %s"
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
