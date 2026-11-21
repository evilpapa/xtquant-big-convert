import json
import os
import sys
import threading
import time
import unittest
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_push_channel import (
    _HAS_MSGPACK,
    ValkeyQuotePushChannel,
    decode_push_payload,
    encode_push_payload,
)


def _msgpack_packb(payload):
    import msgpack

    return msgpack.packb(payload, use_bin_type=True)


class FakePubSub:
    def __init__(self, valkey_client):
        self._valkey = valkey_client
        self._channels = []
        self._closed = False

    def subscribe(self, *channels):
        self._channels.extend(channels)

    def get_message(self, timeout=0.1):
        # Pull one queued message for a subscribed channel, if any.
        for _ in range(50):
            for channel in self._channels:
                queue = self._valkey.messages.setdefault(channel, [])
                if queue:
                    return {"type": "message", "channel": channel, "data": queue.pop(0)}
            time.sleep(0.01)
        return None

    def close(self):
        self._closed = True


class FakeValkey:
    def __init__(self):
        self.messages = {}

    def publish(self, channel, value):
        self.messages.setdefault(channel, []).append(value)
        return 1

    def pubsub(self, ignore_subscribe_messages=True):
        return FakePubSub(self)


class PayloadCodecTest(unittest.TestCase):
    def test_roundtrip(self):
        payload = {"combo_key": "SH,SZ", "data": {"000001.SZ": {"lastPrice": 10.5}}, "ts": 1.5}
        blob = encode_push_payload(payload)
        self.assertEqual(decode_push_payload(blob), payload)

    def test_binary_blob(self):
        # Wire encoding must be bytes (msgpack or utf-8 json), not str.
        blob = encode_push_payload({"a": 1})
        self.assertIsInstance(blob, (bytes, bytearray))

    def test_decode_json_wire_with_msgpack_available(self):
        # 服务端未装 msgpack 时用 json 兜底编码;若客户端装了 msgpack,
        # msgpack.unpackb 会把 json 文本首字节 '{'(0x7B) 当整数解析并抛
        # ExtraData("unpack(b) received extra data")——真实联调中触发。
        # decode 必须识别并回退到 json。
        payload = {"combo_key": "000001.SZ", "data": {"000001.SZ": {"lastPrice": 11.19}}}
        wire = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.assertIsInstance(wire, bytes)
        self.assertEqual(decode_push_payload(wire), payload)

    def test_decode_msgpack_wire_without_msgpack_available(self):
        # 反向:服务端装了 msgpack、客户端未装(仅 json 可用)。msgpack 字节流
        # 通常不是合法 utf-8,json 解码会失败——此时应显式尝试 msgpack 解码,
        # 而不是吞掉异常返回 None。
        payload = {"combo_key": "SH,SZ", "data": {"600000.SH": {"lastPrice": 9.9}}}
        if not _HAS_MSGPACK:
            self.skipTest("msgpack not installed on this client")
        wire = _msgpack_packb(payload)
        self.assertEqual(decode_push_payload(wire), payload)






class ValkeyPushChannelTest(unittest.TestCase):
    def test_connection_failures_reconnect_and_deliver_without_changing_topics(self):
        for stage in ("pubsub", "subscribe", "get_message"):
            with self.subTest(stage=stage):
                valkey_client = FakeValkey()
                failed = FakePubSub(valkey_client)
                replacement = FakePubSub(valkey_client)
                if stage != "pubsub":
                    setattr(failed, stage, mock.Mock(side_effect=ConnectionError("offline failure")))
                valkey_client.pubsub = mock.Mock(side_effect=[
                    ConnectionError("offline failure") if stage == "pubsub" else failed, replacement])
                client = ValkeyQuotePushChannel(valkey_client, account_id="acct")
                received, done = [], threading.Event()

                def callback(topic, data):
                    received.append((topic, data))
                    done.set()

                valkey_client.publish(client._channel("SH"), encode_push_payload({"data": {"x": 1}}))
                client.start_subscriber(["SH"], callback)
                try:
                    self.assertTrue(done.wait(3.0), "connection recovered but callbacks did not")
                    self.assertEqual(valkey_client.pubsub.call_count, 2)
                    self.assertEqual(received, [("SH", {"x": 1})])
                    if stage != "pubsub":
                        self.assertTrue(failed._closed)
                finally:
                    client.stop()
                self.assertTrue(replacement._closed)

    def test_stop_during_reconnect_backoff_does_not_open_another_connection(self):
        valkey_client = FakeValkey()
        failed = FakePubSub(valkey_client)
        failed.get_message = mock.Mock(side_effect=ConnectionError("offline failure"))
        closed = threading.Event()
        failed.close = closed.set
        valkey_client.pubsub = mock.Mock(return_value=failed)
        client = ValkeyQuotePushChannel(valkey_client)
        client.start_subscriber(["SH"], lambda *_: None)
        thread = client._thread
        try:
            self.assertTrue(closed.wait(2.0))
        finally:
            client.stop()
        self.assertFalse(thread.is_alive())
        self.assertEqual(valkey_client.pubsub.call_count, 1)

    def test_pub_sub_roundtrip(self):
        valkey_client = FakeValkey()
        server = ValkeyQuotePushChannel(valkey_client, account_id="acct")
        server.start_publisher()

        received = []
        done = threading.Event()
        client = ValkeyQuotePushChannel(valkey_client, account_id="acct")

        def on_msg(topic, data):
            received.append((topic, data))
            done.set()

        client.start_subscriber(["SH,SZ"], on_msg)
        try:
            time.sleep(0.1)
            server.publish("SH,SZ", {"000001.SZ": {"lastPrice": 10.5}})
            self.assertTrue(done.wait(2.0), "valkey subscriber did not receive the push")
        finally:
            client.stop()
            server.stop()

        self.assertEqual(received[0][0], "SH,SZ")
        self.assertEqual(received[0][1]["000001.SZ"]["lastPrice"], 10.5)

    def test_channel_name_scoped_by_account(self):
        valkey_client = FakeValkey()
        server = ValkeyQuotePushChannel(valkey_client, account_id="acct")
        server.start_publisher()
        server.publish("SH", {"x": 1})
        server.stop()
        self.assertIn("bigqmt:quote_push:acct:SH", valkey_client.messages)


if __name__ == "__main__":
    unittest.main()
