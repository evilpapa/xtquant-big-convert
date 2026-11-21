import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapter_factory import build_app
from bigqmt_signal_trader.adapters.position_sync_valkey import ValkeyPositionSyncSink
from bigqmt_signal_trader.adapters.signal_valkey import ValkeyStreamSignalSource, push_trade_signal
from bigqmt_signal_trader.adapters.state_valkey import ValkeyStateStore
from bigqmt_signal_trader.models import AccountSnapshot, AssetSnapshot, PositionSnapshot


class FakeValkey:
    def __init__(self):
        self.streams = {}
        self.groups = set()
        self.acked = []
        self.kv = {}
        self.hashes = {}
        self.expired = []
        self.next_id = 1

    def xgroup_create(self, stream_key, group_name, id="0-0", mkstream=True):
        key = (stream_key, group_name)
        if key in self.groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        self.groups.add(key)
        self.streams.setdefault(stream_key, [])
        return True

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        result = []
        for stream_key in streams:
            entries = self.streams.get(stream_key, [])[: count or None]
            if entries:
                result.append((stream_key, entries))
        return result

    def xack(self, stream_key, group_name, stream_id):
        self.acked.append((stream_key, group_name, stream_id))
        return 1

    def xadd(self, stream_key, fields, maxlen=None, approximate=False):
        stream_id = "%d-0" % self.next_id
        self.next_id += 1
        self.streams.setdefault(stream_key, []).append((stream_id, fields))
        return stream_id

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return False
        self.kv[key] = value
        return True

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)
        return len(mapping)

    def expire(self, key, seconds):
        self.expired.append((key, seconds))
        return True

    def setex(self, key, seconds, value):
        self.kv[key] = value
        self.expired.append((key, seconds))
        return True


def _payload(**kwargs):
    payload = {
        "signal_id": "sig-valkey-001",
        "account_id": "acct",
        "action": "BUY",
        "stock_code": "600000",
        "amount": 300,
        "created_at": "2026-07-01 09:31:00",
        "expire_at": "2026-07-01 09:36:00",
        "schema_version": 1,
        "force": "false",
    }
    payload.update(kwargs)
    return payload


class ValkeyAdaptersTest(unittest.TestCase):
    def test_stream_source_reads_json_payload_and_acks(self):
        r = FakeValkey()
        stream_key = "bigqmt:signals:acct"
        r.xadd(stream_key, {"payload": json.dumps(_payload())})
        source = ValkeyStreamSignalSource(r, consumer_name="c1")

        signals = source.fetch("acct", 10)
        source.ack(signals[0])

        self.assertEqual(signals[0].signal_id, "sig-valkey-001")
        self.assertFalse(signals[0].force)
        self.assertEqual(r.acked, [(stream_key, "bigqmt-signal-trader", "1-0")])

    def test_stream_source_reads_flat_payload_with_bytes_keys(self):
        r = FakeValkey()
        fields = {key.encode("utf-8"): str(value).encode("utf-8") for key, value in _payload().items()}
        r.streams["bigqmt:signals:acct"] = [("9-0", fields)]
        source = ValkeyStreamSignalSource(r)

        signals = source.fetch("acct", 10)

        self.assertEqual(signals[0].stock_code, "600000")
        self.assertEqual(signals[0].amount, 300)

    def test_state_store_claim_is_idempotent_and_writes_status(self):
        r = FakeValkey()
        source = ValkeyStreamSignalSource(r)
        r.xadd("bigqmt:signals:acct", {"payload": json.dumps(_payload())})
        signal = source.fetch("acct", 1)[0]
        store = ValkeyStateStore(r, account_id="acct")

        self.assertTrue(store.claim(signal, "consumer-a"))
        self.assertFalse(store.claim(signal, "consumer-b"))
        store.mark_finished(signal.signal_id, "SKIPPED", "test")

        status_key = "bigqmt:signal_status:acct:sig-valkey-001"
        self.assertEqual(r.hashes[status_key]["status"], "SKIPPED")
        self.assertEqual(r.hashes[status_key]["message"], "test")

    def test_position_sync_sink_writes_snapshot_and_event(self):
        r = FakeValkey()
        sink = ValkeyPositionSyncSink(r)
        snapshot = AccountSnapshot(
            account_id="acct",
            asset=AssetSnapshot(account_id="acct", cash=100.0, total_asset=1000.0),
            positions={
                "600000.SH": PositionSnapshot(
                    stock_code="600000.SH",
                    volume=100,
                    available=100,
                    cost=10.0,
                    stock_name="PF Bank",
                )
            },
            reason="test",
            updated_at=__import__("datetime").datetime(2026, 7, 1, 9, 31),
        )

        sink.publish(snapshot)

        self.assertIn("bigqmt:positions:acct", r.kv)
        self.assertIn("bigqmt:position_events:acct", r.streams)

    def test_push_trade_signal_uses_account_stream(self):
        r = FakeValkey()

        stream_id = push_trade_signal(r, _payload())

        self.assertEqual(stream_id, "1-0")
        self.assertIn("bigqmt:signals:acct", r.streams)

    def test_factory_wires_valkey_adapters_without_real_valkey(self):
        r = FakeValkey()
        app = build_app(
            config={
                "account_id": "acct",
                "signal_source_type": "valkey",
                "state_store_type": "valkey",
                "position_sync_type": "valkey",
                "valkey_client": r,
            }
        )

        self.assertIsInstance(app.signal_source, ValkeyStreamSignalSource)
        self.assertIsInstance(app.state_store, ValkeyStateStore)
        self.assertIsInstance(app.position_sync_sink, ValkeyPositionSyncSink)


class ValkeyConnectionTest(unittest.TestCase):

    def test_protocol_passed_when_supported(self):
        from unittest import mock

        from bigqmt_signal_trader.adapters import valkey_common

        try:
            import valkey
        except ImportError:
            self.skipTest("valkey-py unavailable")
        with mock.patch.object(valkey, "Valkey") as client:
            valkey_common.build_valkey_client({"host": "h", "port": 6379, "protocol": 3})
        self.assertEqual(client.call_args.kwargs["protocol"], 3)

    def test_both_tcp_url_schemes_build_valkey_clients(self):
        from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client

        try:
            import valkey
        except ImportError:
            self.skipTest("valkey package unavailable")
        for scheme in ("valkey", "redis"):
            with self.subTest(scheme=scheme):
                client = build_valkey_client({
                    "url": scheme + "://user:p%40ss@localhost:6399/7",
                })
                self.assertIsInstance(client, valkey.Valkey)
                kwargs = client.connection_pool.connection_kwargs
                self.assertEqual(kwargs["host"], "localhost")
                self.assertEqual(kwargs["port"], 6399)
                self.assertEqual(kwargs["db"], 7)
                self.assertEqual(kwargs["username"], "user")
                self.assertEqual(kwargs["password"], "p@ss")

    def test_protocol_default_is_resp2(self):
        from unittest import mock

        from bigqmt_signal_trader.adapters import valkey_common

        try:
            import valkey
        except ImportError:
            self.skipTest("valkey-py unavailable")
        with mock.patch.object(valkey, "Valkey") as client:
            valkey_common.build_valkey_client({"host": "h", "port": 6379})
        self.assertEqual(client.call_args.kwargs["protocol"], 2)
        retry = client.call_args.kwargs["retry"]
        command = mock.Mock(side_effect=valkey.TimeoutError)
        with self.assertRaises(valkey.TimeoutError):
            retry.call_with_retry(command, lambda error: None)
        command.assert_called_once()


if __name__ == "__main__":
    unittest.main()
