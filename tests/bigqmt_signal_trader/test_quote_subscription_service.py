import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_push_channel import (
    NullQuotePushChannel,
    ValkeyQuotePushChannel,
)
from bigqmt_signal_trader.quote_subscription_manager import (
    QuoteSubscriptionManager,
    build_quote_subscription_service,
)


class FakeContextInfo:
    def __init__(self):
        self._next = 0

    def subscribe_whole_quote(self, code_list, callback=None):
        self._next += 1
        return self._next

    def unsubscribe_quote(self, sub_id):
        return 0


class FakeValkey:
    def publish(self, channel, value):
        return 1

    def pubsub(self, ignore_subscribe_messages=True):
        raise AssertionError("not used here")


class BuildQuoteSubscriptionServiceTest(unittest.TestCase):
    def test_disabled_returns_none(self):
        result = build_quote_subscription_service(
            FakeContextInfo(), transport_name="valkey", account_id="acct",
            valkey_client=FakeValkey(), enabled=False,
        )
        self.assertIsNone(result)

    def test_valkey_transport_builds_valkey_channel(self):
        service = build_quote_subscription_service(
            FakeContextInfo(), transport_name="valkey", account_id="acct",
            valkey_client=FakeValkey(), enabled=True,
        )
        manager, channel = service
        self.assertIsInstance(manager, QuoteSubscriptionManager)
        self.assertIsInstance(channel, ValkeyQuotePushChannel)


    pass

    def test_no_valkey_shm_gets_the_null_channel(self):
        """（valkey 被关的部署）没有推送线：空通道静默 noop，而不是
        ValkeyQuotePushChannel(None) 每条事件一行 AttributeError。"""
        service = build_quote_subscription_service(
            FakeContextInfo(), transport_name="shm", account_id="acct",
            valkey_client=None, enabled=True,
        )
        manager, channel = service
        self.assertIsInstance(channel, NullQuotePushChannel)
        channel.start_publisher()
        channel.publish("topic", {"x": 1})   # must not raise
        channel.stop()

    def test_manager_on_push_publisher_is_channel_publish(self):
        service = build_quote_subscription_service(
            FakeContextInfo(), transport_name="valkey", account_id="acct",
            valkey_client=FakeValkey(), enabled=True,
        )
        manager, channel = service
        # The assembled manager must publish through the assembled channel.
        self.assertEqual(manager._on_push_publisher, channel.publish)

    def test_heartbeat_timeout_configurable(self):
        service = build_quote_subscription_service(
            FakeContextInfo(), transport_name="valkey", account_id="acct",
            valkey_client=FakeValkey(), enabled=True, heartbeat_timeout_seconds=30.0,
        )
        manager, _channel = service
        self.assertEqual(manager._heartbeat_timeout, 30.0)


if __name__ == "__main__":
    unittest.main()
