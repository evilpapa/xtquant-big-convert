# coding: utf-8
"""An unreachable valkey must not swallow order/trade callbacks (#145).

Reported from a zmq deployment: every callback logged a full traceback ending
in `valkey.exceptions.TimeoutError: Timeout connecting to server`, and no
callbacks arrived. The reporter's own guess -- "好像是用了 valkey 模式回调的?" --
was right, and the reason is a conflation:

    valkey_client = _exec_event_valkey(config)
    if valkey_client is not None:
        return valkey_client          # preferred whenever "available"

"Available" was only ever "configured". valkey-py builds its client lazily and
does not dial until the first command, so a stale valkey block in the config
yields a perfectly healthy-looking client that times out on every publish --
while the zmq push channel, already running for whole-quote data and working
fine, is never even consulted.

Three failures at once: the callback is lost, the working channel stands idle,
and the log fills with one traceback per event.

So: publish falls back to the push channel when the sink fails, valkey is
demoted after repeated failures rather than retried forever, and the traceback
is throttled -- full detail the first few times (issue #76 needed it), a
one-liner after that.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy


class Sink(object):
    def __init__(self, name, fail=False):
        self.name = name
        self.fail = fail
        self.published = []

    def __repr__(self):
        return "<Sink %s>" % self.name


class FakeExecEvents(object):
    """Stands in for the exec_events module: publish routes to the sink."""

    def publish_exec_event(self, sink, account_id, event):
        if sink.fail:
            raise RuntimeError("Timeout connecting to server")
        sink.published.append((account_id, event))


class _StrategyState(unittest.TestCase):
    """Every test here pokes module globals; put them back."""

    def setUp(self):
        self._service = strategy._quote_subscription_service
        self._state = dict(strategy._exec_sink_state)
        self._valkey = strategy._exec_event_valkey
        strategy._exec_sink_state.update(
            {"valkey_failures": 0, "reports": 0, "demoted": False})

    def tearDown(self):
        strategy._quote_subscription_service = self._service
        strategy._exec_sink_state.clear()
        strategy._exec_sink_state.update(self._state)
        strategy._exec_event_valkey = self._valkey

    def _with_channel(self, channel):
        strategy._quote_subscription_service = (object(), channel) if channel else None

    def _with_valkey(self, client):
        strategy._exec_event_valkey = lambda config: client


class SinkChoiceTest(_StrategyState):
    def test_valkey_wins_while_it_works(self):
        valkey, channel = Sink("valkey"), Sink("channel")
        self._with_valkey(valkey)
        self._with_channel(channel)

        self.assertIs(strategy._exec_event_sink({}), valkey)


    def test_a_demoted_valkey_hands_over_to_the_channel(self):
        valkey, channel = Sink("valkey"), Sink("channel")
        self._with_valkey(valkey)
        self._with_channel(channel)
        strategy._exec_sink_state["demoted"] = True

        self.assertIs(strategy._exec_event_sink({}), channel)

    def test_neither_available_is_none_not_a_crash(self):
        self._with_valkey(None)
        self._with_channel(None)

        self.assertIsNone(strategy._exec_event_sink({}))


class FallbackTest(_StrategyState):
    def setUp(self):
        _StrategyState.setUp(self)
        self.events = FakeExecEvents()

    def test_a_working_sink_just_publishes(self):
        sink = Sink("valkey")
        self._with_channel(Sink("channel"))

        ok = strategy._publish_one(self.events, sink, "acct", {"e": 1}, "order", {})

        self.assertTrue(ok)
        self.assertEqual(len(sink.published), 1)

    def test_a_failing_sink_falls_back_to_the_push_channel(self):
        """Without this the callback is simply lost -- the client never learns
        the order happened."""
        valkey, channel = Sink("valkey", fail=True), Sink("channel")
        self._with_channel(channel)

        ok = strategy._publish_one(self.events, valkey, "acct", {"e": 1}, "order", {})

        self.assertTrue(ok)
        self.assertEqual(len(channel.published), 1)

    def test_no_channel_to_fall_back_to_reports_failure(self):
        valkey = Sink("valkey", fail=True)
        self._with_channel(None)

        ok = strategy._publish_one(self.events, valkey, "acct", {"e": 1}, "order", {})

        self.assertFalse(ok)

    def test_it_does_not_retry_the_same_sink_as_its_own_fallback(self):
        channel = Sink("channel", fail=True)
        self._with_channel(channel)

        ok = strategy._publish_one(self.events, channel, "acct", {"e": 1}, "order", {})

        self.assertFalse(ok)
        self.assertEqual(channel.published, [])

    def test_success_clears_the_failure_streak(self):
        """A valkey that recovers must not stay one failure away from demotion."""
        sink = Sink("valkey")
        strategy._exec_sink_state["valkey_failures"] = 2

        strategy._publish_one(self.events, sink, "acct", {"e": 1}, "order", {})

        self.assertEqual(strategy._exec_sink_state["valkey_failures"], 0)


class DemotionTest(_StrategyState):
    def setUp(self):
        _StrategyState.setUp(self)
        self.events = FakeExecEvents()

    def test_repeated_failures_demote_valkey(self):
        valkey, channel = Sink("valkey", fail=True), Sink("channel")
        self._with_channel(channel)

        for _ in range(strategy._EXEC_VALKEY_FAILURE_LIMIT):
            strategy._publish_one(self.events, valkey, "acct", {"e": 1}, "order", {})

        self.assertTrue(strategy._exec_sink_state["demoted"])

    def test_one_failure_is_not_enough(self):
        """A blip should not permanently give up valkey and its replay streams."""
        valkey = Sink("valkey", fail=True)
        self._with_channel(Sink("channel"))

        strategy._publish_one(self.events, valkey, "acct", {"e": 1}, "order", {})

        self.assertFalse(strategy._exec_sink_state["demoted"])

    def test_it_does_not_demote_with_nowhere_to_go(self):
        """Demoting to nothing would turn a noisy failure into a silent one."""
        valkey = Sink("valkey", fail=True)
        self._with_channel(None)

        for _ in range(strategy._EXEC_VALKEY_FAILURE_LIMIT + 2):
            strategy._publish_one(self.events, valkey, "acct", {"e": 1}, "order", {})

        self.assertFalse(strategy._exec_sink_state["demoted"])

    def test_the_limit_is_a_few_events_not_one_and_not_hundreds(self):
        self.assertGreaterEqual(strategy._EXEC_VALKEY_FAILURE_LIMIT, 2)
        self.assertLessEqual(strategy._EXEC_VALKEY_FAILURE_LIMIT, 10)


class ThrottleTest(_StrategyState):
    """Issue #76 earned the traceback; #145 showed it needs a ceiling."""

    def setUp(self):
        _StrategyState.setUp(self)
        self.logged = []
        self._log_err = strategy._log_err
        strategy._log_err = lambda where, message: self.logged.append(message)

    def tearDown(self):
        strategy._log_err = self._log_err
        _StrategyState.tearDown(self)

    @staticmethod
    def _fail_once():
        """Report from inside an except block, the way _publish_one does.

        format_exc() outside one returns "NoneType: None"; calling it directly
        would assert against something the real call site never produces.
        """
        try:
            raise RuntimeError("Timeout connecting to server")
        except RuntimeError as exc:
            strategy._note_exec_publish_failure("order", exc)

    def test_the_first_failures_carry_the_full_traceback(self):
        self._fail_once()

        self.assertIn("Traceback", self.logged[0])

    def test_a_persistent_failure_stops_printing_a_traceback_every_time(self):
        for _ in range(30):
            self._fail_once()

        with_traceback = [m for m in self.logged if "Traceback" in m]
        self.assertLessEqual(len(with_traceback), 3)

    def test_it_still_says_something_occasionally(self):
        """Silence would be its own bug -- the events are still being lost."""
        for _ in range(120):
            self._fail_once()

        later = [m for m in self.logged if "已连续失败" in m]
        self.assertTrue(later)
        self.assertIn("已连续失败", later[0])


if __name__ == "__main__":
    unittest.main()
