# coding: utf-8
"""A deployment must be able to say it has no redis (#147).

Every consumer of the redis client already guards the same way:

    redis_config = dict(config.get("redis") or {})
    if not redis_config:
        return None

and every one of those guards was dead code, because configure_runtime emitted
the block unconditionally from module defaults:

    redis={"host": REDIS_HOST,     # "127.0.0.1"
           "port": REDIS_PORT,     # 6379
           ...}

So config["redis"] was never empty and "I have no redis" was not expressible.
The reporter of #145 -- whose broker QMT does not whitelist the redis import at
all -- had no lever except patching _exec_event_redis to key off the transport,
which would have killed four unrelated features on zmq (the order-identity
store and download jobs were made transport-independent one day earlier, in
f20c58c and 053c9dc, for exactly the opposite reason).

redis_enabled=False now empties the block, and the existing guards do the rest:
nothing dials redis even once. transport=redis overrides it, because there the
bridge itself needs redis and honouring the switch would break the RPC rather
than the optional extras.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_redis_rpc_runtime as runtime


class _RuntimeState(unittest.TestCase):
    KEYS = ("REDIS_ENABLED", "RPC_TRANSPORT", "REDIS_HOST", "REDIS_PORT", "REDIS_DB",
            "REDIS_ENABLED_EXPLICIT")

    def setUp(self):
        self._saved = {k: getattr(runtime, k) for k in self.KEYS}

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(runtime, k, v)

    def _configure(self, enabled, transport, explicit=True):
        runtime.REDIS_ENABLED = enabled
        runtime.REDIS_ENABLED_EXPLICIT = explicit
        runtime.RPC_TRANSPORT = transport


class RedisBlockTest(_RuntimeState):


    def test_disabled_on_shm(self):
        for transport in ("shm",):
            self._configure(False, transport)

            self.assertEqual(runtime._redis_block(), {}, transport)

    def test_the_redis_transport_overrides_the_switch(self):
        """That deployment has no bridge at all without redis; honouring the
        switch would break the RPC instead of the optional extras."""
        for transport in ("redis", "", "default"):
            self._configure(False, transport)

            self.assertNotEqual(runtime._redis_block(), {}, transport)

    def test_the_default_keeps_existing_deployments_unchanged(self):
        self.assertTrue(self._saved["REDIS_ENABLED"])


class ConfigReadingTest(_RuntimeState):
    pass




class ConsumersSeeAnEmptyBlockTest(_RuntimeState):
    """The point of an empty dict: the guards that were dead now fire."""

    def test_the_strategy_builds_no_client_from_an_empty_block(self):
        import bigqmt_signal_trader_strategy as strategy

        saved = strategy._exec_event_redis_client
        service = strategy._rpc_service
        try:
            strategy._exec_event_redis_client = None
            strategy._rpc_service = None

            self.assertIsNone(strategy._exec_event_redis({"redis": {}}))
            self.assertIsNone(strategy._exec_event_redis({}))
        finally:
            strategy._exec_event_redis_client = saved
            strategy._rpc_service = service

    def test_a_populated_block_still_builds_one(self):
        """The switch must not break deployments that do have redis."""
        import bigqmt_signal_trader_strategy as strategy

        saved = strategy._exec_event_redis_client
        service = strategy._rpc_service
        try:
            strategy._exec_event_redis_client = None
            strategy._rpc_service = None

            client = strategy._exec_event_redis(
                {"redis": {"host": "127.0.0.1", "port": 6379, "db": 5}})

            self.assertIsNotNone(client)
        finally:
            strategy._exec_event_redis_client = saved
            strategy._rpc_service = service


class ExampleConfigTest(unittest.TestCase):
    def test_the_switch_is_documented_where_people_will_look(self):
        import io

        path = os.path.join(ROOT, "src",
                            "bigqmt_signal_trader_local_config.example.py")
        text = io.open(path, encoding="utf-8").read()

        self.assertIn("redis_enabled", text)

    def test_the_example_states_what_it_costs(self):
        """Turning it on silently loses strategy_name backfill (#133); saying
        so in the config is cheaper than another issue."""
        import io

        path = os.path.join(ROOT, "src",
                            "bigqmt_signal_trader_local_config.example.py")
        text = io.open(path, encoding="utf-8").read()

        self.assertIn("strategy_name", text)

    def test_it_ships_commented_out(self):
        """Uncommenting is a decision; defaulting to it would change every
        existing deployment."""
        import io

        path = os.path.join(ROOT, "src",
                            "bigqmt_signal_trader_local_config.example.py")
        for line in io.open(path, encoding="utf-8"):
            if "redis_enabled" in line and "#" not in line.split("redis_enabled")[0]:
                self.fail("redis_enabled is live in the example: %r" % line)







class RuntimeConfigReadingTest(_RuntimeState):
    pass

    def test_redis_enabled_explicit_flag_is_tracked(self):
        runtime.REDIS_ENABLED_EXPLICIT = False
        runtime.configure_runtime_redis({"transport": "shm"})
        self.assertFalse(runtime.REDIS_ENABLED_EXPLICIT)

        runtime.configure_runtime_redis({"transport": "shm", "redis_enabled": True})
        self.assertTrue(runtime.REDIS_ENABLED_EXPLICIT)


class ModuleLevelReadTest(unittest.TestCase):
    """直接挂 runtime（不经 DRYRUN 外壳）的部署在模块级读配置——漏读
    transport 会使配置和实际使用的传输不一致。
    reload 一次 runtime 来验证模块级读取。"""

    def test_module_level_reads_transport(self):
        import importlib
        import types

        stub = types.ModuleType("bigqmt_signal_trader_local_config")
        stub.BIGQMT_ACCOUNT_ID = ""
        stub.BIGQMT_REDIS_CONFIG = {"transport": "shm",
                                    "redis_enabled": False}
        saved_module = sys.modules.get("bigqmt_signal_trader_local_config")
        sys.modules["bigqmt_signal_trader_local_config"] = stub
        try:
            importlib.reload(runtime)
            self.assertEqual("shm", runtime.RPC_TRANSPORT)
            self.assertTrue(runtime.REDIS_ENABLED_EXPLICIT)
            self.assertFalse(runtime.REDIS_ENABLED)
        finally:
            if saved_module is None:
                sys.modules.pop("bigqmt_signal_trader_local_config", None)
            else:
                sys.modules["bigqmt_signal_trader_local_config"] = saved_module
            importlib.reload(runtime)   # 恢复默认状态，别污染其它用例





if __name__ == "__main__":
    unittest.main()
