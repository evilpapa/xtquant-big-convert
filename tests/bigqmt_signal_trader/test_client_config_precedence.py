# coding: utf-8
"""Explicit BigQmtRpcClient(valkey_config=...) beats the config module (#289).

The constructor already promises this for host/port/password: it builds
``merged_valkey_config`` as the module's valkey_config updated by the explicit
one. The three feature sections each broke that promise in its own way:

* local_cache read the module section first and used the explicit value only
  as the ``.get`` fallback -- the module won whenever it said anything;
* formula_server ``or``-chained the module section ahead of the explicit
  dict, so any module section discarded the explicit one outright, not even
  a merge;
* full_tick never read valkey_config at all, so its constructor switch did
  nothing even with no config module present.

@shengyy's repro (v0.3.33 and v0.3.38, offline, no Valkey): a module saying
enabled=True for all three and a constructor saying False for all three gave
True True True; with no module, ``full_tick_cache_enabled=True`` gave False.

The order now, for every key of every section: explicit valkey_config >
config module > env / default. Inside the module, load_client_config already
folds BIGQMT_CONFIG's flat keys into each dedicated section with the
flat key winning; that pre-existing intra-module order is not touched.
"""

import os
import sys
import unittest
from types import ModuleType

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient  # noqa: E402

MODULE = "test_client_config_precedence_module"


class _WithModule(object):
    """Install a throwaway client-config module for the duration of a test."""

    def __init__(self, **attrs):
        self.attrs = attrs
        self.saved_env = None

    def __enter__(self):
        module = ModuleType(MODULE)
        for key, value in self.attrs.items():
            setattr(module, key, value)
        sys.modules[MODULE] = module
        self.saved_env = os.environ.get("BIGQMT_CLIENT_CONFIG_MODULE")
        os.environ["BIGQMT_CLIENT_CONFIG_MODULE"] = MODULE
        return module

    def __exit__(self, *exc):
        sys.modules.pop(MODULE, None)
        if self.saved_env is None:
            os.environ.pop("BIGQMT_CLIENT_CONFIG_MODULE", None)
        else:
            os.environ["BIGQMT_CLIENT_CONFIG_MODULE"] = self.saved_env


def _client(**valkey_config):
    return BigQmtRpcClient(account_id="acct", valkey_config=valkey_config)


class ReportedReproTest(unittest.TestCase):
    def test_explicit_false_beats_a_module_saying_true_for_all_three(self):
        with _WithModule(BIGQMT_FORMULA_SERVER_CONFIG={"enabled": True},
                         BIGQMT_LOCAL_CACHE_CONFIG={"enabled": True},
                         BIGQMT_FULL_TICK_CACHE_CONFIG={"enabled": True}):
            client = _client(formula_server={"enabled": False},
                             local_cache_enabled=False,
                             full_tick_cache_enabled=False)

        self.assertFalse(client.formula_server_config["enabled"])
        self.assertFalse(client.local_cache_config["enabled"])
        self.assertFalse(client.full_tick_cache_config["enabled"])

    def test_full_tick_switch_works_with_no_module_at_all(self):
        os.environ.pop("BIGQMT_CLIENT_CONFIG_MODULE", None)
        client = _client(full_tick_cache_enabled=True)

        self.assertTrue(client.full_tick_cache_config["enabled"])


class PrecedenceLadderTest(unittest.TestCase):
    """Each rung, checked on local_cache_enabled with the rung below set."""

    def test_inside_the_module_the_flat_key_still_beats_the_section(self):
        """Pre-existing: load_client_config writes BIGQMT_CONFIG's
        local_cache_enabled over the section's enabled. #289 is about the
        constructor argument; this intra-module order is left as it was."""
        with _WithModule(BIGQMT_LOCAL_CACHE_CONFIG={"enabled": False},
                         BIGQMT_CONFIG={"local_cache_enabled": True}):
            client = _client()

        self.assertTrue(client.local_cache_config["enabled"])

    def test_module_valkey_config_still_counts_when_nothing_else_says(self):
        with _WithModule(BIGQMT_CONFIG={"local_cache_enabled": False}):
            client = _client()

        self.assertFalse(client.local_cache_config["enabled"])

    def test_explicit_beats_both_module_sources(self):
        with _WithModule(BIGQMT_LOCAL_CACHE_CONFIG={"enabled": False},
                         BIGQMT_CONFIG={"local_cache_enabled": False}):
            client = _client(local_cache_enabled=True)

        self.assertTrue(client.local_cache_config["enabled"])

    def test_a_module_section_may_spell_the_flat_key(self):
        """The old code accepted ``full_tick_cache_enabled`` inside the
        dedicated section; keep accepting it."""
        with _WithModule(BIGQMT_FULL_TICK_CACHE_CONFIG={"full_tick_cache_enabled": True}):
            client = _client()

        self.assertTrue(client.full_tick_cache_config["enabled"])


class FormulaServerMergeTest(unittest.TestCase):
    def test_explicit_and_module_dicts_are_merged_key_by_key(self):
        """The or-chain used to drop the explicit dict whole. Now a key the
        caller did not mention keeps the module's value."""
        with _WithModule(BIGQMT_FORMULA_SERVER_CONFIG={"enabled": True, "port": 58600, "host": "10.0.0.1"}):
            client = _client(formula_server={"enabled": False})

        self.assertFalse(client.formula_server_config["enabled"])
        self.assertEqual(58600, client.formula_server_config["port"])
        self.assertEqual("10.0.0.1", client.formula_server_config["host"])

    def test_module_section_alone_still_applies(self):
        with _WithModule(BIGQMT_FORMULA_SERVER_CONFIG={"enabled": False, "port": 1}):
            client = _client()

        self.assertFalse(client.formula_server_config["enabled"])
        self.assertEqual(1, client.formula_server_config["port"])

    def test_explicit_alone_still_applies(self):
        os.environ.pop("BIGQMT_CLIENT_CONFIG_MODULE", None)
        client = _client(formula_server={"enabled": False, "port": 2})

        self.assertFalse(client.formula_server_config["enabled"])
        self.assertEqual(2, client.formula_server_config["port"])


def test_local_dotenv_overrides_implicit_config_but_not_explicit_arguments(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BIGQMT_ACCOUNT_ID", "process-account")
    monkeypatch.setenv("BIGQMT_VALKEY_URL", "valkey://process-host:6379/5")
    monkeypatch.setenv("BIGQMT_VALKEY_HOST", "process-host")
    (tmp_path / ".env").write_text(
        'BIGQMT_ACCOUNT_ID=000123\n'
        'BIGQMT_VALKEY_HOST=file-host\n'
        'BIGQMT_VALKEY_PORT=6381\n'
        'BIGQMT_VALKEY_DB=0\n'
        'BIGQMT_VALKEY_PASSWORD="a#b=${UNCHANGED}"\n'
        'BIGQMT_RPC_TIMEOUT_SECONDS=7\n'
        'BIGQMT_LOCAL_CACHE_ENABLED=0\n',
        encoding="utf-8-sig",
    )
    with _WithModule(BIGQMT_ACCOUNT_ID="module-account", BIGQMT_RPC_TIMEOUT_SECONDS=2,
                     BIGQMT_CONFIG={"url": "valkey://module-host:6379/5"},
                     BIGQMT_LOCAL_CACHE_CONFIG={"enabled": True}):
        client = BigQmtRpcClient()
        explicit = BigQmtRpcClient(account_id="explicit-account",
                                   valkey_config={"url": "valkey://explicit-host:6379/5",
                                                  "local_cache_enabled": True},
                                   timeout_seconds=3)

    assert client.account_id == "000123"
    assert client._valkey_url is None
    assert client.valkey_config["host"] == "file-host"
    assert client.valkey_config["port"] == 6381
    assert client.valkey_config["db"] == 0
    assert client.valkey_config["password"] == "a#b=${UNCHANGED}"
    assert client.timeout_seconds == 7
    assert client.local_cache_config["enabled"] is False
    assert explicit.account_id == "explicit-account"
    assert explicit._valkey_url == "valkey://explicit-host:6379/5"
    assert explicit.timeout_seconds == 3
    assert explicit.local_cache_config["enabled"] is True
    assert os.environ["BIGQMT_ACCOUNT_ID"] == "process-account"


@pytest.mark.parametrize("loader_marker", [True, False])
def test_only_the_external_client_reads_dotenv(tmp_path, monkeypatch, loader_marker):
    from bigqmt_signal_trader import xtquant_compat

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BIGQMT_ACCOUNT_ID", "process-account")
    (tmp_path / ".env").write_text("BIGQMT_ACCOUNT_ID=file-account\n", encoding="utf-8")
    if loader_marker:
        monkeypatch.setattr(xtquant_compat, "__bigqmt_load_local_module", object(), raising=False)
    assert BigQmtRpcClient().account_id == ("process-account" if loader_marker else "file-account")

class OtherKeysFollowTheSameOrderTest(unittest.TestCase):
    def test_full_tick_ttl_from_explicit_beats_module(self):
        with _WithModule(BIGQMT_FULL_TICK_CACHE_CONFIG={"demand_ttl_seconds": 99.0}):
            client = _client(full_tick_demand_ttl_seconds=3.0)

        self.assertEqual(3.0, client.full_tick_cache_config["demand_ttl_seconds"])

    def test_local_cache_dir_from_explicit_beats_module(self):
        with _WithModule(BIGQMT_LOCAL_CACHE_CONFIG={"dir": "/module"}):
            client = _client(local_cache_dir="/explicit")

        self.assertEqual("/explicit", client.local_cache_config["dir"])

    def test_defaults_are_unchanged_when_nothing_is_set(self):
        os.environ.pop("BIGQMT_CLIENT_CONFIG_MODULE", None)
        client = _client()

        self.assertTrue(client.local_cache_config["enabled"])
        self.assertFalse(client.full_tick_cache_config["enabled"])
        self.assertTrue(client.formula_server_config["enabled"])
        self.assertEqual(10.0, client.full_tick_cache_config["demand_ttl_seconds"])


if __name__ == "__main__":
    unittest.main()
