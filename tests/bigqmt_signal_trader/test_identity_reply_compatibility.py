# coding: utf-8
"""Reply safety must not change storage or the first named submit's owner."""
import json

import os
import sys

# Same preamble as every other suite module: the repo src wins over any
# installed copy, which may predate the modules under test.
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
import pytest

from bigqmt_signal_trader import exec_events, valkey_rpc
from bigqmt_signal_trader.adapters.order_dryrun import DryRunOrderGateway
from bigqmt_signal_trader.valkey_safe_value import PREFIX, encode_json
from test_order_identity_valkey_integration import real_valkey, _isolated_valkey, _FilteredValkey


ACCOUNT = "synthetic-account"


def legacy_encode(value):
    return PREFIX + json.dumps(value, ensure_ascii=False, default=str).encode("utf-8").hex().translate(
        str.maketrans("0123456789", "ghijklmnop"))


@pytest.mark.parametrize("value", [None, "", "中文🙂零0", {}, ["600036.SH", "T4_D1"],
                                   {"created_at_ts": 1792052880.123456}])
def test_storage_encoding_is_byte_identical_to_v0368(value):
    assert encode_json(value) == legacy_encode(value)


def submit(client):
    handler = valkey_rpc.BigQmtRpcHandlers(ACCOUNT, None, None,
                                        order_gateway=DryRunOrderGateway(), allow_order_methods=True)
    handler.order_identity_valkey_client = client
    handler._handle_submit_order(dict(stock_code="688001.SH", action="BUY", volume=100,
                                     price=10, remark="reuse", strategy_name="new-owner",
                                     wait_settlement=False))
    return handler


def _require_acl(real_valkey):
    try:
        real_valkey.execute_command('ACL', 'LIST')
    except Exception:
        pytest.skip('valkey ACL unsupported on this server (pre-6.0)')


def test_denied_setrange_does_not_rename_same_remark_in_local_journal(real_valkey):
    import valkey
    _require_acl(real_valkey)
    real_valkey.execute_command("ACL", "SETUSER", "read-owner", "on", "nopass", "~*", "+@all", "-setrange")
    options = real_valkey.connection_pool.connection_kwargs
    limited = valkey.Valkey(host=options["host"], port=options["port"], username="read-owner",
                          socket_timeout=0.25, retry_on_timeout=False)
    try:
        identity = dict(account_id=ACCOUNT, user_order_id="reuse", strategy_name="original-owner",
                        stock_code="688001.SH", created_at_ts=1792052880.123456)
        raw = json.dumps(identity).encode("utf-8")
        key = exec_events.order_identity_key(ACCOUNT, "reuse")
        real_valkey.set(key, raw, px=120000)
        filtered = _FilteredValkey(limited)
        assert filtered.get(key) == raw  # Historical GET succeeds; no migration is necessary.
        handler = submit(filtered)
        assert handler._order_identity_local[(ACCOUNT, "reuse")][1] == "original-owner"
        assert exec_events.order_identity_map(filtered, ACCOUNT, ["reuse"]) == {"reuse": identity}
        assert filtered.evals and all(script == exec_events._READ_ORDER_IDENTITIES
                                      for script, _, _ in filtered.evals)
        assert real_valkey.get(key) == raw and 0 < real_valkey.pttl(key) <= 120000
    finally:
        limited.connection_pool.disconnect()
        real_valkey.execute_command("ACL", "DELUSER", "read-owner")


def test_script_error_other_than_wrong_type_uses_the_original_mget(real_valkey):
    import valkey
    _require_acl(real_valkey)
    real_valkey.execute_command("ACL", "SETUSER", "no-get", "on", "nopass", "~*", "+@all", "-get")
    options = real_valkey.connection_pool.connection_kwargs
    limited = valkey.Valkey(host=options["host"], port=options["port"], username="no-get",
                          socket_timeout=0.25, retry_on_timeout=False)
    try:
        identity = dict(account_id=ACCOUNT, user_order_id="acl", strategy_name="original-owner")
        key = exec_events.order_identity_key(ACCOUNT, "acl")
        real_valkey.set(key, legacy_encode(identity), px=120000)
        with pytest.raises(valkey.ResponseError):
            limited.eval(exec_events._READ_ORDER_IDENTITIES, 1, key, PREFIX)
        filtered = _FilteredValkey(limited)
        assert exec_events.order_identity_map(filtered, ACCOUNT, ["acl"]) == {"acl": identity}
        assert real_valkey.get(key) == legacy_encode(identity).encode("ascii")
    finally:
        limited.connection_pool.disconnect()
        real_valkey.execute_command("ACL", "DELUSER", "no-get")


@pytest.mark.parametrize("bad_type", ["list", "set", "hash"])
def test_wrong_type_batch_key_does_not_hide_other_identities(real_valkey, bad_type):
    identity = dict(account_id=ACCOUNT, user_order_id="good", strategy_name="original-owner")
    good = exec_events.order_identity_key(ACCOUNT, "good")
    bad = exec_events.order_identity_key(ACCOUNT, "bad")
    real_valkey.set(good, legacy_encode(identity))
    if bad_type == "list":
        real_valkey.rpush(bad, "synthetic")
    elif bad_type == "set":
        real_valkey.sadd(bad, "synthetic")
    else:
        real_valkey.hset(bad, "synthetic", "value")
    filtered = _FilteredValkey(real_valkey)
    assert exec_events.order_identity_map(filtered, ACCOUNT, ["bad", "good", "missing"]) == {"good": identity}
    assert len(filtered.evals) == 1
    assert real_valkey.get(good) == legacy_encode(identity).encode("ascii")
    assert real_valkey.type(bad) == bad_type.encode("ascii")


@pytest.mark.parametrize("failure", ["absent", "acl", "timeout", "connection", "response"])
@pytest.mark.parametrize("mget_fails", [False, True])
def test_any_read_only_eval_failure_uses_the_original_read_path(real_valkey, failure, mget_fails):
    import valkey
    identity = dict(account_id=ACCOUNT, user_order_id="reuse", strategy_name="original-owner")
    key = exec_events.order_identity_key(ACCOUNT, "reuse")
    real_valkey.set(key, legacy_encode(identity))

    class Failing(_FilteredValkey):
        def __init__(self, client):
            super().__init__(client)
            self.calls = []
            if failure == "absent":
                self.eval = None

        def eval(self, *args):
            self.calls.append("EVAL")
            errors = {"acl": valkey.ResponseError("NOPERM"), "timeout": valkey.TimeoutError("timeout"),
                      "connection": valkey.ConnectionError("disconnected"),
                      "response": valkey.DataError("Sensitive Data Detected, Forbidden!")}
            raise errors[failure]

        def mget(self, keys):
            self.calls.append("MGET")
            if mget_fails:
                raise valkey.ConnectionError("disconnected")
            return super().mget(keys)

        def get(self, key):
            self.calls.append("GET")
            return super().get(key)

    filtered = Failing(real_valkey)
    assert exec_events.order_identity_map(filtered, ACCOUNT, ["reuse"]) == {"reuse": identity}
    assert filtered.calls == ([] if failure == "absent" else ["EVAL"]) + ["MGET"] + (
        ["GET"] if mget_fails else [])


def test_new_identity_can_be_read_after_a_full_codec_rollback(real_valkey, monkeypatch):
    identity = exec_events.remember_order_identity(real_valkey, ACCOUNT, "reuse", "original-owner", "688001.SH")
    key = exec_events.order_identity_key(ACCOUNT, "reuse")
    stored = real_valkey.get(key)

    def old_decode(raw):
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        if text.startswith(PREFIX):
            text = bytes.fromhex(text[len(PREFIX):].translate(
                str.maketrans("ghijklmnop", "0123456789"))).decode("utf-8")
        return json.loads(text)

    assert old_decode(stored) == identity
    monkeypatch.setattr(exec_events, "decode_json", old_decode)
    handler = submit(_FilteredValkey(real_valkey))
    assert handler._order_identity_local[(ACCOUNT, "reuse")][1] == "original-owner"
    assert real_valkey.get(key) == stored
