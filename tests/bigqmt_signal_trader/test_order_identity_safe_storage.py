# coding: utf-8
"""Identity read-back must survive QMT's response filter and a fresh process.

The filter below reproduces the broker's historical client response regexes,
including its different bytes and MGET formatting. A normal in-memory VK
fake misses #393: the stored JSON is present but QMT refuses to return it.
"""

import json
import os
import re
import sys
from types import SimpleNamespace

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.exec_events import (
    enrich_order_identity,
    order_identity_key,
    order_identity_map,
    remember_order_identity,
)
from bigqmt_signal_trader.adapters.order_dryrun import DryRunOrderGateway
from bigqmt_signal_trader.vk_rpc import BigQmtRpcHandlers


ACCOUNT = "test-account"
REMARK = "GW01741471201"
NAME = "XXX_T4_D1"
PREFIX = "bqidentity:"
SENSITIVE_ERROR = "Sensitive Data Detected, Forbidden!"
_DIGIT_TO_ALPHA = str.maketrans("0123456789", "ghijklmnop")

# Historical Guojin terminal filters. Neither regex knows which
# JSON field it is matching: the 4 and 1 in a strategy name count as operations.
_STOCK_RE = re.compile(
    r"(^|[^\d])+([36]0[\d]{4}|00(000[1-9]|[1-9][\d]{3}|[\d][1-9][\d]{2}|[\d]{2}[1-9][\d]))([^\d]|$)+"
)
_OP_RE = re.compile(r"(^|[^\d])(7[0-5]|6[0-1]|[1345]\d|2[3-9]|\d)([^\d]|$)")


class DataError(Exception):
    pass


class _JsonBytesEncoder(json.JSONEncoder):
    def default(self, value):
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value.decode("gbk")
        return super().default(value)


def _check_response(response):
    text = (json.dumps(response, cls=_JsonBytesEncoder)
            if isinstance(response, (list, dict)) else str(response))
    if _STOCK_RE.findall(text) and _OP_RE.findall(text):
        raise DataError(SENSITIVE_ERROR)
    return response


def _assert_bounded_response(response):
    text = json.dumps(response, cls=_JsonBytesEncoder)
    assert max(map(len, re.findall(r"\D+", text)), default=0) <= len(PREFIX) + 32 + 4
    assert set(re.findall(r"\d+", text)) == {"0"}
    assert not _STOCK_RE.findall(text)
    assert _check_response(response) == response


def _legacy(remark=REMARK, name=NAME, stock_code="600036.SH", account=ACCOUNT):
    return json.dumps({
        "account_id": account,
        "user_order_id": remark,
        "strategy_name": name,
        "stock_code": stock_code,
        # No incidental one/two-digit component: tests control the matches.
        "created_at_ts": 1792052880.123456,
    }, ensure_ascii=False)


def _bytes(value):
    return value.encode("utf-8") if isinstance(value, str) else value


class FilteredVk(object):
    """Only fake the supported VK contract; real Lua gets a separate probe."""

    def __init__(self, decode_responses=False, guard=True):
        self.decode_responses = decode_responses
        self.guard = guard
        self.values = {}
        self.expires_ms = {}
        self.now_ms = 1000000
        self.gets = []
        self.mgets = []
        self.evals = []
        self.sets = []
        self.read_error = None
        self.eval_error = None

    def seed(self, key, value, ttl_ms=123456):
        self.values[key] = _bytes(value)
        self.expires_ms[key] = (None if ttl_ms is None
                                else self.now_ms + ttl_ms)

    def _value(self, key):
        expires = self.expires_ms.get(key)
        if expires is not None and expires <= self.now_ms:
            self.values.pop(key, None)
            self.expires_ms.pop(key, None)
        value = self.values.get(key)
        if value is not None and self.decode_responses:
            return value.decode("utf-8")
        return value

    def _response(self, value):
        if self.read_error is not None:
            raise self.read_error
        return _check_response(value) if self.guard else value

    def get(self, key):
        self.gets.append(key)
        return self._response(self._value(key))

    def mget(self, keys):
        self.mgets.append(list(keys))
        return self._response([self._value(key) for key in keys])

    def set(self, key, value, ex=None, nx=False):
        if nx and self._value(key) is not None:
            return None
        self.sets.append(key)
        self.seed(key, value, None if ex is None else int(ex) * 1000)
        return True

    def setex(self, key, seconds, value):
        return self.set(key, value, ex=seconds)

    def setnx(self, key, value):
        return bool(self.set(key, value, nx=True))

    def expire(self, key, seconds):
        if self._value(key) is None:
            return False
        self.expires_ms[key] = self.now_ms + int(seconds) * 1000
        return True

    def eval(self, script, numkeys, *keys_and_args):
        from bigqmt_signal_trader.exec_events import _MIGRATE_ORDER_IDENTITY
        if script != _MIGRATE_ORDER_IDENTITY:
            raise NotImplementedError("batch scripting covered by real VK tests")
        self.evals.append((script, numkeys, keys_and_args))
        if self.eval_error is not None:
            raise self.eval_error
        assert numkeys == 1
        key, account, remark, prefix = keys_and_args
        assert prefix == PREFIX
        raw = self._value(key)
        if raw is None:
            return None
        raw = _bytes(raw)
        if raw.startswith(prefix.encode("ascii")):
            return self._response(self._value(key))
        try:
            identity = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        if (not isinstance(identity, dict)
                or identity.get("account_id") != account
                or identity.get("user_order_id") != remark):
            return None
        # Emulate Lua's single SETRANGE write: byte content and expiry survive.
        encoded = prefix + raw.hex().translate(_DIGIT_TO_ALPHA)
        self.values[key] = encoded.encode("ascii")
        return self._response(self._value(key))


def _handlers(vk_client):
    # A new handler has no submit-time memory, as after a real restart.
    handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
    handlers.account_id = ACCOUNT
    handlers.order_identity_vk_client = vk_client
    handlers.download_job_vk_client = None
    handlers._order_identity_local = {}
    return handlers


@pytest.mark.parametrize("response", [
    _legacy(),
    _legacy().encode("utf-8"),
    [_legacy().encode("utf-8")],
])
def test_filter_rejects_the_issue_payload_as_text_bytes_and_mget(response):
    with pytest.raises(DataError, match="Sensitive Data Detected"):
        _check_response(response)


def test_filter_can_combine_stock_and_operation_matches_across_mget_values():
    stock_only = _legacy(remark="first", name="alpha")
    digits_only = _legacy(remark="second", stock_code="688001.SH")
    _check_response(stock_only.encode("utf-8"))
    _check_response(digits_only.encode("utf-8"))
    with pytest.raises(DataError, match="Sensitive Data Detected"):
        _check_response([stock_only.encode("utf-8"), digits_only.encode("utf-8")])


@pytest.mark.parametrize("decode_responses", [False, True])
def test_new_identity_survives_filtered_mget_without_a_migration(decode_responses):
    vk = FilteredVk(decode_responses)
    payload = remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")

    found = order_identity_map(vk, ACCOUNT, [REMARK])

    assert found.get(REMARK) == payload
    assert found[REMARK]["user_order_id"] == REMARK
    assert list(vk.values) == [order_identity_key(ACCOUNT, REMARK)]
    assert vk.evals == []
    assert vk.gets == []
    assert len(vk.mgets) == 1


@pytest.mark.parametrize("stock_code", ["600036.SH", "cu2610.SF"])
def test_storage_preserves_chinese_strategy_and_futures_case(stock_code):
    vk = FilteredVk()
    strategy_name = "中文策略_T4_D1"
    remember_order_identity(vk, ACCOUNT, REMARK, strategy_name, stock_code)

    found = order_identity_map(vk, ACCOUNT, [REMARK]).get(REMARK)

    assert found is not None
    assert found["strategy_name"] == strategy_name
    assert found["stock_code"] == stock_code
    assert found["account_id"] == ACCOUNT
    assert found["user_order_id"] == REMARK
    stored = vk.values[order_identity_key(ACCOUNT, REMARK)].decode("ascii")
    assert stored.startswith(PREFIX)
    assert not any(character.isdigit() for character in stored)


def test_event_enrichment_reads_safe_identity_and_keeps_remark():
    vk = FilteredVk()
    remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")
    event = {"remark": REMARK, "strategy_name": "QMT_BRIDGE", "stock_code": ""}

    enriched = enrich_order_identity(vk, ACCOUNT, event)

    assert enriched["strategy_name"] == NAME
    assert enriched["stock_code"] == "600036.SH"
    assert enriched["remark"] == REMARK


def test_fresh_handler_attributes_from_vk_and_probe_reports_the_hit():
    vk = FilteredVk()
    remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")
    handlers = _handlers(vk)
    row = SimpleNamespace(user_order_id=REMARK, strategy_name="QMT_BRIDGE")

    attributed = handlers._attribute_to_strategies(ACCOUNT, [row])
    probe = handlers._handle_probe_order_identity({"remark": REMARK})

    assert attributed[0].strategy_name == NAME
    assert attributed[0].user_order_id == REMARK
    assert probe["valkey_hit"] is True
    assert probe["local_hit"] is False
    assert probe["local_journal_size"] == 0
    assert "lookup_error" not in probe
    assert vk.evals == []


def test_first_named_identity_survives_blank_and_other_strategy_reuse():
    vk = FilteredVk()
    remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")
    key = order_identity_key(ACCOUNT, REMARK)
    first_value = vk.values[key]
    first_expiry = vk.expires_ms[key]
    vk.now_ms += 60000

    remember_order_identity(vk, ACCOUNT, REMARK, "", "600036.SH")
    remember_order_identity(vk, ACCOUNT, REMARK, "OTHER_T2_D3", "600036.SH")

    assert order_identity_map(vk, ACCOUNT, [REMARK])[REMARK]["strategy_name"] == NAME
    assert vk.values[key] == first_value
    assert vk.expires_ms[key] == first_expiry
    assert vk.evals == []


@pytest.mark.parametrize("next_name", ["OTHER_T2_D3", ""])
def test_fresh_handler_reused_remark_keeps_vk_owner_in_local_attribution(next_name):
    vk = FilteredVk()
    remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")
    key = order_identity_key(ACCOUNT, REMARK)
    before = (vk.values[key], vk.expires_ms[key])
    gateway = DryRunOrderGateway()
    handlers = BigQmtRpcHandlers(
        account_id=ACCOUNT, market_data=SimpleNamespace(),
        position_provider=SimpleNamespace(), order_gateway=gateway,
        allow_order_methods=True)
    handlers.order_identity_vk_client = vk
    assert not handlers._order_identity_local

    handlers._handle_submit_order({
        "stock_code": "600036.SH", "action": "BUY", "volume": 100,
        "price": 8.0, "remark": REMARK, "strategy_name": next_name,
        "wait_settlement": False,
    })
    row = SimpleNamespace(user_order_id=REMARK, strategy_name="QMT_BRIDGE")
    attributed = handlers._attribute_to_strategies(ACCOUNT, [row])

    assert attributed[0].strategy_name == NAME
    assert attributed[0].user_order_id == REMARK
    assert handlers._order_identity_local[(ACCOUNT, REMARK)][1] == NAME
    assert (vk.values[key], vk.expires_ms[key]) == before
    # Attribution does not rewrite what the caller actually submitted.
    assert gateway.submitted[0].strategy_name == next_name
    assert gateway.submitted[0].remark == REMARK
    assert gateway.submitted[0].stock_code == "600036.SH"


def test_an_unnamed_safe_identity_can_yield_to_the_first_named_submit():
    vk = FilteredVk()
    remember_order_identity(vk, ACCOUNT, REMARK, "", "600036.SH")

    remember_order_identity(vk, ACCOUNT, REMARK, NAME, "600036.SH")

    assert order_identity_map(vk, ACCOUNT, [REMARK])[REMARK]["strategy_name"] == NAME
    assert vk.evals == []


def test_reused_remark_keeps_the_name_in_a_blocked_legacy_record():
    vk = FilteredVk()
    key = order_identity_key(ACCOUNT, REMARK)
    vk.seed(key, _legacy())
    expiry = vk.expires_ms[key]

    result = remember_order_identity(
        vk, ACCOUNT, REMARK, "OTHER_T2_D3", "600036.SH")

    assert result is None
    assert order_identity_map(vk, ACCOUNT, [REMARK])[REMARK]["strategy_name"] == NAME
    assert vk.expires_ms[key] == expiry
    assert vk.sets == []
    assert len(vk.evals) == 1


@pytest.mark.parametrize("decode_responses", [False, True])
def test_unfiltered_legacy_json_remains_readable_without_eval(decode_responses):
    vk = FilteredVk(decode_responses, guard=False)
    key = order_identity_key(ACCOUNT, REMARK)
    raw = _legacy()
    vk.seed(key, raw)

    found = order_identity_map(vk, ACCOUNT, [REMARK])
    enriched = enrich_order_identity(vk, ACCOUNT, {"user_order_id": REMARK})

    assert found[REMARK] == json.loads(raw)
    assert enriched["strategy_name"] == NAME
    assert vk.values[key] == raw.encode("utf-8")
    assert vk.evals == []


def test_mget_cross_record_rejection_falls_back_without_migrating_readable_json():
    vk = FilteredVk()
    first = _legacy(remark="first", name="alpha")
    second = _legacy(remark="second", stock_code="688001.SH")
    vk.seed(order_identity_key(ACCOUNT, "first"), first)
    vk.seed(order_identity_key(ACCOUNT, "second"), second)

    found = order_identity_map(vk, ACCOUNT, ["first", "second"])

    assert found["first"] == json.loads(first)
    assert found["second"] == json.loads(second)
    assert len(vk.mgets) == 1
    assert len(vk.gets) == 2
    assert vk.evals == []


@pytest.mark.parametrize("decode_responses", [False, True])
@pytest.mark.parametrize("ttl_ms", [123456, None])
def test_blocked_legacy_identity_is_recovered_without_changing_expiry(
        decode_responses, ttl_ms):
    vk = FilteredVk(decode_responses)
    key = order_identity_key(ACCOUNT, REMARK)
    raw = _legacy()
    vk.seed(key, raw, ttl_ms)
    expiry = vk.expires_ms[key]

    found = order_identity_map(vk, ACCOUNT, [REMARK])

    assert found.get(REMARK) == json.loads(raw)
    assert vk.expires_ms[key] == expiry
    assert vk.sets == []
    assert len(vk.evals) == 1
    assert vk.values[key].startswith(PREFIX.encode("ascii"))
    # A second read is now ordinary MGET and must not run the script again.
    assert order_identity_map(vk, ACCOUNT, [REMARK]) == found
    assert len(vk.evals) == 1


@pytest.mark.parametrize("read_path", ["event", "attribution", "probe"])
def test_each_read_path_can_recover_an_existing_blocked_legacy_identity(read_path):
    vk = FilteredVk()
    vk.seed(order_identity_key(ACCOUNT, REMARK), _legacy())
    handlers = _handlers(vk)

    if read_path == "event":
        result = enrich_order_identity(vk, ACCOUNT, {"remark": REMARK})
        assert result.get("strategy_name") == NAME
        assert result["remark"] == REMARK
    elif read_path == "attribution":
        row = SimpleNamespace(user_order_id=REMARK, strategy_name="QMT_BRIDGE")
        result = handlers._attribute_to_strategies(ACCOUNT, [row])
        assert result[0].strategy_name == NAME
        assert result[0].user_order_id == REMARK
    else:
        result = handlers._handle_probe_order_identity({"remark": REMARK})
        assert result["valkey_hit"] is True
        assert result["local_hit"] is False
        assert "lookup_error" not in result
    assert len(vk.evals) == 1


def test_eval_rejection_leaves_existing_json_unchanged_and_probe_diagnosable():
    vk = FilteredVk()
    key = order_identity_key(ACCOUNT, REMARK)
    vk.seed(key, _legacy())
    before = (vk.values[key], vk.expires_ms[key])
    vk.eval_error = RuntimeError("NOPERM EVAL is disabled")

    assert order_identity_map(vk, ACCOUNT, [REMARK]) == {}
    probe = _handlers(vk)._handle_probe_order_identity({"remark": REMARK})

    assert (vk.values[key], vk.expires_ms[key]) == before
    assert len(vk.evals) >= 1
    assert probe["valkey_hit"] is None
    assert probe.get("lookup_error")
    assert vk.sets == []


def test_unrelated_vk_failure_does_not_attempt_legacy_migration():
    vk = FilteredVk()
    key = order_identity_key(ACCOUNT, REMARK)
    vk.seed(key, _legacy())
    vk.read_error = RuntimeError("connection timed out")
    before = (vk.values[key], vk.expires_ms[key])

    assert order_identity_map(vk, ACCOUNT, [REMARK]) == {}
    assert _handlers(vk)._handle_probe_order_identity(
        {"remark": REMARK})["valkey_hit"] is None
    assert (vk.values[key], vk.expires_ms[key]) == before
    assert vk.evals == []


def test_expired_legacy_identity_is_not_resurrected_by_a_query():
    vk = FilteredVk()
    key = order_identity_key(ACCOUNT, REMARK)
    vk.seed(key, _legacy(), ttl_ms=1)
    vk.now_ms += 2

    assert order_identity_map(vk, ACCOUNT, [REMARK]) == {}
    assert key not in vk.values
    assert vk.evals == []
    assert vk.sets == []


@pytest.mark.parametrize("wrong_field", ["account", "remark"])
def test_blocked_legacy_payload_with_wrong_identity_is_never_migrated(wrong_field):
    vk = FilteredVk()
    key = order_identity_key(ACCOUNT, REMARK)
    raw = (_legacy(account="another-account") if wrong_field == "account"
           else _legacy(remark="another-remark"))
    vk.seed(key, raw)
    before = (vk.values[key], vk.expires_ms[key])

    assert order_identity_map(vk, ACCOUNT, [REMARK]) == {}
    assert (vk.values[key], vk.expires_ms[key]) == before
    assert vk.sets == []
