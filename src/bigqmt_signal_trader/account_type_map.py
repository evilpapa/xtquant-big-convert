# coding: utf-8
"""依据 BIGQMT_ACCOUNT_TYPE_MAP 按请求解析 account_type。

默认情况下，BigQmtOrderGateway 和 BigQmtPositionProvider 在初始化时会使用固定的
account_type（``self.account_type = "STOCK"``）。在单账号部署中这是正确的；但在
多账号部署中（一个 QMT 进程同时服务 STOCK 和 FUTURE），网关是共享的，因此
``self.account_type`` 必须与请求中的 *account_id* 匹配，而不能始终使用网关默认值。

BIGQMT_ACCOUNT_TYPE_MAP 是本地配置中的字典，格式为 ``{account_id: account_type}``。
当该配置存在时，account_type_for() 会返回映射后的类型；否则回退到网关自身的
默认类型（``default_type`` 参数），完全保持向后兼容。

一个账号对应多种类型（港股通）
--------------------------------
股票账号可以通过同一个 account id 交易港股通，但终端会将对应的持仓、委托和成交
记录在其他 account type 下，例如调用
``get_trade_detail_data(acc, 'HUGANGTONG', 'POSITION')``，而不是 ``'STOCK'``。
因此，id 到单一类型的表无法满足需求，值可以写成列表：

    BIGQMT_ACCOUNT_TYPE = ["STOCK", "HUGANGTONG", "SHENGANGTONG"]
    BIGQMT_ACCOUNT_TYPE_MAP = {"123456": ["STOCK", "HUGANGTONG"]}

第一个元素是默认类型。请求指明其中其他类型时——客户端会通过
``StockAccount(id, "HUGANGTONG").account_type`` 参数发送该类型——在本次请求期间按
该类型响应（使用线程局部存储，见 request_account_type()；调仓线程和重读工作线程各自
持有自己的作用域）。如果账号未配置请求的类型，则忽略该类型，与之前的行为一致：
部署配置决定账号可以采用哪些交易类型（#92），服务端会针对每个（账号、类型）组合
记录一次拒绝日志。

网关方法中的用法：

    # 修改前（固定 account_type）：
    rows = query(account_id, self.account_type, "ORDER", strategy_name)

    # 修改后（按请求解析）：
    rows = query(account_id, self._resolve_account_type(account_id), "ORDER", strategy_name)
"""

import contextlib
import importlib
import threading


_ACCOUNT_TYPE_MAP = None  # None = not loaded yet; {} = loaded but empty
_PRIMARY_ID = ""          # BIGQMT_ACCOUNT_ID, when the local config has it
_PRIMARY_TYPES = []       # BIGQMT_ACCOUNT_TYPE as a list (empty unless it was a list)
_REQUEST = threading.local()
_refused_logged = set()

# xtconstant codes -> names, for a client that sends the number (StockAccount
# stores the code it was constructed with, not the name).
_TYPE_BY_CODE = {
    1: "FUTURE", 2: "STOCK", 3: "CREDIT", 5: "FUTURE_OPTION", 6: "STOCK_OPTION",
    7: "HUGANGTONG", 11: "SHENGANGTONG", 10: "NEW3BOARD",
}


def normalize_account_types(value):
    """``value`` as a list of upper-case type names, in order, no repeats.

    Accepts a name, a code, or a list/tuple of either; None and "" give [].
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
    else:
        items = [value]
    out = []
    for item in items:
        text = str("" if item is None else item).strip()
        if not text:
            continue
        if text.isdigit():
            text = _TYPE_BY_CODE.get(int(text), "")
            if not text:
                continue
        text = text.upper()
        if text not in out:
            out.append(text)
    return out


def primary_account_type(value, default="STOCK"):
    """The default type out of a str-or-list config value."""
    types = normalize_account_types(value)
    return types[0] if types else str(default or "STOCK").strip().upper()


def _load_map():
    """Load BIGQMT_ACCOUNT_TYPE_MAP from local config, or {} if absent.

    Uses importlib.import_module (not reload) to avoid:
    1. Re-executing a config module that holds credentials.
    2. QMT sandbox monkeypatched importlib (issue noted in PR #135 review):
       the sandbox wraps importlib.reload with a custom loader that may fail
       on C++ callback threads with SystemError.
    Hot config changes should go through reload_deployment() → gateway init,
    which reconstructs the entire object graph.
    """
    global _ACCOUNT_TYPE_MAP, _PRIMARY_ID, _PRIMARY_TYPES
    try:
        cfg = importlib.import_module("bigqmt_signal_trader_qmt_config")
        raw = dict(getattr(cfg, "BIGQMT_ACCOUNT_TYPE_MAP", {}) or {})
        _ACCOUNT_TYPE_MAP = {}
        for key, value in raw.items():
            types = normalize_account_types(value)
            if types:
                _ACCOUNT_TYPE_MAP[str(key)] = types
        _PRIMARY_ID = str(getattr(cfg, "BIGQMT_ACCOUNT_ID", "") or "")
        primary = getattr(cfg, "BIGQMT_ACCOUNT_TYPE", None)
        # Only a LIST here adds types; a plain string is the runtime's
        # business (it resolves ACCOUNT_TYPE from three sources, #92).
        _PRIMARY_TYPES = (normalize_account_types(primary)
                          if isinstance(primary, (list, tuple)) else [])
    except Exception:
        _ACCOUNT_TYPE_MAP = {}
        _PRIMARY_ID = ""
        _PRIMARY_TYPES = []
    return _ACCOUNT_TYPE_MAP


def get_account_type_map():
    """The loaded map, loading it first if needed: {account_id: [types]}."""
    if _ACCOUNT_TYPE_MAP is None:
        _load_map()
    return _ACCOUNT_TYPE_MAP


def account_types_for(account_id, default_type="STOCK"):
    """Every type *account_id* may be addressed as; the first is the default.

    The map's entry when there is one; else the caller's default (a str or a
    list), extended by BIGQMT_ACCOUNT_TYPE's list for the primary account.
    """
    mapping = get_account_type_map()
    types = list(mapping.get(str(account_id or ""), [])) if account_id else []
    if types:
        return types
    types = normalize_account_types(default_type) or ["STOCK"]
    if _PRIMARY_TYPES and (not account_id or not _PRIMARY_ID
                           or str(account_id) == _PRIMARY_ID):
        for extra in _PRIMARY_TYPES:
            if extra not in types:
                types.append(extra)
    return types


def account_type_for(account_id, default_type="STOCK", requested=None):
    """account_type string for *account_id*, falling back to *default_type*.

    This is the core lookup: if BIGQMT_ACCOUNT_TYPE_MAP maps the account_id
    to a type, return it; otherwise return default_type unchanged.

    ``requested`` (or, when None, the type the current request named -- see
    request_account_type) wins when it is one of the account's configured
    types. Anything else is answered as the default, and said so once.
    """
    types = account_types_for(account_id, default_type)
    if requested is None:
        requested = get_request_account_type()
    wanted = normalize_account_types(requested)
    if wanted:
        if wanted[0] in types:
            return wanted[0]
        _note_refused(account_id, wanted[0], types)
    if not account_id:
        return types[0] if isinstance(default_type, (list, tuple)) else default_type
    mapping = get_account_type_map()
    if str(account_id) not in mapping and not _PRIMARY_TYPES:
        # No map, no list: exactly the old behaviour, default returned as-is.
        return default_type
    return types[0]


def _note_refused(account_id, wanted, types):
    key = (str(account_id or ""), wanted)
    if key in _refused_logged:
        return
    _refused_logged.add(key)
    try:
        from .logging_setup import get_logger
        get_logger("rpc").warning(
            "请求的 account_type 为 %s，账号 %s*** 已配置类型为 %s，本次按 %s 应答。"
            "如需支持该类型，请加入 QMT 端的 BIGQMT_ACCOUNT_TYPE 列表或 "
            "BIGQMT_ACCOUNT_TYPE_MAP，随后重启策略。",
            wanted, str(account_id or "")[:3], "/".join(types), types[0])
    except Exception:
        pass


# -- the type this request named --------------------------------------------

def get_request_account_type():
    """The account_type the request on THIS thread named, or None."""
    return getattr(_REQUEST, "account_type", None)


@contextlib.contextmanager
def request_account_type(value):
    """Scope ``value`` as the current request's account_type on this thread.

    Nested scopes restore the outer value. None / "" mean "unset" and are
    scoped too (a settlement pass for a STOCK order must not inherit the
    HUGANGTONG a previous request on the same thread named).
    """
    previous = getattr(_REQUEST, "account_type", None)
    _REQUEST.account_type = value if value not in (None, "") else None
    try:
        yield
    finally:
        _REQUEST.account_type = previous


def reload():
    """Force-reload the map (e.g. after config change)."""
    global _ACCOUNT_TYPE_MAP
    _ACCOUNT_TYPE_MAP = None
    _refused_logged.clear()
    return get_account_type_map()
