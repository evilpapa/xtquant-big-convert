"""通过大 QMT RPC 提供 MiniQMT 兼容接口。

保留 query_stock_positions、get_full_tick 等现有调用形式，由大 QMT
进程调用终端运行时 API。
"""

import os
import json
import math
import time
import uuid
import queue as _queue
from collections import OrderedDict as _OrderedDict
import threading
import importlib
import warnings
import datetime as _dt
from typing import Any, Dict, Iterable, List, Optional
# 本模块使用这三个常量，下方还会暴露全部公开常量，兼容已有 xtconstant 调用。
from xtquant import xtconstant as _xtconstant
from xtquant.xtconstant import ORDER_UNKNOWN, STOCK_BUY, STOCK_SELL
from xtquant.xttype import StockAccount

from .adapters.valkey_common import (
    local_valkey_config, normalize_valkey_url, valkey_client_class, valkey_connection_options,
)
from .code_utils import is_bond_code
from .download_jobs import DONE, FAILED
from .full_tick_cache import request_full_tick_cache, wait_full_tick_cache
from .local_cache import LocalMarketCache
from .order_id import OrderId, order_sys_id_of
from .option_order_type import option_order_type
from .valkey_rpc import TYPED_PAYLOAD_FLAG, call_valkey_rpc
from .logging_setup import get_logger

log = get_logger("xtquant_compat")


# 逐项暴露 xtconstant 的公开常量，保留 xtquant_compat.FIX_PRICE 等调用。
# 单文件构建曾在函数作用域加载模块，使用 import * 会触发语法错误（#76）。
for _const_name in dir(_xtconstant):
    if not _const_name.startswith("_"):
        globals().setdefault(_const_name, getattr(_xtconstant, _const_name))
del _const_name


# get_local_data 回退到 RPC 补缓存时使用的默认字段。
# 保留 MiniQMT K 线的 time 和 openInterest，并提供策略计算涨跌幅所需的 preClose。
DEFAULT_DOWNLOAD_FIELDS = [
    "time", "open", "high", "low", "close", "volume", "amount", "openInterest",
    "preClose",
]
# get_market_data_ex 每批的代码数，避免一次请求的股票过多导致整批超时（#47）。
DEFAULT_MARKET_DATA_CHUNK = 100
_TIME_COL_NAMES = ("stime", "time", "index", "date", "datetime", "timetag")


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)



CLIENT_CONFIG_MODULE_ENV = "BIGQMT_CLIENT_CONFIG_MODULE"
DEFAULT_CLIENT_CONFIG_MODULES = (
    "bigqmt_signal_trader_client_config",
    "bigqmt_signal_trader_qmt_config",
)



class CompatObject:
    """模拟 xtquant 对象式返回值的小型属性对象。"""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        items = ", ".join("%s=%r" % (key, value) for key, value in sorted(self.__dict__.items()))
        return "%s(%s)" % (self.__class__.__name__, items)


class CompatRow(dict):
    """兼具属性访问的 dict，键保持不变。

    MiniQMT 的 *同步* 查询返回终端自己的对象。只有推送路径会构造 xttype
    对象 —— ``on_push_AccountStatus`` 是 ``xttrader`` 唯一读取 ``m_nStatus``
    并做转换的地方 —— 而每个 ``query_*`` 都原样返回
    ``common_op_sync_with_seq`` 的结果，所以账户类数据到手时带着 m_ 前缀属性。

    桥接层过去把正确的字段名装在了错误的容器里：一个 dict，``.m_nStatus``
    会抛 AttributeError，只有 ``["m_nStatus"]`` 能用。继承 dict 在不影响下标
    访问的前提下补上属性访问，因此按现有行为编写的调用方继续可用，对行做 json
    编码或用 ``isinstance(.., dict)`` 判断的代码也同样可用。
    """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def _as_compat_row(row):
    """把原生字段 dict 包装成支持属性访问，其它类型原样返回。"""
    return CompatRow(row) if isinstance(row, dict) else row


def _market_of(stock_code):
    """``XtCancelError.market`` —— xtconstant 的 SH_MARKET (0) / SZ_MARKET (1)。

    代码后缀是桥接层唯一的来源，而撤单路径常常完全不带代码。用 -1 表示
    “未知”，而不是让缺失值冒充上海（默认取 0 就会这样）。
    """
    suffix = str(stock_code or "").rsplit(".", 1)[-1].upper()
    if suffix == "SH":
        return 0
    if suffix == "SZ":
        return 1
    return -1






class XtQuantTraderCallback:
    def on_disconnected(self):
        pass

    def on_stock_order(self, order):
        pass

    def on_stock_trade(self, trade):
        pass

    def on_order_error(self, order_error):
        pass

    def on_cancel_error(self, cancel_error):
        pass

    def on_order_stock_async_response(self, response):
        pass

    def on_cancel_order_stock_async_response(self, response):
        pass

    def on_account_status(self, status):
        pass


def _env_int(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return int(value)


def _env_float(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return float(value)


def _env_bool(name, default=False):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _bool_value(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _first_set(*values):
    """第一个不为 None 的值；全为 None 时返回 None。"""
    for value in values:
        if value is not None:
            return value
    return None


class _ClientSetting(object):
    """单个客户端功能项的取值顺序：显式 ``valkey_config`` 优先，其次本地
    .env、配置模块的 section，最后是环境变量 / 内置默认值——与构造函数对
    host/port/password 已经承诺的顺序一致。

    问题 #289：三个功能块各自以不同方式破坏了这个顺序。``local_cache``
    先读模块 section，只把显式值当作 ``.get`` 的兜底；``formula_server``
    用 ``or`` 把模块 section 串在显式 dict 前面，于是任何模块 section 都会
    直接丢弃显式 dict；``full_tick`` 根本不读 ``valkey_config``，所以即使
    没有模块，它的构造参数也不起作用。

    ``section`` 是 load_client_config 交过来的东西：模块专属的 dict
    （``BIGQMT_LOCAL_CACHE_CONFIG`` 等），已经把模块自己的
    ``BIGQMT_CONFIG`` 扁平键并入。这两者在一个文件内部如何排序，是那个
    函数的职责，此处不变。扁平键带 section 前缀（``local_cache_enabled``），
    section 键去掉前缀（``enabled``）；section 里也可以写扁平键，旧代码接受
    这种写法，所以两种都会查。
    """

    def __init__(self, explicit, section, local_env):
        self.explicit = dict(explicit or {})
        self.section = dict(section or {})
        self.local_env = local_env

    def get(self, flat_key, section_key):
        return _first_set(
            self.explicit.get(flat_key),
            self.local_env.get("BIGQMT_" + flat_key.upper()),
            self.section.get(section_key),
            self.section.get(flat_key),
        )


def _import_optional_module(module_name):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            return None
        raise


def _quote_client_id():
    """全推订阅用的进程级稳定客户端 id。配置或环境变量优先；
    否则读取/创建持久化的 id，使重启后的客户端被服务端识别为同一个订阅者。"""
    local_env = _client_local_env()
    client_config = load_client_config(local_env.get(CLIENT_CONFIG_MODULE_ENV))
    configured = (local_env.get("BIGQMT_QUOTE_CLIENT_ID")
                  or client_config.get("quote_client_id")
                  or os.environ.get("BIGQMT_QUOTE_CLIENT_ID"))
    if configured:
        return str(configured)
    cache_path = os.path.join(os.path.expanduser("~"), ".cache", "bigqmt", "quote_client_id")
    try:
        with open(cache_path, "r") as handle:
            existing = handle.read().strip()
            if existing:
                return existing
    except OSError:
        pass
    new_id = uuid.uuid4().hex
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as handle:
            handle.write(new_id)
    except OSError:
        pass
    return new_id


def _build_quote_push_channel(client):
    """配置了 Valkey 时构造 Valkey 推送订阅者。"""
    from .quote_push_channel import ValkeyQuotePushChannel

    transport_name = str(getattr(client, "transport_name", "valkey") or "valkey").lower()
    if (transport_name not in ("valkey", "", "default")
            and not getattr(client, "_valkey_explicit", True)):
        raise RuntimeError(
            "transport=%s 没有全推推送通道（服务端 0.3.58 起也不再发）："
            "请改用 get_full_tick 轮询；或在客户端配置里显式给出 valkey 块，"
            "全推才会走 valkey pub/sub。" % transport_name)
    return ValkeyQuotePushChannel(client._valkey(), account_id=client.account_id)


def _missing_account_id_message():
    """说明查了哪些地方、该怎么做，而不只是说缺东西。

    原来的提示「Big QMT account_id 是必需的」完全没告诉读者配置去哪儿找了，
    于是放在离运行解释器一个 sys.path 之外的配置文件，看起来和完全没有配置
    一样（问题 #90）。
    """
    searched = list(DEFAULT_CLIENT_CONFIG_MODULES)
    selected = os.environ.get(CLIENT_CONFIG_MODULE_ENV)
    if selected and selected not in searched:
        searched.insert(0, selected)
    found = None
    try:
        config = load_client_config()
        found = (config or {}).get("module")
    except Exception:
        pass

    if found:
        detail = ("imported %s, but it defines no BIGQMT_ACCOUNT_ID"
                  % found)
    else:
        detail = ("none of these modules could be imported: %s"
                  % ", ".join(searched))
    lines = [
        "Big QMT account_id is required -- %s." % detail,
        "Fix it in any one of these ways:",
        "  1. put bigqmt_signal_trader_client_config.py somewhere on sys.path"
        " (the current working directory counts), with BIGQMT_ACCOUNT_ID set;",
        "  2. set BIGQMT_ACCOUNT_ID in the current directory's .env or in the environment;",
        "  3. call bigqmt_signal_trader.xtquant_compat.configure(account_id=...)"
        " before use;",
        "  or run `bigqmt-init`, which writes both config files for you.",
        "configure() also runs at import time, so a config put in place after"
        " importing this module needs configure() called again.",
    ]
    return "\n".join(lines)


def _client_local_env():
    if "__bigqmt_load_local_module" in globals():
        return {}
    from .local_env import read_local_env

    return read_local_env()


def load_client_config(module_name=None):
    """加载本地私有客户端配置，不依赖环境变量。"""
    candidates = []
    selected = module_name or os.environ.get(CLIENT_CONFIG_MODULE_ENV)
    if selected:
        candidates.append(str(selected))
    candidates.extend(name for name in DEFAULT_CLIENT_CONFIG_MODULES if name not in candidates)

    for candidate in candidates:
        module = _import_optional_module(candidate)
        if module is None:
            continue
        valkey_config = local_valkey_config(module)
        account_id = getattr(module, "BIGQMT_ACCOUNT_ID", None) or valkey_config.get("account_id")
        timeout_seconds = getattr(module, "BIGQMT_RPC_TIMEOUT_SECONDS", None)
        if timeout_seconds is None:
            timeout_seconds = valkey_config.get("rpc_timeout_seconds")
        download_wait_seconds = getattr(module, "BIGQMT_DOWNLOAD_WAIT_SECONDS", None)
        if download_wait_seconds is None:
            download_wait_seconds = valkey_config.get("download_wait_seconds")
        download_poll_interval_seconds = getattr(module, "BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS", None)
        if download_poll_interval_seconds is None:
            download_poll_interval_seconds = valkey_config.get("download_poll_interval_seconds")
        full_tick_cache_config = dict(getattr(module, "BIGQMT_FULL_TICK_CACHE_CONFIG", {}) or {})
        for key in (
            "full_tick_cache_enabled",
            "full_tick_demand_ttl_seconds",
            "full_tick_cache_ttl_seconds",
            "full_tick_wait_seconds",
            "full_tick_poll_interval_seconds",
        ):
            if key in valkey_config:
                full_tick_cache_config[key] = valkey_config[key]
        local_cache_config = dict(getattr(module, "BIGQMT_LOCAL_CACHE_CONFIG", {}) or {})
        for key in ("local_cache_enabled", "local_cache_dir", "local_cache_fallback_rpc", "local_cache_format"):
            if key in valkey_config:
                local_cache_config[key.replace("local_cache_", "")] = valkey_config[key]
        formula_server_config = dict(getattr(module, "BIGQMT_FORMULA_SERVER_CONFIG", {}) or {})
        formula_server_config.update(dict(valkey_config.get("formula_server") or {}))
        return {
            "module": candidate,
            "account_id": account_id,
            "valkey_config": valkey_config,
            "timeout_seconds": timeout_seconds,
            "download_wait_seconds": download_wait_seconds,
            "download_poll_interval_seconds": download_poll_interval_seconds,
            "full_tick_cache_config": full_tick_cache_config,
            "local_cache_config": local_cache_config,
            "formula_server_config": formula_server_config,
            "quote_client_id": getattr(module, "BIGQMT_QUOTE_CLIENT_ID", None),
        }
    return {}


def _account_type_param(account):
    """StockAccount 声明的类型名，纯 id 字符串时返回 ""。

    作为每个交易 RPC 的 ``account_type`` 参数发送（股票账户上的港股通：
    StockAccount(id, "HUGANGTONG") 读取 HUGANGTONG 账本）。仅当账户配置为
    该类型时服务端才采纳，否则仍由部署自身的类型作答（见 #92）。
    """
    if account is None or isinstance(account, (str, dict)):
        return ""
    return _account_type_name(getattr(account, "account_type", None))


def _with_account_type(params, account):
    """在 ``params`` 中加入账户声明的类型（若声明了）。"""
    name = _account_type_param(account)
    if name:
        params["account_type"] = name
    return params


def _account_id(account, fallback=""):
    if account is None:
        return str(fallback or "")
    if isinstance(account, str):
        return account
    for name in ("account_id", "m_strAccountID", "id"):
        value = getattr(account, name, None)
        if value:
            return str(value)
    if isinstance(account, dict):
        return str(account.get("account_id") or account.get("id") or fallback or "")
    return str(fallback or "")


def _action_to_order_type(action):
    text = str(action or "").upper()
    if text in ("BUY", str(STOCK_BUY)):
        return STOCK_BUY
    if text in ("SELL", str(STOCK_SELL)):
        return STOCK_SELL
    return 0


def _action_of_row(row):
    """查询到的委托/成交对象的 BUY/SELL 字符串（#372 轮询事件用）。

    方向判据与服务端一致：offset_flag（48 买/49 卖）优先，order_type 兜底。
    """
    offset = _safe_int(getattr(row, "offset_flag", None), -1)
    if offset == 48:
        return "BUY"
    if offset == 49:
        return "SELL"
    order_type = _safe_int(getattr(row, "order_type", None), 0)
    if order_type in (STOCK_BUY, CREDIT_BUY, CREDIT_FIN_BUY, CREDIT_BUY_SECU_REPAY):
        return "BUY"
    if order_type in (STOCK_SELL, CREDIT_SELL, CREDIT_SLO_SELL, CREDIT_SELL_SECU_REPAY):
        return "SELL"
    return ""


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_unix_seconds(value, default=0):
    """把成交/委托时间统一成 Unix 秒（MiniQMT 语义）。

    接受数值时间戳、``YYYY-MM-DD HH:MM:SS[.ffffff]`` 和
    ``YYYYMMDDHHMMSS`` 字符串。其它形式回退到 ``default``。
    """
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%d%H%M%S"):
        try:
            return int(time.mktime(time.strptime(text, fmt)))
        except ValueError:
            continue
    return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, list):
        return value
    return [value]


# 大 QMT 的 m_nOpType -> MiniQMT 的两融族 order_type（#330）。27-32
# 两边编号相同；大 QMT 的 33/34（担保品买入/卖出）对应 MiniQMT 的
# CREDIT_BUY/CREDIT_SELL (23/24)；专项族在那边是 70-75，这里是 40-45。
# 80-83（可转债转股/回售）在 MiniQMT 没有对应常量，直接透传。
_CREDIT_ORDER_TYPE_BY_OP = {
    27: 27, 28: 28, 29: 29, 30: 30, 31: 31, 32: 32,
    33: 23, 34: 24,
    70: 40, 71: 41, 72: 42, 73: 43, 74: 44, 75: 45,
    80: 80, 81: 81, 82: 82, 83: 83,
}


def _credit_order_type_from_op(op_type, fallback):
    """行的原生 opType 对应的 MiniQMT order_type，没有则用 ``fallback``。

    过去 query_stock_orders 只按 BUY/SELL 推导 order_type，于是融资买入
    （opType 27）被读成 STOCK_BUY 23（#330）。这里只映射两融族和可转债族；
    普通 23/24 以及期货/期权编号仍沿用方向判据的结果。
    """
    try:
        value = int(op_type)
    except (TypeError, ValueError):
        return fallback
    return _CREDIT_ORDER_TYPE_BY_OP.get(value, fallback)


# enum_EEntrustTypes（迅投知识库）: 54 融资委托 / 55 融券委托 / 57 信用普通委托。
# reporter 实盘数据（#330, 2026-09-23）: 融资买入 m_eEntrustType=54、担保品
# 买入=57，而 m_nOpType 两者没有区别——0.3.52 按 m_nOpType 的判据因此
# 对这批终端永不生效。m_eEntrustType + 开平方向才是可靠判据。
_CREDIT_ORDER_TYPE_BY_ENTRUST = {
    # (entrust_type, is_buy) -> MiniQMT 的 order_type
    (54, True): 27,    # 融资委托 + 买 = 融资买入 CREDIT_FIN_BUY
    (54, False): 31,   # 融资委托 + 卖 = 卖券还款 CREDIT_SELL_SECU_REPAY
    (55, True): 29,    # 融券委托 + 买 = 买券还券 CREDIT_BUY_SECU_REPAY
    (55, False): 28,   # 融券委托 + 卖 = 融券卖出 CREDIT_SLO_SELL
    (57, True): 23,    # 信用普通委托 + 买 = 担保品买入 CREDIT_BUY
    (57, False): 24,   # 信用普通委托 + 卖 = 担保品卖出 CREDIT_SELL
}

# 「专项」名称只从 m_strOptName 拿（entrust 枚举不分专项）。
_CREDIT_SPECIAL_UPGRADE = {27: 40, 28: 41, 29: 42, 31: 44, 32: 45}


def _credit_order_type_from_entrust(entrust_type, offset_flag, opt_name, fallback):
    """由 m_eEntrustType + 方向得到的 MiniQMT order_type，没有则用 ``fallback``。

    ``opt_name`` 含「专项」时升级到专项族（40-45）；56 信用平仓等未列出的
    类别回退给调用方的下一个判据。
    """
    try:
        entrust = int(entrust_type)
    except (TypeError, ValueError):
        return fallback
    try:
        side = int(offset_flag)
    except (TypeError, ValueError):
        return fallback
    if side == 48:
        is_buy = True
    elif side == 49:
        is_buy = False
    else:
        return fallback
    mapped = _CREDIT_ORDER_TYPE_BY_ENTRUST.get((entrust, is_buy))
    if mapped is None:
        return fallback
    if "专项" in str(opt_name or ""):
        mapped = _CREDIT_SPECIAL_UPGRADE.get(mapped, mapped)
    return mapped


def _credit_order_type_for_row(item, fallback):
    """快照 dict 的两融 order_type：先看 entrust，再看 op（#330）。

    0.3.55+ 的服务端在快照里带 m_eEntrustType；老服务端/老终端没有它时
    退回 0.3.52 的 m_nOpType 映射，再不行才用方向推的 23/24。
    """
    order_type = _credit_order_type_from_entrust(
        item.get("entrust_type"),
        item.get("offset_flag", item.get("direction")),
        item.get("opt_name"),
        None)
    if order_type is not None:
        return order_type
    return _credit_order_type_from_op(item.get("op_type"), fallback)



def _account_type_name(value):
    """账户类型的名称，不限定它到达时的形式。

    StockAccount 保存的是数字代码，而不是构造时传入的字符串：
    StockAccount(id, "CREDIT").account_type 是 3。拿它和服务端的 "CREDIT"
    比较，会让每个两融账户都报不匹配。
    """
    text = str("" if value is None else value).strip()
    if not text:
        return ""
    if text.isdigit():
        try:
            from xtquant.xtconstant import ACCOUNT_TYPE_DICT

            return str(ACCOUNT_TYPE_DICT.get(int(text), "")).strip().upper()
        except Exception:
            return ""
    return text.upper()


def _account_type_code(value):
    """账户类型对应的 xtconstant 数字，不限定它到达时的形式。

    _account_type_name 的镜像。XtOrder / XtTrade / XtPosition 都用 int 携带
    account_type（xttype 把它置为 SECURITY_ACCOUNT），所以名称必须在交给
    调用方之前变回数字。0 表示“没有依据”——回退什么由调用方决定。
    """
    text = str("" if value is None else value).strip()
    if not text:
        return 0
    try:
        return int(text)
    except (TypeError, ValueError):
        pass
    upper = text.upper()
    try:
        from xtquant import xtconstant
    except Exception:
        return 0
    # 不能只建 ACCOUNT_TYPE_DICT 上：大 QMT 内部生效的 xtquant 是终端自带的
    # 那份，它只有本兼容层 538 个名字中的 91 个，且不含这个 dict。客户端也可能
    # 落到那份上——把 QMT 的 python 目录追加到 sys.path 是访问配置模块的公开
    # 做法。于是回退到各个 *_ACCOUNT 常量，两份都有。
    table = getattr(xtconstant, "ACCOUNT_TYPE_DICT", None)
    if isinstance(table, dict):
        for code, name in table.items():
            if str(name).strip().upper() == upper:
                try:
                    return int(code)
                except (TypeError, ValueError):
                    break
    for attribute in ("%s_ACCOUNT" % upper,
                      "SECURITY_ACCOUNT" if upper == "STOCK" else ""):
        if not attribute:
            continue
        code = getattr(xtconstant, attribute, None)
        if isinstance(code, int) and not isinstance(code, bool):
            return int(code)
    return 0


#: 服务端“此答案已降级”标记落在重建后的 frame 上的位置。
#: ``DataFrame.attrs`` 需要 pandas >= 1.0；更老的版本会直接丢掉该标记，
#: 因此下游任何逻辑都不能依赖它存在。
PARTIAL_MARKER_ATTR = "bigqmt_partial"

#: 已经告警过的原因，使轮询调用方只被通知一次，而不是每拉一次 K 线
#: 就通知一次（#139 正是不加节流的逐次日志的代价）。
_partial_warned = set()


def _attach_partial_marker(frame, marker):
    if not isinstance(marker, dict):
        return frame
    try:
        frame.attrs[PARTIAL_MARKER_ATTR] = dict(marker)
    except Exception:
        pass
    return frame


def _partial_marker(frame):
    """frame 上的服务端降级答案标记，没有则为 None。"""
    try:
        marker = frame.attrs.get(PARTIAL_MARKER_ATTR)
    except Exception:
        return None
    return marker if isinstance(marker, dict) else None


def _warn_partial_market_data(markers):
    """每种降级只说明一次：答案不是完整的那个。

    服务端也会记这条日志，但服务端日志在交易机上，调用方看不到——而 #237
    要解决的整个失效模式，正是答案看起来完整、实际并不完整。
    """
    for marker in markers:
        if not isinstance(marker, dict):
            continue
        key = (
            str(marker.get("reason") or ""),
            str(marker.get("period") or ""),
            str(marker.get("source") or ""),
            ",".join(str(name) for name in (marker.get("missing") or [])),
            bool(marker.get("fill_data_dropped")),
        )
        if key in _partial_warned:
            continue
        _partial_warned.add(key)
        message = (
            "bigqmt: %s bars for period=%s were served by %s, not the usual "
            "path: columns served %s%s%s. See DataFrame.attrs[%r]."
            % (marker.get("reason") or "partial",
               marker.get("period") or "?",
               marker.get("source") or "?",
               ",".join(str(name) for name in (marker.get("served") or [])) or "-",
               ("; MISSING %s" % ",".join(str(n) for n in marker["missing"]))
               if marker.get("missing") else "",
               "; fill_data did not reach the terminal"
               if marker.get("fill_data_dropped") else "",
               PARTIAL_MARKER_ATTR))
        warnings.warn(message, stacklevel=2)
        log.warning("%s", message)


def _restore_jsonable(value):
    if isinstance(value, dict):
        marker = value.get("__bigqmt_type__")
        if marker == "DataFrame":
            try:
                import pandas as pd

                frame = pd.DataFrame(value.get("records") or [],
                                     columns=value.get("columns") or None)
            except Exception:
                return value.get("records") or []
            # #237：服务端会标记“这不是所请求的答案”——列少了、参数被丢弃、
            # 换了服务对象。把它带到 frame 上，调用方不用去读终端日志就能看到。
            # 没有这个键的答案按原样重建，因此旧服务端与新客户端仍然一致。
            _attach_partial_marker(frame, value.get("__bigqmt_partial__"))
            return frame
        if marker == "Panel":
            # pandas 1.0 起移除了 Panel，现代客户端无法重建三维对象。它以一种
            # 调用方真正能用的形式返回：{item: DataFrame}（问题 #115）。轴标签
            # 一并带上，供需要知道这个立方体如何切分的人使用。
            return {key: _restore_jsonable(item)
                    for key, item in (value.get("data") or {}).items()}
        if marker == "Series":
            try:
                import pandas as pd

                return pd.Series(value.get("data") or {})
            except Exception:
                return value.get("data") or {}
        return {key: _restore_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_restore_jsonable(item) for item in value]
    return value


_KNOWN_BAR_FIELDS = {
    "time", "open", "high", "low", "close", "volume", "amount",
    "settle", "openInterest", "preClose", "suspendFlag",
}


def _to_documented_market_data_shape(data, field_list, stock_list, period):
    """MiniQMT 文档化的 get_market_data K 线周期契约：
    ``dict[field] -> pd.DataFrame(index=stock_list, columns=time_list)``。

    大 QMT 对单只股票返回裸的长表 DataFrame，对多只股票返回
    ``dict[stock] -> 长表 DataFrame``——两者都不符合契约（报告方 2026-09-10
    的打印结果）。这里在客户端做透视：服务端仍保留长表形状，因此原生 RPC
    调用方和全零修复路径都不受影响，也不需要在 QMT 侧重部署。

    已符合文档的答案（键为字段名）以及任何不是逐股长表的形状（tick 周期、
    空表、标量）都原样透传。
    """
    if str(period or "").lower() == "tick":
        return data
    try:
        import pandas as pd
    except Exception:
        return data

    if hasattr(data, "columns"):
        # 裸 frame：单只股票，没有 dict 包装。
        code = str((_as_list(stock_list) or [""])[0])
        per_stock = {code: data}
    elif isinstance(data, dict) and data:
        keys = [str(k) for k in data.keys()]
        if all(k in _KNOWN_BAR_FIELDS for k in keys):
            return data  # 已是文档化的 {field: 宽表}
        per_stock = data
    else:
        return data

    fields = [str(f) for f in (field_list or []) if str(f) != "time"]
    wide = {}
    for code, frame in per_stock.items():
        # pandas Index 做真值判断会抛异常——这里不能写 `or []`。
        _cols = getattr(frame, "columns", None)
        columns = list(_cols) if _cols is not None else []
        if not columns:
            continue
        # RPC 路径的长表带 'index'，FormulaServer 构造的带
        # 'stime'。两者都是时间轴。
        time_col = next(
            (c for c in ("time", "index", "stime") if c in columns), None)
        if time_col is None:
            return data  # 不是 K 线长表——原样透传
        wanted = fields or [c for c in columns if c != time_col]
        for field in wanted:
            if field not in columns:
                continue
            wide.setdefault(field, {})[code] = frame.set_index(time_col)[field]
    if not wide:
        return data
    out = {field: pd.DataFrame(series).T for field, series in wide.items()}
    # MiniQMT 的 time_list 是字符串（'20260901'，dtype='str'——在实盘 miniQMT
    # 上打印 data['open'].columns 验证过，而不是靠直接 print，那样两种情况下
    # 都看不到引号）。把所有标签统一成 str，使 frame 的 dtype 与 miniQMT 一致。
    for frame in out.values():
        frame.columns = [str(c) for c in frame.columns]
    return out


# xtdata.get_divid_factors 的七个权息列（dict.thinktrader.net「除权数据」一节），
# 顺序就是大 QMT 原生返回里那个 7 元素列表的位置顺序：
#   dict{毫秒时间戳: [每股红利, 每股送转, 每转赠, 配股, 配股价, 是否股改, 复权系数]}
DIVID_FACTOR_COLUMNS = ("interest", "stockBonus", "stockGift",
                        "allotNum", "allotPrice", "gugai", "dr")
# 官方 frame 的完整列：time（毫秒时间戳）在前，然后是七个权息列，全部 float64。
DIVID_FRAME_COLUMNS = ("time",) + DIVID_FACTOR_COLUMNS

# 大 QMT 给的除权日毫秒戳是上海时间零点（实测三个样本 (ms/1000 + 8h) % 86400 == 0）。
# 用固定 +8h 折成 YYYYMMDD，不依赖客户端机器的时区。
_SHANGHAI_OFFSET_S = 8 * 3600


def _divid_day_key(key):
    """大 QMT 的毫秒时间戳键 -> xtdata 用作索引的 YYYYMMDD 字符串。

    已经是 YYYYMMDD 的键（8 位数字）原样返回；不是纯数字的一律不动，不做猜测。
    """
    import datetime as _dt

    text = str(key).strip()
    if len(text) == 8 and text.isdigit():
        return text
    try:
        number = float(text)
    except (TypeError, ValueError):
        return text
    seconds = number / 1000.0 if number > 1e11 else number
    day = _dt.datetime(1970, 1, 1) + _dt.timedelta(seconds=seconds + _SHANGHAI_OFFSET_S)
    return day.strftime("%Y%m%d")


def _divid_factors_frame(data):
    """``dict{ms: [7 个值]}`` -> ``xtdata.get_divid_factors`` 返回的 DataFrame。

    对照真实 xtdata 实测（在实盘 miniQMT 上 ``df.info()``）：

        Index: 19990823 to 20080707            <- 除权日，YYYYMMDD
        time, interest, stockBonus, stockGift,
        allotNum, allotPrice, gugai, dr        <- 8 列，均为 float64

    链路上传的是大 QMT 的原生形状——以当日毫秒时间戳为键、值为按位置排列的
    7 元素列表——所以这里补上日期索引、``time`` 列（毫秒键，float）、列名和
    float64 类型，做法与 ``get_market_data_ex`` 把链路记录转成 frame 相同。
    直接透传该 dict 会让所有按真实 xtdata 编写的调用方在 ``df["dr"]`` 上
    遇到 KeyError。

    行序保持服务端顺序（终端给出的时间顺序）。已是具名 dict 的值按名字读取。
    不是 dict 的一律原样透传，因此错误信封不会被变成空表。
    """
    if not isinstance(data, dict):
        return data
    import pandas as pd

    columns = list(DIVID_FRAME_COLUMNS)
    if not data:
        return pd.DataFrame(columns=columns, dtype="float64")
    rows = {}
    for key, value in data.items():
        try:
            time_ms = float(key)
        except (TypeError, ValueError):
            time_ms = float("nan")
        if isinstance(value, dict):
            if value.get("time") is not None:
                try:
                    time_ms = float(value["time"])
                except (TypeError, ValueError):
                    pass
            factors = [value.get(name) for name in DIVID_FACTOR_COLUMNS]
        else:
            seq = list(value) if isinstance(value, (list, tuple)) else [value]
            factors = (seq + [None] * len(DIVID_FACTOR_COLUMNS))[:len(DIVID_FACTOR_COLUMNS)]
        rows[_divid_day_key(key)] = [time_ms] + factors
    frame = pd.DataFrame.from_dict(rows, orient="index", columns=columns)
    return frame.astype("float64")


def _digits_only(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _time_compare_digits(value):
    """14 位数字字符串，使 QMT 时间戳可按时间先后比较。"""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return ""
    return digits[:14].ljust(14, "0")


def _parse_qmt_stime(value):
    digits = _digits_only(value)
    if len(digits) >= 14:
        try:
            return _dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None
    if len(digits) >= 8:
        try:
            return _dt.datetime.strptime(digits[:8], "%Y%m%d")
        except ValueError:
            return None
    return None


# FormulaServer 快照滞后检测：实测它会把 1m 数据冻结数小时（11:30 后不再
# 更新，收盘后还在发午间数据）。对直连回答的 intraday 数据做时间差检测：
# 滞后即告警 + 本次自动回落 RPC 桥拿实时数据，并在冷却期内跳过直连
# （冷却到期自动重新探测，自愈）。
_FORMULA_STALE_INTRADAY_PERIODS = ("tick", "1m", "3m", "5m", "15m", "30m", "1h")
_FORMULA_STALE_WARN_LAG_SECONDS = 30 * 60
_FORMULA_STALE_COOLDOWN_SECONDS = 120.0
_formula_stale_warned = {}
_formula_stale_until = {"ts": 0.0}


def _formula_bars_stale(data, params):
    """返回 (code, newest_dt, lag_seconds) 或 None。只检测，不告警。"""
    try:
        period = str((params or {}).get("period") or "").lower()
        if period not in _FORMULA_STALE_INTRADAY_PERIODS:
            return None
        now = _dt.datetime.now()
        today = now.date()
        for code, df in (data or {}).items():
            # 公式直连返回的帧时间轴在 stime 列（RangeIndex），
            # 兼容层归一化后的帧在索引上——两种形态都认。
            newest_value = None
            for col in ("stime", "time"):
                if col in list(getattr(df, "columns", [])):
                    newest_value = df[col].iloc[-1]
                    break
            if newest_value is None:
                index = getattr(df, "index", None)
                if index is not None and len(index):
                    newest_value = index[-1]
            newest = _parse_qmt_stime(newest_value)
            if newest is None:
                continue
            lag = (now - newest).total_seconds()
            if (newest.date() < today) or (lag > _FORMULA_STALE_WARN_LAG_SECONDS):
                return (str(code), newest, lag)
    except Exception:
        pass
    return None


def _warn_stale_formula_bars(data, params, hit=None):
    try:
        period = str((params or {}).get("period") or "").lower()
        today = _dt.datetime.now().date()
        if hit is None:
            hit = _formula_bars_stale(data, params)
        if hit is None:
            return
        code, newest, lag = hit
        key = (code, period, str(today))
        if _formula_stale_warned.get(key):
            return
        _formula_stale_warned[key] = True
        log.warning(
            "FormulaServer 返回的 %s %s 数据疑似过期：最新 K 线 %s 落后当前时间 %.0fs。"
            "实时读取将回退到 RPC 桥接（冷却 %.0fs）。",
            code, period, newest, lag, _FORMULA_STALE_COOLDOWN_SECONDS,
        )
    except Exception:
        pass


def _formula_stale_active():
    """冷却期内跳过公式直连（检测到滞后之后的一段时间）。"""
    return time.time() < _formula_stale_until.get("ts", 0.0)


def _qmt_stime_index(value):
    digits = _digits_only(value)
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) >= 8:
        return digits[:8]
    return str(value or "")


_EPOCH_PLACEHOLDER_FLOOR = "19900101"


def _is_epoch_placeholder_label(value):
    """对于早于 A 股市场本身的 K 线标签返回 True（#228）。

    请求一段它没有数据的区间时，大 QMT 不会返回零行——它返回一行盖在纪元上
    的数据（`stime` 为 `19700101`，OHLC 和成交量全为零）。miniQMT 在那里
    返回空表，而那才是本桥接层承诺的契约。

    这一行并非无害：遍历行来推导周期边界的调用方会把它当成真实 K 线，并向
    日历询问 `1969-12-29~1970-01-04`。有一个下游实例就因此在 09:11 盘前挂掉，
    一直空转到有人手动停掉它，而它此前已经落盘了需要人工清理的
    `bar_time=1970-01-01` 行。

    下限取市场自身的起点，不是可调参数：上海证券交易所 1990 年 12 月开市，
    更早的都不可能是真实 K 线。落在 *合理* 日期上的全零行保持原样——停牌日
    成交量确实为零，而且 `fill_data=True` 本来就是故意补空缺。
    """
    digits = _digits_only(value)
    if len(digits) < 8:
        return False
    return digits[:8] < _EPOCH_PLACEHOLDER_FLOOR


def _iso_week_start_label(value):
    """``1w`` K 线标签所属 ISO 周的周一，格式 YYYYMMDD。

    大 QMT 用 ISO 周的 **周日** 标注周线。在实盘终端上实测，19/19 周的周线
    收盘价都等于周一至周日内最后一个交易日的收盘价，其中两周因假期缩短——
    所以该标签是周期结束日，不是周期起始日（#166）。

    从标签本身（而不是上一根 K 线所在位置）倒推周一，才可能为窗口的第一根
    K 线补值。无法解析时返回 None，让调用方什么都不补，而不是靠猜。
    """
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    day = parsed.date()
    return (day - _dt.timedelta(days=day.weekday())).strftime("%Y%m%d")


def _month_start_label(value):
    """K 线标签所属月份的 1 号，格式 YYYYMMDD。"""
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y%m") + "01"


def _quarter_start_label(value):
    """K 线标签所属季度第一个月的 1 号。"""
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    month = ((parsed.month - 1) // 3) * 3 + 1
    return "%04d%02d01" % (parsed.year, month)


def _halfyear_start_label(value):
    """K 线标签所属半年的 1 月 1 日或 7 月 1 日。"""
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    return "%04d0101" % parsed.year if parsed.month <= 6 else "%04d0701" % parsed.year


def _year_start_label(value):
    """K 线标签所属年份的 1 月 1 日。"""
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    return "%04d0101" % parsed.year


def _period_end_label(period, value):
    """K 线标签所属自然周期的最后一个日历日。

    仅用于区分进行中的 K 线（周期尚未结束）与已结束的 K 线。
    无法解析时返回 None。
    """
    parsed = _parse_qmt_stime(value)
    if parsed is None:
        return None
    day = parsed.date()
    if period == "1w":
        return (day - _dt.timedelta(days=day.weekday()) + _dt.timedelta(days=6)).strftime("%Y%m%d")
    if period == "1mon":
        year, month = parsed.year, parsed.month
        end = _dt.date(year + month // 12, month % 12 + 1, 1) - _dt.timedelta(days=1)
        return end.strftime("%Y%m%d")
    if period == "1q":
        month = ((parsed.month - 1) // 3) * 3 + 3
        end = _dt.date(parsed.year + month // 12, month % 12 + 1, 1) - _dt.timedelta(days=1)
        return end.strftime("%Y%m%d")
    if period == "1hy":
        return "%04d0630" % parsed.year if parsed.month <= 6 else "%04d1231" % parsed.year
    if period == "1y":
        return "%04d1231" % parsed.year
    return None


def _bar_float(value):
    """K 线单元格转成 float，非数字一律返回 None。

    NaN 有意算作“不是数字”：preClose 整列为 NaN 与整列为 0 含义相同——
    终端没有作答。
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number


def _frame_bar_labels(frame):
    """按行顺序的 K 线标签，取自规范化后的索引或 stime 列。

    两个来源都先检查是否为日期形状，而不直接信任：没经过
    _normalize_market_data_frame 的 frame 仍带位置索引，否则 0/1/2 会被
    当成 K 线标签。
    """
    for from_index in (True, False):
        try:
            values = list(frame.index) if from_index else list(frame["stime"])
        except Exception:
            continue
        labels = [_qmt_stime_index(value) for value in values]
        if labels and all(len(_digits_only(label)) >= 8 for label in labels):
            return labels
    return []


def _qmt_datetime_to_epoch_ms(dt_value):
    # QMT 的 K 线标签是中国本地时间；MiniQMT 的 time 列是毫秒时间戳。
    china_tz = _dt.timezone(_dt.timedelta(hours=8))
    return int(dt_value.replace(tzinfo=china_tz).timestamp() * 1000)


def _normalize_market_data_frame(df, field_list=None):
    try:
        columns = list(df.columns)
    except Exception:
        return df
    if "stime" not in columns:
        return df

    requested = [str(field) for field in (field_list or [])]
    try:
        out = df.copy()
        stimes = list(out["stime"])
        out.index = [_qmt_stime_index(value) for value in stimes]
        if "time" in out.columns or "time" in requested:
            out["time"] = [
                _qmt_datetime_to_epoch_ms(parsed) if parsed is not None else None
                for parsed in (_parse_qmt_stime(value) for value in stimes)
            ]
        # 丢掉大 QMT “该区间无数据”的占位行（#228）。在派生列生成后按位置
        # 删除，因此保留下来的行仍持有属于自己的数值。
        keep = [
            position for position, value in enumerate(stimes)
            if not _is_epoch_placeholder_label(value)
        ]
        if len(keep) != len(stimes):
            out = out.iloc[keep]
        if requested:
            keep = [field for field in requested if field in out.columns]
            if keep:
                return out[keep]
        if "stime" in out.columns:
            return out.drop(columns=["stime"])
        return out
    except Exception:
        return df


def _ensure_preclose_from_lag(df, field_list=None, period=None):
    """preClose 行级兜底: 该列缺失/为0 且当日 close 有效时, 用前收 lag 补。

    big QMT 对正常股票返回真实 preClose; 但 (a) 早期本地缓存文件缺该列,
    (b) 个别行可能为0。除权除息日 lag(前收) 与官方 preClose 略有偏差, 可接受。

    两条边界（第一版没有，打红了 11 个测试）：

    * **只在调用方要了这列时才补列**——``field_list`` 为空是 MiniQMT 的
      「全字段」，含 preClose；显式给了 ``field_list`` 且没点 preClose 的，
      返回列要和 MiniQMT 一样只有点名的那些，不能多出一列。
    * **有精确来源的周期不用 lag 填 0**：``1w`` 的 preClose 终端恒为 0，
      ``_backfill_pre_close`` 按日线取周首日的真实前收（#166），除权在周首日
      时 lag 是错的；lag 先把 0 盖掉，回填就再也看不到「缺」了。回填关掉或
      没有日线时留 0——沉默的错价比 0 更糟，#166 的测试就是这么钉的。
    """
    try:
        if not hasattr(df, "columns") or "close" not in df.columns:
            return df
        requested = [str(f) for f in (field_list or [])]
        wants_column = not requested or "preClose" in requested
        close = df["close"]
        valid_close = close.notna() & (close > 0)
        lag = close.shift(1)
        fill = lag.where(lag.notna() & (lag > 0), close)
        if "preClose" not in df.columns:
            if not wants_column:
                return df
            # 停牌/无数据帧也保证列存在(填0), 与 MiniQMT 行为一致, 避免下游 KeyError
            out = df.copy()
            out["preClose"] = fill.where(valid_close, 0)
            return out
        if str(period or "") in PRE_CLOSE_BACKFILL_PERIODS:
            return df
        if not bool(valid_close.any()):
            return df
        bad = (df["preClose"].isna() | (df["preClose"] == 0)) & valid_close
        if not bool(bad.any()):
            return df
        out = df.copy()
        out.loc[bad, "preClose"] = fill[bad]
        return out
    except Exception:
        return df


def _ensure_suspend_flag_column(df, field_list=None):
    """suspendFlag 列形状兜底: 空 field_list(=全字段)或点名时, 缺列则补 0。

    合成回落帧(``synth_period_primary_empty``, #237)的 servant
    (ContextInfo.get_market_data)只服务 OHLCV+amount 六列, 实测把
    suspendFlag 混进请求会让整个请求 0 行, 所以服务端给不出这列——
    MiniQMT 的同请求却带全部 11 列。下游按 MiniQMT 习惯读
    ``df["suspendFlag"]`` 会 KeyError; 自己 concat 补列会拿到 NaN,
    对它做整数转换直接 ValueError。

    与 ``_ensure_preclose_from_lag`` 的缺列分支同一条边界: 只在调用方
    要了这列(field_list 为空或点了 suspendFlag)时补列, 显式点名清单
    不多出列。值填 0 与该函数对停牌/无数据帧的处理一致(避免下游
    KeyError); 注意 0 是形状契约的默认值, 不是合成周期的真实停牌
    标志——真值需要按日线聚合(suspendFlag 在日线 RPC 有真值), 可作
    后续增强, 同 preClose 的 #166 精确回填之于 lag 的关系。
    """
    try:
        if not hasattr(df, "columns"):
            return df
        if "suspendFlag" in df.columns:
            return df
        requested = [str(f) for f in (field_list or [])]
        if requested and "suspendFlag" not in requested:
            return df
        out = df.copy()
        out["suspendFlag"] = 0
        return out
    except Exception:
        return df


def _ensure_kline_columns(df, field_list=None, period=None):
    """K 线可选列的形状兜底组合: preClose(lag) 之后补 suspendFlag。"""
    return _ensure_suspend_flag_column(
        _ensure_preclose_from_lag(df, field_list=field_list, period=period),
        field_list=field_list)


def _normalize_market_data_result(data, field_list=None, period=None):
    if not isinstance(data, dict):
        return _ensure_kline_columns(data, field_list=field_list, period=period)
    return {
        code: _ensure_kline_columns(
            _normalize_market_data_frame(frame, field_list=field_list),
            field_list=field_list, period=period)
        for code, frame in data.items()
    }


def _normalize_code_for_filter(code):
    text = str(code or "").strip().upper()
    if "." not in text:
        return text
    return text.split(".", 1)[0]


def _is_hs_a_share(code):
    text = str(code or "").strip().upper()
    pure = _normalize_code_for_filter(text)
    if not (len(pure) == 6 and pure.isdigit()):
        return False
    if text.endswith(".SH"):
        return pure.startswith(("600", "601", "603", "605", "688", "689"))
    if text.endswith(".SZ"):
        return pure.startswith(("000", "001", "002", "003", "300", "301"))
    return pure.startswith(
        ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
    )


def _full_a_share_code(code):
    """确保回调的 stock_code 带交易所后缀。

    原生 MiniQMT 的 XtOrder/XtTrade 携带完整 '600000.SH' 形式。来自旧服务端的
    事件（或回调对象不暴露交易所信息时）可能只带裸 6 位代码；此时按 A 股代码
    区间推断后缀，使消费方可以按完整形式作键。非 6 位或已带后缀的代码
    原样透传。
    """
    text = str(code or "").strip().upper()
    if "." in text or not (len(text) == 6 and text.isdigit()):
        return text
    if text.startswith(("600", "601", "603", "605", "688", "689")):
        return text + ".SH"
    if text.startswith(("000", "001", "002", "003", "300", "301")):
        return text + ".SZ"
    return text


class BigQmtRpcClient:
    def __init__(
        self,
        account_id=None,
        valkey_client=None,
        valkey_config=None,
        timeout_seconds=None,
        transport=None,
    ):
        local_env = _client_local_env()
        client_config = load_client_config(local_env.get(CLIENT_CONFIG_MODULE_ENV))
        valkey_config = valkey_config or {}
        module_valkey_config = client_config.get("valkey_config") or {}
        local_valkey_config = {
            key: local_env[variable]
            for key in ("host", "port", "db", "username", "password", "protocol", "url")
            if (variable := "BIGQMT_VALKEY_" + key.upper()) in local_env
        }
        merged_valkey_config = dict(module_valkey_config)
        merged_valkey_config.update(local_valkey_config)
        merged_valkey_config.update(valkey_config)
        self.account_id = str(
            account_id
            or valkey_config.get("account_id")
            or local_env.get("BIGQMT_ACCOUNT_ID", "").strip()
            or module_valkey_config.get("account_id")
            or client_config.get("account_id")
            or os.environ.get("BIGQMT_ACCOUNT_ID")
            or ""
        )
        self.valkey_client = valkey_client
        db = merged_valkey_config.get("db")
        if db is None or db == "":
            db = os.environ.get("BIGQMT_VALKEY_DB") or 5
        # 「用户显式配了 valkey」和「默认填充」要分得开：非 valkey 传输的客户端
        # 默认没有 valkey 可连，事件线程每轮开头那次 ping 会照
        # 127.0.0.1:6379 拨——在外连即杀的沙箱里一次探测就死（2026-09-24
        # 实盘逐行核对）。显式信号：合并后的配置或环境变量里给了 host /
        # username / password 任何一个。
        self._valkey_explicit = bool(
            merged_valkey_config.get("host")
            or merged_valkey_config.get("username")
            or merged_valkey_config.get("password")
            or merged_valkey_config.get("url")
            or os.environ.get("BIGQMT_VALKEY_HOST")
            or os.environ.get("BIGQMT_VALKEY_USERNAME")
            or os.environ.get("BIGQMT_VALKEY_PASSWORD")
            or os.environ.get("BIGQMT_VALKEY_URL")
        )
        self.valkey_config = {
            "host": merged_valkey_config.get("host") or os.environ.get("BIGQMT_VALKEY_HOST", "127.0.0.1"),
            "port": int(merged_valkey_config.get("port") or os.environ.get("BIGQMT_VALKEY_PORT") or 6379),
            "db": int(db),
            "username": merged_valkey_config.get("username", os.environ.get("BIGQMT_VALKEY_USERNAME") or ""),
            "password": merged_valkey_config.get("password", os.environ.get("BIGQMT_VALKEY_PASSWORD") or ""),
            "protocol": merged_valkey_config.get("protocol") or os.environ.get("BIGQMT_VALKEY_PROTOCOL") or 2,
        }
        self._valkey_url = merged_valkey_config.get("url") or os.environ.get("BIGQMT_VALKEY_URL")
        if local_valkey_config:
            for settings in (valkey_config, local_valkey_config):
                if settings.get("url") or any(
                    settings.get(key) not in (None, "") for key in ("host", "port", "db")
                ):
                    self._valkey_url = settings.get("url")
                    break
        config_timeout = _first_set(
            local_env.get("BIGQMT_RPC_TIMEOUT_SECONDS") or None, client_config.get("timeout_seconds"))
        self.timeout_seconds = float(
            timeout_seconds
            if timeout_seconds is not None
            else config_timeout
            if config_timeout is not None
            else _env_float("BIGQMT_RPC_TIMEOUT_SECONDS", DEFAULT_RPC_TIMEOUT_SECONDS)
        )
        config_download_wait = _first_set(
            local_env.get("BIGQMT_DOWNLOAD_WAIT_SECONDS") or None, client_config.get("download_wait_seconds"))
        self.download_wait_seconds = float(
            config_download_wait
            if config_download_wait is not None
            else _env_float("BIGQMT_DOWNLOAD_WAIT_SECONDS", 1800.0)
        )
        config_download_poll = _first_set(
            local_env.get("BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS") or None,
            client_config.get("download_poll_interval_seconds"))
        self.download_poll_interval_seconds = float(
            config_download_poll
            if config_download_poll is not None
            else _env_float("BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS", 0.5)
        )
        # 功能配置沿用连接配置的优先级（#289）：显式参数、本地 .env、
        # 配置模块、进程环境、默认值。
        full_tick = _ClientSetting(valkey_config, client_config.get("full_tick_cache_config"), local_env)
        self.full_tick_cache_config = {
            "enabled": _bool_value(
                full_tick.get("full_tick_cache_enabled", "enabled"),
                _env_bool("BIGQMT_FULL_TICK_CACHE_ENABLED", False),
            ),
            "demand_ttl_seconds": float(_first_set(
                full_tick.get("full_tick_demand_ttl_seconds", "demand_ttl_seconds"),
                _env_float("BIGQMT_FULL_TICK_DEMAND_TTL_SECONDS", 10.0))),
            "cache_ttl_seconds": float(_first_set(
                full_tick.get("full_tick_cache_ttl_seconds", "cache_ttl_seconds"),
                _env_float("BIGQMT_FULL_TICK_CACHE_TTL_SECONDS", 10.0))),
            "wait_seconds": float(_first_set(
                full_tick.get("full_tick_wait_seconds", "wait_seconds"),
                _env_float("BIGQMT_FULL_TICK_WAIT_SECONDS", 3.5))),
            "poll_interval_seconds": float(_first_set(
                full_tick.get("full_tick_poll_interval_seconds", "poll_interval_seconds"),
                _env_float("BIGQMT_FULL_TICK_POLL_INTERVAL_SECONDS", 0.2))),
        }
        # get_market_data_ex 将结果写入本地缓存。get_local_data 默认通过
        # RPC 补齐缓存，支持下载原始数据后按其他复权方式读取。
        local_cache = _ClientSetting(valkey_config, client_config.get("local_cache_config"), local_env)
        self.local_cache_config = {
            "enabled": _bool_value(
                local_cache.get("local_cache_enabled", "enabled"),
                _env_bool("BIGQMT_LOCAL_CACHE_ENABLED", True),
            ),
            "dir": (
                local_cache.get("local_cache_dir", "dir")
                or os.environ.get("BIGQMT_LOCAL_CACHE_DIR")
                or None
            ),
            "fallback_rpc": _bool_value(
                local_cache.get("local_cache_fallback_rpc", "fallback_rpc"),
                _env_bool("BIGQMT_LOCAL_CACHE_FALLBACK_RPC", True),
            ),
            "format": str(
                local_cache.get("local_cache_format", "format")
                or os.environ.get("BIGQMT_LOCAL_CACHE_FORMAT")
                or "auto"  # 有 pyarrow 时用 Parquet，否则用 pickle
            ),
        }
        # 默认使用 call_valkey_rpc，其余传输交给传输工厂。
        self.transport_name = str(
            transport
            or valkey_config.get("transport")
            or local_env.get("BIGQMT_RPC_TRANSPORT")
            or module_valkey_config.get("transport")
            or os.environ.get("BIGQMT_RPC_TRANSPORT")
            or "valkey"
        ).lower()
        self._transport_instance = None  # 由 _transport() 按需创建
        # FormulaServer（默认 58600 端口）直读历史和参考数据，失败时回退到 RPC。
        # 按字段合并模块配置、本地 .env 和显式参数（#289）。
        formula_config = dict(client_config.get("formula_server_config") or {})
        if local_env.get("BIGQMT_FORMULA_ENABLED"):
            formula_config["enabled"] = _bool_value(local_env["BIGQMT_FORMULA_ENABLED"])
        formula_config.update(valkey_config.get("formula_server") or {})
        if "enabled" not in formula_config:
            formula_config["enabled"] = _env_bool("BIGQMT_FORMULA_ENABLED", True)
        self.formula_server_config = formula_config
        self._formula_router_instance = None  # 由 _formula_router() 按需创建

    def _valkey(self):
        if self.valkey_client is None:
            client_class = valkey_client_class()
            cfg = dict(self.valkey_config)
            options = valkey_connection_options(cfg.pop("protocol"))
            if self._valkey_url:
                url = normalize_valkey_url(self._valkey_url)
                self.valkey_client = client_class.from_url(url, **options)
            else:
                cfg["username"] = cfg["username"] or None
                cfg["password"] = cfg["password"] or None
                self.valkey_client = client_class(**cfg, **options)
        return self.valkey_client

    def _transport(self):
        if self._transport_instance is None:
            if self.transport_name in ("valkey", "", "default"):
                # Valkey 路径由 call_valkey_rpc 构造请求。
                return None
            from .transports.factory import build_transport

            self._transport_instance = build_transport(
                self.transport_name,
                {},
                account_id=self.account_id,
                print_prefix="[bigqmt_client]",
            )
        return self._transport_instance

    def close(self):
        """释放可替换 transport 的连接（若已构造）。

        由 ``BigQmtXtTrader.stop()`` 调用。可重复调用；客户端再次使用时
        transport 会惰性重建。
        """
        transport = self._transport_instance
        self._transport_instance = None
        if transport is not None:
            try:
                transport.stop()
            except Exception:
                pass

    def _formula_router(self):
        """惰性构造 FormulaServer 路由器。从不抛异常——构造不出路由器只意味着
        所有读取都走 RPC。"""
        if self._formula_router_instance is None:
            try:
                from .formula_server import build_router

                self._formula_router_instance = build_router(
                    self.formula_server_config, print_prefix="[bigqmt_formula]"
                )
            except Exception as exc:
                print("[bigqmt_formula] 已禁用（%s：%s）"
                      % (exc.__class__.__name__, exc))

                class _Disabled(object):
                    def supports(self, method):
                        return False

                self._formula_router_instance = _Disabled()
        return self._formula_router_instance

    def call_tracked(self, method, params=None, account_id=None, request_id=None,
                     timeout_seconds=None):
        """用调用方指定的 request_id 执行 ``call``，以便在超时后向服务端
        追问该请求的结果（#303）。"""
        return self.call(method, params, account_id=account_id,
                         timeout_seconds=timeout_seconds, request_id=request_id)

    def call(self, method, params=None, account_id=None, timeout_seconds=None, use_formula=True,
             request_id=None):
        target_account = str(account_id or self.account_id or "")
        if not target_account:
            raise ValueError(_missing_account_id_message())
        wait_seconds = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        # 参考和历史数据优先从 FormulaServer 直读。方法、参数或服务不可用时，
        # 捕获 Unroutable，继续走 RPC 桥。
        # use_formula=False 用于必须拿到最新数据的调用（如 subscribe_quote 的
        # 盘中形成 bar 轮询）——FormulaServer 的快照可能滞后数小时（实测盘中
        # 11:30 后冻结），形成 bar 只能走 RPC 桥读 QMT 实时数据。
        router = self._formula_router() if use_formula else None
        # 冷却期（公式数据刚被检出滞后）只对 get_market_data_ex 跳过直连——
        # 其他方法是静态参考数据，不受时间序列滞后影响，照常走快速路径。
        skip_formula = (
            router is not None
            and method in ("get_market_data_ex", "get_market_data")
            and _formula_stale_active()
        )
        if router is not None and router.supports(method) and not skip_formula:
            from .formula_server import Unroutable

            try:
                result = _restore_jsonable(router.call(method, params or {}))
                if method in ("get_market_data_ex", "get_market_data"):
                    # 直连快照可能滞后（实测冻结数小时）——滞后即告警、
                    # 本次调用自动回落 RPC 桥拿实时数据，并进入冷却期
                    # 让后续调用直接跳过直连（到期重新探测，自愈）。
                    hit = _formula_bars_stale(result, params or {})
                    if hit is not None:
                        _warn_stale_formula_bars(result, params or {}, hit=hit)
                        _formula_stale_until["ts"] = time.time() + _FORMULA_STALE_COOLDOWN_SECONDS
                        return self.call(method, params, account_id=account_id,
                                         timeout_seconds=timeout_seconds, use_formula=False)
                return result
            except Unroutable:
                pass
        transport = self._transport()
        if transport is not None:
            # 与 call_valkey_rpc 使用相同的请求结构。
            request = {
                "schema_version": 1,
                "request_id": str(request_id or uuid.uuid4().hex),
                "account_id": target_account,
                "method": method,
                "params": params or {},
                "ttl_seconds": 60,
                # 客户端的等待时长，供服务端拒绝过期请求（#303）。
                "timeout_seconds": float(wait_seconds),
            }
            response = transport.send_request(request, wait_seconds)
        else:
            response = call_valkey_rpc(
                self._valkey(),
                account_id=target_account,
                method=method,
                params=params or {},
                timeout_seconds=wait_seconds,
                request_id=request_id,
            )
        if not response.get("ok"):
            raise RpcServerRepliedError(
                response.get("error") or "大 QMT RPC 调用失败: %s" % method)
        # server_error 携带 QMT 端诊断（如 passorder 提交但委托没进系统）。
        # 只在交易类方法上设置（读取类恒为空），转成异常让调用方看到真实原因，
        # 而不是把「无委托号」误判为 -1 失败（issue #38）。
        server_error = str(response.get("server_error") or "")
        if server_error:
            raise RpcServerRepliedError(
                "大 QMT %s server_error: %s" % (method, server_error))
        # 传输层确认没有类型标记时，跳过递归还原。
        # 未检查原文（如进程内调用）时，仍遍历数据。
        if response.pop(TYPED_PAYLOAD_FLAG, None) is False:
            return response.get("data")
        return _restore_jsonable(response.get("data"))

    # ------------------------------------------------------------------
    # call_async 立即返回 Future，允许客户端并发等待应答。
    # 下单 RPC 仍由 QMT 主线程串行执行（#63）。
    _ASYNC_RPC_MAX_IN_FLIGHT = 64

    def _async_rpc_pool(self):
        pool = getattr(self, "_rpc_async_pool", None)
        if pool is None:
            from concurrent.futures import ThreadPoolExecutor

            pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bigqmt-rpc-async")
            self._rpc_async_pool = pool
            self._rpc_async_slots = threading.Semaphore(self._ASYNC_RPC_MAX_IN_FLIGHT)
            self._rpc_async_dispatcher = None
        return pool

    def call_async(self, method, params=None, account_id=None, timeout_seconds=None, callback=None):
        """提交 RPC 但不阻塞；返回 concurrent.futures.Future。

        ``callback``（可选）在单个分发线程上收到结果——回调按完成顺序串行
        触发，绝不并发。在途请求数有上限；达到上限时直接抛异常，而不是
        无限制排队。
        """
        pool = self._async_rpc_pool()
        if not self._rpc_async_slots.acquire(timeout=30.0):
            raise RuntimeError(
                "在途 RPC 过多（上限 %d）" % self._ASYNC_RPC_MAX_IN_FLIGHT
            )

        def _run():
            try:
                return self.call(method, params, account_id=account_id,
                                 timeout_seconds=timeout_seconds)
            finally:
                self._rpc_async_slots.release()

        future = pool.submit(_run)
        if callback is not None:
            if self._rpc_async_dispatcher is None:
                from concurrent.futures import ThreadPoolExecutor

                self._rpc_async_dispatcher = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="bigqmt-rpc-dispatch"
                )
            dispatcher = self._rpc_async_dispatcher

            def _deliver(fut):
                try:
                    result = fut.result()
                except Exception as exc:
                    log.warning("call_async 调用 %s 失败：%s", method, exc)
                    return
                try:
                    callback(result)
                except Exception:
                    log.exception("call_async 回调执行失败 method=%s", method)

            future.add_done_callback(lambda fut: dispatcher.submit(_deliver, fut))
        return future

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        account_id = str(self.account_id or "")
        event = {
            "event_type": str(event_type),
            "account_id": account_id,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "payload": payload or {},
        }
        raw = json.dumps(event, ensure_ascii=False, default=str)
        stream_key = stream_template.format(account_id=account_id)
        valkey_client = self._valkey()
        try:
            valkey_client.xadd(stream_key, {"payload": raw}, maxlen=1000, approximate=True)
            # 客户端侧写的流同样要能自己消失（#213）。
            from .adapters.valkey_common import touch_stream_ttl

            touch_stream_ttl(valkey_client, stream_key)
        except Exception:
            pass
        try:
            valkey_client.publish(stream_key, raw)
        except Exception:
            pass
        return event

    def save_quote_subscription(self, seq, payload, active=True):
        account_id = str(self.account_id or "")
        key = "bigqmt:quote_subscriptions:%s" % account_id
        valkey_client = self._valkey()
        if active:
            value = json.dumps(payload or {}, ensure_ascii=False, default=str)
            try:
                valkey_client.hset(key, str(seq), value)
            except Exception:
                pass
        else:
            try:
                valkey_client.hdel(key, str(seq))
            except Exception:
                pass


# 新股申购代码是独立编号，与上市股票的代码不同。分类是 FAIL-CLOSED：
# 无法识别的代码返回 None，由调用方跳过。被替换掉的旧版本以 `return True`
# 结尾（「默认放行」），即它按有利于下单的方向猜测——而这条路径的职责恰恰
# 是把会冻结资金的北交所申购挡在外面。
_IPO_SH_PREFIXES = ("730", "732", "780", "787", "789", "707")
_IPO_SZ_PREFIXES = ("00", "30")
_IPO_BJ_PREFIXES = ("920", "889", "8", "4")


def ipo_market_of(code):
    """返回 "SH" / "SZ" / "BJ"，无法识别时返回 None。"""
    text = str(code or "").strip().upper()
    if not text:
        return None
    for suffix, market in ((".SH", "SH"), (".SZ", "SZ"), (".BJ", "BJ")):
        if text.endswith(suffix):
            return market
    if not text.isdigit():
        return None
    # 顺序很重要：否则北交所的 920/889 会先被更宽松的规则命中。
    if text.startswith(_IPO_BJ_PREFIXES):
        return "BJ"
    if text.startswith(_IPO_SH_PREFIXES):
        return "SH"
    if text.startswith(_IPO_SZ_PREFIXES):
        return "SZ"
    return None


MARKET_TOKENS = frozenset({"SH", "SZ", "BJ", "HK"})
# 显式代码超过这个数量之后，一次 RPC 的超时就比“读取交易所再过滤”的额外
# 开销更值得关注（问题 #104）。在实盘桥上实测：query_orders 1.5s、
# get_asset 1.4s、get_financial_data 热态 0.8s、全市场 get_full_tick 7.7s。
# 旧的 6s 默认值低于普通 QMT 数据调用的成本，而在这里超时比等待更糟：桥接层
# 仍在继续处理被放弃的请求，于是下一次调用排在它后面，一次超时衍生更多超时。
# 全市场快照路径本来就用了 30s，所以只有一个数字而不是两个。
DEFAULT_RPC_TIMEOUT_SECONDS = 30.0

# 批量超时按规模放大：服务端在 adjust 线程上串行执行批量项。盘中每项成本是
# 毫秒级，但柜台断连时约 300ms——于是 100 项的批量连 30s 默认值都撑不到
# （实盘 2026-09-05：客户端放弃并退回逐笔，而仍在运行的批量也完成了，100 笔
# 委托变成 200 笔）。等待时间随 N 放大，让客户端比服务端活得久。
BATCH_TIMEOUT_FLOOR_SECONDS = 15.0
BATCH_TIMEOUT_PER_ITEM_SECONDS = 0.5


class RpcServerRepliedError(RuntimeError):
    """服务端以错误作答——区别于超时/传输失败，后者的请求去向未知。

    这个区分对写入很关键：批量提交处理器只在逐项循环之前抛异常，所以“有回复
    的错误”意味着没有任何一项被执行，重试是安全的。超时则意味着批量可能仍在
    运行，重试会让委托翻倍。"""




LARGE_CODE_LIST = 1000
# 回退先读什么。股票只占交易所挂牌的 8.7%，所以从窄范围起步是 1.08s 对
# 7.4s；只有它没命中时才扩到 "all"。
DEFAULT_FALLBACK_TYPES = ("stock",)

# 交易进程保留多少组 int -> 合同编号 的映射，使调用方把 id 过一遍 JSON 丢掉
# 字符串之后，撤单仍能解析（问题 #113）。
_ORDER_ID_MEMORY = 4096


def _markets_of(codes):
    """给定带后缀代码所在的行情品种标记；若有代码的后缀无法识别则返回空——
    靠过滤一次交易所读取，救不回一个我们定位不了的代码。"""
    markets = set()
    for code in codes or []:
        _, _, suffix = str(code).rpartition(".")
        suffix = suffix.upper()
        if suffix not in MARKET_TOKENS:
            return set()
        markets.add(suffix)
    return markets


# 每个交易所的行情品种标记要按哪些品种去请求。
#
# 服务端会把行情品种标记收窄到 `types`，默认 ("stock",)——于是 "SH" 变成
# 上证A股板块列表。所以不说明想要什么的品种请求会静默丢掉可转债、基金、ETF
# 和指数：它们仍被订阅并推送，但不在第一帧里（#358）。改要 ["all"] 的代价是
# 整个交易所挂牌（26744 个标的，在 adjust 线程上约 7.5s），而这正是
# #247/#104 去掉的东西。于是从实际请求的代码推断品种。
#
# 前缀是各交易所自己的代码段。可转债不列在这里——code_utils.is_bond_code
# 已经拥有那些前缀，重复一遍正是两份拷贝逐渐漂移的起因。
#
#   SH  600/601/603/605/688/689 股票 · 5xx 基金/ETF · 000xxx 指数
#   SZ  00x/30x 股票 · 15x/16x/18x 基金/ETF · 39xxxx 指数 (980xxx.SZ 也是指数，
#       不在「沪深指数」板块里，所以不认它)
#   北交所 43/83/87/88/92 股票（899050.BJ 北证50 是指数，而沪深指数不包含
#       它，所以有意保持不分类）
#
# 在这里匹配不上的代码不做猜测：它改走逐代码的直连路径，更慢但正确。猜测
# 恰恰是代码当初丢失的原因。
_PRIME_TYPES_BY_PREFIX = {
    "SH": ((("600", "601", "603", "605", "688", "689"), ("stock",)),
           (("5",), ("fund", "etf")),
           (("000",), ("index",))),
    # 用 30 而不是 300/301：深证A股已经挂牌 302132.SZ，而 30x 段是创业板
    # 专属，放宽也不会吞掉别的品种。
    "SZ": ((("000", "001", "002", "003", "30"), ("stock",)),
           (("15", "16", "18"), ("fund", "etf")),
           (("39",), ("index",))),
    "BJ": ((("43", "83", "87", "88", "92"), ("stock",)),),
}


def _prime_tick_types(code):
    """把行情品种标记收窄到 `code` 对应的品种，或返回 ()。

    () 表示“无法可靠分类”——调用方不得代这个代码去收窄。
    """
    text = str(code or "").strip().upper()
    if "." not in text:
        return ()
    pure, _, market = text.partition(".")
    if not pure.isdigit():
        return ()
    if is_bond_code(text):
        return ("convertible",)
    for prefixes, kinds in _PRIME_TYPES_BY_PREFIX.get(market, ()):
        if pure.startswith(prefixes):
            return kinds
    return ()


def _full_tick_params(codes, types=None):
    """get_full_tick 的 RPC 参数。`types` 在请求时就把全市场标记收窄到单一
    品种——对回复做过滤仍要为交易所挂牌的所有标的付 QMT 的逐标的成本
    （问题 #104）。"""
    params = {"codes": codes}
    if types:
        params["types"] = [types] if isinstance(types, str) else list(types)
    return params


_FIELD_LIST_NOTICE = {"shown": False}
DIRECT_PATH_FIELDS = ("open", "high", "low", "close", "volume", "amount")

# preClose 被大 QMT 答成 0.0 的周期，因此改由日线补（#166）。
# 在国金 2.1.19.0 / 0.3.19 上只读实测：1d/1mon/1q/1hy/1y 都带真实 preClose，
# 只有 1w 为 0，且对所有试过的代码、两种 fill_data 设置都如此。往这里加周期
# 必须同样先做实测——各周期的标签语义不同，是验证出来的，不是猜的。
# assumed.
PRE_CLOSE_BACKFILL_PERIODS = ("1w",)
_PRE_CLOSE_PERIOD_STARTS = {"1w": _iso_week_start_label}
_PRE_CLOSE_NOTICE = {"shown": False}

# 进行中的 K 线由 QMT 按请求窗口（而不是本地全部数据）合成的那些周期
# （#226）。像 #172 钉住周期列表那样用集合钉住——新周期必须有实测才进来。
RESYNTH_ONGOING_PERIODS = ("1w", "1mon", "1q", "1hy", "1y")
RESYNTH_PERIOD_STARTS = {
    "1w": _iso_week_start_label,
    "1mon": _month_start_label,
    "1q": _quarter_start_label,
    "1hy": _halfyear_start_label,
    "1y": _year_start_label,
}
_RESYNTH_NOTICE = {"shown": False}
_DAILY_RESCUE_NOTICE = {"shown": False}


def _in_trading_session(now):
    """日盘时段内的工作日（集合竞价开盘至收盘后的结算宽限）。重合成过去从
    09:30 开始，但零成交量的进行中 K 线在 09:26 就测到了（#307）：终端的
    救援在集合竞价撮合之后、连续交易开盘之前就给出合成周期 K 线。收盘之后
    进行中的多日 K 线已定稿，原样透传（#226 的盘后对照）。"""
    if now.weekday() > 4:
        return False
    return _dt.time(9, 15) <= now.time() <= _dt.time(15, 5)


def _now():
    return _dt.datetime.now()


def _notice_field_list_cost(field_list):
    """说明一次：点名字段才是快路径的开关。

    这不是在提示谁犯了错：空的 field_list 正确返回全部 11 列，而那只有 RPC
    做得到。但除非有人告诉你它存在，否则这个提速是看不见的（问题 #104）。
    索要直连路径没有的字段是安全的——它会退回 RPC，而不是返回 NaN。"""
    if field_list or _FIELD_LIST_NOTICE["shown"]:
        return
    _FIELD_LIST_NOTICE["shown"] = True
    try:
        log.info(
            "field_list 为空时，get_market_data_ex 会返回全部 11 列，必须通过 RPC 读取。"
            "如果六个 OHLCV 字段 %s 已足够，请将它们作为 field_list 传入；"
            "该路径由 FormulaServer 直连，实测 0.015s，而 RPC 为 5.8s。"
            "指定 preClose / suspendFlag / settelementPrice / openInterest 时"
            "会回退到 RPC，因此更宽的 field_list 安全但不会更快。",
            ", ".join(DIRECT_PATH_FIELDS))
    except Exception:
        pass


# K 线订阅靠轮询，因为服务端没有针对 K 线的推送：桥接层只暴露
# ContextInfo.subscribe_whole_quote，而它只带 tick。间隔是“K 线最多能有多
# 旧”的下限，而不是新鲜度的承诺。
DEFAULT_BAR_POLL_INTERVAL_SECONDS = 3.0


class _BarPoller(object):
    """最新 K 线变化时发出 K 线回调。

    MiniQMT 的 subscribe_quote 逐根推送 K 线更新。这里通过重读最近几根 K 线、
    仅当最新一根变化时才触发来近似，使按 MiniQMT 编写的调用方继续可用。
    每个回调都被包裹：抛异常的订阅者不能杀掉轮询线程并静默终止订阅。
    """

    def __init__(self, fetch, callback, interval_seconds, on_error=None,
                 on_no_data=None):
        self._fetch = fetch
        self._callback = callback
        self._interval = max(0.2, float(interval_seconds))
        self._on_error = on_error
        self._on_no_data = on_no_data
        self._stop = threading.Event()
        self._last_signature = None
        self._reported_no_data = False
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    @staticmethod
    def _signature(data):
        """以低成本识别最新一根 K 线，不假设容器类型。"""
        try:
            for value in (data or {}).values():
                if value is None:
                    continue
                if hasattr(value, "empty"):          # pandas 的 DataFrame
                    if getattr(value, "empty", True):
                        continue
                    return str(value.index[-1]), int(len(value))
                if isinstance(value, (list, tuple)) and value:
                    return repr(value[-1]), len(value)
                if isinstance(value, dict) and value:
                    key = sorted(value.keys())[-1]
                    return repr(key), len(value)
        except Exception:
            return None
        return None

    def _loop(self):
        while not self._stop.is_set():
            try:
                data = self._fetch()
                signature = self._signature(data)
                if signature is None:
                    # 该周期还没有 K 线。订阅是活的，数据出现就会开始触发，
                    # 但在这之前调用方什么都收不到——除非我们明说，否则这与
                    # 订阅已坏无法区分。只报告一次，不按轮询次数重复。
                    if not self._reported_no_data:
                        self._reported_no_data = True
                        if self._on_no_data is not None:
                            self._on_no_data()
                elif signature != self._last_signature:
                    self._reported_no_data = False
                    self._last_signature = signature
                    if self._callback is not None:
                        self._callback(data)
            except Exception as exc:
                if self._on_error is not None:
                    try:
                        self._on_error(exc)
                    except Exception:
                        pass
            self._stop.wait(self._interval)


_VERSION_WARNED = {"shown": False}


def warn_on_version_mismatch(ping_response):
    """当 QMT 侧的桥接层不是本客户端的版本时告警一次。

    部署到 QMT 只是拷文件，而 QMT 在策略重跑之间会保留模块，于是一个过期的
    服务端表现得像“修了但没生效”。在连接时就说明，而不是留给排查去发现。

    版本一致、服务端太老不报版本、或已经说过时静默。从不抛异常：这里在连接
    路径上。
    """
    if _VERSION_WARNED["shown"]:
        return None
    try:
        server = str((ping_response or {}).get("version") or "")
        if not server:
            return None      # 服务端早于版本上报；无可比较
        from .version import __version__ as local

        if server == local:
            return None
        _VERSION_WARNED["shown"] = True
        log.warning(
            "版本不一致：客户端为 %s，QMT 端桥接为 %s。"
            "仅复制文件不会生效；QMT 在重复运行策略时会保留已加载模块，"
            "因此策略也必须重启。可设置 BIGQMT_AUTO_SYNC=1"
            "（或调用 xt_trader.sync_deployment()）将客户端包推送到"
            "QMT 的 Python 目录。",
            local, server)
        return (local, server)
    except Exception:
        return None


def auto_sync_enabled():
    """向运行中的交易终端写入需要显式开启，而不是连接带来的副作用。"""
    return _bool_value(os.environ.get("BIGQMT_AUTO_SYNC"), False)


class BigQmtXtData:
    def __init__(self, client):
        self.client = client
        self._subscribe_seq = int(time.time() * 1000)
        self._cache_obj = None
        self._quote_session = None          # 惰性构造的 WholeQuoteClientSession
        self._quote_session_factory = None  # 测试钩子：返回类会话对象
        self._bar_pollers = {}              # seq -> _BarPoller，用于 K 线周期
        self._bar_poller_lock = threading.Lock()

    def _next_seq(self):
        self._subscribe_seq += 1
        return self._subscribe_seq

    def _local_cache(self):
        cfg = dict(getattr(self.client, "local_cache_config", {}) or {})
        if not _bool_value(cfg.get("enabled"), True):
            return None
        if self._cache_obj is None:
            self._cache_obj = LocalMarketCache(cache_dir=cfg.get("dir"), fmt=cfg.get("format", "auto"))
        return self._cache_obj

    def _call(self, method, **params):
        return self.client.call(method, params)

    def get_full_tick(self, code_list, timeout_seconds=None, types=None, market_fallback=True):
        """获取一批代码的全推 tick 数据。

        参数:
            code_list: 要查询的股票代码。
            timeout_seconds: 单次请求的 RPC 超时。None = 自动（全市场快照 30s，
                否则用客户端默认值 DEFAULT_RPC_TIMEOUT_SECONDS）。查询大量代码时
                调用方可以传更大的值（例如 1256 个 ETF 期权可能需要 150-180s）。
            market_fallback: True（默认）时，大名单直读不全或失败会扩读交易所
                再过滤（#104 的恢复设计）；False 时**不扩读**——缺票照实返回
                部分结果、原异常原样抛出，且市场重读不再把显式超时硬抬到 60s
                （#373：接受部分行情的调用方要的就是这个）。
        """
        codes = list(code_list or [])
        if not codes:
            return {}
        cache_config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        if not types and _bool_value(cache_config.get("enabled"), False):
            valkey_client = self.client._valkey()
            request_full_tick_cache(
                valkey_client,
                self.client.account_id,
                codes,
                demand_ttl_seconds=cache_config.get("demand_ttl_seconds", 10),
                cache_ttl_seconds=cache_config.get("cache_ttl_seconds", 10),
            )
            data = wait_full_tick_cache(
                valkey_client,
                self.client.account_id,
                codes,
                max_age_seconds=cache_config.get("cache_ttl_seconds", 10),
                wait_seconds=cache_config.get("wait_seconds", 3.5),
                poll_interval_seconds=cache_config.get("poll_interval_seconds", 0.2),
            )
            if data is not None:
                return data
            upper_codes = {str(code).strip().upper() for code in codes}
            if upper_codes & {"SH", "SZ", "BJ", "HK"}:
                # 全市场快照必须留在按需缓存上。这里走实时 RPC 会在每次未命中
                # 时传约 5 万行，所以直接把超时抛出来。
                raise TimeoutError("全推 tick 的 valkey 缓存超时: %s" % ",".join(str(code) for code in codes))
            # 代码清单未命中（冷启动 / 快照过期）：改走实时 RPC，使首次调用约
            # 几毫秒，而不是死等 wait_seconds。
            rpc_timeout = timeout_seconds if timeout_seconds is not None else None
            return self.client.call("get_full_tick", _full_tick_params(codes, types), timeout_seconds=rpc_timeout) or {}
        upper_codes = {str(code).strip().upper() for code in codes}
        # 调用方给的超时优先；否则自动判断是否全市场。
        if timeout_seconds is not None:
            rpc_timeout = timeout_seconds
        else:
            rpc_timeout = 30 if upper_codes & {"SH", "SZ", "BJ", "HK"} else None
        if not market_fallback:
            # #373: 不扩读。直读即终读——部分结果照实返回，异常原样抛出，
            # 显式超时就是唯一的超时。
            return self.client.call(
                "get_full_tick", _full_tick_params(codes, types),
                timeout_seconds=rpc_timeout) or {}
        # 市场重读的超时：调用方显式给了就用它的（#373，不再硬抬 60s）；
        # 没给才按市场读的成本抬到 60s。
        market_timeout = timeout_seconds if timeout_seconds is not None else max(rpc_timeout or 0, 60)
        failure = None
        try:
            data = self.client.call(
                "get_full_tick", _full_tick_params(codes, types),
                timeout_seconds=rpc_timeout) or {}
        except Exception as exc:
            data = None
            failure = exc
            if not self._can_fall_back_to_markets(codes, upper_codes):
                raise
        if self._should_fall_back(codes, upper_codes, data):
            fallback_errors = []
            recovered = self._full_tick_via_markets(
                codes, market_timeout, types, errors=fallback_errors)
            if recovered is not None:
                if data:
                    # #373: 直读已拿到的行不能被空回包顶掉——合并，直读行优先。
                    merged = dict(recovered)
                    merged.update(data)
                    return merged
                return recovered
            if failure is not None:
                # 这里的裸 `raise` 没有活跃异常——上面的 except 块已经退出——
                # 于是它产生 "RuntimeError: No active exception to reraise"，
                # 并把真正的超时埋掉（问题 #104 报告的情况）。改为重新抛出实际
                # 失败，并说明为何恢复没有起作用。
                if fallback_errors:
                    log.warning(
                        "get_full_tick：%d 个代码直接读取失败（%s），"
                        "全市场重新读取也失败（%s：%s）；现在抛出最初捕获的异常。",
                        len(codes), failure,
                        fallback_errors[-1].__class__.__name__,
                        fallback_errors[-1])
                else:
                    log.warning(
                        "get_full_tick：%d 个代码直接读取失败（%s），"
                        "且无法通过全市场读取恢复。",
                        len(codes), failure)
                raise failure
        return data or {}

    def _can_fall_back_to_markets(self, codes, upper_codes):
        """只有显式的带后缀代码列表才能这样恢复。"""
        if upper_codes & MARKET_TOKENS:
            return False          # 本来就是全市场请求
        if len(codes) <= LARGE_CODE_LIST:
            return False          # 小名单：这里的失败就是真失败
        return bool(_markets_of(codes))

    def _should_fall_back(self, codes, upper_codes, data):
        if data is None:
            return True           # 请求抛异常了
        if not self._can_fall_back_to_markets(codes, upper_codes):
            return False
        # 简言之：服务端丢了代码，或被截断。缺的任何东西都值得读一次全市场，
        # 而不是静默返回比要求更少的内容（问题 #104）。
        return len(data) < len(set(str(c) for c in codes))

    def _full_tick_via_markets(self, codes, market_timeout, types=None, errors=None):
        """读取这些代码所在的交易所，再过滤到它们。

        长显式名单就是一次 RPC、一个超时，要么全中要么全丢；而市场标记是单个
        廉价的参数，无法被截断。

        先收窄，只有不够时才用 "all"。交易所挂牌以债券为主——"SH" 有 26744
        个标的，其中 2315 只是股票——所以全读要 7.4s，只读股票 1.08s。要恢复
        的代码是股票时，没理由付这个成本（问题 #104）。

        ``market_timeout`` 由调用方解析好：显式超时原样用（#373），缺省时才
        按市场读的成本抬到 60s。
        """
        markets = _markets_of(codes)
        if not markets:
            return None
        wanted = set(str(code) for code in codes)

        attempts = [list(types) if types else list(DEFAULT_FALLBACK_TYPES)]
        if not any(str(k).lower() == "all" for k in attempts[0]):
            attempts.append(["all"])

        merged = {}
        for attempt in attempts:
            merged = {}
            try:
                for market in sorted(markets):
                    snapshot = self.client.call(
                        "get_full_tick", _full_tick_params([market], attempt),
                        timeout_seconds=market_timeout) or {}
                    for key, value in snapshot.items():
                        if str(key) in wanted:
                            merged[key] = value
            except Exception as exc:
                # 在这里吞掉原因，会让调用方无从报告；把它带回去，让抛出时
                # 能点名（问题 #104）。
                if errors is not None:
                    errors.append(exc)
                return None
            if len(merged) >= len(wanted):
                break          # 全部拿到；无需扩读

        log.warning(
            "get_full_tick：%d 个代码未直接返回；已按 %s 重新读取，"
            "使用 %s 并筛选到 %d 条。与这么长的代码列表相比，"
            "使用带 types= 的市场 token 开销更低。",
            len(wanted), "/".join(sorted(markets)), "/".join(attempts[-1]
                                                             if len(merged) < len(wanted)
                                                             else attempts[0]),
            len(merged))
        return merged

    def get_deployment_info(self):
        """QMT 侧桥接层从哪儿运行，以及是哪个构建版本。

        返回 version / package_dir / qmt_python_dir / strategy_dir /
        python_version。用它先确认部署真的落地，再去追一个其实从未存在过的
        修复——QMT 在策略重跑之间保留模块，忘了拷贝和没重新加载看起来一样。
        """
        return self.client.call("get_deployment_info", {}) or {}

    def get_instrument_detail(self, stock_code):
        return self.client.call("get_instrument_detail", {"code": stock_code}) or {}

    def get_instrumentdetail(self, stock_code):
        return self.get_instrument_detail(stock_code)

    def get_instrument_type(self, stock_code, variety_list=None):
        return self._call("get_instrument_type", code=stock_code, variety_list=variety_list)

    def get_stock_type(self, stock_code, variety_list=None):
        """xtdata.get_stock_type 的同名封装 —— 大 QMT 上答不了，直接报错。

        服务端走的是 ContextInfo.get_stock_type(stock)。这个 stub 在大 QMT 上
        存在（缺失会抛 NotImplementedError），但**对任何代码都返回 0**：实测
        股票 600000.SH、ETF 589820.SH、沪市债券 186511.SH、期权
        10011096.SHO 全部是 0，换代码格式（600000 / SH600000 /
        600000.SSE）也一样。

        返回一个恒为 0 的"类型"比报 AttributeError 更糟：报错看得见，一个
        错的分类看不见。所以这里显式拒绝，并指向真正能用的那个：
        get_instrument_type()，实测能区分 stock / fund / etf / bond / index。
        """
        raise NotImplementedError(
            "get_stock_type 在大 QMT 上不可用：服务端的 "
            "ContextInfo.get_stock_type 桩对每个代码都返回 0 "
            "（已对股票、ETF、债券和期权，以及每种代码格式做过实盘验证）。"
            "请改用 get_instrument_type(stock_code) —— 它返回 "
            "{'stock': ..., 'fund': ..., 'etf': ..., 'bond': ..., 'index': ...}。"
        )

    def subscribe_l2thousand(self, stock_code, gear_num=None, callback=None):
        """千档盘口订阅。

        callback 在 RPC 模型下没有回调通道，服务端会忽略它 —— 想要推送请用
        subscribe_whole_quote。这里保留形参只为和 xtdata 签名一致。
        """
        return self._call(
            "subscribe_l2thousand",
            stock_code=stock_code,
            gear_num=0 if gear_num is None else gear_num,
        )

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        name = str(sector_name or "")
        try:
            return self._call("get_stock_list_in_sector", sector_name=sector_name, real_timetag=real_timetag) or []
        except Exception:
            pass
        if name in ("沪深A股", "沪深A股".encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")):
            ticks = self.get_full_tick(["SH", "SZ"])
            return sorted(code for code in ticks.keys() if _is_hs_a_share(code))
        raise NotImplementedError("大 QMT 兼容层不支持 sector: %s" % sector_name)

    def get_market_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        params = dict(
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
        data = self._call("get_market_data", **params)
        # 自愈复权读取（全零 K 线 -> 服务端原始下载后重试）。
        data = self._heal_adjusted("get_market_data", params, data)
        # 文档化的 MiniQMT 形状是 dict[field] -> 以股票为索引、时间为列的
        # DataFrame；大 QMT 发的是长表。要在自愈之后转换，使自愈看到它熟悉的
        # 形状。
        return _to_documented_market_data_shape(data, field_list, stock_list, period)

    def _get_market_data_ex_batch(self, params, timeout_seconds=None, use_formula=True,
                                  heal=True):
        """一次 RPC 取回的 K 线，已修复并规范化。不做缓存。"""
        if use_formula:
            data = self.client.call("get_market_data_ex", params, timeout_seconds=timeout_seconds)
        else:
            # 只在绕路时显式传参——保持既有调用方/桩签名不变。
            data = self.client.call("get_market_data_ex", params, timeout_seconds=timeout_seconds,
                                    use_formula=False)
        # 自愈复权读取（全零 K 线 -> 服务端原始下载后重试）。
        if heal:
            data = self._heal_adjusted("get_market_data_ex", params, data,
                                       timeout_seconds=timeout_seconds)
        # 把大 QMT 以 stime 为索引的 frame 规范成 MiniQMT 形状（以 time 为索引）。
        if isinstance(data, dict):
            # 先取出降级答案标记：规范化会拷贝、切片并删列，而 pandas 只在
            # 尽力而为的程度上传递 ``attrs``——某个标记在这个 pandas 版本上活
            # 下来、下个版本就没了，比没有更糟（#237）。
            markers = dict((code, _partial_marker(frame))
                           for code, frame in data.items())
            data = _normalize_market_data_result(
                data, field_list=params.get("field_list"), period=params.get("period"))
            if isinstance(data, dict) and any(markers.values()):
                for code, marker in markers.items():
                    if marker is not None and code in data:
                        _attach_partial_marker(data[code], marker)
                _warn_partial_market_data(
                    [marker for marker in markers.values() if marker])
        return data

    @staticmethod
    def _pre_close_missing(frame):
        """当 frame 有 preClose 列却没有任何可用值时返回 True。

        “列在，但什么也没说”正是 #166 的症状。没有该列的 frame 本来就没要它；
        只有一个真实值的 frame 来自会作答的终端——两者都不需要补。
        """
        try:
            column = list(frame["preClose"])
        except Exception:
            return False
        for value in column:
            number = _bar_float(value)
            if number is not None and number != 0.0:
                return False
        return True

    def _backfill_pre_close(self, data, period, dividend_type,
                            timeout_seconds=None, use_formula=True, subscribe=None):
        """用日线补全零的 preClose（#166）。

        大 QMT 对每根 ``1w`` K 线都答 preClose = 0.0。正确值（也是 MiniQMT
        给出的值）是该周期第一个交易日的日线 preClose——在除权除息日它就是
        调整后的参考价，因此不能和上一根 K 线的收盘价互换，即使大多数周两者
        相同。

        代价是多一次日线请求，而且只在 frame 真的空手而归时才发：会答 preClose
        的终端不用付，从没要过这列的调用方也不用付（#104）。
        """
        period_start = _PRE_CLOSE_PERIOD_STARTS.get(str(period or ""))
        if period_start is None or not isinstance(data, dict):
            return data

        targets = {}
        for code, frame in data.items():
            if not self._pre_close_missing(frame):
                continue
            spans = [(label, period_start(label)) for label in _frame_bar_labels(frame)]
            spans = [(label, start) for label, start in spans if start]
            if spans:
                targets[code] = spans
        if not targets:
            return data

        starts = [start for spans in targets.values() for _, start in spans]
        ends = [label for spans in targets.values() for label, _ in spans]
        try:
            daily = self.get_market_data_ex(
                field_list=["preClose", "stime"], stock_list=sorted(targets),
                period="1d", start_time=min(starts), end_time=max(ends),
                count=-1, dividend_type=dividend_type,
                # 补出来的行没有真实 preClose，若被返回会被当成某周期的
                # “第一个交易日”（#167）。
                fill_data=False, timeout_seconds=timeout_seconds,
                use_formula=use_formula, backfill_pre_close=False, subscribe=subscribe,
            )
        except Exception as exc:
            # 退回到终端的答案。K 线本身是好的；为这次补值把整次读取搞挂
            # 才是更糟的 bug。
            log.warning("period=%s 的 preClose 回填已跳过：%s", period, exc)
            return data

        for code, spans in targets.items():
            rows = self._daily_pre_close_rows(daily.get(code) if isinstance(daily, dict) else None)
            if not rows:
                continue
            frame = data[code]
            try:
                existing = list(frame["preClose"])
            except Exception:
                continue
            filled = list(existing)
            hits = 0
            for position, (label, start) in enumerate(spans):
                if position >= len(filled):
                    break
                for day, pre_close in rows:      # 升序 -> 首个命中即为所需
                    if start <= day <= label:
                        filled[position] = pre_close
                        hits += 1
                        break
            if not hits:
                continue
            try:
                frame["preClose"] = filled
            except Exception:
                continue
            self._notice_pre_close_backfill(period)
        return data

    @staticmethod
    def _daily_pre_close_rows(frame):
        """升序的 [(YYYYMMDD, preClose)]，跳过没有数字的单元格。"""
        labels = _frame_bar_labels(frame) if frame is not None else []
        if not labels:
            return []
        try:
            values = list(frame["preClose"])
        except Exception:
            return []
        rows = []
        for label, value in zip(labels, values):
            number = _bar_float(value)
            if number is not None:
                rows.append((label, number))
        rows.sort()
        return rows

    def _resynth_ongoing_multiday_bars(self, data, period, start_time,
                                       end_time, dividend_type,
                                       timeout_seconds=None, use_formula=True, subscribe=None):
        """用该周期的日线重建进行中的多日 K 线（#226）。

        大 QMT 用 **请求窗口** 内的 K 线合成进行中的 1w/1mon/1q/1hy/1y K 线，
        所以只覆盖今天的窗口会用今天的成交量回答周线，本周前几天的量丢了。
        MiniQMT 的 get_local_data 用 **全部本地基础数据** 合成，start/end 只
        过滤返回的行——本重建就是让桥接层与之一致的东西。

        在 #226 原有窗口判据之上，还有两个由结果驱动的触发条件（都来自 #307，
        在终端 2.0.8.300 上实盘测得）：

        * reconcile —— 最后一根 K 线就是进行中的周期：无论窗口是什么形状，都用
          该周期的日线合计核对它的成交量。零成交量形态在集合竞价撮合之后
          09:26 就测到（救援合成的那根 K 线不带周期累计量），部分天数形态是
          #226 原本的截断；覆盖整个周期的窗口 **不再** 豁免，因为覆盖到的答案
          也被测出是错的。已经对得上的 K 线原样通过，所以在健康终端上唯一成本
          就是多读一次日线。
        * append —— 最后一根 K 线是已定稿的历史，但请求窗口覆盖当前周期
          （start <= 它的起点）且该周期已有日线：把 **缺失的** 进行中 K 线用
          日线合成后追加。盘中实测：盘内答案干脆丢掉了当前周期的 K 线。

        保留的守卫：仅限多日周期、明确的 start_time、时钟在日盘时段内
        （09:15..15:05）、收盘后 K 线已定稿时不做。

        降级方式与 preClose 回填相同（#166）：日线读取失败或为空时保留终端的
        答案并告警一次；不抛异常。
        """
        if str(period or "") not in RESYNTH_ONGOING_PERIODS or not isinstance(data, dict):
            return data
        start_text = str(start_time or "")
        if not start_text:
            return data          # 按根数读取从未出现截断
        now = _now()
        if not _in_trading_session(now):
            return data
        today = now.strftime("%Y%m%d")
        start_day = _digits_only(start_text)[:8]
        end_day = _digits_only(str(end_time or ""))[:8]
        current_start = RESYNTH_PERIOD_STARTS[period](today)
        if current_start is None:
            return data
        # 只有当窗口两端都跨过进行中周期时才说它“覆盖”了该周期：读纯历史
        # （终点落在过去某周期内）从没要求进行中的 K 线，在那里追加等于造数据。
        covers_current = start_day <= current_start and (
            not end_day or end_day >= current_start)

        reconcile, append = {}, {}
        for code, frame in data.items():
            labels = _frame_bar_labels(frame)
            if not labels:
                # 一行都没有：窗口覆盖该周期时追加。
                if covers_current:
                    append[code] = current_start
                continue
            label = labels[-1]
            period_start = RESYNTH_PERIOD_STARTS[period](label)
            period_end = _period_end_label(period, label)
            if period_start is None or period_end is None:
                continue
            if period_start <= today <= period_end:
                # 就是进行中的那根 K 线：无论窗口形状如何，都按日线合计核对
                # （#307）。
                reconcile[code] = period_start
                continue
            if today > period_end and covers_current:
                # 已定稿的历史，而进行中的 K 线本该也在：用该周期的日线追加
                # 一笔（#307 的缺失形态）。
                append[code] = current_start
        if not reconcile and not append:
            return data

        wanted = sorted(set(reconcile) | set(append))
        first = min(reconcile.values()) if reconcile else current_start
        try:
            daily = self.get_market_data_ex(
                field_list=["open", "high", "low", "close", "volume", "amount"],
                stock_list=wanted, period="1d",
                start_time=first, end_time=today, count=-1,
                dividend_type=dividend_type, fill_data=False,
                timeout_seconds=timeout_seconds, use_formula=use_formula,
                backfill_pre_close=False, resynth_ongoing_multiday=False, subscribe=subscribe,
            )
        except Exception as exc:
            # 终端的答案已足够可服务；为这次重建把整次读取搞挂才是更糟的 bug。
            log.warning("正在形成的 %s K 线重建已跳过：%s", period, exc)
            return data

        rebuilt = appended = 0
        for code, period_start in reconcile.items():
            frame = data.get(code)
            daily_frame = daily.get(code) if isinstance(daily, dict) else None
            summary = self._sum_daily_bars(daily_frame, period_start, today)
            if summary is None:
                continue
            if not self._last_bar_disagrees(frame, summary):
                continue          # 已等于日线合计：不动它
            if self._write_last_bar(frame, summary):
                rebuilt += 1
        for code, period_start in append.items():
            frame = data.get(code)
            daily_frame = daily.get(code) if isinstance(daily, dict) else None
            summary = self._sum_daily_bars(daily_frame, period_start, today)
            if summary is None:
                continue          # 该周期还没有日线：无可补充
            label = _period_end_label(period, period_start)
            if label is None:
                continue
            if self._append_ongoing_bar(frame, label, summary):
                appended += 1
        if rebuilt or appended:
            self._notice_resynth_ongoing(period, rebuilt + appended, appended)
        return data

    def _rescue_ongoing_daily_bar(self, data, period, end_time, dividend_type,
                                  timeout_seconds=None):
        """读取止于上一交易日时，补上今天正在形成的 1d K 线
        （#226/#307 系列中的 1d 情形）。

        实盘日线读取在盘中是带今天的部分 K 线的（2.1.33 盘中实测），但有两条
        读取路径会丢掉它：今天开盘前写入的客户端缓存 frame——从
        download_history_data2 到这次读取之间没有任何东西刷新缓存——以及某些
        终端上增量下载后立刻读本地库。守卫与多日重合成相同（工作日且时钟在
        09:15..15:05、请求窗口仍覆盖今天），降级也相同：实时读取失败或为空时
        保留原答案并告警一次。追加的行只存在于返回的 frame 中，绝不写回缓存。
        """
        if str(period or "") != "1d" or not isinstance(data, dict) or not data:
            return data
        now = _now()
        if not _in_trading_session(now):
            return data
        today = now.strftime("%Y%m%d")
        end_day = _digits_only(str(end_time or ""))[:8]
        if end_day and end_day < today:
            return data          # 纯历史窗口从没要求今天
        wanted = []
        for code, frame in data.items():
            labels = _frame_bar_labels(frame) if frame is not None else None
            if not labels or str(labels[-1]) < today:
                wanted.append(code)
        if not wanted:
            return data
        try:
            live = self.get_market_data_ex(
                field_list=["open", "high", "low", "close", "volume", "amount"],
                stock_list=wanted, period="1d",
                start_time=today, end_time=today, count=-1,
                dividend_type=dividend_type, fill_data=False,
                timeout_seconds=timeout_seconds, use_formula=False,
                backfill_pre_close=False, resynth_ongoing_multiday=False,
            )
        except Exception as exc:
            # 本地答案已足够可服务；为追加这一根把整次读取搞挂才是更糟的 bug。
            log.warning("正在形成的 1d K 线补救已跳过：%s", exc)
            return data
        appended = 0
        for code in wanted:
            live_frame = live.get(code) if isinstance(live, dict) else None
            summary = self._sum_daily_bars(live_frame, today, today)
            if summary is None:
                continue          # 还没有实时 K 线（盘前）：无可补充
            if self._append_ongoing_bar(data[code], today, summary):
                appended += 1
        if appended:
            self._notice_1d_rescue(appended)
        return data

    @staticmethod
    def _notice_1d_rescue(appended):
        """说明一次：今天正在形成的日线是在这里追加的。"""
        if _DAILY_RESCUE_NOTICE["shown"]:
            return
        _DAILY_RESCUE_NOTICE["shown"] = True
        try:
            log.info(
                "本地结果缺少 %d 根正在形成的 1d K 线，已从实时日线读取中追加"
                "（#226/#307 同类问题中的 1d 场景）：在开盘前缓存的客户端数据，"
                "或增量下载后立即读取的本地存储，可能只到上一交易日。"
                "如需返回原始结果，请传入 resynth_ongoing_multiday=False。",
                appended)
        except Exception:
            pass

    @staticmethod
    def _last_bar_disagrees(frame, summary):
        """最后一根 K 线的成交量与日线合计不同时返回 True。

        成交量是核对键（#307）：目前测到的每种坏形态——09:26 的零成交量救援
        K 线、#226 的窗口截断、部分天数合成——都体现在这里。没有要求 volume
        列的 frame 无法证明正确；报告不一致可让重建继续作为这些情况的兜底。
        """
        try:
            values = list(frame["volume"])
        except Exception:
            return True          # 没有 volume 列：无法证明一致
        if not values:
            return True
        wanted = summary.get("volume")
        if wanted is None:
            return False         # 没有可核对的对象
        current = _bar_float(values[-1])
        if current is None:
            return True
        return abs(current - wanted) > max(1.0, abs(wanted) * 1e-6)

    @staticmethod
    def _append_ongoing_bar(frame, label, summary):
        """追加缺失的进行中 K 线，只写 frame 已有的列。"""
        row = {name: value for name, value in summary.items()
               if value is not None and name in list(frame.columns)}
        if not row:
            return False
        try:
            frame.loc[label] = row
            return True
        except Exception:
            return False

    @staticmethod
    def _sum_daily_bars(frame, period_start, today):
        """由一个进行中周期的日线算出 open/high/low/close/volume/amount，
        无可合计时返回 None。"""
        labels = _frame_bar_labels(frame) if frame is not None else None
        if not labels:
            return None
        try:
            columns = {name: list(frame[name]) for name in
                       ("open", "high", "low", "close", "volume", "amount")}
        except Exception:
            return None
        rows = []
        for index, label in enumerate(labels):
            day = _digits_only(label)[:8]
            if not day or day < period_start or day > today:
                continue
            rows.append({name: _bar_float(columns[name][index])
                         for name in columns})
        rows = [row for row in rows if row["close"] is not None]
        if not rows:
            return None
        out = {
            "open": next((row["open"] for row in rows if row["open"] is not None), None),
            "high": max((row["high"] for row in rows if row["high"] is not None), default=None),
            "low": min((row["low"] for row in rows if row["low"] is not None), default=None),
            "close": next((row["close"] for row in reversed(rows) if row["close"] is not None), None),
        }
        for name in ("volume", "amount"):
            values = [row[name] for row in rows if row[name] is not None]
            out[name] = sum(values) if values else None
        return out

    @staticmethod
    def _write_last_bar(frame, summary):
        """把重建出的值写入最后一根 K 线，只写调用方真正要过的列。
        有写入时返回 True。"""
        wrote = False
        for name, value in summary.items():
            if value is None:
                continue
            try:
                values = list(frame[name])
            except Exception:
                continue          # 该列从未被要求
            if not values:
                continue
            values[-1] = value
            try:
                frame[name] = values
                wrote = True
            except Exception:
                continue
        return wrote

    @staticmethod
    def _notice_resynth_ongoing(period, count, appended=0):
        """说明一次：进行中的 K 线是在这里重建的，不是 QMT 给的。"""
        if _RESYNTH_NOTICE["shown"]:
            return
        _RESYNTH_NOTICE["shown"] = True
        try:
            log.info(
                "%d 根正在形成的 %s K 线已用该周期的日线核对，"
                "并追加了 %d 根缺失 K 线（问题 #226、#307）："
                "大 QMT 会按请求窗口合成本周期数据，或在盘中丢弃这些 K 线；"
                "miniQMT 则返回该周期的累计数据。"
                "如需返回原始结果，请传入 resynth_ongoing_multiday=False。",
                count - appended, period, appended)
        except Exception:
            pass

    @staticmethod
    def _notice_pre_close_backfill(period):
        """说明一次：这些 preClose 是推导出来的，不是终端给的。"""
        if _PRE_CLOSE_NOTICE["shown"]:
            return
        _PRE_CLOSE_NOTICE["shown"] = True
        try:
            log.info(
                "period=%s 的每根 K 线 preClose 都为 0.0，"
                "因此已按各周期首个交易日的日线 preClose 回填（问题 #166）。"
                "该值与 MiniQMT 的结果一致，除息日也是如此；"
                "但这里是本地计算值，并非终端返回。"
                "如需原始 K 线，请传入 backfill_pre_close=False。",
                period)
        except Exception:
            pass

    def get_market_data_ex(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        chunk_size=None,
        timeout_seconds=None,
        use_formula=True,
        backfill_pre_close=True,
        resynth_ongoing_multiday=True,
        heal=True,
        subscribe=None,
    ):
        """通过 RPC 拉 K 线，每批 ``chunk_size`` 个代码。

        ``heal=False`` 跳过自愈——自愈会在原始库未就绪时改用服务端原始下载
        作答。唯一必须关掉它的是 ``download_history_data2`` 的就绪轮询
        （#275）：它在等自己刚提交的下载，而在这里自愈会每 1.5s 一轮重新提交
        同一次下载，把它等待的落地时间一再推后，直到 60s 预算耗尽。

        ``subscribe`` 是 QMT 原生签名的最后一个参数：True（大 QMT 默认）会把
        查过的标的塞进常驻内存订阅池，批量拉分钟线时终端内存单调上涨直到崩溃
        （#361）；False 只读本地已下载数据。缺省 None = 不传、维持原生默认。
        批量历史回补请显式传 ``subscribe=False``。

        缓存贯通：取到的内容都会写入本地缓存（按 dividend_type 分键），因此它
        始终是最新的——这对前复权数据很重要，它的历史会随每次分红重新缩放。

        分批存在的原因是：一个携带全部代码的请求共用一个 RPC 超时（默认 6s），
        于是宽的 stock_list 会超时并丢掉整次拉取，而不是降级（问题 #47）。拆批
        让每个请求小到能回答，仍然失败的批次只损失它自己的代码——其余照常返回。

        ``chunk_size=0`` 恢复旧的单请求行为。

        空的 ``field_list`` 表示“全部字段”，而那只有 RPC 路径能回答：
        FormulaServer 有六个 K 线列加 time，没有那四个日线列
        （settelementPrice、openInterest、preClose、suspendFlag）。点名你真正
        想要的字段，才是打开直连路径的钥匙——实测 0.015s 对 5.8s（问题 #104）。

        向它索要它没有的字段，现在会在路由器处被拒绝并改由 RPC 提供。以前它会
        用一列 NaN 作答，于是点名全部 11 列看起来是免费提速，实际悄悄少给了
        四列（见 formula_server.SERVED_FIELDS）。
        """
        _notice_field_list_cost(field_list)
        codes = list(stock_list or [])
        base = dict(
            field_list=list(field_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
        if subscribe is not None:
            # 只在调用方给了才传（#361）：不给 = 大 QMT 原生默认 True，行为不变。
            base["subscribe"] = bool(subscribe)
        step = DEFAULT_MARKET_DATA_CHUNK if chunk_size is None else int(chunk_size)

        if step <= 0 or len(codes) <= step:
            data = self._get_market_data_ex_batch(
                dict(base, stock_list=codes), timeout_seconds=timeout_seconds,
                use_formula=use_formula, heal=heal,
            )
        else:
            data = {}
            failures = []
            for index in range(0, len(codes), step):
                batch = codes[index:index + step]
                try:
                    part = self._get_market_data_ex_batch(
                        dict(base, stock_list=batch), timeout_seconds=timeout_seconds,
                        use_formula=use_formula, heal=heal,
                    )
                except Exception as exc:
                    # 丢掉一批不能连带丢掉其它批：在请求了 500 个代码时，
                    # 部分结果胜过异常。
                    failures.append((batch, exc))
                    continue
                if isinstance(part, dict):
                    data.update(part)
            if failures and not data:
                # 什么都没有回来——抛出第一个原因，而不是静默返回空 dict。
                raise failures[0][1]
            for batch, exc in failures:
                print("[bigqmt_client] get_market_data_ex 批量读取失败"
                      "（codes=%d，first=%s）：%s"
                      % (len(batch), batch[0] if batch else "", exc))

        if backfill_pre_close:
            # 在写缓存之前，这样缓存保留的是修正后的 K 线，而不是全零那份
            # （#166）。
            data = self._backfill_pre_close(
                data, period=period, dividend_type=dividend_type,
                timeout_seconds=timeout_seconds, use_formula=use_formula, subscribe=subscribe,
            )

        if resynth_ongoing_multiday:
            # 与回填同一规则：缓存保留重建后的 K 线，而不是被窗口截断的那根
            # （#226）。
            data = self._resynth_ongoing_multiday_bars(
                data, period=period, start_time=start_time, end_time=end_time,
                dividend_type=dividend_type, timeout_seconds=timeout_seconds,
                use_formula=use_formula, subscribe=subscribe,
            )

        cache = self._local_cache()
        if cache is not None and isinstance(data, dict):
            for code, df in data.items():
                try:
                    cache.write(code, period, df, dividend_type=dividend_type)
                except Exception as exc:
                    log.warning("本地缓存写入失败 code=%s period=%s：%s",
                                code, period, exc)
        return data

    def get_local_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        data_dir=None,
        resynth_ongoing_multiday=True,
    ):
        """从 **客户端** 本地缓存读取 K 线——不发 RPC 给大 QMT。

        返回 dict {code: DataFrame}。缓存未命中的代码会被省略，除非开启了
        local_cache_fallback_rpc（那时会通过 RPC 取回并缓存）。

        无缓存的 RPC 分支从终端本地文件作答，并做与 get_market_data_ex 相同的
        进行中多日核对（#226/#307）：关掉缓存后，这就是调用方读取可信本地数据
        所用的那个调用，而终端合成的周期答案在那里同样会丢掉或清零进行中的
        K 线。
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        cache = self._local_cache()
        if cache is None:
            # 缓存已关 -> 行为等同普通 RPC 本地数据读取。
            data = self._call(
                "get_local_data",
                field_list=_as_list(field_list),
                stock_list=codes,
                period=period,
                start_time=start_time,
                end_time=end_time,
                count=count,
                dividend_type=dividend_type,
                fill_data=fill_data,
                data_dir=data_dir,
            )
            if (resynth_ongoing_multiday
                    and str(period or "") in RESYNTH_ONGOING_PERIODS):
                # 原始本地读取对单代码返回裸 frame，对多代码返回 {code: frame}
                # 的 dict（#307 就是通过 dict 形状报告的）；把单代码 frame 包装
                # 起来，使两者走同一套核对，然后再拆包。
                single = None
                payload = data
                if not isinstance(data, dict) and codes:
                    single = codes[0]
                    payload = {single: data}
                if isinstance(payload, dict) and payload:
                    payload = self._resynth_ongoing_multiday_bars(
                        payload, period=period, start_time=start_time,
                        end_time=end_time, dividend_type=dividend_type,
                    )
                data = payload.get(single) if single is not None else payload
            if resynth_ongoing_multiday and str(period or "") == "1d":
                # 上面那段在 1d 上的孪生：本地答案丢掉的正在形成的日线，从
                # 实时读取补上。单代码 frame 同样走一遍包装/拆包。
                single = None
                payload = data
                if not isinstance(data, dict) and codes:
                    single = codes[0]
                    payload = {single: data}
                payload = self._rescue_ongoing_daily_bar(
                    payload, period=period, end_time=end_time,
                    dividend_type=dividend_type)
                data = payload.get(single) if single is not None else payload
            if isinstance(data, dict):
                return {code: _ensure_kline_columns(frame, field_list=field_list, period=period)
                        for code, frame in data.items()}
            return _ensure_kline_columns(data, field_list=field_list, period=period)
        fields = list(field_list or [])
        result = {}
        missing = []
        for code in codes:
            df = cache.read(code, period, start_time, end_time, count, dividend_type=dividend_type)
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                result[code] = self._select_fields(
                    _normalize_market_data_frame(df, field_list=fields),
                    fields,
                )
            else:
                missing.append(code)
        if missing and _bool_value(self.client.local_cache_config.get("fallback_rpc"), False):
            fetched = self._pull_and_cache(missing, period, start_time, end_time, count, dividend_type)
            for code in missing:
                df = fetched.get(code)
                if df is not None and getattr(df, "shape", (0,))[0] > 0:
                    result[code] = self._select_fields(
                        _ensure_kline_columns(
                            _normalize_market_data_frame(df, field_list=fields),
                            field_list=fields, period=period),
                        fields,
                    )
        if result and resynth_ongoing_multiday and str(period or "") == "1d":
            # 缓存分支才是过期 frame 咬人的地方：今天开盘前写入的文件会用最后
            # 一根停在上一个交易日的 K 线服务一整天，而下载从不刷新它。
            result = self._rescue_ongoing_daily_bar(
                result, period=period, end_time=end_time,
                dividend_type=dividend_type)
        return result

    @staticmethod
    def _select_fields(df, fields):
        if not fields:
            return df
        try:
            keep = [c for c in df.columns if c in fields or (c in _TIME_COL_NAMES and c != "stime")]
            return df[keep] if keep else df
        except Exception:
            return df

    @staticmethod
    def _is_all_zero_any(data):
        """识别复权 K 线全为零的症状（服务端缺原始数据）。

        大 QMT 由原始 K 线 + 分红因子计算前/后复权 K 线；服务端缺这些数据时
        价格列会全为 0.0（只有最后一根可能带实时价）。递归处理 DataFrame、
        {code: DataFrame} 和 {field: {code: [..]}} 三种形状。
        """
        try:
            if data is None:
                return False
            cols = getattr(data, "columns", None)
            if cols is not None:  # pandas 的 DataFrame
                if "close" not in list(cols):
                    return False
                closes = data["close"]
                if len(closes) == 0:
                    return False
                head = closes.iloc[:-1] if len(closes) > 1 else closes
                return bool((head == 0).all())
            if isinstance(data, dict):
                return any(BigQmtXtData._is_all_zero_any(v) for v in data.values())
            if isinstance(data, (list, tuple)) and data and all(
                isinstance(x, (int, float)) for x in data
            ):
                head = data[:-1] if len(data) > 1 else data
                return bool(head) and all(x == 0 for x in head)
            return False
        except Exception:
            return False

    @staticmethod
    def _is_placeholder_frame(frame):
        """单个代码的 frame 是无成交占位：每根 K 线 ``volume == 0`` 且
        ``suspendFlag == 1``。

        对没有本地 K 线的窗口，大 QMT 会用占位 K 线作答而不是空表，填充内容
        还因构建而异。国金终端填 0（即 ``_is_all_zero_any`` 抓到的全零形状）。
        0.3.40 上的一个华泰终端则把前收填进 open/high/low/close（#339，
        @wolfeee：20260918 起 002594.SZ 的 1m 答了 150 根日期为 20260919——
        一个周六——的 K 线，全为 84.3，volume 0，suspendFlag 1）。非零价格会
        绕过全零检测器，于是自愈从未触发，调用方拿到一整天的横盘假 K 线而
        没有任何提示。两列都是必需的：没有 ``suspendFlag`` 的 frame
        （FormulaServer 的六列路径）永远不会命中，而任何一处非零成交量都说明
        有真实成交。
        """
        cols = getattr(frame, "columns", None)
        if cols is None:
            return False
        cols = list(cols)
        if "volume" not in cols or "suspendFlag" not in cols:
            return False
        if len(frame) == 0:
            return False
        try:
            return bool((frame["volume"] == 0).all() and (frame["suspendFlag"] == 1).all())
        except Exception:
            return False

    @staticmethod
    def _is_placeholder_all(data):
        """返回的每个 frame 都是占位（见 ``_is_placeholder_frame``）。

        有意用 ``all`` 而不是 ``any``：真正停牌的股票确实会诚实地答出同样形状，
        而组合读取中出现这样一只，绝不能把整次读取变成每次调用的自愈
        （下载 + 睡眠 + 重读）。所有代码同时走平才是原始库未填充的信号，与非
        复权分支上的多数守卫同理。
        """
        try:
            cols = getattr(data, "columns", None)
            if cols is not None:
                return BigQmtXtData._is_placeholder_frame(data)
            if isinstance(data, dict) and data:
                frames = [v for v in data.values() if getattr(v, "columns", None) is not None]
                if not frames:
                    return False
                return all(BigQmtXtData._is_placeholder_frame(v) for v in frames)
            return False
        except Exception:
            return False

    def _ensure_server_raw(self, codes, period, start_time, end_time):
        """触发服务端原始下载，使复权 K 线可以算出。"""
        try:
            self.client.call(
                "download_history_data2",
                {
                    "stock_list": list(codes),
                    "period": period,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                timeout_seconds=60.0,
            )
        except Exception:
            pass

    @staticmethod
    def _served_codes(data):
        """服务端实际返回的代码，覆盖两种返回形状：
        get_market_data_ex 以代码为键（{code: DataFrame}），get_market_data
        以字段为键（{field: {code: [..]}}）——在错误的层级上读键会让每个代码
        都显得缺失。

        只有携带至少一行（#387）的代码才算数：服务端会为每个被请求、但原生
        读取没有返回的代码填入空信封，而把空信封算作已返回，会让缺失多数自愈
        守计算出 missing=0 从而完全跳过原始下载——17 个原生失败的分钟代码中
        有 14 个以空 frame 返回，自愈从未触发。占位 frame 仍算作已返回：它们
        有行（volume 0 / suspendFlag 1），所以上面的 #339 占位分支仍然管它们。"""
        if not isinstance(data, dict):
            return set()

        def _rows(payload):
            try:
                return len(payload) if payload is not None else 0
            except TypeError:
                return 0

        if any(isinstance(value, dict) for value in data.values()):
            return {code for value in data.values() if isinstance(value, dict)
                    for code, rows in value.items() if _rows(rows) > 0}
        return {code for code, frame in data.items() if _rows(frame) > 0}

    def _heal_adjusted(self, method, params, data, wait_seconds=2.0, timeout_seconds=None):
        """对来自未就绪原始库的读取做自愈：若复权拉取全为零，或非复权拉取缺了
        大部分请求的代码，或每个代码都返回无成交占位 frame（#339），则触发
        服务端原始下载，等待异步落地，重试一次。"""
        dividend_type = str(params.get("dividend_type") or "none").lower()
        codes = list(params.get("stock_list") or params.get("stock_code") or [])
        if not codes:
            return data
        if self._is_placeholder_all(data):
            # 两个分支都适用：占位填充是原始库的答案，不是复权产物，所以非复权
            # 读取也会拿到同样的横盘 K 线——而那些代码算作 *已返回*，这正是下面
            # 的缺失多数守卫看不到它们的原因。
            pass
        elif dividend_type in ("", "none"):
            # 非复权 K 线从不被填零，所以全零检测器不适用——代码 *缺失* 意味着
            # 服务端根本没有它的原始 K 线。大 QMT 的原始库不会全市场自动填充
            # （2026-08-30 一条 --tick 管道只读到 5225 个代码中的 8 个，因为从来
            # 没人下载过原始日线），所以也用同样方式自愈。但全市场读取总有几个
            # 服务端永远给不了的代码（退市、停牌、无行情权限），而在 *任何* 代码
            # 缺失时自愈会让它们变成每次调用的成本——每次都是原始下载 + 睡眠 +
            # 全量重读。只在多数缺失时自愈：那才是原始库未填充的信号。
            served = self._served_codes(data)
            missing = sum(1 for code in codes if code not in served)
            if missing < max(1, len(codes) // 2):
                return data
        elif not self._is_all_zero_any(data):
            return data
        self._ensure_server_raw(
            codes,
            params.get("period", "1d"),
            params.get("start_time", ""),
            params.get("end_time", ""),
        )
        time.sleep(wait_seconds)
        if timeout_seconds is not None:
            healed = self.client.call(method, params, timeout_seconds=timeout_seconds)
        else:
            healed = self._call(method, **params)
        if self._is_placeholder_all(healed):
            # 下载没有改变答案：要么终端这个窗口本就没有数据可下（真停牌，或
            # 报告者那种下载 RPC 本身回 False 的终端，#339）——那就直说，而不是
            # 交回一个读起来像 K 线的横盘 frame。
            log.warning(
                "%s %s %s %s~%s：服务端下载并重试后，每根 K 线仍是"
                "volume=0 / suspendFlag=1。这些是大 QMT 的占位 K 线，并非真实成交；"
                "终端本地没有该窗口的 %s 数据。请检查此终端的 "
                "download_history_data2（probe_capabilities -> qmt_globals），"
                "或先在终端“数据管理”中下载该周期。",
                method, ",".join(codes[:5]) + (",..." if len(codes) > 5 else ""),
                params.get("period", "1d"), params.get("start_time", ""),
                params.get("end_time", ""), params.get("period", "1d"),
            )
        return healed

    def _pull_and_cache(self, codes, period, start_time, end_time, count, dividend_type="none"):
        """通过 RPC 拉取代码（get_market_data_ex 已经会缓存它们）。"""
        data = self.get_market_data_ex(
            field_list=DEFAULT_DOWNLOAD_FIELDS,
            stock_list=list(codes),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
        )
        out = {}
        for code in codes:
            df = data.get(code) if isinstance(data, dict) else None
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                out[code] = df
        return out

    def subscribe_quote(self, stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
        """按 MiniQMT 的方式订阅一个标的：回调会持续触发。

        过去它只调用一次回调、之后再无消息——挂着订阅的名字做一次性拉取，比
        没有更糟，因为看起来像是能用的（问题 #95）。

        tick 走全推推送通道；单个代码就是单元素代码列表。K 线没有服务端推送
        ——桥接层只暴露 ContextInfo.subscribe_whole_quote，而它只带 tick——所以
        K 线靠轮询，最新一根变化时才发出。
        """
        payload = {
            "stock_code": stock_code,
            "period": period,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        }

        if str(period).lower() in ("tick", "full_tick"):
            session = self._whole_quote_session()
            session.start()
            seq = session.subscribe_whole_quote([stock_code], callback=callback)
            # 全推回调是增量的；先用一次快照给它打底，使订阅者不至于在第一次
            # 变化之前什么都拿不到。
            if callback is not None:
                try:
                    callback(self.get_full_tick([stock_code]))
                except Exception:
                    pass
            self._record_subscription(seq, payload)
            return seq

        seq = self._next_seq()

        def fetch():
            # 盘中形成 bar 必须读 QMT 实时数据——FormulaServer 快照可能滞后数
            # 小时（实测 11:30 后冻结，收盘后还停在午间数据），订阅推送如果走
            # 直连会把"刚完成的 bar"发成数小时前的旧快照（issue #104 实测）。
            return self.get_market_data_ex(
                stock_list=[stock_code], period=period,
                start_time=start_time, end_time=end_time, count=count or 1,
                use_formula=False)

        poller = _BarPoller(
            fetch, callback, self._bar_poll_interval_seconds(),
            on_error=lambda exc: log.debug(
                "subscribe_quote 轮询失败 code=%s period=%s：%s",
                stock_code, period, exc),
            on_no_data=lambda: log.warning(
                "subscribe_quote：%s 没有 %s 的 K 线；订阅仍然有效，"
                "但在终端取得该周期数据前不会推送。"
                "请检查该周期是否已下载（get_market_data_ex 对该周期返回空表）。",
                period, stock_code))
        with self._bar_poller_lock:
            self._bar_pollers[seq] = poller
        poller.start()
        self._record_subscription(seq, payload)
        return seq

    def _bar_poll_interval_seconds(self):
        config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        value = (config.get("bar_poll_interval_seconds")
                 or os.environ.get("BIGQMT_BAR_POLL_INTERVAL_SECONDS"))
        try:
            return float(value)
        except (TypeError, ValueError):
            return DEFAULT_BAR_POLL_INTERVAL_SECONDS

    def _record_subscription(self, seq, payload, active=True):
        """只是记账——服务端没有任何东西消费它，而且它需要 Valkey 客户端，
        zmq 部署没有。绝不让它弄坏一个本来可用的订阅。"""
        try:
            self.client.save_quote_subscription(seq, dict(payload, seq=seq), active=active)
            self.client.publish_event(
                "subscribe_quote" if active else "unsubscribe_quote",
                dict(payload, seq=seq))
        except Exception:
            pass

    def subscribe_quote2(self, stock_code, period="1d", start_time="", end_time="", count=0, dividend_type=None, callback=None):
        return self.subscribe_quote(
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            callback=callback,
        )

    def _whole_quote_session(self):
        if self._quote_session is None:
            if self._quote_session_factory is not None:
                self._quote_session = self._quote_session_factory()
            else:
                self._quote_session = self._build_quote_session()
        return self._quote_session

    def _build_quote_session(self):
        from .whole_quote_session import WholeQuoteClientSession

        client = self.client

        def rpc_call(method, params):
            return client.call(method, params)

        return WholeQuoteClientSession(
            rpc_call=rpc_call,
            push_channel=self._build_quote_push_channel(),
            client_id=_quote_client_id(),
            heartbeat_interval_seconds=_env_float("BIGQMT_QUOTE_HEARTBEAT_SECONDS", 3.0),
            sub_id_func=self._next_seq,
        )

    def _build_quote_push_channel(self):
        return _build_quote_push_channel(self.client)

    def quote_subscription_status(self):
        """桥接层认为还活着的全推组合，以及各自的陈旧程度。

        用来诊断“有东西一直在推，但我的 seq 丢了”：一直很新鲜的组合说明有
        LIVE keepalive 在喂它（某个遗留的客户端进程）；超过心跳超时后静默的，
        即将被服务端回收（实盘实测：客户端死后约 30s）。
        """
        return self.client.call("quote_subscription_status", {})

    def quote_unsubscribe_all(self):
        """杀掉桥接层上的所有全推订阅，不需要 seq。

        keepalive 对未知 sub_id 是空操作，所以被强制清掉的组合会一直静默，直到
        有人重新订阅。
        """
        return self.client.call("quote_unsubscribe_all", {})

    _SUBSCRIBE_PRIME_MAX_CODES = 100

    def subscribe_whole_quote(self, code_list, callback=None):
        session = self._whole_quote_session()
        session.start()
        sub_id = session.subscribe_whole_quote(code_list, callback=callback)
        # 大 QMT 的全推回调是增量的（只带变化的标的），所以先用一次完整
        # get_full_tick 快照给回调打底。
        #
        # 有意用 types=["all"]：推送侧是 ContextInfo 自己的
        # subscribe_whole_quote，不做收窄，因此收窄过的快照会先给订阅者 2315
        # 只股票，然后开始推全部 26744 个标的。打底必须覆盖推送覆盖的范围。
        #
        # 巨大的单列代码名单（例如 3000 只股票）会让服务端 adjust 线程通过
        # get_full_tick(上千个代码) 阻塞数秒。改为从代码里取出交易所标记
        # （SH/SZ/...），用这些标记调用 get_full_tick——QMT 把交易所标记当作
        # 全交易所操作（很快）。然后把结果过滤到调用方真正要的代码。
        #
        # 标记路径不能传 ["all"]——那正是 #247 去掉的 26744 个标的的读取——
        # 所以它按请求代码实际所属的品种传（#358）。那里什么都不说，就是 100
        # 个代码以上时可转债从第一帧消失、而推送仍在送它们的原因。
        if callback is not None:
            try:
                # 无条件调用，包括收到空快照时：这是 #247 之前的做法，而一个
                # 等待首次回调的订阅者，不能因为打底恰好为空就挂住。
                callback(self._prime_snapshot(code_list))
            except Exception:
                pass
        return sub_id

    # QMT 确实会为全市场快照作答的交易所标记。
    # 在实盘终端上实测：SH 和 SZ 一起在约 330ms 内返回 5218 行，BJ 返回 343。
    # 期货标记 SF DF ZF IF INE GF 全部返回 **0 行**，所以把期货名单走标记
    # 路径会得到空打底，订阅者静默地永远收不到第一帧。这就是 #95 的形态再现：
    # 订阅看着是活的，只是快照缺了。
    _WHOLE_MARKET_TOKENS = ("SH", "SZ", "BJ")

    def _prime_snapshot(self, code_list):
        """subscribe_whole_quote 的首帧快照。

        逐代码的 get_full_tick 在 QMT 的 adjust 线程上跑，成本随名单增长：
        实盘终端上的中位数是 100 个代码约 170ms、200 个约 500ms、500 个约
        2.5s、1000 个约 9.5s，3000 个直接 TimeoutError——而 adjust 线程在这段
        时间里全程阻塞，使 drain 停滞并把其它 RPC 都排在后面（#247）。
        全市场标记约 330ms 且与规模无关，所以超过阈值后标记路径更优。

        代码 **原样** 传给 QMT；只对交易所后缀做大写化用于路由，且只对大写副本
        做匹配。大 QMT 有 cu2610.SF 而没有 CU2610.SF，所以发送前把代码转大写会
        静默弄死期货订阅（#58/#95）。
        """
        codes = [str(c).strip() for c in (code_list or []) if str(c or "").strip()]
        if not codes:
            return {}
        if len(codes) <= self._SUBSCRIBE_PRIME_MAX_CODES:
            return self.get_full_tick(codes, types=["all"]) or {}

        # 按这个代码的交易所能否整体打底来分流。
        whole, direct = [], []
        for code in codes:
            suffix = code.rsplit(".", 1)[-1].upper() if "." in code else ""
            (whole if suffix in self._WHOLE_MARKET_TOKENS else direct).append(code)

        snapshot = {}
        if whole:
            wanted = set()
            tokens = set()
            kinds = set()
            for code in whole:
                code_kinds = _prime_tick_types(code)
                if not code_kinds:
                    # 未分类：服务端会把标记收窄成股票并丢掉这个代码而不说明
                    # （#358）。改成直接读它——慢胜过静默缺失。
                    direct.append(code)
                    continue
                wanted.add(code.upper())
                tokens.add(code.rsplit(".", 1)[-1].upper())
                kinds.update(code_kinds)
            if tokens:
                # types= 才能让推送侧和打底覆盖同一批标的：推送是 ContextInfo
                # 自己的 subscribe_whole_quote，完全不做收窄。
                full = self.get_full_tick(sorted(tokens), types=sorted(kinds)) or {}
                snapshot.update(
                    (k, v) for k, v in full.items() if str(k).upper() in wanted)
        if direct:
            # 这些没有全市场标记（期货，以及任何新东西）。长名单会慢，但慢而
            # 正确胜过快而空：在 #247 之前这是唯一路径，所以不是退步。
            snapshot.update(self.get_full_tick(direct, types=["all"]) or {})
        return snapshot

    def unsubscribe_quote(self, seq):
        # 现在有三类句柄：推送会话持有的全推/tick 订阅、这里持有的 K 线轮询，
        # 以及只作为 valkey 记账存在过的历史 seq。
        with self._bar_poller_lock:
            poller = self._bar_pollers.pop(seq, None)
        if poller is not None:
            poller.stop()
            self._record_subscription(seq, {}, active=False)
            return 0

        session = self._quote_session
        if session is not None and session.has_subscription(seq):
            session.unsubscribe_quote(seq)
            self._record_subscription(seq, {}, active=False)
            return 0

        self._record_subscription(seq, {}, active=False)
        return 0

    def unsubscribe_quotes(self, seqs):
        """批量释放全推句柄：一次 RPC，一次 topic 重同步（#418）。

        逐个句柄调用 ``unsubscribe_quote`` 每次都要一次 RPC 加一次订阅者重建
        ——一次 26 个句柄的退出付了 26 次 RPC 和 25 次重建，耗时数秒。这里只
        在服务端释放 **本** 客户端点名的句柄（共享组合为其它订阅者保留；
        quote_unsubscribe_all 仍是最后手段）。属于 K 线轮询的句柄保留其逐项
        语义（它们是本地线程，完全不发 RPC）。返回服务端的报告：released /
        unknown / native_errors。
        """
        wanted = [s for s in (seqs or [])]
        session_owned = []
        session = self._quote_session
        with self._bar_poller_lock:
            pollers = []
            for seq in wanted:
                poller = self._bar_pollers.pop(seq, None)
                if poller is not None:
                    pollers.append((seq, poller))
                elif session is not None and session.has_subscription(seq):
                    session_owned.append(seq)
        for seq, poller in pollers:
            poller.stop()
            self._record_subscription(seq, {}, active=False)
        report = None
        if session_owned and session is not None:
            report = session.unsubscribe_quotes(session_owned)
            for seq in session_owned:
                self._record_subscription(seq, {}, active=False)
        for seq in wanted:
            if seq not in session_owned:
                self._record_subscription(seq, {}, active=False)
        if report is None:
            report = {"released": [], "unknown": [], "native_errors": []}
        return report

    def stop_all_subscriptions(self):
        """停掉本对象持有的所有 K 线轮询。守护线程本来会随进程退出而死；
        这是给测试和回收客户端但不退出进程的调用方用的。"""
        with self._bar_poller_lock:
            pollers = list(self._bar_pollers.values())
            self._bar_pollers.clear()
        for poller in pollers:
            poller.stop()
        return len(pollers)

    def run(self):
        while True:
            time.sleep(3600)

    def get_divid_factors(self, stock_code, start_time="", end_time=""):
        """除权除息因子，返回 DataFrame，对齐 ``xtdata.get_divid_factors``。

        行是除权日（毫秒时间戳，同官方保留原始键），列是 ``interest`` /
        ``stockBonus`` / ``stockGift`` / ``allotNum`` / ``allotPrice`` /
        ``gugai`` / ``dr``。线上仍是大 QMT 原生的 ``dict{时间戳: [7 个数]}``，
        走原始 RPC（含 ``getDividFactors`` 别名）拿到的还是那个 dict。
        """
        data = self._call("get_divid_factors", stock_code=stock_code,
                          start_time=start_time, end_time=end_time)
        return _divid_factors_frame(data)

    def submit_download_history_data(self, stock_code, period, start_time="", end_time="", incrementally=None):
        return self._call(
            "submit_download_history_data", stock_code=stock_code, period=period,
            start_time=start_time, end_time=end_time, incrementally=incrementally,
        )

    def submit_download_history_data2(self, stock_list, period, start_time="", end_time="", incrementally=None):
        return self._call(
            "submit_download_history_data2", stock_list=_as_list(stock_list), period=period,
            start_time=start_time, end_time=end_time, incrementally=incrementally,
        )

    def get_download_status(self, job_id):
        return self._call("get_download_status", job_id=job_id)

    def wait_download(self, job_id, timeout=None, poll_interval=None, callback=None):
        """Poll from the client and return the terminal or last known job status."""
        timeout = float(timeout if timeout is not None else self.client.download_wait_seconds)
        interval = float(poll_interval if poll_interval is not None else self.client.download_poll_interval_seconds)
        if not math.isfinite(timeout) or not math.isfinite(interval):
            raise ValueError("invalid download wait timing")
        deadline = time.monotonic() + max(0, timeout)
        previous = None
        while True:
            status = self.get_download_status(job_id)
            if callback is not None and status != previous:
                callback(dict(status))
            if status["state"] in (DONE, FAILED):
                return status
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return status
            previous = status
            time.sleep(min(remaining, max(0.05, interval)))

    def download_history_data2(self, stock_list, period, start_time="", end_time="", callback=None, incrementally=None, dividend_type="none", chunk_size=None, download_timeout_seconds=180.0, data_wait_seconds=10.0):
        """通过 RPC 从大 QMT 拉 K 线并缓存到本地，分批进行。

        与 xtdata.download_history_data2 对应：此后 get_local_data(..., 同一
        dividend_type) 就地读本地数据、不再发 RPC。每批都会重新实时拉取，
        所以重跑能让缓存保持最新——前复权数据需要这一点。``callback``（可选）
        每只股票调用一次，参数 {finished, total, stockcode}——xtdata 风格。
        返回 {finished, total}。

        服务端下载对 **每个** dividend_type 都会执行，与 xtdata 语义一致
        （“填充本地 QMT 库”）。过去非复权拉取会跳过它，使非复权下载变成空操作
        却仍报告进度（问题 #47）。

        复权数据（dividend_type != none）还额外依赖它：大 QMT 由 **原始** 历史
        + 分红因子计算复权 K 线，两者都不存在时
        get_market_data_ex(dividend_type='front') 返回全零收盘价（实盘验证）。

        ``download_timeout_seconds`` 只覆盖服务端下载；给得宽是因为冷代码配宽
        窗口可能要几分钟。

        ``data_wait_seconds`` 是每批对首拉为空的代码继续轮询的时长。服务端下载
        在数据落地后才返回（实测：20,726 根 1m K 线在 0.6-1.4s 下载结束后 0.3s
        可读），所以首拉为空几乎总意味着终端对该代码没有数据（停牌、新股、
        退市），等也不会来。旧的 60s 默认值会让这样一个代码拖住整批一分钟
        （#339：300 个合约上“每个合约 12s”）。10s 是几次轮询，够覆盖数据落地
        偏晚的终端。

        服务端下载是尽力而为的，前提是客户端拉取仍能补救它（缓存开启）；本地
        缓存关闭时它就是全部工作——它失败会抛异常，而不是报告
        {finished: total}，那正是问题 #47 的虚假进度。缓存开启时，一旦某个代码
        拉取回来为空也同样适用：没有任何东西替代的失败下载会抛异常并点名代码
        （#339）。
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        if not codes:
            return {"finished": 0, "total": 0}

        # 先做服务端下载，对每一个 dividend_type。
        #
        # 过去只在请求复权时才跑，理由是未复权拉取可以直接由
        # get_market_data_ex 服务。可它读的是大 QMT 已有的东西——并不去取任何
        # 数据。于是未复权的“下载”没碰 QMT 侧库，却仍通过回调报告
        # {finished: N}：给一件从未发生的工作画进度条（问题 #47，也是 #39 的
        # 真正原因，那个 issue 基于对本函数不完整的解读被关掉了）。
        #
        # xtdata.download_history_data 的意思是“填充本地 QMT 库”，调用方依赖
        # 这一点：FormulaServer 和 get_local_data 都读它，而这样“下载”过的代码
        # 在那里是零根 K 线。
        #
        # 复权数据还额外 **需要** 它：QMT 由原始 K 线 + 分红因子计算前/后复权
        # K 线，两者都必须在服务端存在，否则结果全为零。
        server_download_error = None
        try:
            download_reply = self.client.call(
                "download_history_data2",
                {
                    "stock_list": codes,
                    "period": period,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                timeout_seconds=float(download_timeout_seconds),
            )
            if not download_reply:
                # 服务端显式回 False = 下载任务没被受理（#339：恒 False 曾经
                # 从这里漏过去，下面的空拉被当成「停牌/退市」容忍掉，最终报出
                # finished==total 的假进度）。失败必须响（#47 契约）。
                server_download_error = RuntimeError(
                    "服务端报告 download_history_data2 = %r：终端没有受理这次下载任务"
                    "（codes=%d period=%s）" % (
                        download_reply, len(codes), period))
        except Exception as exc:
            # 只有在下面的拉取还能补救下载时才尽力而为（数据已在服务端）。
            # 本地缓存关闭时就没有拉取——服务端下载就是全部工作，失败之后仍报
            # {finished: total} 就是问题 #47 的虚假进度。
            server_download_error = exc

        if self._local_cache() is None:
            # 本地缓存关闭：只做服务端下载（步骤 1）。数据落在服务端 DAT 文件
            # 里，所以这是真工作——不是问题 #47 的假进度下载。对所有周期（tick
            # 和 K 线一样）统一跳过客户端拉取，进度按代码报告而无需任何拉取。
            if server_download_error is not None:
                raise server_download_error
            total = len(codes)
            for index, code in enumerate(codes, 1):
                if callback is not None:
                    try:
                        callback({"finished": index, "total": total, "stockcode": code})
                    except Exception:
                        pass
            return {"finished": total, "total": total}

        total = len(codes)
        step = int(chunk_size or 300)
        if step <= 0:
            step = 300
        finished = 0
        skipped_all = []
        for i in range(0, total, step):
            batch = codes[i:i + step]
            # QMT 的下载全局是「提交任务即返回」，数据在服务端异步落地
            # （秒~分钟级）。下载后立刻读只能看到旧数据——issue #66 里
            #「tick 只能获得最近 1 天」的真正原因就是这个竞态：数据还没落地
            # 就已经被读走并缓存了空结果。这里分批轮询，直到批内每个代码都
            # 出现真实数据行或超时（超时容忍停牌/退市等确实无数据的代码）。
            deadline = time.time() + float(data_wait_seconds)
            while True:
                # get_market_data_ex 是 cache-through：每次轮询都会写入缓存，
                # 最后一次（数据齐或超时）的结果即最终缓存内容。
                data = self.get_market_data_ex(
                    field_list=DEFAULT_DOWNLOAD_FIELDS,
                    stock_list=batch,
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    count=-1,
                    dividend_type=dividend_type,
                    fill_data=False,  # fill 会用全 0 占位行冒充数据，轮询判定必须关掉
                    # 单次拉取的 RPC 超时不是轮询预算：300 个代码 x 2 万根
                    # K 线的批次是多秒级的回复。
                    timeout_seconds=max(float(data_wait_seconds), 60.0),
                    subscribe=False,  # 填充缓存不得占用原生订阅。
                    # 等的就是刚提交的那笔下载。heal 看到「还没落地」会把它原样
                    # 再提交一遍、睡 2 秒、再读——每轮如此，等待目标被反复推后，
                    # 单票冷启动必然打满 60 秒（#275）。轮询里的读不参与 heal。
                    heal=False,
                )
                ready = 0
                for code in batch:
                    df = (data or {}).get(code)
                    if df is not None and getattr(df, "shape", (0,))[0] > 0:
                        ready += 1
                if ready >= len(batch) or time.time() >= deadline:
                    break
                time.sleep(1.5)
            if server_download_error is not None and ready < len(batch):
                # 服务端下载失败，且对批次中部分代码拉取也没找到可补救的内容：
                # 下载没有发生，也没有任何东西替代它。在这里报告
                # {finished: total} 就是 #47 的虚假进度换个分支再现（#339）。
                # 下载失败但 K 线本来就在服务端的，仍会在上面完成
                # （ready == len(batch)）；下载 **没有** 失败之后的空代码保留
                # 宽容超时（停牌/退市代码没有 K 线可等）。
                missing = [code for code in batch
                           if getattr((data or {}).get(code), "shape", (0,))[0] == 0]
                raise RuntimeError(
                    "download_history_data2 %s %s~%s: 服务端下载失败（%s），"
                    "且拉取对 %s 没有取到任何行——这些代码什么都没下载" % (
                        period, start_time, end_time, server_download_error,
                        ",".join(missing)))
            skipped = []
            if start_time and ready < len(batch):
                no_data = [code for code in batch
                           if getattr((data or {}).get(code), "shape", (0,))[0] == 0]
                skipped = self._download_skipped_dead_zone(
                    no_data, period, start_time, end_time)
                if skipped:
                    skipped_all.extend(skipped)
                    # #397：终端注入式下载只从本地最后一根 K 线往后追加——这些
                    # 窗口在它之前结束，所以即使任务被接受也没写入。要点名它们，
                    # 而不是报告虚假进度。
                    log.warning(
                        "download_history_data2 %s %s~%s：%d 个代码位于"
                        "仅追加下载的死区（窗口结束时间早于终端本地最后一根 K 线，"
                        "因此没有写入任何数据）。它们被归入 'skipped'，"
                        "而不是 'finished'：%s",
                        period, start_time, end_time, len(skipped),
                        ",".join(skipped[:5]))
            for code in batch:
                if code in skipped:
                    continue
                finished += 1
                if callback is not None:
                    try:
                        callback({"finished": finished, "total": total, "stockcode": code})
                    except Exception:
                        pass
        result = {"finished": finished, "total": total}
        if skipped_all:
            result["skipped"] = skipped_all
        return result

    def _download_skipped_dead_zone(self, codes, period, start_time, end_time):
        """轮询为空的代码属于只追加的死区，而不是真的没有数据。

        #397：终端注入式下载只从本地最后一根 K 线往后追加；终点早于那根 K 线
        的窗口会被静默地不写入。要把它和真正没有数据的代码（停牌/退市/从未
        下载——也就是上面那套宽容处理针对的情形）区分开，需要一次尾部读取：
        有更晚数据的代码在死区里；没有的则不在。批量、count=1、关闭 heal——
        便宜，而且只读。
        """
        if not codes or not start_time:
            return []
        try:
            tails = self.get_market_data_ex(
                field_list=["close"], stock_list=list(codes), period=period,
                count=1, dividend_type="none", fill_data=False,
                heal=False, subscribe=False, timeout_seconds=60.0)
        except Exception:
            return []   # 无法判断：沿用旧的宽容收尾
        end_digits = _time_compare_digits(end_time)
        if not end_digits:
            return []
        skipped = []
        for code in codes:
            frame = (tails or {}).get(code)
            if frame is None or getattr(frame, "shape", (0,))[0] == 0:
                continue
            try:
                last_stamp = str(list(frame.index)[-1])
            except Exception:
                continue
            if _time_compare_digits(last_stamp) > end_digits:
                skipped.append(code)
        return skipped

    def download_history_data(self, stock_code, period, start_time="", end_time="", incrementally=None, dividend_type="none"):
        return self.download_history_data2([stock_code], period, start_time, end_time, dividend_type=dividend_type)

    def local_cache_stats(self):
        """返回客户端本地缓存的（缓存文件, 周期）。"""
        cache = self._local_cache()
        return cache.stats() if cache is not None else (0, [])

    def get_trading_dates(self, market, start_time="", end_time="", count=-1):
        return self._call("get_trading_dates", market=market, start_time=start_time, end_time=end_time, count=count)

    def get_holidays(self):
        return self._call("get_holidays")

    def download_holiday_data(self, incrementally=True):
        return self._call("download_holiday_data", incrementally=incrementally)

    def download_his_st_data(self, incrementally=True):
        return self._call("download_his_st_data", incrementally=incrementally)

    def get_ipo_info(self, start_time="", end_time=""):
        return self._call("get_ipo_info", start_time=start_time, end_time=end_time)

    def get_etf_info(self):
        return self._call("get_etf_info")

    def download_etf_info(self):
        return self._call("download_etf_info")

    # 下面四个的服务端实现和 RPC 白名单一直都在（market_bigqmt 的
    # download_* 方法 + valkey_rpc 的 MARKET_DATA_METHODS），只是客户端漏了这层
    # 包装，于是外部调用直接撞 AttributeError（issue #130）。
    def download_sector_data(self):
        return self._call("download_sector_data")

    def download_cb_data(self):
        return self._call("download_cb_data")

    def download_index_weight(self):
        return self._call("download_index_weight")

    def download_history_contracts(self, incrementally=True):
        # 形参保留是为了和 xtdata.download_history_contracts(incrementally=True)
        # 签名一致；大 QMT 那边这个调用没有增量参数，服务端按全量下载处理。
        return self._call("download_history_contracts")

    def get_option_list(
        self,
        undl_code,
        dedate,
        opttype="",
        isavailavle=None,
        available=None, # 修正拼写错误
    ):
        if (
            available is not None
            and isavailavle is not None
            and bool(isavailavle) != bool(available)
        ):
            raise ValueError("isavailavle 与 available 不能同时提供")
        selected_available = (
            bool(available)
            if available is not None
            else bool(isavailavle) if isavailavle is not None else False
        )
        return self._call(
            "get_option_list",
            undl_code=undl_code,
            dedate=dedate,
            opttype=opttype,
            isavailavle=selected_available,
        )

    def get_his_option_list(self, undl_code, dedate):
        return self._call("get_his_option_list", undl_code=undl_code, dedate=dedate)

    def get_his_option_list_batch(self, undl_code, start_time="", end_time=""):
        return self._call("get_his_option_list_batch", undl_code=undl_code, start_time=start_time, end_time=end_time)

    def get_financial_data(self, stock_list, table_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def download_financial_data(self, stock_list, table_list=None, start_time="", end_time="", incrementally=None):
        return self._call(
            "download_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            incrementally=incrementally,
        )

    def download_financial_data2(self, stock_list, table_list=None, start_time="", end_time="", callback=None):
        result = self._call(
            "download_financial_data2",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
        )
        if callback is not None:
            callback(result)
        return result

    def get_sector_list(self, allow_fallback=False):
        """板块名称，或说明终端无法列出板块的错误。

        ``allow_fallback=True`` 显式选择那 13 个内置知名板块，它们仍会驱动
        ``get_stock_list_in_sector``。大 QMT 根本无法枚举真实板块，而未经要求
        就交回内置列表，会让假答案和真答案无法区分（#143）。
        """
        return self._call("get_sector_list", allow_fallback=bool(allow_fallback))

    def get_sector_info(self, sector_name=""):
        return self._call("get_sector_info", sector_name=sector_name)

    def get_markets(self):
        return self._call("get_markets")

    def get_market_last_trade_date(self, market):
        return self._call("get_market_last_trade_date", market=market)

    def call_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None):
        return self._call(
            "call_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )

    def subscribe_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None, callback=None):
        result = self._call(
            "subscribe_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )
        if callback is not None:
            callback(result)
        return result

    def unsubscribe_formula(self, request_id):
        return self._call("unsubscribe_formula", request_id=request_id)

    def get_formula_result(self, request_id, start_time="", end_time="", count=-1, timeout_second=-1):
        return self._call(
            "get_formula_result",
            request_id=request_id,
            start_time=start_time,
            end_time=end_time,
            count=count,
            timeout_second=timeout_second,
        )

    def gen_factor_index(self, data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
        return self._call(
            "gen_factor_index",
            data_name=data_name,
            formula_name=formula_name,
            vars=vars,
            sector_list=list(sector_list or []),
            start_time=start_time,
            end_time=end_time,
            period=period,
            dividend_type=dividend_type,
        )

    # ------------------------------------------------------------------
    # 扩展行情/基本面方法（对应 ContextInfo 方法，走 RPC 白名单）。
    # 仅对最常用的显式声明签名；其余通过 __getattr__ 自动转发。
    # ------------------------------------------------------------------

    def get_longhubang(self, stock_list=None, start_time="", end_time="", count=-1):
        return self._call(
            "get_longhubang",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            count=count,
        )

    def get_top10_share_holder(self, stock_list, data_name, start_time, end_time, report_type="report_time"):
        return self._call(
            "get_top10_share_holder",
            stock_list=list(stock_list or []),
            data_name=data_name,
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_holder_num(self, stock_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_holder_num",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_turnover_rate(self, stock_code=None, start_time="19720101", end_time="22010101"):
        return self._call(
            "get_turnover_rate",
            stock_code=list(stock_code or []),
            start_time=start_time,
            end_time=end_time,
        )

    def get_industry(self, industry_name):
        return self._call("get_industry", industry_name=industry_name)

    def bsm_price(self, opt_type, target_price, strike_price, risk_free, sigma, days, dividend=0):
        return self._call(
            "bsm_price",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            risk_free=risk_free,
            sigma=sigma,
            days=days,
            dividend=dividend,
        )

    def bsm_iv(self, opt_type, target_price, strike_price, option_price, risk_free, days, dividend=0):
        return self._call(
            "bsm_iv",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            option_price=option_price,
            risk_free=risk_free,
            days=days,
            dividend=dividend,
        )

    def get_option_iv(self, opt_code):
        return self._call("get_option_iv", opt_code=opt_code)

    def get_option_analytics(
        self,
        opt_code,
        option_price=None,
        underlying_price=None,
        as_of=None,
        risk_free_rate=None,
        dividend_yield=None,
        price_period="1m",
        include_native_iv=False,
    ):
        """返回单个期权合约的客户端 IV 和希腊字母。"""
        from .option_analytics_client import get_option_analytics

        return get_option_analytics(
            self,
            opt_code,
            option_price=option_price,
            underlying_price=underlying_price,
            as_of=as_of,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            price_period=price_period,
            include_native_iv=include_native_iv,
        )

    def get_option_chain_analytics(
        self,
        undl_code,
        dedate,
        opttype="",
        isavailavle=False,
        underlying_price=None,
        as_of=None,
        risk_free_rate=None,
        dividend_yield=None,
        price_period="1m",
    ):
        """返回某个期权到期日的批量客户端 IV 和希腊字母。"""
        from .option_analytics_client import get_option_chain_analytics

        return get_option_chain_analytics(
            self,
            undl_code,
            dedate,
            opttype=opttype,
            isavailavle=isavailavle,
            underlying_price=underlying_price,
            as_of=as_of,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            price_period=price_period,
        )

    def get_option_detail_data(self, stockcode):
        return self._call("get_option_detail_data", stockcode=stockcode)

    def get_option_detail_data_batch(self, stockcodes, timeout_seconds=300.0):
        """用一次 RPC 取回多个期权详情；QMT 内部按代码循环。"""
        return self.client.call(
            "get_option_detail_data_batch",
            {"stockcodes": list(stockcodes)},
            timeout_seconds=timeout_seconds,
        ) or {}

    def get_option_undl_data(self, undl_code_ref=""):
        return self._call("get_option_undl_data", undl_code_ref=undl_code_ref)

    def get_option_undl(self, opt_code):
        return self._call("get_option_undl", opt_code=opt_code)

    def get_raw_financial_data(self, field_list, stock_list, start_time, end_time, report_type="report_time", data_type="dict"):
        return self._call(
            "get_raw_financial_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
            data_type=data_type,
        )

    def get_factor_data(self, field_list, stock_list, start_date, end_date):
        return self._call(
            "get_factor_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_date=start_date,
            end_date=end_date,
        )

    def get_north_finance_change(self, period):
        return self._call("get_north_finance_change", period=period)

    def get_hkt_statistics(self, stock_code):
        return self._call("get_hkt_statistics", stock_code=stock_code)

    def get_hkt_details(self, stock_code):
        return self._call("get_hkt_details", stock_code=stock_code)

    # 自定义板块写入（issue #143）。每一个都在服务端写入后回读校验，所以
    # 「没报错」现在真的代表写进去了 —— 以前 create_sector 是静默空操作。
    def create_sector(self, sector_name, stock_list):
        return self._call("create_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def create_sector_folder(self, parent_node, folder_name, overwrite=False):
        return self._call("create_sector_folder", parent_node=parent_node,
                          folder_name=folder_name, overwrite=overwrite)

    def reset_sector_stock_list(self, sector, stock_list):
        return self._call("reset_sector_stock_list", sector=sector,
                          stock_list=list(stock_list or []))

    def add_stock_to_sector(self, sector, stock_code):
        return self._call("add_stock_to_sector", sector=sector, stock_code=stock_code)

    def remove_stock_from_sector(self, sector, stock_code):
        return self._call("remove_stock_from_sector", sector=sector, stock_code=stock_code)

    def get_stock_name(self, stock):
        return self._call("get_stock_name", stock=stock)

    # ------------------------------------------------------------------
    # 合约/品种基础查询（ContextInfo 扩展，大 QMT 独有）—— issue #262
    #
    # README「RPC 接口」表的「合约/品种」一行按名字列了这些方法，服务端
    # 白名单和适配器也一直有，缺的只是这层同名包装 —— 于是
    # `xtdata.get_open_date("600519.SH")` 抛 AttributeError，而报错信息里
    # 没有任何东西告诉你其实可以走 call_method（#262，和 #130 同一类缺口）。
    #
    # 下面每一个的取舍都来自 2026-09-09 对实盘桥（国金大 QMT）的逐个实测，
    # 结果见 docs/RPC_API_REFERENCE.md 3.12。**凡是对任何代码都答同一个
    # 空值的，这里不给假答案**：一个恒为 0/None 的返回值看不见，
    # AttributeError 看得见（get_stock_type 就是这么定的）。
    #
    # 「恒为空」这个判据本身要小心用：get_bvol 一度被判成「对任何代码都答
    # 0」而拒绝转发，实际是取样全是收盘后只剩 15:00 集合竞价的股票 —— 换
    # 逆回购（204001.SH 1506858 / 131810.SZ 2404676）和 511990.SH（22883）
    # 立刻有值。分母取错就会把一个能用的方法判死，所以现在它照常转发。
    # ------------------------------------------------------------------

    def get_instrument(self, stock_code):
        """RPC 侧的方法名。`get_instrument_detail` / `get_instrumentdetail`
        是它的别名，三个名字回同一份合约详情。"""
        return self.get_instrument_detail(stock_code)

    def get_ticks(self, code_list, timeout_seconds=None, types=None):
        """RPC 侧的方法名；MiniQMT 那边叫 `get_full_tick`，二者等价。"""
        return self.get_full_tick(code_list, timeout_seconds=timeout_seconds,
                                  types=types)

    def get_last_close(self, stock):
        """昨收价。实测 600519.SH -> 1309.3，与 get_instrument_detail 的
        PreClose 一致。"""
        return self._call("get_last_close", stock=stock)

    def get_last_volume(self, stock):
        """最新**流通股本**，不是「昨天的成交量」。

        官方释义就是「获取最新流通股本」，名字具有误导性。实测
        600519.SH -> 1250081601.0、601398.SH -> 269612212539.0，与
        get_instrument_detail 的 FloatVolume 逐位相同；同一天 601398.SH 的
        成交量是 2154432 手，差了五个数量级。要成交量请读 get_ticks()。
        """
        return self._call("get_last_volume", stock=stock)

    def get_open_date(self, stock):
        """上市日期，int yyyymmdd。

        实测 600519.SH -> 20010827、510300.SH -> 20120528，与
        get_instrument_detail 的 OpenDate 一致。
        """
        return self._call("get_open_date", stock=stock)

    def get_contract_expire_date(self, stock):
        """到期日。**返回字符串**，不是 int。

        实测股票/ETF -> '99999999'（无到期日），终端里没有的合约 -> '0'。
        get_instrument_detail 的 ExpireDate 是同一个值的 int 版，需要数字
        比较时用那个。
        """
        return self._call("get_contract_expire_date", stock=stock)

    def get_float_caps(self, stockcode):
        """流通**股本**（股数），不是流通市值。

        实测和 get_last_volume 逐位相同（601398.SH -> 269612212539，
        = get_instrument_detail 的 FloatVolume）。同一天该股昨收 7.94 元，
        流通市值应是 2 万亿量级 —— 按「市值」用会差一个价格的倍数。
        """
        return self._call("get_float_caps", stockcode=stockcode)

    def get_total_share(self, stockcode):
        """总股本。实测 601398.SH -> 356406257089，确实和流通股本
        （269612212539）不同，= get_instrument_detail 的 TotalVolume。"""
        return self._call("get_total_share", stockcode=stockcode)

    def get_weight_in_index(self, mtkindexcode, stockcode):
        """某只股票在某指数中的绝对权重，**单位是 %**。

        实测 ('000300.SH','600519.SH') -> 5.801、('000016.SH','600519.SH')
        -> 16.232、('000905.SH','600519.SH') -> 0.0（不是成分股）—— 会随
        指数和个股变化，不是常数。
        """
        return self._call("get_weight_in_index", mtkindexcode=mtkindexcode,
                          stockcode=stockcode)

    def get_risk_free_rate(self, index=-1):
        """无风险利率（官方说是十年期国债收益率 CGB10Y），单位 %。

        实测这台终端恒返回 3.5，`index`（K 线索引号）传 -1/0/1/100/5000
        都一样 —— 也就是说它给的是终端里的一个设置值，不是随 K 线走的
        CGB10Y 序列。拿来做期权定价的常数可以，当历史利率序列用不行。
        """
        return self._call("get_risk_free_rate", index=index)

    def get_svol(self, stock):
        """内盘成交量 —— **盘中窗口量，不是当日累计内盘**。

        和配对的 get_bvol 一起看才读得懂（2026-09-09 收盘后实测）：

        - 尾盘只剩 15:00 收盘集合竞价的代码上，`svol + bvol` **恰好等于最后
          一根 1 分钟 K 线的成交量**：601398.SH 32586+0、510300.SH 59408+0、
          000001.SZ 5177+0、511990.SH 0+22883，逐位等于
          get_market_data_ex(['volume'], period='1m') 的末根。集合竞价一个
          价位撮合、没有主动方，所以整根落进单侧、另一侧为 0 —— 落哪一侧
          不固定（511990.SH 落在外盘）。
        - 连续交易到 15:30 的逆回购上，两侧都非零，但 `svol + bvol` 既不是
          末根 1 分钟 K 线也不是当日成交量：204001.SH 43226876+1506858
          =44733734，末根 5565745，当日 2093850077 —— 大约是尾盘几分钟的量，
          **具体窗口没能定死**。

        所以：`svol + bvol ≠ 日成交量`，两个都不是当日内外盘。要当日口径请
        自己按 tick 或 K 线累计。
        """
        return self._call("get_svol", stock=stock)

    def get_bvol(self, stock):
        """外盘成交量 —— 语义同 get_svol，见那边的实测记录。

        它**不是**恒 0：实测 204001.SH -> 1506858、131810.SZ -> 2404676、
        511990.SH -> 22883。股票在收盘后答 0，是因为那时最后一根 K 线是
        15:00 集合竞价、整根都落进内盘，不是这个方法答不了。
        """
        return self._call("get_bvol", stock=stock)

    def get_turn_over_rate(self, stockcode):
        """换手率（单值版）—— 这台终端上答不了，直接报错。

        实测对 600519.SH / 000001.SZ / 510300.SH / 000300.SH / 601398.SH
        全部返回 None，换代码格式（600519 / SH600519）也一样，收盘后重测
        仍是 None（不是「非交易时段才空」）。区间版 get_turnover_rate 在同
        一次运行里返回空 DataFrame —— 而它按官方文档需要先下载财务数据
        （股本）与日线数据，本终端两样都没下过，所以没能区分「stub 坏了」
        和「缺基础数据」。
        """
        raise NotImplementedError(
            "get_turn_over_rate 在这台大 QMT 终端上不可用：服务端的 "
            "ContextInfo.get_turn_over_rate 桩对每个代码都返回 None "
            "（收盘后已对股票、ETF、指数和多种代码格式做过实盘验证）。同一次"
            "运行里区间版 get_turnover_rate 返回的是空 DataFrame，而它要求一个"
            "本终端未满足的前置条件：必须先下载财务数据（股本）和日线"
            "（download_financial_data / download_history_data）。如果你的终端"
            "有这些数据，可以显式调用 "
            "xtdata.call_method(\"get_turn_over_rate\", stockcode=...)。否则自行"
            "推导：get_ticks()[code]['pvolume'] / get_last_volume(code)——"
            "pvolume 的单位是股，与流通股本一致；而 ['volume'] 的单位是手，"
            "算出来会小 100 倍。"
        )

    # int32 最大值。合约乘数不可能是这个数，它是「没有值」的哨兵。
    CONTRACT_MULTIPLIER_SENTINEL = 2147483647

    def get_contract_multiplier(self, stockcode):
        """合约乘数。**答案是哨兵值时报错，不往外递。**

        实测这台终端对股票 / ETF / 期权 / 期货代码一律返回 2147483647
        （int32 上限，即「没有值」），而它自己也没有期货行情：
        get_instrument('IF2612.IF') / ('cu2610.SF') 都是 {}，
        get_his_contract_list('IF') 是 0 条。

        把 2147483647 当乘数用会把下单金额算错 20 亿倍，所以这里回读结果、
        对上哨兵就报错。有期货数据的终端能正常返回时照常放行。
        """
        answer = self._call("get_contract_multiplier", stockcode=stockcode)
        try:
            is_sentinel = int(answer) == self.CONTRACT_MULTIPLIER_SENTINEL
        except (TypeError, ValueError):
            is_sentinel = False
        if is_sentinel:
            raise NotImplementedError(
                "get_contract_multiplier(%r) 返回了 %s —— 这是本终端在没有该代码"
                "乘数时返回的 int32 哨兵值（已实盘验证：股票、ETF、期权、期货"
                "代码都这样，而同一台终端根本没有期货数据："
                "get_instrument('IF2612.IF') 是 {}，get_his_contract_list"
                "('IF') 为空）。把它当乘数用会让下单价错 2e9 倍，所以不返回它。"
                "请检查期货行情是否已订阅，或改读 "
                "get_instrument_detail(code)['VolumeMultiple']。"
                % (stockcode, self.CONTRACT_MULTIPLIER_SENTINEL))
        return answer

    def get_close_price(self, market, stock_code, real_timetag, period=86400000, divid_type=0):
        return self._call(
            "get_close_price",
            market=market,
            stock_code=stock_code,
            real_timetag=real_timetag,
            period=period,
            divid_type=divid_type,
        )

    def get_main_contract(self, code_market):
        return self._call("get_main_contract", code_market=code_market)

    def get_his_contract_list(self, market):
        return self._call("get_his_contract_list", market=market)

    def get_date_location(self, date):
        return self._call("get_date_location", date=date)

    def get_his_st_data(self, stock_code):
        return self._call("get_his_st_data", stock_code=stock_code)

    def get_his_index_data(self, stock_code):
        return self._call("get_his_index_data", stock_code=stock_code)

    def call_method(self, method, **params):
        """通用逃生口：按名字调用任何 RPC 行情方法。

        用于上面没有显式封装的 ContextInfo 方法（例如
        ``xtdata.call_method("get_last_close", stock="000001.SZ")``、
        ``xtdata.call_method("get_float_caps", stockcode="000001.SZ")``）。
        可调用方法的完整列表见 ``MARKET_DATA_METHODS``。
        """
        return self._call(method, **params)

    # ------------------------------------------------------------------
    # L2 行情（需 L2 权限 + 原生 xtdata SDK 行情服务）
    # ------------------------------------------------------------------

    def get_l2_quote(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_quote", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_order(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_order", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_transaction(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_transaction", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    # ------------------------------------------------------------------
    # 指数权重 / 交易日历 / 交易时段 / 可转债 / 品种判断
    # ------------------------------------------------------------------

    def get_index_weight(self, index_code):
        return self._call("get_index_weight", index_code=index_code)

    def get_trading_calendar(self, market, start_time="", end_time="", tradetimes=False):
        return self._call("get_trading_calendar", market=market, start_time=start_time,
                          end_time=end_time, tradetimes=tradetimes)

    def get_trade_times(self, stockcode):
        return self._call("get_trade_times", stockcode=stockcode)

    def get_cb_info(self, stockcode):
        return self._call("get_cb_info", stockcode=stockcode)

    def is_stock_type(self, stock, tag):
        return self._call("is_stock_type", stock=stock, tag=tag)

    # ------------------------------------------------------------------
    # 板块增删
    # ------------------------------------------------------------------

    def add_sector(self, sector_name, stock_list):
        return self._call("add_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def remove_sector(self, sector_name):
        return self._call("remove_sector", sector_name=sector_name)

    # ------------------------------------------------------------------
    # 时间戳转换（纯计算）
    # ------------------------------------------------------------------

    @staticmethod
    def datetime_to_timetag(datetime_str, format="%Y%m%d%H%M%S"):
        import datetime as _dt
        try:
            return int(_dt.datetime.strptime(str(datetime_str), format).timestamp() * 1000)
        except Exception:
            return 0

    @staticmethod
    def timetag_to_datetime(timetag, format):
        import datetime as _dt
        try:
            return _dt.datetime.fromtimestamp(int(timetag) / 1000.0).strftime(format)
        except Exception:
            return ""

    @staticmethod
    def timetagToDateTime(timetag, format):
        return BigQmtXtData.timetag_to_datetime(timetag, format)


class BigQmtXtTrader:
    def __init__(
        self,
        path=None,
        session_id=None,
        account_id=None,
        valkey_client=None,
        valkey_config=None,
        timeout_seconds=None,
    ):
        self.path = path
        self.session_id = session_id
        self.client = BigQmtRpcClient(
            account_id=account_id,
            valkey_client=valkey_client,
            valkey_config=valkey_config,
            timeout_seconds=timeout_seconds,
        )
        self.callback = None
        self._event_thread = None
        self._event_running = False
        # 异步下单提交（问题 #50）。一个工作线程，首次使用时启动，因此从不调用
        # order_stock_async 的客户端不付任何代价。
        self._async_order_queue = _queue.Queue()
        self._async_order_thread = None
        self._async_order_lock = threading.Lock()
        # 结果（回复/错误 + #51 的屏障释放）在这个第二线程上触发，所以慢的用户
        # 回调——或等待迟到订单号的有限等待——永远不会挡住下一次提交
        # （问题 #181）。
        self._async_callback_queue = _queue.Queue()
        self._async_callback_thread = None
        # 异步撤单提交——与异步下单相同的“工作队列”模式。
        # cancel_order_stock_async 过去就地调用阻塞 RPC，使 15 次撤单耗时约
        # 30s。现在撤单在工作线程上运行，并把积压合并成一次 RPC。
        self._async_cancel_queue = _queue.Queue()
        self._async_cancel_thread = None
        # 复用 _async_order_lock 做启动串行化。
        # int -> 合同编号，用于以 OrderId 形式发出的 id（问题 #113）。
        self._order_sys_ids = _OrderedDict()
        # on_account_status 过去即使对两融部署也硬编码报 "STOCK"（问题 #103）。
        # 服务端才是权威——客户端的 StockAccount(..., "CREDIT") 从不上传——所以
        # 优先用 ping 报告的，其次用调用方声明的。
        self._server_account_type = ""
        self._server_account_types = []
        self._declared_account_type = ""
        # 执行事件监听器真正订阅成功后置位；start() 等它而不是盲睡。重连轮次
        # 从不清除它——start() 只等一次，重新订阅不会让它回到未启动状态。
        self._event_ready = threading.Event()
        try:
            self.event_listener_ready_timeout = float(
                os.environ.get("BIGQMT_EVENT_READY_TIMEOUT") or 1.0)
        except (TypeError, ValueError):
            self.event_listener_ready_timeout = 1.0
        # 账户查询缓存兜底（#243）。默认关闭：失败的 POSITION/ASSET 查询必须
        # 传到调用方，就像它在所有非 valkey 传输上已经做到的那样。改用最近的
        # valkey 快照作答，会把“查询失败”变成“这是你持仓”，而那是策略绝不能
        # 拿到的错误答案。
        #
        # 有意 **不** 绑定 local_cache_enabled：那个键是客户端的 *行情* 缓存。
        # 两个不同的缓存，两个开关——把它们混为一谈，正是
        # local_cache_enabled=False 看起来应该关掉这里却没关的原因。
        # 按交给我们的配置读：BigQmtRpcClient 会规范化自己的副本并丢掉不认识的
        # 键，所以这里不能用 self.client.valkey_config 作来源。
        cache_config = dict((load_client_config() or {}).get("valkey_config") or {})
        cache_config.update(dict(valkey_config or {}))
        self.account_cache_fallback = _bool_value(
            cache_config.get("account_cache_fallback"),
            _env_bool("BIGQMT_ACCOUNT_CACHE_FALLBACK", False),
        )
        try:
            self.account_cache_max_age_seconds = float(
                cache_config.get("account_cache_max_age_seconds")
                or os.environ.get("BIGQMT_ACCOUNT_CACHE_MAX_AGE") or 30.0)
        except (TypeError, ValueError):
            self.account_cache_max_age_seconds = 30.0

    def _snapshot_age_seconds(self, snapshot):
        """缓存快照有多旧；快照没说时返回 None。

        不带 ``updated_at`` 的快照按不可用处理，而不是当作新鲜：设这个上下限的
        全部意义，就是拒绝用无法确定时间的“事实”作答。
        """
        stamp = (snapshot or {}).get("updated_at")
        if not stamp:
            return None
        text = str(stamp).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                parsed = _dt.datetime.strptime(text.split("+")[0].strip(), fmt)
            except ValueError:
                continue
            return max(0.0, (_dt.datetime.now() - parsed).total_seconds())
        try:  # Unix 秒
            return max(0.0, time.time() - float(text))
        except (TypeError, ValueError):
            return None

    def _account_cache_usable(self, account_id, what):
        """到底能否用缓存快照回答 `what`？

        当兜底开关开启、数据带时间、且未超过时效上限时返回该快照。其它情况
        返回 None，由调用方重新抛出原始错误。
        """
        if not self.account_cache_fallback:
            return None
        if not self._valkey_cache_enabled():
            return None
        snapshot = self._cached_position_snapshot(account_id)
        if not snapshot:
            return None
        age = self._snapshot_age_seconds(snapshot)
        if age is None:
            log.warning(
                "%s: 拒绝用缓存回答 —— 快照没有 updated_at，无法判断新旧", what)
            return None
        if age > self.account_cache_max_age_seconds:
            log.warning(
                "%s: 拒绝用缓存回答 —— 快照已 %.1fs 前（上限 %.1fs）",
                what, age, self.account_cache_max_age_seconds)
            return None
        log.warning(
            "%s: 原生查询失败，改用 %.1fs 前的 valkey 缓存作答（"
            "account_cache_fallback=True 打开的行为）", what, age)
        return snapshot

    def _cached_position_snapshot(self, account_id):
        key = "bigqmt:positions:%s" % str(account_id or self.client.account_id or "")
        try:
            raw = self.client._valkey().get(key)
        except Exception:
            return {}
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
        except Exception:
            return {}

    def _cached_positions(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        if isinstance(positions, dict):
            return positions
        if isinstance(positions, list):
            return {str(item.get("stock_code") or idx): item for idx, item in enumerate(positions)}
        return {}

    def _cached_asset(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        asset = snapshot.get("asset") if isinstance(snapshot, dict) else None
        return asset if isinstance(asset, dict) else {}

    def _valkey_cache_enabled(self):
        return str(getattr(self.client, "transport_name", "valkey") or "valkey").lower() in (
            "valkey",
            "",
            "default",
        )

    def register_callback(self, callback):
        self.callback = callback
        return 0

    def start(self):
        # 启动实时执行事件监听器，使注册过的回调的 on_stock_order /
        # on_stock_trade 在大 QMT 推送时立刻触发。
        self._start_event_listener()
        return 0

    def connect(self):
        try:
            if self.client.account_id:
                pong = self.client.call("ping")
                self._note_server_account_type(pong)
                mismatch = warn_on_version_mismatch(pong)
                if mismatch and auto_sync_enabled():
                    self.sync_deployment()
            self._fire_account_status()
            return 0
        except Exception:
            # connect 失败必须拆掉 start() 拉起的事件监听线程。否则它持有的
            # Valkey pubsub 订阅(每实例 4 个 exec 事件频道)随丢弃的实例永久
            # 留在服务端: 调用方重连风暴每次重试泄漏一个, 实测单日 8000+ 个
            # subscribe 连接, 逼近 maxclients 后整个 Valkey 拒绝新连接。
            try:
                self.stop()
            except Exception:
                log.exception("连接失败，且事件监听器清理失败")
            raise

    def sync_deployment(self, dry_run=False):
        """把本客户端的包推入 QMT 的 python 目录。

        拷贝在这里执行，不在 QMT 内部：交易进程在盘中重写自己的代码，会把
        源码树里当时的内容（包括改到一半的编辑）直接搬上实盘终端。

        绝不写配置文件。之后策略仍须重启——QMT 在重跑之间把模块留在
        sys.modules 里，所以只拷贝本身不改变任何东西。
        """
        from .sync import sync_deployment as _sync

        # get_deployment_info 在 BigQmtXtData 上；本类从来没有它，所以
        # xt_trader.sync_deployment() 死于 AttributeError。直接调用 RPC——
        # 交易端用的客户端是同一个。
        info = self.client.call("get_deployment_info", {}) or {}
        target = info.get("qmt_python_dir") or ""
        if not target:
            log.warning("sync_deployment：桥接端未返回 qmt_python_dir"
                        "（服务端版本可能过旧？），未复制任何文件")
            return {"updated": [], "error": "no qmt_python_dir reported"}

        result = _sync(target, dry_run=dry_run)
        if result.get("error"):
            log.warning("sync_deployment：%s", result["error"])
        elif result["updated"]:
            log.warning(
                "sync_deployment：%d 个文件已%s，目录=%s。"
                "请重启策略；QMT 在重复运行时会保留已加载模块，"
                "重启前本次更新不会生效。配置文件未改动：%s",
                len(result["updated"]),
                "准备更新" if dry_run else "更新",
                target, ", ".join(result["skipped_config"]) or "无")
        else:
            log.info("sync_deployment：已是最新版本（%d 个文件内容相同）",
                     result["identical"])
        return result

    def subscribe(self, account):
        declared = _account_type_name(getattr(account, "account_type", None))
        if declared:
            self._declared_account_type = declared
            self._warn_on_account_type_mismatch()
        if not self.client.account_id:
            self.client.account_id = _account_id(account)
        # 既然账户已知，就（重新）启动监听器；账户变化时循环在约 1s 内重新
        # 订阅该账户的频道。
        self._start_event_listener()
        self._fire_account_status()
        # 等执行事件监听器真正订阅上，而不是固定睡一秒然后祈祷（#247 当时
        # 在这里发了 time.sleep(1) 并标注“原因不明”）。竞态是真实的：
        # _start_event_listener 启动一个守护线程就返回，所以在 subscribe() 与
        # 那个线程内部 pubsub.subscribe() 之间发布的事件会丢——订阅后立刻下单的
        # 调用方可能错过自己的成交回调。start() 形状相同，但 MiniQMT 调用方是
        # 通过 subscribe() 拿到事件的，而睡眠就在那里。
        # 上限与原睡眠的 1s 相同，所以最坏情况不变差，而常规情况（几毫秒就订阅
        # 上）不再付这个成本。BIGQMT_EVENT_READY_TIMEOUT=0 关闭该等待。
        self._await_event_listener()
        return 0

    def _await_event_listener(self):
        timeout = self.event_listener_ready_timeout
        if timeout <= 0:
            return False
        return self._event_ready.wait(timeout)

    def stop(self):
        # 先排空：已排队的委托必须在拆除之前发出去。队列为空时无成本
        # （问题 #156）。
        self._drain_async_orders_on_exit()
        self._event_running = False
        thread = self._event_thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        self._event_thread = None
        # 交易端在 __init__ 里构造了自己的客户端，所以它同时拥有 transport，
        # 由它释放 transport 连接。
        try:
            self.client.close()
        except Exception:
            pass
        return 0

    def _start_event_listener(self):
        if self._event_thread is not None and self._event_thread.is_alive():
            return
        self._event_running = True
        self._event_thread = threading.Thread(
            target=self._event_loop, name="bigqmt-exec-events", daemon=True
        )
        self._event_thread.start()

    def _note_server_account_type(self, pong):
        """记住部署声明的交易账户类型。"""
        try:
            reported = str((pong or {}).get("account_type") or "").strip().upper()
            listed = [str(t or "").strip().upper()
                      for t in ((pong or {}).get("account_types") or [])]
        except Exception:
            return
        if reported:
            self._server_account_type = reported
            self._server_account_types = [t for t in listed if t]
            self._warn_on_account_type_mismatch()

    def _warn_on_account_type_mismatch(self):
        """调用方与部署不一致时说出来。

        客户端要 CREDIT 而部署是 STOCK 时，会拿到全零的资产行且完全没有错误
        ——那就是问题 #92，它让报告者花了很久，因为没有任何地方说过两者不一致。
        """
        server = self._server_account_type
        declared = self._declared_account_type
        if not server or not declared or server == declared:
            return
        # 配置了多个类型的部署（股票账户上的港股通）按 StockAccount 声明的
        # 类型回答每个请求。
        if declared in (getattr(self, "_server_account_types", None) or []):
            return
        log.warning(
            "account_type 不匹配：客户端请求 %s，但 QMT 端部署配置为 %s。"
            "客户端 StockAccount 的类型不会传到服务端；请在 QMT 端本地配置中"
            "设置 BIGQMT_ACCOUNT_TYPE = %r，然后重启策略。"
            "在此之前查询会按 %s 应答（信用账户按 STOCK 读取时，"
            "资产行会全部返回 0）。",
            declared, server, declared, server)

    def _fire_account_status(self):
        """连接/订阅之后触发 on_account_status（与 MiniQMT 对齐）。

        大 QMT 没有按策略的账户状态推送；RPC 链路连通后我们合成一次 CONNECTED
        状态，使那些先等 on_account_status 再交易的客户端代码继续可用。
        """
        callback = self.callback
        if callback is None:
            return
        try:
            callback.on_account_status(
                CompatObject(
                    account_id=str(self.client.account_id or ""),
                    account_type=(self._server_account_type
                                  or self._declared_account_type or "STOCK"),
                    status=1,  # ACCOUNT_STATUS_ONLINE（MiniQMT 的 XtAccountStatus）
                )
            )
        except Exception:
            log.exception("用户回调执行失败：on_account_status")

    def _event_loop_poll(self):
        """一轮轮询：通过对比查询结果合成执行事件（#372）。

        没有推送通道、valkey 也不可达时，查询 RPC 仍然通——按
        「服务端给副账号做的 #320 轮询」同一个模式，在客户端按 sysid+status
        diff，合成与推送完全同形的 on_stock_order / on_stock_trade /
        on_order_error。首轮只建状态不补发（已有委托不是「新事件」）；回调
        延迟 = 轮询间隔（BIGQMT_EXEC_POLL_SECONDS，默认 1s）。查询连续失败
        （比如 valkey 传输的 valkey 宕了）时退回外层重新选通道。
        """
        from .exec_events import normalize_order_error_event

        interval = _env_float("BIGQMT_EXEC_POLL_SECONDS", 1.0)
        account_id = str(self.client.account_id or "")
        order_state = None    # sysid -> status；None = 还没打首轮底
        seen_trades = set()
        failures = 0
        while self._event_running:
            if str(self.client.account_id or "") != account_id:
                return       # 账户变了 -> 外层循环重新选通道
            try:
                orders = self.query_stock_orders(account_id) or []
                trades = self.query_stock_trades(account_id) or []
            except Exception as exc:
                failures += 1
                if failures == 1 or failures % 20 == 0:
                    log.warning("成交回报轮询本轮失败，已连续 %d 次：%s：%s",
                                failures, exc.__class__.__name__, exc)
                if failures >= 5:
                    return   # 通道可能整体坏了（比如 valkey 宕了）-> 外层重选
                time.sleep(interval)
                continue
            failures = 0
            if order_state is None:
                # 首轮打底，不补发既有委托/成交。
                order_state = {}
                for order in orders:
                    sysid = str(getattr(order, "order_sysid", "") or "")
                    if sysid:
                        order_state[sysid] = str(getattr(order, "order_status", "") or "")
                for trade in trades:
                    tid = str(getattr(trade, "trade_id", "") or "")
                    if tid:
                        seen_trades.add(tid)
                self._event_ready.set()      # 见 valkey 路径（#247）
                time.sleep(interval)
                continue
            for order in orders:
                sysid = str(getattr(order, "order_sysid", "") or "")
                if not sysid:
                    continue
                status = str(getattr(order, "order_status", "") or "")
                prev = order_state.get(sysid)
                order_state[sysid] = status
                if prev == status:
                    continue
                event = {
                    "event_type": "order",
                    "account_id": account_id,
                    "order_sys_id": sysid,
                    "stock_code": str(getattr(order, "stock_code", "") or ""),
                    "action": _action_of_row(order),
                    "status": status,
                    "price": getattr(order, "price", 0.0),
                    "volume": getattr(order, "order_volume", getattr(order, "volume", 0)),
                    "traded_volume": getattr(order, "traded_volume", 0),
                    "order_remark": str(getattr(order, "order_remark", "") or ""),
                    "strategy_name": str(getattr(order, "strategy_name", "") or ""),
                    # 过 _order_from_dict 重算 order_type 需要的原生字段：
                    # 信用判据（entrust/op/offset）一并带，和推送同形。
                    "offset_flag": getattr(order, "offset_flag", None),
                    "direction": getattr(order, "direction", None),
                    "op_type": getattr(order, "op_type", None),
                    "entrust_type": getattr(order, "entrust_type", None),
                    "opt_name": getattr(order, "opt_name", ""),
                    "source": "poll",
                    "created_at_ts": time.time(),
                }
                try:
                    self._dispatch_event(event)
                except Exception:
                    pass
                if status == "57":
                    # 废单/拒单同推 order_error（normalize 读的就是
                    # m_strCancelInfo / status_msg 这一族字段）。
                    try:
                        self._dispatch_event(normalize_order_error_event(order, account_id))
                    except Exception:
                        pass
            for trade in trades:
                tid = str(getattr(trade, "trade_id", "") or "")
                if not tid or tid in seen_trades:
                    continue
                seen_trades.add(tid)
                event = {
                    "event_type": "trade",
                    "account_id": account_id,
                    "trade_id": tid,
                    "order_sys_id": str(getattr(trade, "order_sysid", "") or ""),
                    "stock_code": str(getattr(trade, "stock_code", "") or ""),
                    "action": _action_of_row(trade),
                    "volume": getattr(trade, "traded_volume", getattr(trade, "volume", 0)),
                    "price": getattr(trade, "traded_price", getattr(trade, "price", 0.0)),
                    "offset_flag": getattr(trade, "offset_flag", None),
                    "direction": getattr(trade, "direction", None),
                    "op_type": getattr(trade, "op_type", None),
                    "entrust_type": getattr(trade, "entrust_type", None),
                    "opt_name": getattr(trade, "opt_name", ""),
                    "source": "poll",
                    "created_at_ts": time.time(),
                }
                try:
                    self._dispatch_event(event)
                except Exception:
                    pass
            time.sleep(interval)

    def _on_push_exec_event(self, topic, data):
        """推送通道回调。载荷已经是解码后的 dict，不像 Valkey 路径交来的是原始
        bytes。"""
        try:
            self._dispatch_event(data)
        except Exception:
            pass

    def _event_loop(self):
        """接收执行事件，镜像服务端的 sink 选择。

        服务端只要能构造出 Valkey 客户端就 **优先** 发布到 Valkey（它的频道带
        stream，可做短时回放），即使 RPC 传输是 zmq；只有在 Valkey 发布反复
        失败之后才落到行情推送通道（策略 _exec_event_sink，问题 #145）。这个
        循环过去却按传输来选——zmq 就只走推送通道——于是一个 Valkey 可用的 zmq
        部署把每个委托/成交事件都发布到 Valkey，而客户端在推送通道上收听：回调
        从不触发（问题 #144；2026-09-02 复现，当天的事件积在 Valkey stream 里，
        而 zmq 传输的监听器什么也没看到）。

        每轮重连都重新选择，使客户端能跟随一个在盘中降级 Valkey 的服务端（它
        的 Valkey 发布失败，通常也意味着我们的 Valkey 读取会失败）。
        """
        while self._event_running:
            valkey_client = self._exec_events_valkey_or_none()
            if valkey_client is not None:
                self._event_loop_valkey(valkey_client)
            else:
                # mysql 没有推送通道（valkey 传输的 valkey 宕了也是）——
                # 查询 RPC 还通，就轮询合成回调（#372）；查询也连续失败时
                # 它会自己退回来，外层重新探 valkey。
                self._event_loop_poll()

    def _exec_events_valkey_or_none(self):
        """执行事件频道可用的 Valkey 客户端，或 None。

        _valkey() 只构造客户端对象；连接是惰性的，所以不可达的服务端也会返回
        一个。要 ping 它——频道选择必须反映可达性，而不是配置。

        非 valkey 传输且没有显式 valkey 配置时整步跳过（不建 client、不 ping），
        避免去连未配置的 Valkey。
        """
        transport_name = str(getattr(self.client, "transport_name", "valkey") or "valkey").lower()
        if (transport_name not in ("valkey", "", "default")
                and not getattr(self.client, "_valkey_explicit", True)):
            return None
        try:
            client = self.client._valkey()
            if client is None:
                return None
            client.ping()
            return client
        except Exception:
            return None

    def _event_loop_valkey(self, valkey_client):
        """一轮 Valkey：订阅按账户划分的频道，直到账户变化或连接断开，然后
        返回以便重新选择。"""
        from .exec_events import (
            order_channel,
            trade_channel,
            order_error_channel,
            cancel_error_channel,
        )

        account_id = str(self.client.account_id or "")
        pubsub = None
        try:
            pubsub = valkey_client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(
                order_channel(account_id),
                trade_channel(account_id),
                order_error_channel(account_id),
                cancel_error_channel(account_id),
            )
            # 真正订阅上了——释放 start() 的有限等待（#247）。
            self._event_ready.set()
            while self._event_running:
                if str(self.client.account_id or "") != account_id:
                    return  # 账户变了 -> 重连并重新订阅
                message = pubsub.get_message(timeout=1.0)
                if not message or message.get("type") != "message":
                    continue
                self._dispatch_event(message.get("data"))
        except Exception:
            time.sleep(1.0)
        finally:
            try:
                if pubsub is not None:
                    pubsub.close()
            except Exception:
                pass

    def _dispatch_event(self, raw):
        """接受原始 bytes/str（Valkey 发布/订阅）或已解码的 dict。

        推送通道自己完成 msgpack/json 解码，所以交过来的是 dict——str(dict)
        不是合法 JSON，会在这里被静默丢弃。
        """
        callback = self.callback
        if callback is None:
            return
        if isinstance(raw, dict):
            event = raw
        else:
            try:
                text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
                event = json.loads(text)
            except Exception:
                return
        if not isinstance(event, dict):
            return
        # 放行超时的屏障, 再决定这条事件是直通还是暂存 (issue #51)。
        try:
            self._sweep_order_barriers()
            if event.get("event_type") in ("order", "trade", "order_error", "cancel_error") and self._hold_if_pending(event):
                return
        except Exception:
            pass  # 屏障故障绝不能吞掉事件
        self._deliver_event(event)

    def _deliver_event(self, event):
        callback = self.callback
        if callback is None:
            return
        account_id = str(event.get("account_id") or self.client.account_id or "")
        try:
            event_type = event.get("event_type")
            if event_type == "trade":
                callback.on_stock_trade(self._trade_from_dict(account_id, event))
            elif event_type == "order":
                callback.on_stock_order(self._order_from_dict(account_id, event))
            elif event_type == "order_error":
                _sysid = str(event.get("order_sys_id") or "")
                callback.on_order_error(
                    CompatObject(
                        # xttype.XtOrderError 同时点名两者，而 account_id 在本
                        # 方法开头已经解析过了。
                        account_id=account_id,
                        account_type=self._account_type_value(event),
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sysid=_sysid,       # MiniQMT 规范名 (issue #65)
                        order_sys_id=_sysid,      # 兼容别名
                        # order_id 必须和 on_stock_order/order_stock 同型
                        # (OrderId int 子类)，否则调用方拿到的废单号对不上
                        # 下单返回值，也没法拿它撤单 (#363)。
                        order_id=self._order_object_id(_sysid),
                        stock_code=event.get("stock_code") or "",
                        order_remark=str(
                            event.get("order_remark") or event.get("remark")
                            or event.get("user_order_id") or ""
                        ),
                        strategy_name=str(event.get("strategy_name") or ""),
                        status=_safe_int(event.get("status", event.get("order_status")), 0),
                    )
                )
            elif event_type == "cancel_error":
                _sysid = str(event.get("order_sys_id") or "")
                callback.on_cancel_error(
                    CompatObject(
                        account_id=account_id,
                        account_type=self._account_type_value(event),
                        market=_market_of(event.get("stock_code")),
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sysid=_sysid,       # MiniQMT 规范名 (issue #65)
                        order_sys_id=_sysid,
                        order_id=self._order_object_id(_sysid),  # 同 #363
                        stock_code=event.get("stock_code") or "",
                        order_remark=str(
                            event.get("order_remark") or event.get("remark")
                            or event.get("user_order_id") or ""
                        ),
                    )
                )
        except Exception:
            # 业务回调异常不能打崩事件线程，但必须留痕（issue: 静默吞错）。
            log.exception(
                "用户回调执行失败 event_type=%s account=%s",
                event.get("event_type"),
                account_id,
            )

    def run_forever(self):
        while True:
            time.sleep(3600)

    def query_stock_asset(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_asset", _with_account_type({"account_id": account_id}, account),
                                    account_id=account_id) or {}
        except Exception:
            # #243：默认让失败透出。只有显式开启 account_cache_fallback，并且
            # 快照带时间且足够新时，才从缓存作答——届时日志里会说明这一点。
            if self._account_cache_usable(account_id, "query_stock_asset") is None:
                raise
            data = self._cached_asset(account_id)
            if not data:
                raise
        if (
            data.get("cash") is None
            and data.get("total_asset") is None
            and self._account_cache_usable(account_id, "query_stock_asset(empty)") is not None
        ):
            data = self._cached_asset(account_id) or data
        fetch_balance = _safe_float(data.get("fetch_balance"), None)
        cash = data.get("cash")
        total_asset = data.get("total_asset")
        frozen_cash = data.get("frozen_cash")
        market_value = data.get("market_value")
        if market_value is None and cash is not None and total_asset is not None:
            # total_asset = cash(可用) + frozen_cash + market_value。旧服务端
            # 既不发 frozen_cash 也不发 market_value；不扣 frozen_cash 去推导会
            # 把市值高估一个冻结金额，所以只要服务端给了就减掉它。
            market_value = _safe_float(total_asset) - _safe_float(cash)
            if frozen_cash is not None:
                market_value -= _safe_float(frozen_cash)
        return CompatObject(
            account_id=account_id,
            # xttype.XtAsset 携带它。#133 给 order/trade/position 加了
            # account_type；资产对象被漏掉了。
            account_type=self._account_type_value(),
            cash=_safe_float(cash, 0.0) if cash is not None else None,
            available_cash=_safe_float(cash, 0.0) if cash is not None else None,
            fetch_balance=fetch_balance,
            m_dFetchBalance=fetch_balance,
            # MiniQMT 的 XtAsset 总是暴露 frozen_cash，所以默认 0.0 而不是
            # None：调用方会拿它做算术。
            frozen_cash=_safe_float(frozen_cash, 0.0) if frozen_cash is not None else 0.0,
            total_asset=_safe_float(total_asset, 0.0) if total_asset is not None else None,
            market_value=_safe_float(market_value, 0.0) if market_value is not None else 0.0,
            # ===== 原生 xtquant 字段名别名（兼容 m_ 前缀访问）=====
            m_strAccountID=account_id,
            m_nAccountType=self._account_type_value(),
            m_dCash=_safe_float(cash, 0.0) if cash is not None else None,
            m_dAvailableCash=_safe_float(cash, 0.0) if cash is not None else None,
            m_dFrozenCash=_safe_float(frozen_cash, 0.0) if frozen_cash is not None else 0.0,
            m_dTotalAsset=_safe_float(total_asset, 0.0) if total_asset is not None else None,
            m_dMarketValue=_safe_float(market_value, 0.0) if market_value is not None else 0.0,
        )

    def query_secu_account(self, account):
        """query_stock_asset 的别名（#406）。

        不在 xtquant 公开文档里，但光大 2.1.29 终端在下单校验与账户会话探测
        链上会按属性名回调 trader 对象的 query_secu_account；缺这个属性时
        AttributeError 直接把当前操作打断（实盘下单被迫人工兜底、清算后每
        5 分钟一条报错）。账户信息探针要的就是这份资产答案
        （cash / frozen / market_value / total_asset）。
        """
        return self.query_stock_asset(account)

    def query_secu_account_async(self, account, callback=None):
        return self._async_query(self.query_secu_account, account, callback)

    def _position_object(self, account_id, item):
        volume = _safe_int(item.get("volume"))
        available = _safe_int(item.get("available", item.get("can_use_volume")))
        cost = _safe_float(item.get("cost", item.get("avg_price")))
        price = _safe_float(item.get("price", item.get("last_price")), cost)
        stock_code = str(item.get("stock_code") or "")
        stock_name = str(item.get("stock_name") or "")
        market_value = item.get("market_value")
        if market_value is None:
            market_value = price * volume
        return CompatObject(
            # 以前硬编码 2（SECURITY_ACCOUNT），信用账户上就是错的 —— 和 #103
            # 报的 on_account_status 同一类。现在跟服务端说的走。
            account_type=self._account_type_value(item),
            account_id=account_id,
            stock_code=stock_code,
            stock_name=stock_name,
            volume=volume,
            can_use_volume=available,
            enable_amount=available,
            available_amount=available,
            avg_price=cost,
            price=price,
            open_price=_safe_float(item.get("open_price"), cost),
            cost_price=cost,
            market_value=_safe_float(market_value, 0.0),
            frozen_volume=_safe_int(item.get("frozen_volume")),
            on_road_volume=_safe_int(item.get("on_road_volume")),
            yesterday_volume=_safe_int(item.get("yesterday_volume"), volume),
            direction=_safe_int(item.get("direction"), 48),
            # ===== 原生 xtquant 字段名别名（兼容 m_ 前缀访问）=====
            m_strAccountID=account_id,
            m_strStockCode=stock_code,
            m_strStockName=stock_name,
            m_nVolume=volume,
            m_nCanUseVolume=available,
            m_nCanUseVol=available,
            m_nEnableAmount=available,
            m_dOpenPrice=_safe_float(item.get("open_price"), cost),
            m_dAvgPrice=cost,
            m_dLastPrice=price,
            m_dMarketValue=_safe_float(market_value, 0.0),
            m_nFrozenVolume=_safe_int(item.get("frozen_volume")),
            m_nOnRoadVolume=_safe_int(item.get("on_road_volume")),
            m_nYesterdayVolume=_safe_int(item.get("yesterday_volume"), volume),
            m_nDirection=_safe_int(item.get("direction"), 48),
        )

    @staticmethod
    def _position_items(data):
        if isinstance(data, dict):
            return list(data.values())
        return _as_list(data)

    def query_stock_positions(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_positions", _with_account_type({"account_id": account_id}, account),
                                    account_id=account_id) or {}
        except Exception:
            if self._account_cache_usable(account_id, "query_stock_positions") is None:
                raise
            data = self._cached_positions(account_id)
            if not data:
                raise
        return [self._position_object(account_id, item) for item in self._position_items(data)]

    def query_position_statistics(self, account):
        """日内持仓统计（期货），对应 MiniQMT 的 ``query_position_statistics``。

        返回 :class:`XtPositionStatistics` 风格的 :class:`CompatObject` 列表。
        服务端通过 ``get_trade_detail_data(..., "POSITION_STATISTICS")`` 查询。
        """
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_position_statistics",
            _with_account_type({"account_id": account_id}, account),
            account_id=account_id,
        ) or {}
        return [self._position_statistics_object(account_id, item) for item in _as_list(data)]

    def _position_statistics_object(self, account_id, item):
        return CompatObject(
            account_id=account_id,
            exchange_id=str(item.get("exchange_id") or ""),
            exchange_name=str(item.get("exchange_name") or ""),
            product_id=str(item.get("product_id") or ""),
            instrument_id=str(item.get("instrument_id") or ""),
            instrument_name=str(item.get("instrument_name") or ""),
            stock_code=str(item.get("stock_code") or ""),
            direction=_safe_int(item.get("direction"), 0),
            hedge_flag=_safe_int(item.get("hedge_flag"), 0),
            position=_safe_int(item.get("position"), 0),
            yesterday_position=_safe_int(item.get("yesterday_position"), 0),
            today_position=_safe_int(item.get("today_position"), 0),
            can_close_vol=_safe_int(item.get("can_close_vol"), 0),
            position_cost=_safe_float(item.get("position_cost"), None),
            avg_price=_safe_float(item.get("avg_price"), None),
            position_profit=_safe_float(item.get("position_profit"), None),
            float_profit=_safe_float(item.get("float_profit"), None),
            open_price=_safe_float(item.get("open_price"), None),
            used_margin=_safe_float(item.get("used_margin"), None),
            used_commission=_safe_float(item.get("used_commission"), None),
            frozen_margin=_safe_float(item.get("frozen_margin"), None),
            frozen_commission=_safe_float(item.get("frozen_commission"), None),
            instrument_value=_safe_float(item.get("instrument_value"), None),
            open_times=_safe_int(item.get("open_times"), 0),
            open_volume=_safe_int(item.get("open_volume"), 0),
            cancel_times=_safe_int(item.get("cancel_times"), 0),
            last_price=_safe_float(item.get("last_price"), None),
            rise_ratio=_safe_float(item.get("rise_ratio"), None),
            product_name=str(item.get("product_name") or ""),
            royalty=_safe_float(item.get("royalty"), None),
            expire_date=str(item.get("expire_date") or ""),
            assest_weight=_safe_float(item.get("assest_weight"), None),
            increase_by_settlement=_safe_float(item.get("increase_by_settlement"), None),
            margin_ratio=_safe_float(item.get("margin_ratio"), None),
            float_profit_divide_by_used_margin=_safe_float(
                item.get("float_profit_divide_by_used_margin"), None
            ),
            float_profit_divide_by_balance=_safe_float(
                item.get("float_profit_divide_by_balance"), None
            ),
            today_profit_loss=_safe_float(item.get("today_profit_loss"), None),
            yesterday_init_position=_safe_int(item.get("yesterday_init_position"), 0),
            frozen_royalty=_safe_float(item.get("frozen_royalty"), None),
            today_close_profit_loss=_safe_float(item.get("today_close_profit_loss"), None),
            close_profit=_safe_float(item.get("close_profit"), None),
            ft_product_name=str(item.get("ft_product_name") or ""),
            open_cost=_safe_float(item.get("open_cost"), None),
            # ===== 原生 xtquant 字段名别名（m_ 前缀访问）=====
            m_strAccountID=account_id,
            m_strStockCode=str(item.get("stock_code") or ""),
            m_strExchangeID=str(item.get("exchange_id") or ""),
            m_strExchangeName=str(item.get("exchange_name") or ""),
            m_strProductID=str(item.get("product_id") or ""),
            m_strInstrumentID=str(item.get("instrument_id") or ""),
            m_strInstrumentName=str(item.get("instrument_name") or ""),
            m_nDirection=_safe_int(item.get("direction"), 0),
            m_nHedgeFlag=_safe_int(item.get("hedge_flag"), 0),
            m_nPosition=_safe_int(item.get("position"), 0),
            m_nYestodayPosition=_safe_int(item.get("yesterday_position"), 0),
            m_nTodayPosition=_safe_int(item.get("today_position"), 0),
            m_nCanCloseVol=_safe_int(item.get("can_close_vol"), 0),
            m_dPositionCost=_safe_float(item.get("position_cost"), None),
            m_dAvgPrice=_safe_float(item.get("avg_price"), None),
            m_dPositionProfit=_safe_float(item.get("position_profit"), None),
            m_dFloatProfit=_safe_float(item.get("float_profit"), None),
            m_dOpenPrice=_safe_float(item.get("open_price"), None),
            m_dUsedMargin=_safe_float(item.get("used_margin"), None),
            m_dUsedCommission=_safe_float(item.get("used_commission"), None),
            m_dFrozenMargin=_safe_float(item.get("frozen_margin"), None),
            m_dFrozenCommission=_safe_float(item.get("frozen_commission"), None),
            m_dInstrumentValue=_safe_float(item.get("instrument_value"), None),
            m_nOpenTimes=_safe_int(item.get("open_times"), 0),
            m_nOpenVolume=_safe_int(item.get("open_volume"), 0),
            m_nCancelTimes=_safe_int(item.get("cancel_times"), 0),
            m_dLastPrice=_safe_float(item.get("last_price"), None),
            m_dRiseRatio=_safe_float(item.get("rise_ratio"), None),
            m_strProductName=str(item.get("product_name") or ""),
            m_dRoyalty=_safe_float(item.get("royalty"), None),
            m_strExpireDate=str(item.get("expire_date") or ""),
            m_dAssestWeight=_safe_float(item.get("assest_weight"), None),
            m_dIncreaseBySettlement=_safe_float(item.get("increase_by_settlement"), None),
            m_dMarginRatio=_safe_float(item.get("margin_ratio"), None),
            m_dFloatProfitDivideByUsedMargin=_safe_float(
                item.get("float_profit_divide_by_used_margin"), None
            ),
            m_dFloatProfitDivideByBalance=_safe_float(
                item.get("float_profit_divide_by_balance"), None
            ),
            m_dTodayProfitLoss=_safe_float(item.get("today_profit_loss"), None),
            m_nYestodayInitPosition=_safe_int(item.get("yesterday_init_position"), 0),
            m_dFrozenRoyalty=_safe_float(item.get("frozen_royalty"), None),
            m_dTodayCloseProfitLoss=_safe_float(item.get("today_close_profit_loss"), None),
            m_dCloseProfit=_safe_float(item.get("close_profit"), None),
            m_strFtProductName=str(item.get("ft_product_name") or ""),
            m_dOpenCost=_safe_float(item.get("open_cost"), None),
        )

    def query_stock_position(self, account, stock_code):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "query_stock_position",
                _with_account_type({"account_id": account_id, "stock_code": stock_code}, account),
                account_id=account_id,
            )
        except Exception:
            if self._account_cache_usable(account_id, "query_stock_position") is None:
                raise
            normalized = str(stock_code or "").strip().upper()
            data = None
            for code, item in self._cached_positions(account_id).items():
                if str(code).upper() == normalized or str(code).split(".", 1)[0].upper() == normalized:
                    data = item
                    break
            if data is None:
                raise
        if not data:
            return None
        return [
            self._position_object(account_id, item)
            for item in [data]
        ][0]

    def query_stock_orders(self, account, cancelable_only=False, strategy_name=""):
        # strategy_name 默认 ""（返回全部）：与服务端一致，避免下单用的策略名
        # 与查询默认值不匹配导致委托查不到（strategy_name 陷阱）。
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_orders",
            _with_account_type({
                "account_id": account_id,
                "cancelable_only": bool(cancelable_only),
                "strategy_name": strategy_name,
            }, account),
            account_id=account_id,
        ) or []
        return [self._order_from_dict(account_id, item) for item in _as_list(data)]

    def query_stock_order(self, account, order_id):
        order_id = str(order_id or "")
        for order in self.query_stock_orders(account, cancelable_only=False):
            if str(order.order_id) == order_id or str(order.order_sysid) == order_id:
                return order
        return None

    def query_stock_trades(self, account, strategy_name=""):
        # 默认 "" = 查询账户全部成交 (与服务端 _handle_query_trades 一致)。
        # 旧默认 "bigqmt_signal_trader" 会过滤掉其他策略名的成交;
        # 按策略过滤时由调用方显式传入。
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_trades",
            _with_account_type({"account_id": account_id, "strategy_name": strategy_name}, account),
            account_id=account_id,
        ) or []
        return [self._trade_from_dict(account_id, item) for item in _as_list(data)]

    def describe_trade_detail_fields(self, account, detail_types=None):
        """QMT 自己的 ORDER / DEAL 行携带哪些属性。只有名字。

        调试辅助，不属于 MiniQMT：当某个字段返回为空时，它说明是终端没提供，
        还是桥接层没有转发。这两者从客户端看完全一样，而每次排查都要部署加重启
        （#113、#130、#133）。

            xt_trader.describe_trade_detail_fields(account)
            -> {'ORDER': {'rows': 15, 'attributes': [...], 'error': ''}, ...}
        """
        account_id = _account_id(account, self.client.account_id)
        params = _with_account_type({"account_id": account_id}, account)
        if detail_types:
            params["detail_types"] = list(detail_types)
        return self.client.call("describe_trade_detail_fields", params,
                                account_id=account_id) or {}

    def reload_deployment(self, reason="", account=None):
        """重新导入已部署的包并重跑 init，无需重启。

        重载 **排期** 完成就立即返回——它在下一个 adjust tick 执行，因为执行重载
        会让 RPC 服务停止应答本请求。结果请轮询 reload_status()（或
        get_deployment_info()）。

        会刷新 bigqmt_signal_trader/ 下的所有内容。它无法刷新
        bigqmt_signal_trader_strategy.py 或 BIGQMT_VALKEY_DRYRUN 入口——那些由
        QMT 执行，而模块无法重载自己正在其中运行的那一个。那边的改动仍需重启
        策略。
        """
        account_id = _account_id(account, self.client.account_id)
        return self.client.call("reload_deployment", {"reason": str(reason or "")},
                                account_id=account_id) or {}

    def reload_status(self, account=None):
        """最近一次 reload_deployment 的结果，或仍在等待中的部分。"""
        account_id = _account_id(account, self.client.account_id)
        return self.client.call("reload_status", {},
                                account_id=account_id) or {}

    def query_execution_snapshot(
        self,
        account,
        order_strategy_name="bigqmt_signal_trader",
        trade_strategy_name="",
    ):
        """用一次 RPC 往返查询委托与账户级成交。"""
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_execution_snapshot",
            _with_account_type({
                "account_id": account_id,
                "order_strategy_name": order_strategy_name,
                "trade_strategy_name": trade_strategy_name,
            }, account),
            account_id=account_id,
        ) or {}
        result = dict(data) if isinstance(data, dict) else {}
        result["orders"] = [
            self._order_from_dict(account_id, item)
            for item in _as_list(result.get("orders"))
        ]
        result["trades"] = [
            self._trade_from_dict(account_id, item)
            for item in _as_list(result.get("trades"))
        ]
        return result

    def order_stock(
        self,
        account,
        stock_code,
        order_type,
        order_volume,
        price_type,
        price,
        strategy_name,
        order_remark,
    ):
        data = self.order_stock_result(
            account, stock_code, order_type, order_volume, price_type,
            price, strategy_name, order_remark,
        )
        return self._order_id(data.get("order_sys_id"))

    def _order_id(self, order_sys_id):
        """MiniQMT 的返回约定：正整数，失败时 -1。

        大 QMT 只有券商的合同编号字符串，所以 OrderId 两者都带（问题 #113）。
        提交本身失败时服务端发来的是 **字符串** "-1"，而它过去会被原样返回
        ——为真、且永远不等于 -1，于是被拒的委托读成了成功。
        """
        text = str(order_sys_id or "").strip()
        if not text or text == "-1":
            return -1
        order_id = OrderId(text)
        self._remember_order_id(order_id)
        return order_id

    def _order_object_id(self, order_sys_id):
        """XtOrder / XtTrade 的 ``order_id``：int，空的保持为空。

        与 order_stock 的返回值不同，这里没有 -1——查询结果要么有 id，要么没有。
        """
        text = str(order_sys_id or "").strip()
        if not text:
            return OrderId("")
        order_id = OrderId(text)
        self._remember_order_id(order_id)
        return order_id

    def _remember_order_id(self, order_id):
        """保留 int -> 合同编号，使往返一趟之后撤单仍可用。

        把 id 存进 JSON 或数据库的调用方拿回来的是纯 int，丢了字符串那一半。
        有界：这是便利，不是账本。
        """
        sys_id = getattr(order_id, "order_sys_id", "")
        if not sys_id or str(int(order_id)) == sys_id:
            return                       # 无需记忆：两者一致
        table = self._order_sys_ids
        table[int(order_id)] = sys_id
        while len(table) > _ORDER_ID_MEMORY:
            table.popitem(last=False)

    def _resolve_order_sys_id(self, value):
        """调用方传给撤单的那个东西对应的券商字符串。"""
        carried = getattr(value, "order_sys_id", None)
        if carried:
            return str(carried)
        if isinstance(value, int) and not isinstance(value, bool):
            remembered = self._order_sys_ids.get(int(value))
            if remembered:
                return remembered
        return order_sys_id_of(value)

    def order_stock_result(
        self, account, stock_code, order_type, order_volume, price_type,
        price, strategy_name, order_remark, wait_settlement=True,
    ):
        """通过 RPC 提交一笔委托。

        ``wait_settlement=False`` 告诉服务端在 passorder 返回时就回复，而不是
        压着回复等 QMT 分配订单号。异步路径用它；订单号随后通过 order_callback
        到达（问题 #50）。
        """
        account_id = _account_id(account, self.client.account_id)
        user_order_id = str(order_remark or "").strip()
        if not user_order_id:
            user_order_id = "bqrpc:%s:%s" % (int(time.time() * 1000), uuid.uuid4().hex[:10])
        payload = _with_account_type({
            "account_id": account_id,
            "stock_code": stock_code,
            "order_type": order_type,
            "order_volume": order_volume,
            "price_type": price_type,
            "price": price,
            "strategy_name": strategy_name,
            "order_remark": user_order_id,
        }, account)
        if not wait_settlement:
            payload["wait_settlement"] = False
        tracked = getattr(self.client, "call_tracked", None)
        if tracked is None:
            # 无法自报请求的客户端（测试替身）：#303 之前的契约，超时确实未知。
            try:
                return self.client.call("order_stock", payload, account_id=account_id) or {}
            except TimeoutError as exc:
                raise TimeoutError(
                    "order_stock RPC 超时；user_order_id=%s。重试前请先查询委托/成交，避免重复下单。%s"
                    % (user_order_id, exc)
                )
        request_id = uuid.uuid4().hex
        try:
            return tracked("order_stock", payload, account_id=account_id,
                           request_id=request_id) or {}
        except TimeoutError as exc:
            return self._order_stock_after_timeout(request_id, account_id, user_order_id, exc)

    # order_stock 超时之后：继续向服务端追问结果的时长与频率。覆盖服务端自己的
    # 结算等待（order_settle_timeout_seconds，3s）并留有余量。
    ORDER_TIMEOUT_FOLLOWUP_SECONDS = 5.0
    ORDER_TIMEOUT_FOLLOWUP_INTERVAL_SECONDS = 0.5

    def _order_stock_after_timeout(self, request_id, account_id, user_order_id, exc):
        """把“超时、状态未知”变成一个答案（#303）。

        突发时桥接层逐笔执行委托，所以一个请求可能超出调用方的耐心。服务端把每个
        委托请求的结果保留十分钟，``get_request_outcome`` 在监听线程上读取它——
        即使 adjust 线程忙也能到达。

            settled     我们错过的回复：当作它已到达一样返回
            refused     分派前已过期：**没有** 下单
            unknown     从未被取出：取出时会被拒绝（已过期），所以同样
                        **没有** 下单
            dispatched  passorder 已执行，订单号仍在查：再追问几秒，然后如实说明

        没有该方法（早于 #303）的服务端会让查询失败；那就落到旧消息，它对
        “不知道”是诚实的。
        """
        deadline = time.time() + float(self.ORDER_TIMEOUT_FOLLOWUP_SECONDS)
        last_state = None
        while True:
            try:
                outcome = self.client.call(
                    "get_request_outcome", {"request_id": request_id},
                    account_id=account_id, timeout_seconds=2.0) or {}
            except Exception:
                outcome = None
            if not isinstance(outcome, dict) or "state" not in outcome:
                break
            last_state = str(outcome.get("state") or "")
            if last_state == "settled":
                response = outcome.get("response") or {}
                if not response.get("ok"):
                    raise RpcServerRepliedError(
                        response.get("error") or "大 QMT RPC 调用失败: order_stock")
                server_error = str(response.get("server_error") or "")
                if server_error:
                    raise RpcServerRepliedError(
                        "大 QMT order_stock server_error: %s" % server_error)
                return _restore_jsonable(response.get("data")) or {}
            if last_state in ("refused", "unknown"):
                raise TimeoutError(
                    "order_stock RPC 超时；user_order_id=%s。桥接层未下出"
                    "这笔委托（分派前%s）：可以安全重试，最好降低并发或加长超时"
                    "——委托在 QMT 策略线程上逐笔执行。%s"
                    % (user_order_id,
                       "已过期被拒" if last_state == "refused" else "从未被取出",
                       exc))
            if time.time() >= deadline:
                break
            time.sleep(self.ORDER_TIMEOUT_FOLLOWUP_INTERVAL_SECONDS)
        if last_state in ("dispatched", "dispatching"):
            raise TimeoutError(
                "order_stock RPC 超时；user_order_id=%s。这笔委托的 passorder "
                "已执行，但我们停止等待时 QMT 还没分配订单号：它是活的。"
                "重试前请按备注找到它，否则重试就是重复下单。%s"
                % (user_order_id, exc))
        raise TimeoutError(
            "order_stock RPC 超时；user_order_id=%s。重试前请先查询委托/成交，避免重复下单。%s"
            % (user_order_id, exc)
        )

    def _async_order_worker(self):
        """排空已排队的异步委托；自身从不触发用户回调。

        单个工作线程而不是线程池：服务端本来就在 QMT 的 adjust 线程上串行处理
        委托 RPC，这里的并发收益很小，而串行化能让结果单元按提交顺序入队。
        回调在回调工作线程上运行，所以慢的用户回调——或等待迟到订单号的有限
        等待——永远不会挡住下一次提交（问题 #181）。单笔委托时可省下回调的
        成本；有积压时下面的批量路径还能省下往返。
        """
        while True:
            job = self._async_order_queue.get()
            if job is None:          # 关闭哨兵
                self._async_order_queue.task_done()
                return
            jobs = [job]
            stopping = False
            # 把已经在等的都取走。N 笔委托一次 RPC 而不是 N 次：否则积压会在
            # 往返上串行化（问题 #181）。这里不做任何等待——队列里只有一个任务
            # 时仍走单笔路径。
            while len(jobs) < self.ASYNC_BATCH_MAX:
                try:
                    extra = self._async_order_queue.get_nowait()
                except _queue.Empty:
                    break
                if extra is None:
                    stopping = True      # 本批之后再停，而不是改为现在停
                    self._async_order_queue.task_done()
                    break
                jobs.append(extra)
            try:
                self._submit_async_jobs(jobs)
            except Exception:
                # 工作线程一死，之后所有异步委托都会跟着丢。
                log.exception("异步委托提交失败 jobs=%d", len(jobs))
            finally:
                for _ in jobs:
                    self._async_order_queue.task_done()
            if stopping:
                return

    def _async_callback_worker(self):
        """按入队（= 提交）顺序串行触发结果单元。

        用户回调在这里运行，而不是在提交工作线程上。#51 的屏障释放在同一个单元
        里随行，所以顺序契约——一笔委托的 async_response 先于它的 order/trade
        事件——与全部在单线程上运行时完全一致。
        """
        while True:
            unit = self._async_callback_queue.get()
            try:
                if unit is None:      # 关闭哨兵
                    return
                self._fire_async_outcome(unit)
            except Exception:
                # 分发线程若死掉，之后每个回调和每个已布防的屏障都会被搁浅，
                # 所以它绝不能死。
                log.exception("异步回调分发失败 seq=%s",
                              unit.get("seq") if isinstance(unit, dict) else "?")
            finally:
                self._async_callback_queue.task_done()

    def _ensure_async_order_worker(self):
        with self._async_order_lock:
            if self._async_callback_thread is None or not self._async_callback_thread.is_alive():
                self._async_callback_thread = threading.Thread(
                    target=self._async_callback_worker,
                    name="bigqmt-async-callback", daemon=True,
                )
                self._async_callback_thread.start()
            if self._async_order_thread is not None and self._async_order_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._async_order_worker, name="bigqmt-async-order", daemon=True
            )
            self._async_order_thread = thread
            thread.start()

    def _register_exit_drain(self):
        """atexit 钩子，只注册一次：一个入队后立刻退出的“发了就不管”脚本，
        不能静默丢掉已排队的委托（问题 #156）。"""
        with self._async_order_lock:
            if getattr(self, "_exit_drain_registered", False):
                return
            self._exit_drain_registered = True
        import atexit
        atexit.register(self._drain_async_orders_on_exit)

    def _drain_async_orders_on_exit(self):
        """在 stop()/进程退出时尽力冲刷已排队的异步委托。

        异步工作线程是守护线程：入队后立刻退出的脚本会把它在队列中途杀掉，第一
        笔之后的所有委托无声丢失（问题 #156：“循环下单只有第一条成功，加 sleep
        才正常”——sleep 的作用是让进程活着；实盘复现为 1/3 对 3/3 到达 QMT）。
        先排空队列，再给已布防的屏障一个有界时刻，使在途回复能触发回调。
        从不抛异常：它在解释器退出和 stop() 内部运行。
        """
        try:
            self.wait_async_orders(timeout=self.ASYNC_EXIT_DRAIN_SECONDS)
        except Exception:
            pass
        barrier_lock = getattr(self, "_async_barrier_lock", None)
        if barrier_lock is None:
            return
        deadline = time.time() + self.ASYNC_EXIT_CALLBACK_GRACE_SECONDS
        try:
            while time.time() < deadline:
                with barrier_lock:
                    if not getattr(self, "_async_barrier", None):
                        return
                time.sleep(0.05)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 异步撤单——镜像异步委托的工作队列模式。
    # cancel_order_stock_async 过去每笔撤单都阻塞在 RPC 上；现在它入队后立即
    # 返回。工作线程排空队列，并把积压合并成一次 cancel_orders_batch RPC。
    # ------------------------------------------------------------------

    def _async_cancel_worker(self):
        """排空已排队的异步撤单；有积压时合并成批。"""
        while True:
            job = self._async_cancel_queue.get()
            if job is None:
                self._async_cancel_queue.task_done()
                return
            jobs = [job]
            stopping = False
            while len(jobs) < self.ASYNC_CANCEL_BATCH_MAX:
                try:
                    extra = self._async_cancel_queue.get_nowait()
                except _queue.Empty:
                    break
                if extra is None:
                    stopping = True
                    self._async_cancel_queue.task_done()
                    break
                jobs.append(extra)
            try:
                self._submit_async_cancel_jobs(jobs)
            except Exception:
                log.exception("异步撤单提交失败 jobs=%d", len(jobs))
            finally:
                for _ in jobs:
                    self._async_cancel_queue.task_done()
            if stopping:
                return

    def _ensure_async_cancel_worker(self):
        with self._async_order_lock:
            if self._async_callback_thread is None or not self._async_callback_thread.is_alive():
                self._async_callback_thread = threading.Thread(
                    target=self._async_callback_worker,
                    name="bigqmt-async-callback", daemon=True,
                )
                self._async_callback_thread.start()
            if self._async_cancel_thread is not None and self._async_cancel_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._async_cancel_worker,
                name="bigqmt-async-cancel", daemon=True,
            )
            self._async_cancel_thread = thread
            thread.start()

    def _submit_async_cancel_jobs(self, jobs):
        """逐笔提交撤单，或按账户合并成批。"""
        if len(jobs) < self.ASYNC_CANCEL_BATCH_MIN:
            self._submit_async_cancel_single(jobs[0])
            return
        groups = {}
        for job in jobs:
            seq, args, kwargs = job
            account_id = _account_id(args[0], self.client.account_id)
            # 按 id 和类型：同一股票账户下 STOCK 与 HUGANGTONG 的撤单在服务端
            # 是两本不同的账。
            groups.setdefault((account_id, _account_type_param(args[0])), []).append(job)
        for (account_id, _type), group in groups.items():
            if len(group) < self.ASYNC_CANCEL_BATCH_MIN:
                for job in group:
                    self._submit_async_cancel_single(job)
            else:
                self._submit_async_cancel_batch(account_id, group)

    def _submit_async_cancel_single(self, job):
        """在工作线程上执行一次撤单；把它的回调入队。"""
        seq, args, kwargs = job
        account = args[0]
        order_id = args[1] if len(args) > 1 else kwargs.get("order_id", "")
        market = args[2] if len(args) > 2 else kwargs.get("market", "")
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "cancel_order_stock_sysid",
                _with_account_type({
                    "account_id": account_id,
                    "market": market,
                    "order_sysid": self._resolve_order_sys_id(order_id),
                }, account),
                account_id=account_id,
            ) or {}
            ok = bool(data.get("success", data))
        except Exception as exc:
            self._enqueue_async_outcome({
                "kind": "cancel_error", "seq": seq,
                "order_id": order_id, "market": market,
                "error_id": getattr(exc, "errno", 0),
                "error_msg": str(exc),
            })
            return
        self._enqueue_async_outcome({
            "kind": "cancel_response", "seq": seq,
            "order_id": order_id, "market": market,
            "success": ok,
        })

    def _submit_async_cancel_batch(self, account_id, group):
        """一次 RPC 撤 N 笔；每项一个回调。

        失败分类与委托批量相同（#195）：服务端 **拒绝** 的批量（以错误作答——
        处理器在逐项循环之前就抛异常，所以什么都没跑）或答空的批量，可以安全
        地退回逐笔重试；超时/传输失败意味着撤单可能正在执行，重试会造成重复
        撤单——此时按项报告“结果未知”。"""
        payload = []
        account = group[0][1][0] if group and group[0][1] else None
        for seq, args, kwargs in group:
            order_id = args[1] if len(args) > 1 else kwargs.get("order_id", "")
            market = args[2] if len(args) > 2 else kwargs.get("market", "")
            payload.append({
                "account_id": account_id,
                "order_sysid": self._resolve_order_sys_id(order_id),
                "market": market,
            })
        try:
            results = self.client.call(
                "cancel_order_stock_batch",
                _with_account_type({"account_id": account_id, "items": payload}, account),
                account_id=account_id,
            ) or []
        except RpcServerRepliedError as exc:
            log.warning("服务端拒绝批量撤销 %d 笔（%s）；正在逐笔提交",
                        len(group), exc)
            for job in group:
                try:
                    self._submit_async_cancel_single(job)
                except Exception:
                    log.exception("批量撤销被拒后，异步撤单仍然失败")
            return
        except Exception as exc:
            log.exception("批量撤销 %d 笔的结果未知；不会重新提交",
                          len(group))
            for seq, args, kwargs in group:
                order_id = args[1] if len(args) > 1 else kwargs.get("order_id", "")
                market = args[2] if len(args) > 2 else kwargs.get("market", "")
                self._enqueue_async_outcome({
                    "kind": "cancel_error", "seq": seq,
                    "order_id": order_id, "market": market,
                    "error_id": -4,
                    "error_msg": ("cancel batch outcome unknown (%s: %s); the "
                                  "cancels MAY BE RUNNING -- check the order "
                                  "status before retrying"
                                  % (exc.__class__.__name__, exc)),
                })
            return
        if not results:
            # 服务端为每项追加一条结果，所以什么都没回来意味着批量从未执行
            # ——而不是每笔撤单都失败了。退回逐笔，那本来也是会发生的结果。
            log.warning("批量撤销 %d 笔未返回结果；正在逐笔提交", len(group))
            for job in group:
                try:
                    self._submit_async_cancel_single(job)
                except Exception:
                    log.exception("批量撤销返回空结果后，异步撤单仍然失败")
            return
        by_index = {}
        for position, entry in enumerate(results):
            if isinstance(entry, dict):
                by_index[int(entry.get("index", position))] = entry
        for index, (seq, args, kwargs) in enumerate(group):
            order_id = args[1] if len(args) > 1 else kwargs.get("order_id", "")
            market = args[2] if len(args) > 2 else kwargs.get("market", "")
            entry = by_index.get(index) or {}
            if entry.get("success", False):
                self._enqueue_async_outcome({
                    "kind": "cancel_response", "seq": seq,
                    "order_id": order_id, "market": market,
                    "success": True,
                })
            else:
                self._enqueue_async_outcome({
                    "kind": "cancel_error", "seq": seq,
                    "order_id": order_id, "market": market,
                    "error_id": int(entry.get("code") or -1),
                    "error_msg": str(entry.get("error")
                                     or entry.get("message")
                                     or "cancel batch item failed"),
                })

    def cancel_order_stock_batch(self, account, cancels, timeout_seconds=None):
        """一次 RPC 撤 N 笔委托。

        *cancels* 里每一项是带 ``order_sysid``（或 ``order_sys_id`` /
        ``order_id``）以及可选 ``market`` 的 dict。返回逐项结果 dict 的列表。
        """
        account_id = _account_id(account, self.client.account_id)
        payload = []
        for item in cancels or []:
            entry = dict(item or {})
            entry.setdefault("account_id", account_id)
            payload.append(entry)
        params = _with_account_type({"account_id": account_id, "items": payload}, account)
        if timeout_seconds is None:
            timeout_seconds = max(
                float(getattr(self.client, "timeout_seconds",
                              DEFAULT_RPC_TIMEOUT_SECONDS)),
                2.0 + 0.5 * len(payload),
            )
        return self.client.call(
            "cancel_order_stock_batch",
            params,
            account_id=account_id,
            timeout_seconds=timeout_seconds,
        ) or []

    # ------------------------------------------------------------------
    # issue #51 A: 同一笔委托的 async_response 必须先于它的 order/trade 到达。
    #
    # 两条回调走的是不同线程和不同通道: async_response 在异步下单的工作线程上
    # 触发, order/trade 来自 Valkey pub/sub 监听线程。服务端在 order_callback
    # 里先推事件再回 RPC, 所以顺序颠倒是常态而非偶发。
    #
    # 做法是给「已提交但尚未收到 async_response」的委托设一道屏障: 它的
    # order/trade 事件先暂存, 等 response 触发后按到达顺序放行。延迟只加在
    # 异步下单这一条路径上——手工下单、同步下单、以及任何未登记的委托一律直通。
    # ------------------------------------------------------------------
    ASYNC_BARRIER_TIMEOUT_SECONDS = 10.0
    # response 触发前等屏障从暂存的委托事件里学到委托号的上限（issue #72）。
    # 委托号异步分配：推送通常比 RPC 应答快，几百毫秒内就能学到；超时则按
    # 原样发 response（order_id 回落 remark），不拖住回调。
    ASYNC_SYSID_WAIT_SECONDS = 2.0

    # 工作线程醒来时，只合并 **已经** 排队的任务（问题 #181）。绝不为凑批而等：
    # 单独一笔不能因为别的业务繁忙而变慢。2 是一次往返优于两次的最小积压量，
    # 500 是服务端对 submit_orders_batch 自己的上限。
    ASYNC_BATCH_MIN = 2
    ASYNC_BATCH_MAX = 500
    # stop()/进程退出时：等队列里已排队的 async 委托发完的上限，以及给
    # 在途 response 触发回调的宽限（issue #156）。
    ASYNC_EXIT_DRAIN_SECONDS = 5.0
    ASYNC_EXIT_CALLBACK_GRACE_SECONDS = 3.0
    # 异步撤单：与异步委托相同的入队并合批模式（问题 #50 应用于
    # cancel_order_stock_async）。一次 RPC 撤 N 笔而不是 N 次往返：15 笔撤单
    # 从约 30s 降到约 2s。
    ASYNC_CANCEL_BATCH_MIN = 2
    ASYNC_CANCEL_BATCH_MAX = 500

    def _order_barrier(self):
        barrier = getattr(self, "_async_barrier", None)
        if barrier is None:
            barrier = {}
            self._async_barrier = barrier
            self._async_barrier_lock = threading.Lock()
        return barrier

    @staticmethod
    def _async_remark(args, kwargs):
        """下单调用里的 order_remark —— 拿到 order_sys_id 之前唯一的关联键。"""
        return str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "") or "")

    def _arm_order_barrier(self, remark, seq):
        """登记一笔待响应的委托。remark 为空则不设屏障(无从关联)。"""
        if not remark:
            return
        self._order_barrier()
        with self._async_barrier_lock:
            # remark 不强制唯一(网格类策略常复用同一 remark)。同 remark 的上一笔
            # 可能还扣着暂存事件, 直接覆盖会把它们永久丢掉——丢事件比顺序错乱
            # 更糟, 所以接管旧 entry 并在锁外放行它的事件。
            superseded = self._async_barrier.pop(remark, None)
            self._async_barrier[remark] = {
                "seq": seq,
                "sys_ids": set(),
                "events": [],
                "deadline": time.time() + self.ASYNC_BARRIER_TIMEOUT_SECONDS,
            }
        for event in (superseded or {}).get("events", []):
            self._deliver_event(event)

    def _release_order_barrier(self, remark, seq=None):
        """response 已触发, 按到达顺序放行暂存的事件。"""
        if not remark:
            return
        self._order_barrier()
        with self._async_barrier_lock:
            entry = self._async_barrier.get(remark)
            if entry is None:
                return
            if seq is not None and entry["seq"] != seq:
                # 同 remark 的后一笔委托已接管屏障; 前一笔的 response 不该放它。
                return
            entry = self._async_barrier.pop(remark, None)
        for event in (entry or {}).get("events", []):
            self._deliver_event(event)

    def _sweep_order_barriers(self):
        """放行超时未收到 response 的委托。

        没有这一步, 一次失败的提交会把它的事件永久扣住——丢事件比顺序错乱更糟。
        """
        now = time.time()
        expired = []
        with self._async_barrier_lock:
            for remark, entry in list(self._async_barrier.items()):
                if now >= entry["deadline"]:
                    expired.append(self._async_barrier.pop(remark))
        for entry in expired:
            for event in entry.get("events", []):
                self._deliver_event(event)

    def _hold_if_pending(self, event):
        """属于待响应委托则暂存并返回 True, 否则返回 False 直通。"""
        barrier = self._order_barrier()
        if not barrier:
            return False
        remark = str(event.get("remark") or event.get("user_order_id") or "")
        sys_id = str(event.get("order_sys_id") or "")
        with self._async_barrier_lock:
            entry = barrier.get(remark) if remark else None
            if entry is None and sys_id:
                # 成交事件可能没有 remark; 用委托事件里学到的 order_sys_id 关联。
                for candidate in barrier.values():
                    if sys_id in candidate["sys_ids"]:
                        entry = candidate
                        break
            if entry is None:
                return False
            if sys_id:
                entry["sys_ids"].add(sys_id)
            entry["events"].append(event)
            return True

    @staticmethod
    def _async_job_fields(args, kwargs):
        """从两种调用约定之一取出 (account, stock_code, ...)。

        order_stock_async 原样转发 *args/**kwargs，所以一个任务可能带其中任一种。
        批量载荷需要把它们具名。
        """
        names = ("account", "stock_code", "order_type", "order_volume",
                 "price_type", "price", "strategy_name", "order_remark")
        out = {}
        for index, name in enumerate(names):
            if name in kwargs:
                out[name] = kwargs[name]
            elif len(args) > index:
                out[name] = args[index]
        return out

    def _submit_async_jobs(self, jobs):
        """提交单个任务或已排队的积压，按账户保持 seq 顺序。"""
        if len(jobs) < self.ASYNC_BATCH_MIN:
            seq, args, kwargs = jobs[0]
            self._submit_async_single(seq, args, kwargs)
            return
        # order_stock_batch 只接受一个 account_id，所以混合积压按账户拆开——把
        # 其余任务用第一个任务的账户提交，会把委托下到错误的账户上。账户内保持
        # seq 顺序；跨账户的顺序从来不是契约。
        groups = {}
        for job in jobs:
            fields = self._async_job_fields(job[1], job[2])
            account_id = _account_id(fields.get("account"), self.client.account_id)
            # 按 id 和类型（股票账户 id 上的港股通委托在服务端是另一本账）。
            key = (account_id, _account_type_param(fields.get("account")))
            groups.setdefault(key, []).append((job, fields))
        for (account_id, _type), group in groups.items():
            if len(group) < self.ASYNC_BATCH_MIN:
                for (seq, args, kwargs), _fields in group:
                    self._submit_async_single(seq, args, kwargs)
            else:
                self._submit_async_batch(account_id, group)

    def _submit_async_batch(self, account_id, group):
        """用一次 RPC 提交同一账户的积压，每个任务一个结果。

        每个任务都有意置 wait_settlement=False。这本来就是异步契约要求的
        （#69），同时也绕开批量处理器里一个潜在 bug：_handle_submit_order 把该
        标志默认为 True，并把每项的结算寄存在唯一的 _pending_settlement 槽位里，
        于是批量中只有最后一项能拿回它的订单号。

        回退，以及何时允许回退：结果为 **未知** 的批量（超时、连接断开）绝不能
        重新提交——服务端可能仍在执行它，重交会让委托翻倍（实盘：100 项的批量
        超过了 30s 超时，回退重交，最终落地 200 笔）。只有两种失败形态可以退回
        逐笔：服务端 **以错误作答**（处理器在逐项循环之前抛异常，所以什么都没
        跑）以及结果列表为空（处理器为每项追加一条，所以空意味着它从未执行）。
        """
        payload = []
        for (seq, args, kwargs), fields in group:
            item = {name: value for name, value in fields.items()
                    if name != "account" and value is not None}
            item["wait_settlement"] = False
            # 每项唯一的 signal_id，理由与单笔路径自行造一个相同（#190）。它
            # 还让“无备注”情形对早于幂等标志的服务端安全：批量标签回退到
            # signal_id，因此不同 id 不会在去重中相撞。显式 order_remark 仍然
            # 优先，并且仍以用户写的样子到达 QMT。
            # "rpc-<hex>"，字符级与 _handle_submit_order 造的一致，所以无备注的
            # 异步委托在 QMT 上显示的备注仍是 0.3.20 时的 "bqrpc:rpc-<hex>"——
            # 那个字符串在备注里可见，也是结算查找所匹配的东西（#152），因此它
            # 的形状不能随意漂移。
            item.setdefault("signal_id", "rpc-%s" % uuid.uuid4().hex)
            payload.append(item)
        try:
            # 用第一个任务的 StockAccount，而不是裸 id：order_stock_batch 会把
            # 它声明的类型一起发过去（港股通）。
            batch_account = (group[0][1] or {}).get("account") or account_id
            results = self.order_stock_batch(batch_account, payload,
                                             idempotent=False) or []
        except RpcServerRepliedError as exc:
            log.warning("服务端拒绝异步批量委托 %d 笔（%s）；正在逐笔提交",
                        len(group), exc)
            results = None
        except Exception as exc:
            # 超时 / 传输失败：批量去向未知。**不要** 重新提交——改为把每项报告
            # 为结果未知。消息必须说明委托可能是活的，因为它们确实可能是：失败
            # 就重试的调用方会重复下单。
            log.exception("异步批量委托 %d 笔的结果未知；不会重新提交",
                          len(group))
            for (seq, args, kwargs), fields in group:
                self._enqueue_batch_outcome(seq, fields, {
                    "success": False, "code": -4,
                    "error": ("batch outcome unknown (%s: %s); the orders MAY "
                              "BE LIVE -- query orders before retrying"
                              % (exc.__class__.__name__, exc)),
                })
            return
        if not results:
            if results is not None:
                # 服务端为每项追加一条结果，所以什么都没回来意味着批量从未执行
                # ——而不是每笔都失败了。在这里报告 N 个失败是最危险的读法：失败
                # 就重试的调用方会把它们全部再下一次。
                log.warning("异步批量委托 %d 笔未返回结果；正在逐笔提交",
                            len(group))
            for (seq, args, kwargs), _fields in group:
                try:
                    self._submit_async_single(seq, args, kwargs)
                except Exception:
                    log.exception("批量回退后，异步委托仍然失败 seq=%s", seq)
            return
        by_index = {}
        for position, entry in enumerate(results):
            if isinstance(entry, dict):
                by_index[int(entry.get("index", position))] = entry
        for index, ((seq, args, kwargs), fields) in enumerate(group):
            self._enqueue_batch_outcome(seq, fields, by_index.get(index))

    def _enqueue_batch_outcome(self, seq, fields, entry):
        """把批量结果中的一项转换成该任务的结果单元。

        批量项 **不做** 等待迟到订单号。对 N 项逐个串行等
        ASYNC_SYSID_WAIT_SECONDS 会抵消批量的意义：300 笔委托将花 600s 等待，
        而不是 0.6s 提交。订单号还没回来的项用备注作答，真正的 id 仍会像其它
        情况一样通过 order_callback 推送到达。
        """
        remark = str(fields.get("order_remark") or "")
        stock_code = str(fields.get("stock_code") or "")
        entry = entry if isinstance(entry, dict) else {}
        if not entry or not entry.get("success", False):
            self._enqueue_async_outcome({
                "kind": "error", "seq": seq, "remark": remark,
                "stock_code": stock_code,
                "order_remark": remark,
                "error_id": int(entry.get("code") or -1),
                "error_msg": str(entry.get("error")
                                 or "order missing from batch result"),
            })
            return
        order_sys_id = str(entry.get("order_sys_id") or "")
        user_order_id = str(entry.get("user_order_id") or remark or "")
        self._enqueue_async_outcome({
            "kind": "response", "seq": seq, "remark": remark,
            "stock_code": stock_code,
            "strategy_name": str(fields.get("strategy_name") or ""),
            "order_remark": remark,
            "order_sys_id": order_sys_id,
            "user_order_id": user_order_id,
            "wait_for_sysid": False,
        })

    def _has_push_channel(self):
        """执行事件到底能否到达本客户端（#345）。

        Valkey 通过发布/订阅承载它们；没有推送路径的传输无法送达回复之后产生的
        事件。
        """
        transport_name = str(getattr(self.client, "transport_name", "valkey") or "valkey").lower()
        return transport_name in ("valkey", "", "default")

    def _submit_async_single(self, seq, args, kwargs):
        """提交一个任务并把结果入队。在委托工作线程上运行。"""
        stock_code = str(kwargs.get("stock_code") or (args[1] if len(args) > 1 else ""))
        remark = self._async_remark(args, kwargs)
        order_remark = str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "") or "")
        try:
            # wait_settlement=False：passorder 一返回就应答，不在 worker 里等
            # 服务端结算（那是 #69 要的吞吐）。委托号从推送事件学——屏障暂存的
            # 委托事件里会带上（触发 response 前至多等 2s，学不到就回落 remark）。
            #
            # 没有推送通道的传输学不到任何事件，服务端为拒单
            # 推的 order_error 也到不了（#345）。那里让 worker 等服务端结算：
            # 调用方本来就不阻塞，而 server_error 会走下面的 except 变成
            # on_order_error，否则一张资金不足被终端拦下的单永远没有回音。
            result = self.order_stock_result(
                *args, wait_settlement=not self._has_push_channel(), **kwargs)
        except Exception as exc:
            self._enqueue_async_outcome({
                "kind": "error", "seq": seq, "remark": remark,
                "stock_code": stock_code, "order_remark": order_remark,
                "error_id": getattr(exc, "errno", 0), "error_msg": str(exc),
            })
            return

        order_sys_id = ""
        user_order_id = ""
        if isinstance(result, dict):
            order_sys_id = str(result.get("order_sys_id") or result.get("order_sysid") or "")
            user_order_id = str(result.get("user_order_id") or "")
        elif result is not None:
            order_sys_id = str(result)

        # 提交本身失败时 order_stock 返回 -1。服务端还会为废单推一条
        # order_error；两者携带的信息不同（RPC 提交失败 vs QMT 拒单细节），所以
        # 都保留可用。
        if order_sys_id == "-1" or result == -1:
            self._enqueue_async_outcome({
                "kind": "error", "seq": seq, "remark": remark,
                "stock_code": stock_code, "order_remark": order_remark,
                "error_id": -1,
                "error_msg": "order submit failed (order_stock returned -1)",
            })
            return

        self._enqueue_async_outcome({
            "kind": "response", "seq": seq, "remark": remark,
            "stock_code": stock_code,
            "strategy_name": str(kwargs.get("strategy_name") or (args[6] if len(args) > 6 else "")),
            "order_remark": order_remark,
            "order_sys_id": order_sys_id,
            "user_order_id": user_order_id,
            "wait_for_sysid": True,
        })

    def _enqueue_async_outcome(self, unit):
        self._async_callback_queue.put(unit)

    def _fire_async_outcome(self, unit):
        """单个任务的回调，在分发线程上。

        屏障释放在同一个单元里、位于用户回调 **之后**，所以 #51 契约——一笔委托
        的 async_response 先于被它扣住的 order/trade 事件——与全部在单线程上
        运行时保持不变。
        """
        kind = unit.get("kind", "")
        if kind in ("cancel_response", "cancel_error"):
            self._fire_async_cancel_outcome(unit)
            return
        callback = self.callback
        seq = unit["seq"]
        remark = unit["remark"]
        try:
            if unit["kind"] == "error":
                if callback is not None:
                    callback.on_order_error(
                        CompatObject(
                            account_id=self.client.account_id,
                            account_type=self._account_type_value(),
                            strategy_name=unit.get("strategy_name", ""),
                            error_id=unit["error_id"],
                            error_msg=unit["error_msg"],
                            order_sysid="",          # MiniQMT 规范名 (issue #65)
                            order_sys_id="",
                            order_id=self._order_object_id(""),   # int (#113)
                            stock_code=unit["stock_code"],
                            seq=seq,
                            order_remark=unit["order_remark"],
                        )
                    )
            elif callback is not None:
                # 原生 XtOrderResponse 形状：一个参数携带
                # account_id/order_id/seq/error_msg。
                #
                # 委托号异步分配（#50）：服务端应答时通常还没有。触发 response 前
                # 先等屏障从暂存的委托事件里学到委托号（事件推送一般比 RPC 应答
                # 快，bounded 2s），否则 order_id 只能回落成 remark（issue #72）。
                # 这个等如今在回调线程上——它拖住的只是后续回调，不再是后续下单。
                order_sys_id = unit["order_sys_id"]
                if unit.get("wait_for_sysid") and not order_sys_id and remark:
                    wait_deadline = time.time() + self.ASYNC_SYSID_WAIT_SECONDS
                    while time.time() < wait_deadline:
                        learned = ""
                        with self._async_barrier_lock:
                            entry = self._async_barrier.get(remark)
                            if entry and entry["sys_ids"]:
                                learned = sorted(entry["sys_ids"])[0]
                        if learned:
                            order_sys_id = learned
                            break
                        time.sleep(0.05)
                callback.on_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        account_type=self._account_type_value(),
                        seq=seq,
                        order_id=self._order_object_id(order_sys_id or unit["user_order_id"]),
                        order_sysid=order_sys_id,    # MiniQMT 规范名 (issue #65)
                        order_sys_id=order_sys_id,
                        stock_code=unit["stock_code"],
                        strategy_name=unit["strategy_name"],
                        order_remark=unit["order_remark"],
                        error_msg="",
                    ),
                )
        except Exception:
            log.exception("用户回调执行失败 async outcome seq=%s kind=%s",
                          seq, unit.get("kind"))
        finally:
            # response/error 已触发 -> 放行这笔委托暂存的 order/trade (issue #51)。
            self._release_order_barrier(remark, seq)

    def _fire_async_cancel_outcome(self, unit):
        """触发 on_cancel_order_stock_async_response / on_cancel_error。"""
        callback = self.callback
        seq = unit["seq"]
        order_id = unit.get("order_id", "")
        try:
            if unit["kind"] == "cancel_error":
                if callback is not None:
                    callback.on_cancel_error(
                        CompatObject(
                            account_id=self.client.account_id,
                            account_type=self._account_type_value(),
                            # 没有任何代码会走到这条路径（stock_code 就在下方
                            # 设为 ""），所以市场未知。
                            market=-1,
                            error_id=unit["error_id"],
                            error_msg=unit["error_msg"],
                            # 这里曾缺 seq，而响应路径有——一个无法关联的错误，
                            # 正是“哪一笔撤单失败了？”无从回答的原因。
                            seq=seq,
                            order_sysid=str(order_id or ""),
                            order_sys_id=str(order_id or ""),
                            order_id=self._order_object_id(order_id),
                            stock_code="",
                        )
                    )
            elif callback is not None:
                ok = unit.get("success", False)
                callback.on_cancel_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        account_type=self._account_type_value(),
                        seq=seq,
                        success=bool(ok),
                        cancel_result=0 if ok else -1,
                        # 原生撤单返回值表达的是“请求已发出”，不是“委托已撤销”
                        # ——它在撤单已落地时曾为假（#148），在处理不存在的委托时
                        # 为真（#151）。绝不能把它说成拒单；委托状态推送（54）或
                        # 一次查询才是确认。
                        error_msg="" if ok else (
                            "cancel not confirmed by the counter; the order may "
                            "still get cancelled -- check the order-status push "
                            "or query before assuming either way"),
                        order_sysid=str(order_id or ""),
                        order_sys_id=str(order_id or ""),
                        order_id=self._order_object_id(order_id),
                    ),
                )
        except Exception:
            log.exception(
                "用户回调执行失败 async cancel outcome seq=%s kind=%s",
                seq, unit.get("kind"),
            )

    def order_stock_async(self, *args, **kwargs):
        """把委托入队并立刻返回其 seq（MiniQMT 语义）。

        过去这里就地调用 order_stock，于是它要阻塞整个 RPC 往返，外加——#44 那次
        改动之后——服务端等 QMT 分配订单号的全部时间。每笔 0.5-1s，正好抵消了
        异步 API 的意义（问题 #50）。

        现在提交在工作线程上运行，结果通过 on_order_stock_async_response /
        on_order_error 到达，两者都带 seq 以便调用方关联。返回 seq 而不碰网络。

        工作线程是守护线程：主线程在入队后立刻退出的脚本会在队列中途杀掉它，
        丢掉每一笔还没提交的委托（问题 #156）。stop() 和 atexit 钩子都会排空队列
        （有界）；回调仍要求进程活着——进程没了它们无法到达。长跑策略不受影响。
        """
        seq = self._next_async_seq()
        # 屏障要在入队之前设好: 委托可能在本函数返回之前就被推送出来。
        self._arm_order_barrier(self._async_remark(args, kwargs), seq)
        self._ensure_async_order_worker()
        self._register_exit_drain()
        self._async_order_queue.put((seq, args, kwargs))
        return seq

    def wait_async_orders(self, timeout=10.0):
        """阻塞直到所有已排队的异步委托都已提交 **且** 其回调已触发。

        用于测试和关闭流程；API 本身是“发了就不管”。超时时返回 False 而不是
        挂住。两个队列都用 task_done 记账，所以它覆盖在途提交和在途回调，而不
        只是排空的队列。
        """
        deadline = time.time() + float(timeout)
        for name in ("_async_order_queue", "_async_cancel_queue",
                      "_async_callback_queue"):
            queue_obj = getattr(self, name, None)
            if queue_obj is None:
                continue
            while queue_obj.unfinished_tasks:
                if time.time() > deadline:
                    return False
                time.sleep(0.005)
        return True

    def order_stock_batch(self, account, orders, batch_id="", idempotent=True,
                          timeout_seconds=None):
        """用一次 RPC 提交 N 笔委托。

        ``idempotent``（默认 True，未改变）是批量契约：每一项都需要标签，已经
        提交过的标签会答成功而不再下单，因此重试的批量不会重复下单。对从未同意
        这一点的调用方请传 False——order_stock_async 把积压路由到这里（#181）
        曾因此丢单（#190）。

        未指定时等待时间随 N 放大：服务端逐项串行执行，而每项成本从毫秒级
        （盘中）摆动到约 300ms（柜台断连），所以固定默认值会把一个慢但活着的
        批量变成客户端超时——而重试的批量会让委托翻倍。
        """
        account_id = _account_id(account, self.client.account_id)
        payload = []
        for item in orders or []:
            entry = dict(item or {})
            entry.setdefault("account_id", account_id)
            payload.append(entry)
        params = _with_account_type({"account_id": account_id, "orders": payload}, account)
        if not idempotent:
            params["idempotent"] = False
        if batch_id:
            params["batch_id"] = str(batch_id)
        if timeout_seconds is None:
            timeout_seconds = max(
                float(getattr(self.client, "timeout_seconds",
                              DEFAULT_RPC_TIMEOUT_SECONDS)),
                BATCH_TIMEOUT_FLOOR_SECONDS
                + BATCH_TIMEOUT_PER_ITEM_SECONDS * len(payload),
            )
        return self.client.call(
            "order_stock_batch",
            params,
            account_id=account_id,
            timeout_seconds=timeout_seconds,
        ) or []

    def passorder(self, op_type, order_type, account, order_code, price_type,
                  price, volume, strategy_name="", quick_trade=None,
                  user_order_id="", dry_run=False):
        """原样直通 QMT 的原生 passorder（路线 2）。

        参数顺序与 QMT 自己的签名一致——opType、orderType、accountid、
        orderCode、prType、price、volume、strategyName、quickTrade、
        userOrderId——ContextInfo 由服务端提供（它只存在于 QMT 内部）。与
        order_stock 不同，它暴露 orderType 和 quickTrade，并且 **不跑**
        order_stock 的任何安全网：不做 opType 校验、不做代码大小写归一（#95）、
        不做结算回读。原生契约由你自己负责。

        order_type / quick_trade 留空时取服务端配置（1101 / 2），所以常见情形
        读起来仍像 order_stock。

        passorder 是异步的，不返回订单号（QMT 稍后在 order_callback 推送里分配
        合同编号），与 order_stock_async 相同。本方法返回服务端的 ack dict。

        dry_run=True 返回服务端 **将要** 传给 passorder 的 11 元组而不下单——
        用它核对你的映射。
        """
        account_id = _account_id(account, self.client.account_id)
        params = _with_account_type({
            "account_id": account_id,
            "op_type": op_type,
            "order_code": order_code,
            "price_type": price_type,
            "price": price,
            "volume": volume,
            "strategy_name": strategy_name,
            "user_order_id": user_order_id,
        }, account)
        if order_type is not None:
            params["order_type"] = order_type
        if quick_trade is not None:
            params["quick_trade"] = quick_trade
        if dry_run:
            params["dry_run"] = True
        return self.client.call("passorder", params, account_id=account_id)

    def cancel_order_stock_sysid(self, account, market, order_sysid):
        """MiniQMT 契约：成功返回 0，失败返回 -1（问题 #113）。

        这里过去返回 bool，那比类型不匹配更糟——它把含义反了过来。
        ``if trader.cancel_order_stock(...) == 0`` 是 MiniQMT 代码检查成功的方式，
        而 ``False == 0`` 为真，于是 *失败* 的撤单读成成功，成功读成失败。
        """
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "cancel_order_stock_sysid",
            _with_account_type({
                "account_id": account_id,
                "market": market,
                # 发送券商自己的合同编号，而不是我们由它推导出的 int。
                "order_sysid": self._resolve_order_sys_id(order_sysid),
            }, account),
            account_id=account_id,
        ) or {}
        return 0 if bool(data.get("success", data)) else -1

    def cancel_order_stock(self, account, order_id):
        return self.cancel_order_stock_sysid(account, "", order_id)

    def unsubscribe(self, account):
        # MiniQMT xttrader.unsubscribe(account) — 取消账户订阅。
        # Big QMT RPC 模式下账户是被动响应，unsubscribe 为 no-op。
        return 0

    # ------------------------------------------------------------------
    # 账户 / 融资融券扩展查询
    # 这些在 MiniQMT 走 XtQuantServer RPC；Big QMT 经
    # get_trade_detail_data 查询，需相应账户权限（两融账户等）。
    # 无权限/上下文未绑定时服务端降级为 []。
    # ------------------------------------------------------------------

    def _query_account_list(self, account, method):
        account_id = _account_id(account, self.client.account_id)
        try:
            rows = self.client.call(method, _with_account_type({"account_id": account_id}, account),
                                    account_id=account_id) or []
        except Exception:
            return []
        # MiniQMT 用属性访问这些（见 CompatRow）。服务端已经转发了终端自己的
        # m_ 名字，所以错的只是容器。
        if isinstance(rows, list):
            return [_as_compat_row(row) for row in rows]
        return _as_compat_row(rows)

    def query_account_infos(self, account=None):
        return self._query_account_list(account, "query_account_infos")

    def query_account_status(self, account=None):
        return self._query_account_list(account, "query_account_status")

    # -- 可转债 转股 / 回售（大 QMT 有、MiniQMT 没有）-----------------------
    def convert_bond(self, account, stock_code, volume, strategy_name="", order_remark=""):
        """可转债转股：把 ``volume`` 张转债转成正股（大 QMT passorder opType 80 / 82）。

        MiniQMT 没有这个操作。账户类型决定编号：普通户 80，信用户 82，按
        ``account.account_type``（StockAccount 的第二个参数）选。返回和
        ``order_stock`` 一样的 order_id；没有买卖方向，`price` 送 0。
        转股不可撤销，请先核对代码是转债代码、数量是张数。
        """
        return self._convertible_op("convert", account, stock_code, volume,
                                    strategy_name, order_remark)

    def sell_back_bond(self, account, stock_code, volume, strategy_name="", order_remark=""):
        """可转债回售：把 ``volume`` 张转债按回售条款卖回发行人（opType 81 / 83）。

        同 ``convert_bond``：普通户 81，信用户 83，按账户类型选；只在回售
        申报期内有效，其余时间柜台会拒。
        """
        return self._convertible_op("sell_back", account, stock_code, volume,
                                    strategy_name, order_remark)

    def _convertible_op(self, action, account, stock_code, volume, strategy_name, order_remark):
        from .adapters.order_bigqmt import convertible_optype_for

        account_type = _account_type_name(getattr(account, "account_type", None)) \
            or getattr(self, "_declared_account_type", "") or "STOCK"
        op_type = convertible_optype_for(action, account_type)
        data = self.order_stock_result(
            account, stock_code, op_type, int(volume), 11, 0.0,
            strategy_name, order_remark,
        )
        return self._order_id(data.get("order_sys_id"))

    def query_credit_detail(self, account):
        """信用账户明细，读终端缓存的信用账号对象（同步）。

        大 QMT 走 get_trade_detail_data(accId, 'CREDIT', 'ACCOUNT')。这份是
        「非查柜台」的本地缓存（官方参考 3.14），券商没推就是空的 —— 空列表
        时用 query_credit_account() 问柜台那一份（#201 / #202）。
        """
        return self._query_account_list(account, "query_credit_detail")

    def query_credit_account(self, account=None, wait_seconds=None,
                             max_age_seconds=None):
        """信用账户明细，查柜台的那一份（官方参考 6.13）。

        大 QMT 那边是异步的：query_credit_account 立刻返回，结果从
        credit_account_callback 出来。桥这边替你发查询、等回调、缓存结果，
        所以这个调用本身是同步的。

        **返回的是最近一次柜台答案，不是这一次查询的结果。** 回调实测投递在
        MainThread 上（handler 也在那条线程），所以这一次的新答案只可能在本次
        调用返回之后才落进缓存 —— 想要它，隔一会儿再调一次。看 fresh /
        age_seconds 判断新旧，不要只看 rows（#202）。

        缓存**不会自己刷新**：没人调就永远不刷。所以默认超过 120 秒的数据
        直接不给（`dropped_stale`），免得有人拿几小时前的维持担保比例去做
        决策还毫无察觉。传 max_age_seconds=0 显式放弃这层保护。

        **要实时的维持担保比例 / 可用额度，请用 query_credit_detail** ——
        同步、读终端本地缓存、没有陈旧问题。这条查柜台的路是对账用的。

        Args:
            wait_seconds: 等回调的秒数。默认 0（不等）—— 回调若与 handler 同在
                一条线程，等待永远等不到，只会白占 adjust 主线程。
            max_age_seconds: 缓存超过这个年龄就不返回 rows。默认 120，0 = 不限。

        Returns:
            dict: rows / count / fresh / stale / age_seconds / max_age_seconds /
                dropped_stale / dropped_stale_reason / query_issued /
                not_issued_reason / callbacks_seen / callback_thread / seq /
                error / callback_bound
        """
        account_id = _account_id(account, self.client.account_id)
        params = _with_account_type({"account_id": account_id}, account)
        if wait_seconds is not None:
            params["wait_seconds"] = float(wait_seconds)
        if max_age_seconds is not None:
            params["max_age_seconds"] = float(max_age_seconds)
        answer = self.client.call("query_credit_account", params, account_id=account_id)
        # 信封仍是 dict；它的行就是 query_credit_detail 返回的同一种原生两融行，
        # 而 #271 让那些行可属性访问（CompatRow）。这条路径绕过了
        # _query_account_list 因此被漏掉，于是 ``r["rows"][0].m_dAssureAsset``
        # 会抛异常，而详情路径能作答。同样的容器改动，同样的理由。
        if isinstance(answer, dict) and isinstance(answer.get("rows"), list):
            answer = dict(answer)
            answer["rows"] = [_as_compat_row(row) for row in answer["rows"]]
        return answer

    def query_stk_compacts(self, account):
        return self._query_account_list(account, "query_stk_compacts")

    def query_credit_subjects(self, account):
        return self._query_account_list(account, "query_credit_subjects")

    def query_credit_slo_code(self, account):
        return self._query_account_list(account, "query_credit_slo_code")

    def query_credit_assure(self, account):
        return self._query_account_list(account, "query_credit_assure")

    def query_appointment_info(self, account):
        return self._query_account_list(account, "query_appointment_info")

    def query_smt_secu_info(self, account):
        return self._query_account_list(account, "query_smt_secu_info")

    def query_smt_secu_rate(self, account, stock_code, max_term, fare_way, credit_type, trade_type):
        account_id = _account_id(account, self.client.account_id)
        try:
            return self.client.call(
                "query_smt_secu_rate",
                _with_account_type({"account_id": account_id, "stock_code": stock_code, "max_term": max_term,
                                    "fare_way": fare_way, "credit_type": credit_type,
                                    "trade_type": trade_type}, account),
                account_id=account_id,
            ) or []
        except Exception:
            return []

    def query_ipo_data(self, account=None, stock_type=""):
        """新股申购信息 (大 QMT get_ipo_data).
        stock_type: "" 全部, "STOCK" 新股, "BOND" 新债.
        8-28 修复: 直接调 get_ipo_data 带 type 参数 (原走 query_appointment_info
        传 account_id 导致返回空)."""
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "get_ipo_data",
                {"type": stock_type},
                account_id=account_id,
            )
            # get_ipo_data 以申购代码为键的 dict 作答。
            if isinstance(data, dict):
                return data
            if data:
                # 非空且不是 dict：旧服务端仍在把它交给明细行规范化器，而那个
                # 规范化器按键遍历 dict 并丢弃每个值——真实的新股以 [{}, {}]
                # 形式到达。把它强转成 {} 会静默报告“今天没有新股”，所以这里
                # 直接说明，而不是吞掉。
                log.warning(
                    "query_ipo_data：服务端返回 %s，而不是映射对象；"
                    "QMT 端桥接版本过旧，无法保持 get_ipo_data 的返回结构，"
                    "因此结果为空。请更新服务端。",
                    type(data).__name__)
            return {}
        except Exception:
            return {}

    def query_new_purchase_limit(self, account):
        """新股申购额度 (大 QMT get_new_purchase_limit).
        返回 {板块: 额度} 或 {} (失败/无权限)."""
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "get_new_purchase_limit",
                _with_account_type({"account_id": account_id}, account),
                account_id=account_id,
            ) or {}
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}

    def ipo_subscribe_all(self, account=None, stock_type="STOCK",
                          markets=("SH", "SZ"), dry_run=False,
                          strategy_name="ipo"):
        """订阅今天的新股。这里没有任何东西跑在定时器上。

        这有意是一个你去发起的调用，而不是桥接层自己承担的行为：它下真实委托，
        所以必须是操作者在那天要求的。它还会走 order_stock，因此遵守
        rpc_allow_order_methods 并继承网关的 passorder 设置（orderType 1101、
        prType 11 指定价、quickTrade 2——2 是 API 参考对非 K 线上下文要求的值）。

        markets: 要订阅的交易所。SH/SZ 申购由市值支撑、不冻结资金；北交所冻结
            资金，所以默认排除。无法识别所属市场的代码会被 **跳过**，绝不在猜测
            下订阅。
        dry_run: 只返回计划，不下任何单。

        每个候选返回一个 dict：stock_code、name、price、volume、
        action（"subscribed" / "skipped" / "failed"）和 reason。
        """
        allowed = set(str(m).upper() for m in (markets or ()))
        results = []
        for code, info in (self.query_ipo_data(account, stock_type=stock_type) or {}).items():
            info = info or {}
            entry = {
                "stock_code": code,
                "name": str(info.get("name") or ""),
                "price": _safe_float(info.get("issuePrice"), 0.0),
                "volume": _safe_int(info.get("maxPurchaseNum"), 0),
                "action": "skipped",
                "reason": "",
            }
            market = ipo_market_of(code)
            if market is None:
                entry["reason"] = "market not identified"
            elif market not in allowed:
                entry["reason"] = "%s not in %s" % (market, sorted(allowed))
            elif entry["price"] <= 0 or entry["volume"] <= 0:
                entry["reason"] = "issuePrice/maxPurchaseNum missing or non-positive"
            elif dry_run:
                entry["action"] = "planned"
            else:
                try:
                    entry["result"] = self.ipo_subscribe(
                        account, code, entry["volume"], entry["price"],
                        strategy_name=strategy_name,
                        order_remark="ipo:%s" % code)
                    entry["action"] = "subscribed"
                except Exception as exc:
                    entry["action"] = "failed"
                    entry["reason"] = "%s: %s" % (exc.__class__.__name__, exc)
            results.append(entry)
        return results

    def ipo_subscribe(self, account, stock_code, volume, price, strategy_name="ipo",
                      order_remark="ipo_sub"):
        """新股申购 (打新). 复用现有 order_stock RPC (passorder opType=23 指定价).
        stock_code 应为 get_ipo_data 返回的申购代码 (带后缀), price=发行价.
        返回 {order_sys_id} 或 {} (失败)."""
        return self.order_stock_result(
            account, stock_code, STOCK_BUY, int(volume),
            FIX_PRICE, float(price), strategy_name, order_remark,
        )

    # ------------------------------------------------------------------
    # async 变体：MiniQMT 的 *_async 方法返回 seq 后异步回调。
    # 在 RPC 模型里请求-响应本就是同步的，这里直接转发到同步实现并
    # 返回一个递增 seq，让旧代码 ``xt_trader.query_stock_positions_async(acc)``
    # 不报错（回调仍由 register_callback 注册的回调在事件来时触发）。
    # ------------------------------------------------------------------

    _async_seq = 0

    def _next_async_seq(self):
        BigQmtXtTrader._async_seq += 1
        return BigQmtXtTrader._async_seq

    def _async_query(self, sync_call, account, callback, *args, **kwargs):
        """共享的异步查询辅助函数。

        MiniQMT 的 *_async 查询方法接受一个回调并把结果交给它（返回 None）。为
        兼容起见我们接受一个 **可选** 回调：给了就同步调用 callback(result)
        （我们的 RPC 本来就是同步的）并像 MiniQMT 一样返回 None；没给就保留我们
        返回 seq 的扩展，使既有调用方不受影响。
        """
        result = sync_call(account, *args, **kwargs)
        if callback is not None:
            try:
                callback(result)
            except Exception:
                log.exception(
                    "用户回调执行失败：%s",
                    getattr(sync_call, "__name__", "async_query_callback"),
                )
            return None
        return self._next_async_seq()

    def query_stock_asset_async(self, account, callback=None):
        return self._async_query(self.query_stock_asset, account, callback)

    def query_stock_positions_async(self, account, callback=None):
        return self._async_query(self.query_stock_positions, account, callback)

    def query_stock_orders_async(self, account, cancelable_only=False, callback=None):
        if callback is not None:
            result = self.query_stock_orders(account, cancelable_only)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_stock_trades_async(self, account, callback=None):
        return self._async_query(self.query_stock_trades, account, callback)

    def query_account_infos_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_infos(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_account_status_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_status(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_credit_detail_async(self, account, callback=None):
        return self._async_query(self.query_credit_detail, account, callback)

    def query_stk_compacts_async(self, account, callback=None):
        return self._async_query(self.query_stk_compacts, account, callback)

    def query_credit_subjects_async(self, account, callback=None):
        return self._async_query(self.query_credit_subjects, account, callback)

    def query_credit_slo_code_async(self, account, callback=None):
        return self._async_query(self.query_credit_slo_code, account, callback)

    def query_credit_assure_async(self, account, callback=None):
        return self._async_query(self.query_credit_assure, account, callback)

    def query_ipo_data_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_ipo_data(account)
            try:
                callback(result)
            except Exception:
                log.exception("用户回调执行失败：query_ipo_data_async")
            return None
        return self._next_async_seq()

    def query_new_purchase_limit_async(self, account, callback=None):
        return self._async_query(self.query_new_purchase_limit, account, callback)

    def query_appointment_info_async(self, account, callback=None):
        return self._async_query(self.query_appointment_info, account, callback)

    def cancel_order_stock_async(self, account, order_id):
        """把撤单入队并立刻返回其 seq（不阻塞）。

        过去这里就地调用 cancel_order_stock，每笔撤单都要阻塞整个 RPC 往返。
        15 笔撤单各约 2s 串行 = 30s。现在撤单在工作线程上运行，积压会合并成
        一次 RPC（cancel_orders_batch），所以 15 笔撤单总共约 2s。

        结果通过 on_cancel_order_stock_async_response 或 on_cancel_error 在回调
        工作线程上到达。
        """
        seq = self._next_async_seq()
        self._ensure_async_cancel_worker()
        self._register_exit_drain()
        self._async_cancel_queue.put((seq, (account, order_id), {}))
        return seq

    def cancel_order_stock_sysid_async(self, account, market, order_sysid):
        """按 sysid 撤单入队并立刻返回其 seq。"""
        seq = self._next_async_seq()
        self._ensure_async_cancel_worker()
        self._register_exit_drain()
        self._async_cancel_queue.put(
            (seq, (account, order_sysid, market), {})
        )
        return seq

    def set_relaxed_response_order_enabled(self, enabled=True):
        # 内部行为开关，RPC 模式下无意义，no-op。
        return 0

    def smt_appointment_async(self, account, stock_code, apt_days, apt_volume,
                              fare_ratio, sub_rare_ratio, fine_ratio, begin_date):
        # SMB/预约打新走独立通道，RPC 桥不支持；返回 -1 表示失败（对齐 MiniQMT
        # 语义：seq 为 -1 表示委托失败）。
        return -1

    def _account_type_value(self, item=None):
        """XtOrder / XtTrade / XtPosition 的 account_type，int 形式。

        先问服务端（它才知道这个部署交易的是什么类型——#103），其次是调用方
        声明的，最后是 SECURITY_ACCOUNT，使该字段永不缺失。持仓过去硬编码为 2，
        而委托和成交则完全不带它（#133）。
        """
        code = _account_type_code((item or {}).get("account_type"))
        if code:
            return code
        code = _account_type_code(self._server_account_type
                                 or self._declared_account_type)
        if code:
            return code
        try:
            from xtquant.xtconstant import SECURITY_ACCOUNT

            return int(SECURITY_ACCOUNT)
        except Exception:
            return 2

    def _order_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = (option_order_type(item.get("direction"), item.get("offset_flag"), action)
                      if self._account_type_value(item) == 6 else _action_to_order_type(action))
        # #330 跟修：entrust 判据优先，op 判据兜底
        order_type = _credit_order_type_for_row(item, order_type)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or item.get("order_id") or "")
        return CompatObject(
            account_id=account_id,
            stock_code=_full_a_share_code(item.get("stock_code")),
            order_type=order_type,
            order_status=_safe_int(item.get("status", item.get("order_status")), ORDER_UNKNOWN),
            order_volume=_safe_int(item.get("volume", item.get("order_volume"))),
            traded_volume=_safe_int(item.get("traded_volume")),
            price=_safe_float(item.get("price")),
            traded_price=_safe_float(
                item.get("traded_price", item.get("avg_traded_price", item.get("m_dTradedPrice")))
            ),
            # 柜台的精确成交金额 (issue #173) -- ccxt 适配层的
            # order["cost"]。旧部署不发这个键，那就是 0.0；不在这里
            # 拿 traded_price × traded_volume 兑，因为那是估算值，让它
            # 冒充柜台金额正好是这个 issue 要避开的事。
            trade_amount=_safe_float(
                item.get("trade_amount", item.get("m_dTradeAmount"))
            ),
            order_sysid=order_sysid,
            # MiniQMT 里 order_id 是 int 形式的委托编号，order_sysid 是字符串
            # 形式的柜台编号。两者都由同一个合同编号得到（问题 #113）。
            order_id=self._order_object_id(
                order_sysid or str(item.get("user_order_id") or "")),
            strategy_name=str(item.get("strategy_name") or ""),
            order_remark=str(item.get("remark") or item.get("user_order_id") or ""),
            # MiniQMT XtOrder.order_time 是 Unix 秒。服务端订单事件只发
            # created_at_ts 不发 order_time，所以没有显式 order_time 时
            # 用 created_at_ts 兜底；两者都没有才落到 0（不要当成 1970 年）。
            order_time=_safe_int(item.get("order_time") or item.get("created_at_ts"), 0),
            # MiniQMT XtOrder.status_msg —— 废单时柜台给的原因 (issue #60)。
            status_msg=str(item.get("status_msg") or ""),
            price_type=item.get("price_type"),
            # xttype.XtOrder 契约里有、以前没发的字段 (issue #133)。旧部署不发
            # 这些键，所以每个都要能在缺失时给出 MiniQMT 语义的默认值，而不是
            # 让调用方撞 AttributeError —— 那正是这个 issue 报的现象。
            account_type=self._account_type_value(item),
            instrument_name=str(item.get("instrument_name") or ""),
            secu_account=str(item.get("secu_account") or ""),
            offset_flag=item.get("offset_flag"),
            direction=item.get("direction"),
        )

    def _trade_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = (option_order_type(item.get("direction"), item.get("offset_flag"), action)
                      if self._account_type_value(item) == 6 else _action_to_order_type(action))
        order_type = _credit_order_type_for_row(item, order_type)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or "")
        trade_id = str(item.get("trade_id") or "")
        traded_volume = _safe_int(item.get("volume", item.get("traded_volume")))
        traded_price = _safe_float(item.get("price", item.get("traded_price")))
        amount = item.get("amount")
        if not amount:
            # 服务端未取到金额（缺失或 0）时按 价格 * 数量 估算，保证盈亏统计不为 0。
            amount = traded_price * traded_volume
        return CompatObject(
            account_id=account_id,
            stock_code=_full_a_share_code(item.get("stock_code")),
            order_type=order_type,
            order_sysid=order_sysid,
            order_id=self._order_object_id(order_sysid),
            trade_id=trade_id,
            # MiniQMT 字段契约: traded_id/traded_time 是业务代码读取的名字。
            traded_id=trade_id,
            traded_volume=traded_volume,
            traded_price=traded_price,
            # 优先级: 服务端真实成交时间(traded_time) -> 事件到达时间(created_at_ts)
            # -> traded_at 字符串解析。
            traded_time=_to_unix_seconds(
                item.get("traded_time") or item.get("created_at_ts") or item.get("traded_at")
            ),
            traded_amount=_safe_float(amount, 0.0),
            traded_at=str(item.get("traded_at") or ""),
            strategy_name=str(item.get("strategy_name") or ""),
            order_remark=str(item.get("user_order_id") or item.get("remark") or ""),
            # 同 _order_from_dict：xttype.XtTrade 契约里有而以前没发的 (issue #133)。
            account_type=self._account_type_value(item),
            instrument_name=str(item.get("instrument_name") or ""),
            secu_account=str(item.get("secu_account") or ""),
            commission=_safe_float(item.get("commission"), 0.0),
            offset_flag=item.get("offset_flag"),
            direction=item.get("direction"),
        )


XtQuantTrader = BigQmtXtTrader


_default_client = None
xt_trader = None
xtdata = None


def configure(account_id=None, valkey_client=None, valkey_config=None, timeout_seconds=None):
    global _default_client, xt_trader, xtdata
    _default_client = BigQmtRpcClient(
        account_id=account_id,
        valkey_client=valkey_client,
        valkey_config=valkey_config,
        timeout_seconds=timeout_seconds,
    )
    if xt_trader is None:
        xt_trader = BigQmtXtTrader(account_id=_default_client.account_id, valkey_client=_default_client.valkey_client)
    xt_trader.client = _default_client
    if xtdata is None:
        xtdata = BigQmtXtData(_default_client)
    else:
        xtdata.client = _default_client
    return xt_trader, xtdata


def get_default_client():
    global _default_client
    if _default_client is None:
        configure()
    return _default_client


configure()


__all__ = [
    "BigQmtRpcClient",
    "BigQmtXtData",
    "BigQmtXtTrader",
    "CompatObject",
    "StockAccount",
    "XtQuantTrader",
    "XtQuantTraderCallback",
    "configure",
    "get_default_client",
    "load_client_config",
    "xt_trader",
    "xtdata",
]
