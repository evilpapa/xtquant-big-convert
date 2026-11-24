"""Big QMT FormulaServer RPC 的直接客户端（默认端口 58600）。

为何需要它
---------------
:mod:`vk_rpc` 中的 RPC 桥接会将所有读取操作都路由到 QMT *策略*进程：
客户端 -> VK / zmq -> QMT Python 线程 -> ContextInfo -> 返回。每次读取都需要约
13 ms（VK），或约 0.7 ms（zmq），但在 GIL 抛转 500 ms 的情况下会出现延迟，而且每次读取都会
与策略自身争抢 QMT 主线程的 GIL。

FormulaServer 是同一 QMT 终端内的 C++ 行情/参考数据服务，通过
``config/formulaserver/formulaserver.ini`` 中的
``[server_formula] address`` 指定监听端口（默认为 58600）。QMT 自带的客户端位于
``bin.x64/Lib/site-packages/qmt_api``。直接通信可以完全绕过策略进程：实测 p50
为 **0.07 ms**，且不存在 GIL 竞争。

能做什么与不能做什么
-------------------------
FormulaServer 仅提供行情/参考数据。所有账户、持仓、委托和成交方法都会返回
``ErrorID 200005 未找到该服务``，``getFullTick`` 和 ``getQuote`` 也是如此。因此，
这是只读快速通道，绝不能替代 RPC 桥接；交易、账户查询和五档快照仍应走桥接。

尽管 FormulaServer 暴露了类似接口，但以下方法仍被刻意排除在路由之外：

* ``get_trading_dates``——FormulaServer 要求传入*股票代码*（如
  ``000001.SZ``）；传入市场（如 ``SH``）会静默返回 ``[]``，而我们的调用方传入的正是市场。
* ``get_instrument`` / ``get_instrument_detail``——在国金构建中，
  此处的 getInstrumentDetail 返回的日参考字段（TotalVolume、OpenDate）为零或过期快照，
  而桥接的 ContextInfo 路径能正确返回（#392）。合约元数据属于参考数据记录，
  因此应通过桥接读取。
* ``get_divid_factors`` / ``get_risk_free_rate``——参数语义不同
  （区间与单个日期、index 与 timetag）。日历或股息因子一旦错误，比请求较慢更糟糕。
  2026-09-04 曾有人（就是我本人）尝试将其路由过去，理由是速度快 40 候且不会抛异常：
  查询 000001.SZ 在 20260612 和 20251015 的数据时，FormulaServer 两次都返回了相同记录
  ``{673113600000: [0.3, 0.4, ...]}``，这是一个 1991 年的时间戳；而桥接在这两个除权日中
  正确返回的是 0.36 和 0.236。速度快、没有异常都不能证明答案正确；必须校验数值。
* 复权行情——参见 :func:`_market_data_params`；``dividendType`` 似乎被服务端忽略，
  因此只路由不复权请求。

这里的每个失败都不影响主流程：:class:`FormulaServerRouter` 会将该方法报告为不可路由，
调用方随后回退到普通 RPC 路径。
"""

import os
import socket
import struct
import threading
import time
import zlib


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 58600
DEFAULT_TIMEOUT_SECONDS = 3.0
# After a transport failure, stop trying for this long so a dead/absent
# FormulaServer costs one timeout rather than one per call.
DEFAULT_FAILURE_COOLDOWN_SECONDS = 30.0

NET_CMD_RPC = 3
COMPRESS_ZLIB = 1
COMPRESS_DOUBLE_ZLIB = 2

# FormulaServer's "method not found" code. Distinct from a transport failure:
# it means the server is healthy and simply does not implement the call.
ERROR_METHOD_NOT_FOUND = 200005


class FormulaServerError(RuntimeError):
    """FormulaServer answered with a non-zero status (bad params, no such method)."""

    def __init__(self, message, error_id=None):
        RuntimeError.__init__(self, message)
        self.error_id = error_id


class FormulaServerUnavailable(RuntimeError):
    """The FormulaServer could not be reached (connect/IO/protocol failure)."""


# ---------------------------------------------------------------------------
# BSON codec
# ---------------------------------------------------------------------------
# FormulaServer frames BSON documents. pymongo's ``bson`` is used when present
# (faster, battle-tested); otherwise the minimal codec below covers the types
# this wire actually carries. Keeping a built-in path means an external client
# needs no pymongo just to read market data.

def _load_bson():
    for module_name in ("bson", "xtquant.xtbson.bson36"):
        try:
            module = __import__(module_name, fromlist=["BSON"])
        except Exception:
            continue
        if hasattr(module, "BSON"):
            return module
    return None


_BSON = _load_bson()


def _encode_document(pairs):
    body = b"".join(_encode_element(str(key), value) for key, value in pairs)
    return struct.pack("<i", len(body) + 5) + body + b"\x00"


def _encode_element(name, value):
    key = name.encode("utf-8") + b"\x00"
    if value is None:
        return b"\x0a" + key
    # bool before int: bool is an int subclass.
    if isinstance(value, bool):
        return b"\x08" + key + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        if -2147483648 <= value <= 2147483647:
            return b"\x10" + key + struct.pack("<i", value)
        return b"\x12" + key + struct.pack("<q", value)
    if isinstance(value, float):
        return b"\x01" + key + struct.pack("<d", value)
    if isinstance(value, bytes):
        return b"\x05" + key + struct.pack("<i", len(value)) + b"\x00" + value
    if isinstance(value, str):
        raw = value.encode("utf-8") + b"\x00"
        return b"\x02" + key + struct.pack("<i", len(raw)) + raw
    if isinstance(value, (list, tuple)):
        return b"\x04" + key + _encode_document(
            (str(index), item) for index, item in enumerate(value)
        )
    if isinstance(value, dict):
        return b"\x03" + key + _encode_document(value.items())
    raise TypeError("cannot BSON-encode %s" % type(value).__name__)


def _decode_document(data, pos):
    size = struct.unpack_from("<i", data, pos)[0]
    end = pos + size
    pos += 4
    out = {}
    while pos < end - 1:
        type_byte = data[pos] if isinstance(data[pos], int) else ord(data[pos])
        pos += 1
        terminator = data.index(b"\x00", pos)
        name = data[pos:terminator].decode("utf-8", "replace")
        pos = terminator + 1
        out[name], pos = _decode_element(type_byte, data, pos)
    return out, end


def _decode_element(type_byte, data, pos):
    if type_byte == 0x01:
        return struct.unpack_from("<d", data, pos)[0], pos + 8
    if type_byte == 0x02:
        length = struct.unpack_from("<i", data, pos)[0]
        pos += 4
        return data[pos:pos + length - 1].decode("utf-8", "replace"), pos + length
    if type_byte == 0x03:
        return _decode_document(data, pos)
    if type_byte == 0x04:
        doc, end = _decode_document(data, pos)
        try:
            ordered = sorted(doc, key=lambda key: int(key))
        except (TypeError, ValueError):
            ordered = sorted(doc)
        return [doc[key] for key in ordered], end
    if type_byte == 0x05:
        length = struct.unpack_from("<i", data, pos)[0]
        pos += 5  # int32 length + 1 subtype byte
        return data[pos:pos + length], pos + length
    if type_byte == 0x08:
        flag = data[pos] if isinstance(data[pos], int) else ord(data[pos])
        return bool(flag), pos + 1
    if type_byte in (0x09, 0x12):
        return struct.unpack_from("<q", data, pos)[0], pos + 8
    if type_byte == 0x0A:
        return None, pos
    if type_byte == 0x10:
        return struct.unpack_from("<i", data, pos)[0], pos + 4
    if type_byte == 0x11:
        return struct.unpack_from("<Q", data, pos)[0], pos + 8
    raise ValueError("unsupported BSON type byte 0x%02x" % type_byte)


def bson_encode(document):
    if _BSON is not None:
        return _BSON.BSON.encode(document)
    return _encode_document(document.items())


def bson_decode(payload):
    if _BSON is not None:
        return _BSON.BSON(payload).decode()
    return _decode_document(payload, 0)[0]


# ---------------------------------------------------------------------------
# Address discovery
# ---------------------------------------------------------------------------

def read_formulaserver_port(qmt_root):
    """Read ``[server_formula] address`` from a QMT install's formulaserver.ini.

    ``qmt_root`` is the terminal directory (the one holding ``bin.x64`` and
    ``config``). Returns the port int, or None when the file is absent or
    unparsable — callers then fall back to :data:`DEFAULT_PORT`.
    """
    if not qmt_root:
        return None
    path = os.path.join(str(qmt_root), "config", "formulaserver", "formulaserver.ini")
    try:
        try:
            import configparser
        except ImportError:  # pragma: no cover - py2 safety net
            import ConfigParser as configparser
        parser = configparser.ConfigParser()
        if not parser.read(path):
            return None
        address = parser.get("server_formula", "address")
    except Exception:
        return None
    if ":" not in str(address):
        return None
    try:
        return int(str(address).rsplit(":", 1)[1])
    except (TypeError, ValueError):
        return None


def resolve_address(config=None):
    """Resolve (host, port) for the FormulaServer.

    Priority: explicit ``host``/``port`` > ``formulaserver.ini`` under
    ``qmt_root`` > ``BIGQMT_FORMULA_HOST``/``BIGQMT_FORMULA_PORT`` > defaults.
    The address binds ``0.0.0.0`` in QMT's shipped config, so a remote client
    can reach it too when the firewall allows.
    """
    config = dict(config or {})
    host = str(config.get("host") or os.environ.get("BIGQMT_FORMULA_HOST") or DEFAULT_HOST)
    port = config.get("port")
    if not port:
        port = read_formulaserver_port(config.get("qmt_root"))
    if not port:
        port = os.environ.get("BIGQMT_FORMULA_PORT")
    try:
        port = int(port)
    except (TypeError, ValueError):
        port = DEFAULT_PORT
    return host, port


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class FormulaServerClient(object):
    """Thread-safe BSON-over-TCP client for FormulaServer.

    One socket is shared under a lock. FormulaServer matches responses by
    sequence number, so concurrent use of a single socket would require
    demultiplexing; serializing is simpler and, at 0.07ms per call, ample.
    """

    def __init__(
        self,
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        print_prefix="[bigqmt_formula]",
    ):
        self.host = str(host or DEFAULT_HOST)
        self.port = int(port or DEFAULT_PORT)
        self.timeout_seconds = float(timeout_seconds or DEFAULT_TIMEOUT_SECONDS)
        self.print_prefix = print_prefix
        self._lock = threading.Lock()
        self._socket = None
        self._seq = 0

    # -- wire ------------------------------------------------------------
    def _connect_locked(self):
        if self._socket is not None:
            return self._socket
        try:
            sock = socket.create_connection((self.host, self.port), self.timeout_seconds)
            sock.settimeout(self.timeout_seconds)
        except Exception as exc:
            raise FormulaServerUnavailable(
                "connect %s:%s failed: %s" % (self.host, self.port, exc)
            )
        self._socket = sock
        return sock

    def _close_locked(self):
        if self._socket is not None:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def close(self):
        with self._lock:
            self._close_locked()

    def _recv_exactly(self, sock, length):
        chunks = []
        remaining = length
        while remaining > 0:
            more = sock.recv(remaining)
            if not more:
                raise FormulaServerUnavailable("socket closed mid-message")
            chunks.append(more)
            remaining -= len(more)
        return b"".join(chunks)

    def _request_locked(self, func, params):
        sock = self._connect_locked()
        self._seq += 1
        seq = self._seq
        body = bson_encode({"func": str(func), "params": dict(params or {})})
        tag = ((seq >> 32) & 0x0F) << 8
        packet = struct.pack(
            "!IIHH%ds" % len(body),
            len(body) + 12,
            seq & 0xFFFFFFFF,
            NET_CMD_RPC,
            tag,
            body,
        )
        try:
            sock.sendall(packet)
        except Exception as exc:
            raise FormulaServerUnavailable("send failed: %s" % exc)
        # Subscription pushes share the socket; skip anything that is not our seq.
        while True:
            try:
                header = self._recv_exactly(sock, 4)
                pack_len = struct.unpack_from("!I", header, 0)[0]
                rest = self._recv_exactly(sock, pack_len - 4)
            except FormulaServerUnavailable:
                raise
            except Exception as exc:
                raise FormulaServerUnavailable("recv failed: %s" % exc)
            raw = header + rest
            try:
                got_seq, _cmd, got_tag, payload_bytes = struct.unpack_from(
                    "!IHH%ds" % (pack_len - 12), raw, 4
                )
                if (got_tag & 7) in (COMPRESS_ZLIB, COMPRESS_DOUBLE_ZLIB):
                    payload_bytes = zlib.decompress(payload_bytes)
                got_seq = ((got_tag >> 8) & 0x0F) << 32 | got_seq
                payload = bson_decode(payload_bytes)
            except Exception as exc:
                raise FormulaServerUnavailable("decode failed: %s" % exc)
            if got_seq != seq:
                continue
            if payload.get("status") == 0:
                return payload.get("params")
            detail = payload.get("params")
            error_id = None
            if isinstance(detail, dict):
                error_id = detail.get("ErrorID")
            raise FormulaServerError(
                "%s failed: %r" % (func, detail), error_id=error_id
            )

    def request(self, func, params=None):
        """Call ``func`` and return its ``params`` payload.

        Retries once on a transport failure, since QMT restarts (or an idle
        socket reaped by the server) show up as a dead socket on first use.
        """
        with self._lock:
            try:
                return self._request_locked(func, params)
            except FormulaServerError:
                raise
            except FormulaServerUnavailable:
                self._close_locked()
                return self._request_locked(func, params)

    def ping(self):
        """Cheap liveness probe. True when FormulaServer answers at all.

        A ``FormulaServerError`` still counts as alive — the server replied.
        """
        try:
            self.request("getLastVolume", {"stockCode": "000001.SZ"})
            return True
        except FormulaServerError:
            return True
        except FormulaServerUnavailable:
            return False

    def __repr__(self):
        return "<FormulaServerClient %s:%s>" % (self.host, self.port)


# ---------------------------------------------------------------------------
# Method mapping
# ---------------------------------------------------------------------------

def _first(params, names, default=None):
    for name in names:
        if name in params and params[name] is not None:
            return params[name]
    return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _require_code(params, names):
    code = _first(params, names)
    text = str(code or "").strip()
    if not text:
        raise ValueError("a stock code is required (one of %s)" % ", ".join(names))
    return text


def _scalar_result(raw, params):
    return (raw or {}).get("result")


def _list_result(raw, params):
    return (raw or {}).get("result") or []


def _last_volume_params(params):
    return {"stockCode": _require_code(params, ("stock", "code", "stock_code", "stockcode"))}


def _total_share_params(params):
    return {"stockCode": _require_code(params, ("stockcode", "code", "stock_code", "stock"))}


def _contract_multiplier_params(params):
    return {"contractCode": _require_code(params, ("stockcode", "code", "stock_code", "contract_code"))}


def _main_contract_params(params):
    return {"codeMarket": _require_code(params, ("code_market", "codeMarket", "code"))}


def _sector_params(params):
    name = str(_first(params, ("sector_name", "sectorName", "sector"), "") or "").strip()
    if not name:
        raise ValueError("sector_name is required")
    # ContextInfo's real_timetag defaults to -1; FormulaServer's realtime
    # defaults to 0. Both return identical constituents (verified), so normalize
    # the sentinel rather than forward a value the server never documents.
    realtime = _first(params, ("real_timetag", "realtime"), 0)
    try:
        realtime = int(realtime)
    except (TypeError, ValueError):
        realtime = 0
    if realtime < 0:
        realtime = 0
    return {"sectorName": name, "realtime": realtime}


def _weight_in_index_params(params):
    index_code = _first(params, ("mtkindexcode", "index_code", "indexCode"))
    stock_code = _first(params, ("stockcode", "stock_code", "code"))
    if not index_code or not stock_code:
        raise ValueError("mtkindexcode and stockcode are required")
    return {"indexCode": str(index_code), "stockCode": str(stock_code)}


# What FormulaServer actually answers with data, measured against a live
# terminal. It accepts ANY field name and returns a column either way -- the
# ones outside this set come back as all-NaN, which looks like an answer and
# is not:
#
#     field_list=[...11 names...]  0.015s  preClose=nan   suspendFlag=nan
#     field_list=[]                (RPC)   preClose=9.0   suspendFlag=0
#
# Same shape, same column count, twelve times faster, silently wrong. The four
# it cannot serve are daily metadata (settelementPrice, openInterest, preClose,
# suspendFlag) rather than bar data, which is why they are missing here.
#
# A whitelist, not a blacklist: an unfamiliar field name goes to RPC and is
# merely slow. Guessing that it might be served risks being quietly wrong,
# which is the failure this guard exists to remove.
SERVED_FIELDS = frozenset((
    "open", "high", "low", "close", "volume", "amount", "time", "stime",
))


def _market_data_params(params):
    fields = _as_list(_first(params, ("field_list", "fields"), None))
    codes = _as_list(_first(params, ("stock_list", "stock_code", "stockCodes"), None))
    if not fields or not codes:
        raise ValueError("field_list and stock_list are required")
    unservable = sorted(set(str(field) for field in fields) - SERVED_FIELDS)
    if unservable:
        # Same rule as the dividend_type guard below: a request this path
        # cannot answer honestly goes to RPC instead of coming back as NaN.
        raise ValueError(
            "field(s) %s come back as NaN here; RPC has the real values"
            % ", ".join(unservable))
    dividend_type = str(params.get("dividend_type") or "none").lower()
    # FormulaServer returns byte-identical bars for dividendType none/front, so
    # adjustment is not applied here. Serving an adjusted request from this path
    # would silently hand back unadjusted prices — refuse and let RPC answer.
    if dividend_type not in ("", "none"):
        raise ValueError("adjusted bars (dividend_type=%s) are not served here" % dividend_type)
    period = str(params.get("period") or "1d")
    # FormulaServer 只服务 K 线周期；tick（分笔）/L2 类周期它静默返回空——
    # 数据明明在本地却读到 0 行（issue #66）。拒绝路由，让 RPC 桥回答。
    if period == "tick" or period.startswith("l2"):
        raise ValueError("period=%s is not served by FormulaServer" % period)
    # synth_fallback_only（#237）是桥端的诊断开关：FormulaServer 没有这个概念，
    # 照常回答就会让「回落路径通了」看起来成立，而回落根本没跑。拒绝路由。
    if str(params.get("synth_fallback_only") or "").strip().lower() not in (
            "", "0", "false", "no", "none"):
        raise ValueError("synth_fallback_only is served by the RPC bridge only")
    count = params.get("count", -1)
    try:
        count = int(count)
    except (TypeError, ValueError):
        count = -1
    return {
        "fields": [str(field) for field in fields],
        "stockCodes": [str(code) for code in codes],
        "startTime": str(params.get("start_time") or ""),
        "endTime": str(params.get("end_time") or ""),
        "period": period,
        "dividendType": "none",
        "count": count,
    }


def _market_data_result(raw, params):
    """Translate FormulaServer's flat bar list into the RPC path's payload.

    Wire shape is ``[code, [time, [field, value, ...], time, [...]], code, ...]``.
    We emit the same ``__bigqmt_type__: DataFrame`` envelope the QMT-side adapter
    builds, so the client's ``_restore_jsonable`` rebuilds identical DataFrames
    whichever path answered.
    """
    flat = (raw or {}).get("result") or []
    fields = [str(field) for field in (_first(params, ("field_list", "fields"), None) or [])]
    columns = list(fields)
    if columns and "stime" not in columns:
        columns.insert(0, "stime")

    parsed = {}
    for index in range(0, len(flat) - 1, 2):
        code = str(flat[index])
        timeline = flat[index + 1] or []
        records = []
        for offset in range(0, len(timeline) - 1, 2):
            stamp = timeline[offset]
            pairs = timeline[offset + 1] or []
            record = {"stime": stamp}
            for cursor in range(0, len(pairs) - 1, 2):
                record[str(pairs[cursor])] = pairs[cursor + 1]
            records.append(record)
        parsed[code] = records

    requested = [str(code) for code in _as_list(_first(params, ("stock_list", "stock_code"), None))]
    for code in parsed:
        if code not in requested:
            requested.append(code)
    return {
        code: {
            "__bigqmt_type__": "DataFrame",
            "columns": columns,
            "records": parsed.get(code) or [],
        }
        for code in requested
    }


# our RPC method -> (FormulaServer func, param builder, result adapter)
METHOD_MAP = {
    # get_instrument / get_instrument_detail / get_instrumentdetail used to
    # map to getInstrumentDetail here. Removed in #392: on the Guojin build
    # FormulaServer answers the daily reference fields dishonestly --
    # TotalVolume 0 for 43% of a 300-code sample, OpenDate 0 for 32%, and
    # 22% non-zero but stale (up to 87.7% off) -- while the RPC path
    # (ContextInfo.get_instrument_detail) answered all 300 correctly. A
    # well-formed answer whose values are wrong is worse than no answer,
    # and the result adapter cannot tell stale from true, so these read
    # through the bridge like every other record of reference data.
    "get_last_volume": ("getLastVolume", _last_volume_params, _scalar_result),
    "get_total_share": ("getTotalShare", _total_share_params, _scalar_result),
    "get_contract_multiplier": ("getContractMultiplier", _contract_multiplier_params, _scalar_result),
    "get_main_contract": ("getMainContract", _main_contract_params, _scalar_result),
    "get_weight_in_index": ("getWeightInIndex", _weight_in_index_params, _scalar_result),
    "get_stock_list_in_sector": ("getStockListInSector", _sector_params, _list_result),
    "get_market_data_ex": ("getMarketData", _market_data_params, _market_data_result),
    # Same bars, different contract: get_market_data documents
    # dict[field]->wide-frame, which the client pivots after this answers.
    # Routing it here takes a 500+-code read off the QMT strategy thread
    # (repeated RPC timeouts, 2026-09-10 report).
    "get_market_data": ("getMarketData", _market_data_params, _market_data_result),
}

SUPPORTED_METHODS = tuple(sorted(METHOD_MAP))


class Unroutable(Exception):
    """This call cannot be served by FormulaServer — use the RPC bridge."""


class FormulaServerRouter(object):
    """Routes supported read methods to FormulaServer, or declines.

    :meth:`call` raises :class:`Unroutable` for anything it cannot serve —
    method not mapped, params that do not translate, server down, feature
    disabled. Callers treat that as "fall back to RPC".
    """

    def __init__(
        self,
        client=None,
        enabled=True,
        methods=None,
        failure_cooldown_seconds=DEFAULT_FAILURE_COOLDOWN_SECONDS,
        print_prefix="[bigqmt_formula]",
        config=None,
    ):
        self.enabled = bool(enabled)
        self.print_prefix = print_prefix
        self.failure_cooldown_seconds = float(
            failure_cooldown_seconds or DEFAULT_FAILURE_COOLDOWN_SECONDS
        )
        if client is None and self.enabled:
            host, port = resolve_address(config)
            timeout = float((config or {}).get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
            client = FormulaServerClient(
                host=host, port=port, timeout_seconds=timeout, print_prefix=print_prefix
            )
        self.client = client
        if methods:
            self.methods = set(str(name) for name in methods) & set(METHOD_MAP)
        else:
            self.methods = set(METHOD_MAP)
        self._unavailable_until = 0.0
        self._announced = False
        # Methods the server itself rejected as unimplemented — never retried.
        self._unimplemented = set()
        self.hits = 0
        self.misses = 0

    def _available(self):
        if not self.enabled or self.client is None:
            return False
        return time.time() >= self._unavailable_until

    def _mark_unavailable(self, reason):
        self._unavailable_until = time.time() + self.failure_cooldown_seconds
        print(
            "%s 暂不可用，接下来 %.0f 秒走 RPC：%s"
            % (self.print_prefix, self.failure_cooldown_seconds, reason)
        )

    def supports(self, method):
        return (
            str(method) in self.methods
            and str(method) not in self._unimplemented
            and self._available()
        )

    def call(self, method, params=None):
        """Serve ``method`` from FormulaServer, or raise :class:`Unroutable`."""
        method = str(method)
        if not self.supports(method):
            raise Unroutable(method)
        func, build_params, adapt_result = METHOD_MAP[method]
        try:
            wire_params = build_params(dict(params or {}))
        except Exception as exc:
            # Params that do not translate are a per-call condition, not a
            # server fault — do not trip the breaker.
            self.misses += 1
            raise Unroutable("%s: %s" % (method, exc))
        try:
            raw = self.client.request(func, wire_params)
        except FormulaServerError as exc:
            self.misses += 1
            if exc.error_id == ERROR_METHOD_NOT_FOUND:
                self._unimplemented.add(method)
                print(
                    "%s 此终端未实现 %s，后续调用走 RPC"
                    % (self.print_prefix, method)
                )
            raise Unroutable("%s: %s" % (method, exc))
        except FormulaServerUnavailable as exc:
            self.misses += 1
            self._mark_unavailable(str(exc))
            raise Unroutable("%s: %s" % (method, exc))
        except Exception as exc:
            self.misses += 1
            self._mark_unavailable("%s: %s" % (exc.__class__.__name__, exc))
            raise Unroutable("%s: %s" % (method, exc))
        try:
            result = adapt_result(raw, dict(params or {}))
        except Exception as exc:
            self.misses += 1
            raise Unroutable("%s: result adaptation failed: %s" % (method, exc))
        self.hits += 1
        if not self._announced:
            self._announced = True
            print(
                "%s 已连接 %s:%s（%d 个方法可直读）"
                % (self.print_prefix, self.client.host, self.client.port, len(self.methods))
            )
        return result

    def stats(self):
        return {
            "enabled": self.enabled,
            "hits": self.hits,
            "misses": self.misses,
            "available": self._available(),
            "unimplemented": sorted(self._unimplemented),
            "methods": sorted(self.methods),
        }

    def close(self):
        if self.client is not None:
            self.client.close()


def build_router(config=None, print_prefix="[bigqmt_formula]"):
    """Build a router from a ``formula_server`` config dict.

    Recognised keys: ``enabled`` (default True), ``host``, ``port``,
    ``qmt_root``, ``timeout_seconds``, ``methods``, ``failure_cooldown_seconds``.
    ``enabled=False`` yields a router that declines everything, so callers need
    no None checks.
    """
    config = dict(config or {})
    enabled = config.get("enabled", True)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() not in ("0", "false", "no", "off")
    return FormulaServerRouter(
        enabled=bool(enabled),
        methods=config.get("methods"),
        failure_cooldown_seconds=config.get("failure_cooldown_seconds"),
        print_prefix=print_prefix,
        config=config,
    )
