"""Valkey client helpers for Big QMT signal trader."""

import os


def local_valkey_config(module):
    return dict(getattr(module, "BIGQMT_CONFIG", getattr(module, "BIGQMT_CONFIG", None)) or {})


def normalize_valkey_url(url):
    scheme, separator, address = url.partition("://")
    scheme = scheme.lower()
    if not separator or scheme not in ("valkey", "redis", "valkeys", "unix"):
        raise ValueError("Valkey URL must use valkey://, redis://, valkeys:// or unix://")
    return ("valkey" if scheme == "redis" else scheme) + separator + address


def valkey_client_class():
    try:
        from valkey import Valkey
    except ImportError as exc:
        raise RuntimeError("Install xtquant-big-convert[valkey] to use Valkey.") from exc
    return Valkey


def _float_or_none(value, default=None):
    if value is None:
        return default
    if value == "":
        return default
    text = str(value).strip()
    if text.lower() in ("none", "null"):
        return None
    return float(value)


def valkey_connection_options(protocol):
    """Use RESP2 by default and avoid retrying writes with unknown outcomes."""
    from valkey.backoff import NoBackoff
    from valkey.retry import Retry

    return {"protocol": int(protocol), "retry": Retry(NoBackoff(), 0)}


def build_valkey_client(config=None):
    config = config or {}
    client_class = valkey_client_class()

    protocol = int(config.get("protocol") or os.environ.get("BIGQMT_VALKEY_PROTOCOL") or 2)
    kwargs = dict(
        socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
        socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
    )
    kwargs.update(valkey_connection_options(protocol))
    url = config.get("url") or os.environ.get("BIGQMT_VALKEY_URL")
    if url:
        return client_class.from_url(normalize_valkey_url(url), **kwargs)

    db = config.get("db")
    if db is None or db == "":
        db = os.environ.get("BIGQMT_VALKEY_DB") or 5
    kwargs.update(
        host=config.get("host") or os.environ.get("BIGQMT_VALKEY_HOST") or "127.0.0.1",
        port=int(config.get("port") or os.environ.get("BIGQMT_VALKEY_PORT") or 6379),
        db=int(db),
        username=config.get("username") or os.environ.get("BIGQMT_VALKEY_USERNAME") or None,
        password=config.get("password") or os.environ.get("BIGQMT_VALKEY_PASSWORD") or None,
        health_check_interval=int(config.get("health_check_interval", 30)),
    )
    return client_class(**kwargs)


def decode_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def valkey_mapping_to_text(mapping):
    return {decode_text(key): decode_text(value) for key, value in (mapping or {}).items()}


# 事件流的滑动过期时间。
#
# maxlen 只挡住单个键的无限增长，挡不住**键本身永远不消失**：换个账号、停用一个
# 部署、跑一次探测，键就永远留在 valkey 里。实测线上七个事件流全是永久键
# （position_events 843KB、order_events 1.41MB…）。
#
# 所以每次 xadd 之后顺手续一次期：还在写的流一直续上，停了的自然消失。一天足够
# 覆盖「昨天收盘到今天开盘」的回放需求，又不会让废弃账号的键长期占着（#213）。
EVENT_STREAM_TTL_SECONDS = 86400


def touch_stream_ttl(valkey_client, key, ttl_seconds=EVENT_STREAM_TTL_SECONDS):
    """给事件流续期。失败不能影响写入本身 —— 这是清理，不是主路径。"""
    try:
        ttl = int(ttl_seconds)
    except (TypeError, ValueError):
        return False
    if ttl <= 0:
        return False
    try:
        valkey_client.expire(key, ttl)
        return True
    except Exception:
        return False


_STREAMS_DEAD = False


def streams_dead():
    return _STREAMS_DEAD


def note_stream_failure(exc, log=None):
    """True when the endpoint reports that XADD is unavailable.

    The first such failure logs once and marks streams dead process-wide.
    Any other exception returns False and changes nothing (transient valkey
    issues stay retried).
    """
    global _STREAMS_DEAD
    if "unknown command" not in str(exc or "").lower():
        return False
    if _STREAMS_DEAD:
        return True
    _STREAMS_DEAD = True
    message = (
        "The Valkey endpoint did not recognize XADD: replay streams are disabled "
        "for this process; pub/sub callbacks keep working. Verify that the endpoint "
        "supports streams to restore replay."
    )
    try:
        if log is not None:
            log.warning(message)
        else:
            print("[bigqmt] " + message)
    except Exception:
        pass
    return True
