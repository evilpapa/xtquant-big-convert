"""VK client helpers for Big QMT signal trader."""

import os


def local_vk_config(module):
    return dict(getattr(module, "BIGQMT_CONFIG", None) or {})


def normalize_vk_url(url):
    scheme, separator, address = url.partition("://")
    scheme = scheme.lower()
    if not separator or scheme not in ("valkey", "redis", "valkeys", "unix"):
        raise ValueError("VK URL must use valkey://, redis://, valkeys:// or unix://")
    return ("valkey" if scheme == "redis" else scheme) + separator + address


def vk_client_class():
    try:
        from valkey import Valkey
    except ImportError as exc:
        raise RuntimeError("Install xtquant-big-convert[valkey] to use VK.") from exc
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


def vk_connection_options(protocol):
    """Use RESP2 by default and avoid retrying writes with unknown outcomes."""
    from valkey.backoff import NoBackoff
    from valkey.retry import Retry

    return {"protocol": int(protocol), "retry": Retry(NoBackoff(), 0)}


def build_vk_client(config=None, *, environ=None):
    config = config or {}
    environ = os.environ if environ is None else environ
    client_class = vk_client_class()

    protocol = int(config.get("protocol") or environ.get("BIGQMT_VK_PROTOCOL") or 2)
    kwargs = dict(
        socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
        socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
    )
    kwargs.update(vk_connection_options(protocol))
    url = config.get("url") or environ.get("BIGQMT_VK_URL")
    if url:
        return client_class.from_url(normalize_vk_url(url), **kwargs)

    db = config.get("db")
    if db is None or db == "":
        db = environ.get("BIGQMT_VK_DB") or 5
    kwargs.update(
        host=config.get("host") or environ.get("BIGQMT_VK_HOST") or "127.0.0.1",
        port=int(config.get("port") or environ.get("BIGQMT_VK_PORT") or 6379),
        db=int(db),
        username=config.get("username") or environ.get("BIGQMT_VK_USERNAME") or None,
        password=config.get("password") or environ.get("BIGQMT_VK_PASSWORD") or None,
        health_check_interval=int(config.get("health_check_interval", 30)),
    )
    return client_class(**kwargs)


def decode_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def vk_mapping_to_text(mapping):
    return {decode_text(key): decode_text(value) for key, value in (mapping or {}).items()}


# 事件流的滑动过期时间。
#
# maxlen 只挡住单个键的无限增长，挡不住**键本身永远不消失**：换个账号、停用一个
# 部署、跑一次探测，键就永远留在 VK 里。实测线上七个事件流全是永久键
# （position_events 843KB、order_events 1.41MB…）。
#
# 所以每次 xadd 之后顺手续一次期：还在写的流一直续上，停了的自然消失。一天足够
# 覆盖「昨天收盘到今天开盘」的回放需求，又不会让废弃账号的键长期占着（#213）。
EVENT_STREAM_TTL_SECONDS = 86400


def touch_stream_ttl(vk_client, key, ttl_seconds=EVENT_STREAM_TTL_SECONDS):
    """给事件流续期。失败不能影响写入本身 —— 这是清理，不是主路径。"""
    try:
        ttl = int(ttl_seconds)
    except (TypeError, ValueError):
        return False
    if ttl <= 0:
        return False
    try:
        vk_client.expire(key, ttl)
        return True
    except Exception:
        return False


_STREAMS_DEAD = False


def streams_dead():
    return _STREAMS_DEAD


def note_stream_failure(exc, log=None):
    """当端点报告 XADD 不可用时为 true。

    第一个此类故障会记录一次，并将流标记为进程范围内的死亡。
    任何其他异常都会返回 False 并且不会更改任何内容（瞬态 VK 问题仍需重试）。
    """
    global _STREAMS_DEAD
    if "unknown command" not in str(exc or "").lower():
        return False
    if _STREAMS_DEAD:
        return True
    _STREAMS_DEAD = True
    message = (
        "The VK endpoint did not recognize XADD: replay streams are disabled "
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
