"""Transport factory: pick a transport backend by name.

``build_transport(name, config, ...)`` returns a ready transport instance.
``name`` is the ``rpc.transport`` config value (default ``"valkey"``). Unknown
names raise :class:`ValueError`.
"""

from .base import RpcTransport
from .vk_transport import VkTransport


KNOWN_TRANSPORTS = ("valkey", "serial", "shm")


def transport_supports_drain(name):
    """这个传输自己实现了 drain_request_queue 吗（不构造实例）。

    调用方要判断「能不能走 adjust 轮询」，而**构造实例是有副作用的** ——
    建 VK 连接、起线程。用它做探测污染过测试环境一次，所以这里只导入
    模块、比对类属性，不 build。

    工厂是「有哪些传输」的唯一来源，这个判断也该在这里，而不是让调用方
    各自维护一张名单。
    """
    from .base import RpcTransport

    name = str(name or "").lower()
    try:
        if name in ("valkey", "", "default"):
            from .vk_transport import VkTransport as cls
        elif name == "serial":
            from .serial_transport import SerialTransport as cls
        else:
            return False
    except ImportError:
        # 只吞 ImportError，AttributeError 之类必须炸出来。
        return False
    own = getattr(cls, "drain_request_queue", None)
    return own is not None and own is not getattr(RpcTransport, "drain_request_queue", None)


def build_transport(
    name,
    config=None,
    account_id="",
    print_prefix="[bigqmt_rpc]",
):
    """Construct a transport by name.

    ``config`` is the ``rpc`` config dict.
    VK reads the channel and queue keys
    (``request_channel_template`` etc.) plus ``vk_client``/
    ``response_vk_client`` that the caller may inject.
    """
    config = config or {}
    name = str(name or "").lower()

    if name in ("valkey", "", "default"):
        return _build_vk(config, account_id, print_prefix)
    if name == "serial":
        from .serial_transport import SerialTransport

        return SerialTransport(account_id=account_id, print_prefix=print_prefix,
                               **dict(config.get("serial") or {}))
    if name == "shm":
        return _build_shm(config, account_id, print_prefix)
    raise ValueError(
        "unknown rpc transport %r (known: %s)" % (name, ", ".join(KNOWN_TRANSPORTS))
    )


def _build_vk(config, account_id, print_prefix):
    vk_client = config.get("vk_client")
    if vk_client is None:
        from ..adapters.vk_common import build_vk_client

        vk_config = dict(config.get("valkey") or {})
        vk_client = build_vk_client(vk_config)
    response_vk_client = config.get("response_vk_client")
    if response_vk_client is None:
        response_vk_client = vk_client
    return VkTransport(
        vk_client,
        account_id=account_id,
        response_vk_client=response_vk_client,
        request_channel_template=config.get(
            "request_channel_template", "bigqmt:rpc:req:{account_id}"
        ),
        request_queue_template=config.get(
            "request_queue_template", "bigqmt:rpc:queue:{account_id}"
        ),
        response_channel_template=config.get(
            "response_channel_template", "bigqmt:rpc:resp:{account_id}:{request_id}"
        ),
        response_list_template=config.get(
            "response_list_template", "bigqmt:rpc:respq:{account_id}:{request_id}"
        ),
        response_key_template=config.get(
            "response_key_template", "bigqmt:rpc:resp:{account_id}:{request_id}"
        ),
        response_ttl_seconds=int(config.get("response_ttl_seconds", 60)),
        queue_poll_interval_seconds=float(config.get("queue_poll_interval_seconds", 0.02)),
        debug_log_limit=int(config.get("debug_log_limit", 0)),
        print_prefix=print_prefix,
    )


def _build_shm(config, account_id, print_prefix):
    from .shm_transport import SharedMemoryTransport

    return SharedMemoryTransport(
        account_id=account_id,
        print_prefix=print_prefix,
        **dict(config.get("shm") or {})
    )
