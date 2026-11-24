# coding: utf-8
"""Multi-account RPC service: one strategy instance serving multiple accounts.

When ``BIGQMT_ACCOUNT_TYPE_MAP`` (in the local config) maps multiple account IDs
to different account types (e.g. ``{"123456": "STOCK", "789012": "FUTURE"}``),
a single QMT terminal can serve both accounts from one strategy instance.

This module provides:

- :func:`build_multi_account_rpc_service` — the entry point called from
  ``_build_rpc_service`` in the strategy module. When the map has only one
  entry (or is empty), it falls back to the single-service builder unchanged.
- :class:`MultiAccountRpcServiceManager` — a thin wrapper that delegates
  start/stop/drain to all services and exposes the primary's attributes.
- :class:`SecondaryHandlersProxy` — injects the secondary account_id into
  every ``handle()`` call, so per-request account_type resolution (from
  ``account_type_map.py``, PR #135) routes correctly for the secondary
  channel.

Architecture
~~~~~~~~~~~~

One ``BigQmtRpcHandlers`` instance is shared across all RPC services.  The
primary service runs on the adjust thread (``background_threads=False``);
secondary services run on background threads (``background_threads=True``),
with trade-context requests (submit, cancel, query) deferred to the primary's
drain loop via ``pending`` queues.

Per-request account_type resolution (PR #135) means the gateway's
``_resolve_account_type(account_id)`` returns the correct type for each
request's account_id — no save/restore of ``self.account_type`` is needed,
and no monkey-patching of the handlers class is required.
"""

import logging

logger = logging.getLogger(__name__)


class SecondaryHandlersProxy:
    """Inject secondary account_id into params for every ``handle()`` call.

    Both primary and secondary RPC services share the same handlers instance,
    so ``handlers.account_id`` is always the primary's.  This proxy ensures the
    secondary's account_id is injected into ``params``, allowing per-request
    account_type resolution to route correctly for secondary-channel requests
    (including ping, which otherwise has no account_id in params).

    Attribute access is delegated to the proxied handlers, so this is
    transparent to callers.
    """

    def __init__(self, handlers, account_id):
        object.__setattr__(self, "_proxied", handlers)
        object.__setattr__(self, "_secondary_account_id", str(account_id or ""))

    def handle(self, method, params=None):
        params = dict(params or {})
        if "account_id" not in params and self._secondary_account_id:
            params["account_id"] = self._secondary_account_id
        return self._proxied.handle(method, params)

    def __getattr__(self, name):
        return getattr(self._proxied, name)

    def __setattr__(self, name, value):
        setattr(self._proxied, name, value)


class MultiAccountRpcServiceManager:
    """Manages primary + secondary RPC services for multi-account deployment.

    Delegates start/stop/drain to all services.  Exposes the primary service's
    attributes (``vk``, ``listen_vk``, ``account_id``, etc.) so that the
    strategy module can treat a single-service or multi-service deployment
    identically.
    """

    def __init__(self, services, handlers, exec_poller=None):
        self._services = services
        self.handlers = handlers
        self._primary = services[0]
        self.account_id = services[0].account_id
        self.vk = services[0].vk
        self.listen_vk = services[0].listen_vk
        # QMT 只为绑定的主账号触发委托和成交回调，其他账号通过轮询获取回报（#320）。
        self.exec_poller = exec_poller

    def start(self):
        for s in self._services:
            try:
                s.start()
            except Exception as e:
                logger.error("多账号 RPC 启动失败，账号前缀=%s：%s", s.account_id[:3], e)

    def stop(self):
        for s in self._services:
            try:
                s.stop()
            except Exception:
                pass
            # Close secondary's transport listen_vk so zombie brpop/pubsub
            # threads exit immediately instead of surviving the join timeout
            # and competing for requests after reload.  The primary's
            # listen_vk is shared with the service-level attribute and
            # must NOT be closed here — it is managed by the primary's own
            # stop() lifecycle.
            if s is not self._primary:
                transport = getattr(s, "_transport", None)
                if transport is not None:
                    lr = getattr(transport, "listen_vk", None)
                    if lr is not None:
                        try:
                            lr.close()
                        except Exception:
                            pass
                # Also close the service-level listen_vk if it is a
                # separate connection (not shared with primary).
                slr = getattr(s, "listen_vk", None)
                if slr is not None and slr is not getattr(
                    self._primary, "listen_vk", None
                ):
                    try:
                        slr.close()
                    except Exception:
                        pass

    def drain_request_queue(self, max_items=20):
        return sum(
            s.drain_request_queue(max_items)
            for s in self._services
            if hasattr(s, "drain_request_queue")
        )

    def drain_pending(self, max_items=20, budget_seconds=None):
        processed = sum(
            s.drain_pending(max_items, budget_seconds=budget_seconds)
            for s in self._services
            if hasattr(s, "drain_pending")
        )
        poller = self.exec_poller
        if poller is not None:
            try:
                poller.poll()
            except Exception as e:
                logger.error("多账号：附属账号交易回报轮询失败：%s", e)
        return processed

    def secondary_exec_poll_status(self):
        poller = self.exec_poller
        return poller.status() if poller is not None else None

    def __getattr__(self, name):
        return getattr(self._primary, name)


def build_multi_account_rpc_service(context_info, app, config, build_single_fn):
    """Build one ``VkPubSubRpcService`` per account in the type map.

    Falls back to the single-service builder (``build_single_fn``) when the
    map is empty or has only one entry — zero behavior change for
    single-account deployments.

    Parameters
    ----------
    context_info, app, config :
        Forwarded to ``build_single_fn`` for the primary service.
    build_single_fn : callable
        The original ``_build_rpc_service`` from the strategy module.

    Returns
    -------
    VkPubSubRpcService or MultiAccountRpcServiceManager or None
    """
    from .account_type_map import get_account_type_map

    account_map = get_account_type_map()
    if not account_map:
        return build_single_fn(context_info, app, config)

    primary = build_single_fn(context_info, app, config)
    if primary is None:
        return None

    if len(account_map) <= 1:
        return primary

    # Build secondary service(s) — same handlers, different channel
    services = [primary]
    for aid in account_map:
        if str(aid) == str(primary.account_id):
            continue
        sec = _build_secondary(primary, str(aid), config)
        if sec:
            services.append(sec)

    logger.info(
        "multi_account: %d services, primary=%s***",
        len(services),
        primary.account_id[:3],
    )
    poller = _build_secondary_exec_poller(
        context_info, primary, [s.account_id for s in services[1:]], config)
    return MultiAccountRpcServiceManager(services, primary.handlers, exec_poller=poller)


class _PushSink(object):
    """publish(topic, data) over the quote push channel's publisher; no
    ``xadd``, so exec_events.publish_exec_event takes the push-channel path."""

    def __init__(self, publisher):
        self._publisher = publisher

    def publish(self, topic, data):
        return self._publisher(topic, data)


def _push_sink_from_handlers(handlers):
    manager = getattr(handlers, "quote_subscription_manager", None)
    publisher = getattr(manager, "_on_push_publisher", None)
    return _PushSink(publisher) if callable(publisher) else None


def _build_secondary_exec_poller(context_info, primary, secondary_accounts, config):
    """The #320 poller, or None when exec events are off / no VK sink /
    the gateway cannot read native rows / the interval is 0."""
    exec_config = dict(config.get("exec_events") or {})
    enabled = exec_config.get("enabled", True)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() not in ("0", "false", "no", "off", "")
    if not enabled or not secondary_accounts:
        return None
    rpc_config = dict(config.get("rpc") or {})
    try:
        interval = float(rpc_config.get("secondary_exec_poll_seconds", 1.0))
    except (TypeError, ValueError):
        interval = 1.0
    if interval <= 0:
        return None
    gateway = getattr(primary.handlers, "order_gateway", None)
    query_rows = getattr(gateway, "query_native_rows", None)
    sink = getattr(primary, "vk", None)
    identity_vk = sink
    if sink is None:
        # 没有 VK 时，可通过推送通道的 exec:* 主题发送交易回报（#76）。
        sink = _push_sink_from_handlers(primary.handlers)
    if not callable(query_rows) or sink is None:
        logger.warning("多账号：无法轮询附属账号交易回报（网关=%s，发布端可用=%s）",
                       type(gateway).__name__, sink is not None)
        return None
    from .secondary_exec_poll import SecondaryExecPoller, build_row_publisher

    publish = build_row_publisher(sink, context_info=context_info, identity_vk=identity_vk)
    poller = SecondaryExecPoller(
        secondary_accounts, query_rows, publish, interval_seconds=interval,
        log=lambda text: logger.warning("多账号：%s", text))
    logger.info("多账号：每 %.1f 秒轮询一次附属账号 %s 的交易回报",
                interval, [a[:3] + "***" for a in poller.accounts])
    return poller


def _build_secondary(primary, account_id, config):
    """为附属账号创建与主账号共享处理器的 VkPubSubRpcService。

    附属账号有独立的 VK 连接，停止时不会影响主账号的监听器。
    SecondaryHandlersProxy 将附属账号的 account_id 注入每个请求。
    """
    rpc_config = dict((config.get("rpc") or {}))
    transport_name = str(rpc_config.get("transport") or "valkey").lower()
    if transport_name not in ("valkey", "", "default"):
        logger.warning("多账号：传输方式 %r 不支持附属账号服务（账号前缀=%s）",
                       transport_name, account_id[:3])
        return None
    try:
        from .vk_rpc import VkPubSubRpcService
        from .transports.vk_transport import VkTransport
        from .adapters import vk_common
    except ImportError:
        logger.warning("多账号：导入依赖失败，无法创建附属账号服务")
        return None

    # 附属账号沿用主账号的 VK 地址、数据库和凭据，使用独立连接。
    try:
        vk_config = dict(config.get("valkey") or {})
        vk_config.update(dict(rpc_config.get("valkey") or {}))
        if vk_config.get("socket_timeout") in (None, ""):
            vk_config["socket_timeout"] = 10
        secondary_listen_vk = vk_common.build_vk_client(vk_config)
    except Exception:
        # Fallback: share primary's connection (suboptimal but functional)
        secondary_listen_vk = primary.listen_vk

    transport = VkTransport(
        vk_client=secondary_listen_vk,
        account_id=account_id,
        response_vk_client=primary.vk,
        request_channel_template=rpc_config.get(
            "request_channel_template", "bigqmt:rpc:req:{account_id}"),
        request_queue_template=rpc_config.get(
            "request_queue_template", "bigqmt:rpc:queue:{account_id}"),
        response_channel_template=rpc_config.get(
            "response_channel_template",
            "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_list_template=rpc_config.get(
            "response_list_template",
            "bigqmt:rpc:respq:{account_id}:{request_id}"),
        response_key_template=rpc_config.get(
            "response_key_template",
            "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_ttl_seconds=int(rpc_config.get("response_ttl_seconds", 60)),
        print_prefix="[bigqmt_rpc_2nd]",
    )

    return VkPubSubRpcService(
        vk_client=secondary_listen_vk,
        response_vk_client=primary.vk,
        handlers=SecondaryHandlersProxy(primary.handlers, account_id),
        account_id=account_id,
        request_channel_template=rpc_config.get(
            "request_channel_template", "bigqmt:rpc:req:{account_id}"),
        response_channel_template=rpc_config.get(
            "response_channel_template",
            "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_key_template=rpc_config.get(
            "response_key_template",
            "bigqmt:rpc:resp:{account_id}:{request_id}"),
        response_ttl_seconds=int(rpc_config.get("response_ttl_seconds", 60)),
        max_queue_size=int(rpc_config.get("max_queue_size", 200)),
        process_in_listener=True,
        listener_methods=rpc_config.get("listener_methods") or ("*",),
        background_threads=True,
        transport=transport,
    )
