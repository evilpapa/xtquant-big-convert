"""Shared-memory transport stub.

Reserved for a future low-latency same-host backend. Selecting this transport
raises an error until the backend is implemented.
"""

from .base import RpcTransport, TransportError


class SharedMemoryTransport(RpcTransport):
    name = "shm"

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]", **kwargs):
        super(SharedMemoryTransport, self).__init__(
            account_id=account_id, print_prefix=print_prefix
        )

    def _unsupported(self):
        raise TransportError("shared-memory transport is not implemented yet")

    def send_request(self, request, timeout_seconds):
        self._unsupported()

    def send_response(self, request, response):
        self._unsupported()

    def start_receiving(self, on_request, **kwargs):
        self._unsupported()
