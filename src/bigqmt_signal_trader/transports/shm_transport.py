"""Same-session Windows RPC over two bounded shared-memory mailboxes.

One call per endpoint is in flight. The caller mutex serializes clients; the
short-lived guard mutex protects the header and both slots. The strategy never
waits for the guard during adjust. Only the response event wakes a waiter;
checking the header and ticket under the guard is the source of truth.
"""

import hashlib
import json
import math
import struct
import sys
import threading
import time
import uuid

from .base import RpcTransport, TransportError, TransportTimeout


_MAGIC = b"BQSHM\x01\x00\x00"
# Magic/version, slot capacities, per-call ticket, monotonic deadline, lengths.
_HEADER = struct.Struct("<8sII16sdII")
_EMPTY_TICKET = bytes(16)
_TIMEOUT = 0x102
_ABANDONED = 0x80
_MIN_BYTES = 4096
_MAX_BYTES = 256 * 1024 * 1024


def _slot_size(value):
    value = int(value)
    if not _MIN_BYTES <= value <= _MAX_BYTES:
        raise ValueError("SHM slot size must be between 4096 and 268435456 bytes")
    return value


def _encode(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode(data):
    message = json.loads(data.decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("SHM frame must be a JSON object")
    return message


def _remaining_ms(deadline):
    return max(0, math.ceil((deadline - time.monotonic()) * 1000))


class SharedMemoryTransport(RpcTransport):
    name = "shm"

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]", namespace="qmt_shm",
                 max_request_bytes=1024 * 1024, max_response_bytes=32 * 1024 * 1024):
        super().__init__(account_id, print_prefix)
        if not self.account_id.strip() or len(self.account_id) > 128:
            raise ValueError("SHM requires an account_id of 1–128 characters")
        if not isinstance(namespace, str) or not namespace.strip() or len(namespace) > 128:
            raise ValueError("SHM namespace must be 1–128 characters")
        digest = hashlib.sha256(_encode([namespace, self.account_id])).hexdigest()
        self._name = "Local\\bigqmt.shm." + digest
        self.max_request_bytes = _slot_size(max_request_bytes)
        self.max_response_bytes = _slot_size(max_response_bytes)
        self._response_at = _HEADER.size + self.max_request_bytes
        self._size = self._response_at + self.max_response_bytes
        self._server = None
        self._server_lock = threading.RLock()
        self._active_ticket = None
        self._pending_response = None
        self._published_count = 0

    def _open(self, *, create):
        if sys.platform != "win32":
            raise TransportError("SHM transport requires Windows")
        try:
            from ._win_shm import WindowsShm

            return WindowsShm(self._name, self._size, create=create)
        except (ImportError, OSError) as exc:
            raise TransportError("Cannot open Windows SHM IPC: %s" % exc) from exc

    def _write_header(self, wire, ticket, deadline, request_size, response_size):
        _HEADER.pack_into(wire.buffer, 0, _MAGIC, self.max_request_bytes,
                          self.max_response_bytes, ticket, deadline, request_size, response_size)

    def _read_header(self, wire, lock_result):
        if lock_result == _ABANDONED:
            # A peer died inside the critical section. Never read partial data.
            wire.buffer[:len(_MAGIC)] = bytes(len(_MAGIC))
            wire.notify_reply()
            raise TransportError("SHM guard was abandoned; restart the SHM server")
        magic, req_cap, resp_cap, ticket, deadline, req_len, resp_len = _HEADER.unpack_from(wire.buffer)
        if magic != _MAGIC or (req_cap, resp_cap) != (self.max_request_bytes, self.max_response_bytes):
            raise TransportError("SHM server stopped or protocol/slot sizes do not match")
        if req_len > req_cap or resp_len > resp_cap or not math.isfinite(deadline):
            raise TransportError("Invalid SHM header; restart the SHM server")
        return ticket, deadline, req_len, resp_len

    def start_receiving(self, on_request, background_threads=False):
        if background_threads:
            raise ValueError("SHM handlers must be drained on the strategy thread")
        with self._server_lock:
            if self._server is not None:
                raise TransportError("SHM server is already running")
            wire = self._open(create=True)
            try:
                try:
                    self._write_header(wire, _EMPTY_TICKET, 0.0, 0, 0)
                finally:
                    wire.release_initial_guard()
            except BaseException:
                wire.close()
                raise
            self._server = wire
            super().start_receiving(on_request)

    def send_request(self, request, timeout_seconds):
        wait = float(timeout_seconds)
        if not math.isfinite(wait) or not 0 < wait <= 86400:
            raise ValueError("SHM timeout must be positive and at most 86400 seconds")
        if request.get("account_id") != self.account_id:
            raise TransportError("SHM request account_id does not match the endpoint")
        request_id, method = request.get("request_id"), request.get("method")
        if not all(isinstance(value, str) and 0 < len(value) <= 128 for value in (request_id, method)):
            raise TransportError("SHM requires request_id and method of 1–128 characters")
        deadline = time.monotonic() + wait
        try:
            payload = _encode(request)
        except (ValueError, TypeError, RecursionError) as exc:
            raise TransportError("Cannot encode SHM request") from exc
        if len(payload) > self.max_request_bytes:
            raise TransportError("SHM request exceeds max_request_bytes; not dispatched")
        ticket = uuid.uuid4().bytes
        try:
            with self._open(create=False) as wire:
                with wire.locked(wire.caller, _remaining_ms(deadline)) as caller_state:
                    if caller_state == _TIMEOUT:
                        raise TransportTimeout("SHM endpoint busy; request not dispatched")
                    with wire.locked(wire.guard, _remaining_ms(deadline)) as state:
                        if state == _TIMEOUT:
                            raise TransportTimeout("SHM deadline elapsed; request not dispatched")
                        self._read_header(wire, state)
                        if time.monotonic() >= deadline:
                            raise TransportTimeout("SHM deadline elapsed; request not dispatched")
                        wire.buffer[_HEADER.size:_HEADER.size + len(payload)] = payload
                        self._write_header(wire, ticket, deadline, len(payload), 0)
                    raw = self._wait_response(wire, ticket, deadline)
        except TransportTimeout:
            raise
        except OSError as exc:
            # No retry: an I/O error can follow a successful dispatch.
            raise TransportError("SHM IPC failed: %s" % exc) from exc
        return self._decode_response(raw, request_id, method)

    def _wait_response(self, wire, ticket, deadline):
        notified = False
        while (wait_ms := _remaining_ms(deadline)) > 0:
            # Windows wait resolution can report timeout before the shared QPC
            # deadline. Do not let the caller give up while dispatch is still valid.
            if not notified:
                notified = wire.wait(wire.ready, wait_ms) != _TIMEOUT
                if not notified:
                    continue
            with wire.locked(wire.guard, _remaining_ms(deadline)) as state:
                if state == _TIMEOUT:
                    continue  # We consumed the event; retry the guard, not the event.
                current, _, _, size = self._read_header(wire, state)
                # An earlier call can leave the auto-reset event signaled.
                if current == ticket and size:
                    return wire.buffer[self._response_at:self._response_at + size]
                notified = False
        raise TransportTimeout("SHM RPC timed out after submission; outcome may be unknown")

    def _decode_response(self, raw, request_id, method):
        try:
            from ..vk_rpc import loads_rpc_response

            response = loads_rpc_response(raw.decode("utf-8"))
        except (ValueError, TypeError, AttributeError, RecursionError) as exc:
            raise TransportError("Invalid SHM response") from exc
        if (not isinstance(response, dict) or response.get("request_id") != request_id
                or response.get("account_id") != self.account_id or response.get("method") != method):
            raise TransportError("SHM response envelope does not match the request")
        return response

    def drain_request_queue(self, max_items=20):
        if int(max_items) <= 0:
            return 0
        with self._server_lock:
            wire = self._server
            if wire is None:
                return 0
            try:
                with wire.locked(wire.guard, 0) as state:
                    if state == _TIMEOUT:
                        return 0
                    ticket, deadline, size, _ = self._read_header(wire, state)
                    self._flush_response(wire, ticket, deadline)
                    if not size:
                        return 0
                    self._write_header(wire, ticket, deadline, 0, 0)
                    if time.monotonic() >= deadline:
                        return 0
                    raw = wire.buffer[_HEADER.size:_HEADER.size + size]
            except (OSError, TransportError) as exc:
                self._fail(exc)
                return 0
            try:
                request = _decode(raw)
                if (request.get("account_id") != self.account_id
                        or not all(isinstance(request.get(key), str) and 0 < len(request[key]) <= 128
                                   for key in ("request_id", "method"))):
                    raise ValueError("invalid SHM envelope")
            except (ValueError, RecursionError):
                print("%s SHM discarded an invalid request" % self.print_prefix)
                return 0
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return 0
            self._active_ticket = ticket
            # Do not trust client-supplied RPC expiry or reply-routing fields.
            request.update(_shm_ticket=ticket, _shm_deadline=deadline,
                           _received_at=time.time(), timeout_seconds=remaining)
        self.deliver(request)
        return 1

    def _flush_response(self, wire, current, deadline):
        pending = self._pending_response
        self._pending_response = None
        if pending is None:
            return
        ticket, payload = pending
        if ticket != current or time.monotonic() >= deadline:
            return
        wire.buffer[self._response_at:self._response_at + len(payload)] = payload
        self._write_header(wire, ticket, deadline, 0, len(payload))
        wire.notify_reply()
        self._published_count += 1

    def send_response(self, request, response):
        with self._server_lock:
            ticket = request.get("_shm_ticket")
            wire = self._server
            if wire is None or not ticket or ticket != self._active_ticket:
                return
            try:
                payload = _encode(response)
            except (ValueError, TypeError, RecursionError):
                payload = None
            if payload is None or len(payload) > self.max_response_bytes:
                error = {key: request[key] for key in ("request_id", "account_id", "method")}
                payload = _encode(dict(error, schema_version=1, ok=False, data=None, error=(
                    "SHM response exceeds max_response_bytes or could not be encoded; "
                    "RPC was already handled")))
            self._pending_response = (ticket, payload)
            try:
                with wire.locked(wire.guard, 0) as state:
                    if state != _TIMEOUT:
                        current, deadline, _, _ = self._read_header(wire, state)
                        self._flush_response(wire, current, deadline)
            except (OSError, TransportError) as exc:
                self._fail(exc)

    def _fail(self, exc):
        print("%s SHM IPC stopped: %s" % (self.print_prefix, exc))
        self.stop()

    def stop(self):
        with self._server_lock:
            super().stop()
            wire, self._server = self._server, None
            self._active_ticket = self._pending_response = None
            if wire is None:
                return
            try:
                with wire.locked(wire.guard, 0) as state:
                    if state != _TIMEOUT:
                        wire.buffer[:len(_MAGIC)] = bytes(len(_MAGIC))
                wire.notify_reply()
            except OSError as exc:
                print("%s SHM shutdown notification failed: %s" % (self.print_prefix, exc))
            finally:
                wire.close()
