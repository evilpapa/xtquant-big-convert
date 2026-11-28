"""QMT-side client for a local Windows named pipe."""

import re
import sys

from .base import TransportError
from .serial_transport import DEFAULT_MAX_FRAME_BYTES, SerialTransport


DEFAULT_PIPE_NAME = r"\\.\pipe\qmt_bridge"
_PREFIX = "\\\\.\\pipe\\"
_NAME = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")


def local_pipe_name(name):
    """Accept a simple name or its explicit local Win32 pipe path only."""
    if not isinstance(name, str):
        raise ValueError("pipe name must be text")
    suffix = name[len(_PREFIX):] if name.lower().startswith(_PREFIX) else name
    if suffix in (".", "..") or not _NAME.fullmatch(suffix):
        raise ValueError("expected a simple local pipe name or a \\\\.\\pipe\\ path")
    return _PREFIX + suffix


def open_pipe_client(name):
    if sys.platform != "win32":
        raise TransportError("Named pipe transport requires Windows")
    from ._win_pipe import PipeClient

    return PipeClient(local_pipe_name(name))


class _ConsoleLogger:
    @staticmethod
    def warning(message, *args):
        print("[bigqmt_pipe] " + (message % args))

    error = warning


class PipeTransport(SerialTransport):
    """Reuse framed IPC; only the byte stream changes, never the RPC dispatch."""

    name = "pipe"
    link_label = "命名管道"
    _logger = _ConsoleLogger()

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]", name=DEFAULT_PIPE_NAME,
                 max_frame_bytes=DEFAULT_MAX_FRAME_BYTES):
        super().__init__(account_id=account_id, print_prefix=print_prefix,
                         port=local_pipe_name(name), max_frame_bytes=max_frame_bytes)

    def _open_port(self):
        return open_pipe_client(self.port)
