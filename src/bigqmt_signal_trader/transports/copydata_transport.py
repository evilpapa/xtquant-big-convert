"""QMT-side client for local Windows WM_COPYDATA hidden-window IPC."""

import re
import sys

from .base import TransportError
from .serial_transport import DEFAULT_MAX_FRAME_BYTES, SerialTransport


DEFAULT_WINDOW_NAME = "qmt_bridge"
DEFAULT_SEND_TIMEOUT_MS = 2000
WINDOW_TITLE_PREFIX = "BigQmtCopyData:"
_NAME = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")


def local_window_name(name):
    """Validate a local WM_COPYDATA channel name and return its short form."""
    if not isinstance(name, str):
        raise ValueError("copydata window name must be text")
    suffix = name[len(WINDOW_TITLE_PREFIX):] if name.startswith(WINDOW_TITLE_PREFIX) else name
    if suffix in (".", "..") or not _NAME.fullmatch(suffix):
        raise ValueError("expected a simple local copydata window name or BigQmtCopyData: title")
    return suffix


def local_window_title(name):
    """Return the discoverable Win32 hidden window title for ``name``."""
    return WINDOW_TITLE_PREFIX + local_window_name(name)


def open_copydata_client(window_name, *, send_timeout_ms=DEFAULT_SEND_TIMEOUT_MS):
    if sys.platform != "win32":
        raise TransportError("WM_COPYDATA transport requires Windows")
    from ._win_copydata import CopyDataClient

    return CopyDataClient(local_window_name(window_name), send_timeout_ms=send_timeout_ms)


class _ConsoleLogger:
    @staticmethod
    def warning(message, *args):
        print("[bigqmt_copydata] " + (message % args))

    error = warning


class CopyDataTransport(SerialTransport):
    """Reuse framed IPC; only the local Windows message channel changes."""

    name = "copydata"
    link_label = "窗口消息"
    _logger = _ConsoleLogger()

    def __init__(
        self,
        account_id="",
        print_prefix="[bigqmt_rpc]",
        window_name=DEFAULT_WINDOW_NAME,
        name=None,
        max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
        send_timeout_ms=DEFAULT_SEND_TIMEOUT_MS,
    ):
        if name is not None:
            if window_name != DEFAULT_WINDOW_NAME and window_name != name:
                raise ValueError("specify either window_name or name for copydata transport")
            window_name = name
        self.send_timeout_ms = max(50, int(send_timeout_ms))
        super().__init__(
            account_id=account_id,
            print_prefix=print_prefix,
            port=local_window_title(window_name),
            max_frame_bytes=max_frame_bytes,
        )

    def _open_port(self):
        return open_copydata_client(self.port, send_timeout_ms=self.send_timeout_ms)
