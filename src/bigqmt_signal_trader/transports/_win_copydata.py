"""Non-blocking bidirectional local IPC over Windows WM_COPYDATA hidden windows.

Imported only when a Windows WM_COPYDATA endpoint opens, so non-Windows hosts
and other transports do not load Win32 user32/kernel32 bindings.
"""

import ctypes as ct
from contextlib import suppress
from ctypes import wintypes as wt
import hashlib
import threading

from .base import LocalIpcChannel, TransportError
from .copydata_transport import DEFAULT_SEND_TIMEOUT_MS, local_window_title


_u = ct.WinDLL("user32", use_last_error=True)
_k = ct.WinDLL("kernel32", use_last_error=True)

_BOOL = wt.BOOL
_DWORD = wt.DWORD
_HANDLE = wt.HANDLE
_HINSTANCE = wt.HINSTANCE
_HWND = wt.HWND
_INT = ct.c_int
_LONG_PTR = ct.c_ssize_t
_LPARAM = wt.LPARAM
_LPCWSTR = wt.LPCWSTR
_LRESULT = ct.c_ssize_t
_PTR = ct.c_void_p
_UINT = wt.UINT
_ULONG_PTR = ct.c_size_t
_WPARAM = wt.WPARAM

_WM_DESTROY = 0x0002
_WM_CLOSE = 0x0010
_WM_COPYDATA = 0x004A
_GWLP_WNDPROC = -4
_SMTO_NORMAL = 0x0000
_SMTO_ABORTIFHUNG = 0x0002

_ERROR_ALREADY_EXISTS = 183
_ERROR_INVALID_WINDOW_HANDLE = 1400
_ERROR_CLASS_ALREADY_EXISTS = 1410
_ERROR_TIMEOUT = 1460

BRIDGE_WINDOW_CLASS = "BigQmtCopyDataBridge"
CLIENT_WINDOW_CLASS = "BigQmtCopyDataClient"

# Distinguish our IPC frames from unrelated Windows WM_COPYDATA broadcasts.
_MAGIC_DATA = 0x42514344        # "BQCD"
_MAGIC_DISCONNECT = 0x42514358  # "BQCX"

_REPLY_REJECTED = 0
_REPLY_OK = 1
_REPLY_FULL = 2
_REPLY_BUSY = 3

_MAX_COPYDATA_CHUNK = 64 * 1024
_DEFAULT_MAX_BUFFER_BYTES = 16 * 1024 * 1024


class _COPYDATASTRUCT(ct.Structure):
    _fields_ = [
        ("dwData", _ULONG_PTR),
        ("cbData", _DWORD),
        ("lpData", _PTR),
    ]


class _WNDCLASSEXW(ct.Structure):
    _fields_ = [
        ("cbSize", _UINT),
        ("style", _UINT),
        ("lpfnWndProc", _PTR),
        ("cbClsExtra", _INT),
        ("cbWndExtra", _INT),
        ("hInstance", _HINSTANCE),
        ("hIcon", wt.HICON),
        ("hCursor", _HANDLE),
        ("hbrBackground", _HANDLE),
        ("lpszMenuName", _LPCWSTR),
        ("lpszClassName", _LPCWSTR),
        ("hIconSm", wt.HICON),
    ]


_WNDPROC = ct.WINFUNCTYPE(_LRESULT, _HWND, _UINT, _WPARAM, _LPARAM)


def _bind(library, name, result, *args):
    fn = getattr(library, name)
    fn.restype, fn.argtypes = result, args
    return fn


_get_module_handle = _bind(_k, "GetModuleHandleW", _HINSTANCE, _LPCWSTR)
_create_mutex = _bind(_k, "CreateMutexW", _HANDLE, _PTR, _BOOL, _LPCWSTR)
_close_handle = _bind(_k, "CloseHandle", _BOOL, _HANDLE)

_def_window_proc = _bind(_u, "DefWindowProcW", _LRESULT, _HWND, _UINT, _WPARAM, _LPARAM)
_register_class = _bind(_u, "RegisterClassExW", wt.ATOM, ct.POINTER(_WNDCLASSEXW))
_create_window = _bind(
    _u, "CreateWindowExW", _HWND,
    _DWORD, _LPCWSTR, _LPCWSTR, _DWORD,
    _INT, _INT, _INT, _INT,
    _HWND, _HANDLE, _HINSTANCE, _PTR,
)
_destroy_window = _bind(_u, "DestroyWindow", _BOOL, _HWND)
_set_window_text = _bind(_u, "SetWindowTextW", _BOOL, _HWND, _LPCWSTR)
_find_window = _bind(_u, "FindWindowW", _HWND, _LPCWSTR, _LPCWSTR)
_is_window = _bind(_u, "IsWindow", _BOOL, _HWND)
_get_message = _bind(_u, "GetMessageW", _BOOL, ct.POINTER(wt.MSG), _HWND, _UINT, _UINT)
_translate_message = _bind(_u, "TranslateMessage", _BOOL, ct.POINTER(wt.MSG))
_dispatch_message = _bind(_u, "DispatchMessageW", _LRESULT, ct.POINTER(wt.MSG))
_post_message = _bind(_u, "PostMessageW", _BOOL, _HWND, _UINT, _WPARAM, _LPARAM)
_post_quit_message = _bind(_u, "PostQuitMessage", None, _INT)
_send_message_timeout = _bind(
    _u, "SendMessageTimeoutW", _LRESULT,
    _HWND, _UINT, _WPARAM, _LPARAM, _UINT, _UINT, ct.POINTER(_ULONG_PTR),
)
_set_window_long_ptr = _bind(
    _u,
    "SetWindowLongPtrW" if hasattr(_u, "SetWindowLongPtrW") else "SetWindowLongW",
    _LONG_PTR,
    _HWND, _INT, _LONG_PTR,
)

_CLASS_LOCK = threading.Lock()
_REGISTERED_CLASSES = set()


def _ensure_window_class(class_name):
    with _CLASS_LOCK:
        if class_name in _REGISTERED_CLASSES:
            return
        hinstance = _get_module_handle(None)
        wc = _WNDCLASSEXW()
        wc.cbSize = ct.sizeof(_WNDCLASSEXW)
        wc.lpfnWndProc = ct.cast(_def_window_proc, _PTR)
        wc.hInstance = hinstance
        wc.lpszClassName = class_name
        ct.set_last_error(0)
        atom = _register_class(ct.byref(wc))
        if not atom:
            error = ct.get_last_error()
            if error != _ERROR_CLASS_ALREADY_EXISTS:
                raise ct.WinError(error)
        _REGISTERED_CLASSES.add(class_name)


class CopyDataDisconnected(OSError):
    """The connected WM_COPYDATA peer window closed or rejected the session."""


def _send_copydata(target_hwnd, sender_hwnd, tag, data, timeout_ms):
    if not target_hwnd or not _is_window(target_hwnd):
        raise CopyDataDisconnected("WM_COPYDATA peer window is closed")
    buf = ct.create_string_buffer(data, len(data)) if data else None
    cds = _COPYDATASTRUCT(
        dwData=tag,
        cbData=len(data),
        lpData=ct.addressof(buf) if buf is not None else None,
    )
    result = _ULONG_PTR(0)
    ct.set_last_error(0)
    ok = _send_message_timeout(
        target_hwnd,
        _WM_COPYDATA,
        _WPARAM(sender_hwnd or 0),
        _LPARAM(ct.addressof(cds)),
        _SMTO_ABORTIFHUNG | _SMTO_NORMAL,
        int(timeout_ms),
        ct.byref(result),
    )
    if not ok:
        error = ct.get_last_error()
        if error == _ERROR_INVALID_WINDOW_HANDLE or not _is_window(target_hwnd):
            raise CopyDataDisconnected("WM_COPYDATA peer window is closed")
        if error in (0, _ERROR_TIMEOUT):
            raise CopyDataDisconnected("WM_COPYDATA send timed out; peer window is unresponsive")
        raise ct.WinError(error)
    return int(result.value)


class _HiddenMessageWindow:
    """Dedicated hidden Win32 window with its own GetMessageW pump thread."""

    def __init__(self, class_name, title, on_data, on_disconnect, thread_name):
        self._class_name = class_name
        self._title = title
        self._on_data = on_data
        self._on_disconnect = on_disconnect
        self._ready = threading.Event()
        self._init_error = None
        self._wndproc = None
        self.hwnd = None
        self._closed = False
        self._thread = threading.Thread(target=self._run, name=thread_name, daemon=True)
        self._thread.start()
        self._ready.wait(5.0)
        if self._init_error is not None:
            self._thread.join(1.0)
            raise self._init_error
        if not self.hwnd:
            self._thread.join(1.0)
            raise TransportError("Timed out creating hidden WM_COPYDATA window")

    def is_alive(self):
        hwnd = self.hwnd
        return bool(not self._closed and hwnd and _is_window(hwnd))

    def _wnd_proc(self, hwnd, msg, wparam, lparam):
        try:
            if msg == _WM_COPYDATA:
                if not lparam:
                    return _REPLY_REJECTED
                cds = ct.cast(lparam, ct.POINTER(_COPYDATASTRUCT)).contents
                sender = int(wparam) if wparam else 0
                tag = int(cds.dwData)
                if tag == _MAGIC_DISCONNECT:
                    self._on_disconnect(sender)
                    return _REPLY_OK
                if tag != _MAGIC_DATA:
                    return _REPLY_REJECTED
                size = int(cds.cbData)
                if size == 0:
                    return self._on_data(sender, b"")
                if size > _MAX_COPYDATA_CHUNK or not cds.lpData:
                    return _REPLY_REJECTED
                payload = ct.string_at(cds.lpData, size)
                return self._on_data(sender, payload)
            if msg == _WM_CLOSE:
                _destroy_window(hwnd)
                return 0
            if msg == _WM_DESTROY:
                _post_quit_message(0)
                return 0
        except Exception:
            return _REPLY_REJECTED
        return _def_window_proc(hwnd, msg, wparam, lparam)

    def _run(self):
        hwnd = None
        try:
            _ensure_window_class(self._class_name)
            hinstance = _get_module_handle(None)
            # Create with empty title first so FindWindowW cannot discover the
            # window before our WndProc is installed.
            ct.set_last_error(0)
            hwnd = _create_window(
                0, self._class_name, "", 0,
                0, 0, 0, 0,
                None, None, hinstance, None,
            )
            if not hwnd:
                raise ct.WinError(ct.get_last_error())
            self._wndproc = _WNDPROC(self._wnd_proc)
            ct.set_last_error(0)
            prev = _set_window_long_ptr(
                hwnd, _GWLP_WNDPROC, ct.cast(self._wndproc, _PTR).value,
            )
            if prev == 0 and (error := ct.get_last_error()) != 0:
                raise ct.WinError(error)
            if self._title and not _set_window_text(hwnd, self._title):
                raise ct.WinError(ct.get_last_error())
            self.hwnd = hwnd
        except Exception as exc:
            if hwnd:
                with suppress(Exception):
                    _destroy_window(hwnd)
            self._init_error = exc
            self._closed = True
            self._wndproc = None
            self._ready.set()
            return

        self._ready.set()
        msg = wt.MSG()
        try:
            while True:
                ret = _get_message(ct.byref(msg), None, 0, 0)
                if ret <= 0:
                    break
                _translate_message(ct.byref(msg))
                _dispatch_message(ct.byref(msg))
        finally:
            self._closed = True
            self.hwnd = None
            self._wndproc = None

    def close(self):
        hwnd = self.hwnd
        if hwnd and not self._closed:
            _post_message(hwnd, _WM_CLOSE, 0, 0)
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(2.0)


class _CopyDataStream(LocalIpcChannel):
    """Shared byte-stream and poll()/send() logic for WM_COPYDATA endpoints."""

    def __init__(self, *, send_timeout_ms=DEFAULT_SEND_TIMEOUT_MS,
                 max_buffer_bytes=_DEFAULT_MAX_BUFFER_BYTES):
        self._send_timeout_ms = max(50, int(send_timeout_ms))
        self._max_buffer_bytes = max(65536, int(max_buffer_bytes))
        self._lock = threading.Lock()
        self._rx = bytearray()
        self._peer_hwnd = None
        self._peer_disconnected = False
        self._window = None

    def _current_target(self):
        with self._lock:
            target = self._peer_hwnd
            disconnected = self._peer_disconnected
        if disconnected or not target or self._window is None or not self._window.is_alive():
            raise CopyDataDisconnected("WM_COPYDATA peer disconnected")
        return target, self._window.hwnd

    def read(self, size):
        limit = max(0, int(size))
        with self._lock:
            if limit == 0 or not self._rx:
                return b""
            data = bytes(self._rx[:limit])
            del self._rx[:limit]
            return data

    def write(self, data):
        view = memoryview(bytes(data))
        if not view:
            return 0
        target, self_hwnd = self._current_target()
        sent = 0
        while sent < len(view):
            chunk = bytes(view[sent:sent + _MAX_COPYDATA_CHUNK])
            reply = _send_copydata(target, self_hwnd, _MAGIC_DATA, chunk, self._send_timeout_ms)
            if reply == _REPLY_OK:
                sent += len(chunk)
            elif reply == _REPLY_FULL:
                break
            elif reply == _REPLY_BUSY:
                raise CopyDataDisconnected("WM_COPYDATA bridge is busy with another client")
            else:
                raise CopyDataDisconnected("WM_COPYDATA peer rejected message")
        return sent


class CopyDataClient(_CopyDataStream):
    """QMT-side hidden window endpoint connecting to a bridge hidden window."""

    def __init__(self, window_name, *, send_timeout_ms=DEFAULT_SEND_TIMEOUT_MS,
                 max_buffer_bytes=_DEFAULT_MAX_BUFFER_BYTES):
        super().__init__(send_timeout_ms=send_timeout_ms, max_buffer_bytes=max_buffer_bytes)
        title = local_window_title(window_name)
        bridge_hwnd = _find_window(BRIDGE_WINDOW_CLASS, title)
        if not bridge_hwnd:
            raise TransportError(
                "Cannot find local WM_COPYDATA bridge window; start the Windows bridge first"
            )
        self._peer_hwnd = int(bridge_hwnd)
        self._window = _HiddenMessageWindow(
            CLIENT_WINDOW_CLASS,
            "",
            self._on_data,
            self._on_disconnect,
            thread_name="bigqmt-copydata-wnd",
        )
        try:
            reply = _send_copydata(
                self._peer_hwnd, self._window.hwnd, _MAGIC_DATA, b"", self._send_timeout_ms,
            )
            if reply == _REPLY_BUSY:
                raise TransportError("WM_COPYDATA bridge is busy with another client")
            if reply != _REPLY_OK:
                raise TransportError(
                    "WM_COPYDATA bridge window did not accept connection; start the Windows bridge first"
                )
        except Exception:
            self._window.close()
            raise

    def _on_data(self, sender, payload):
        if sender != self._peer_hwnd:
            return _REPLY_REJECTED
        with self._lock:
            if len(self._rx) + len(payload) > self._max_buffer_bytes:
                return _REPLY_FULL
            if payload:
                self._rx.extend(payload)
            return _REPLY_OK

    def _on_disconnect(self, sender):
        if sender == self._peer_hwnd:
            with self._lock:
                self._peer_disconnected = True

    @property
    def in_waiting(self):
        with self._lock:
            if self._rx:
                return len(self._rx)
            disconnected = self._peer_disconnected
            target = self._peer_hwnd
        if disconnected or self._window is None or not self._window.is_alive() or not _is_window(target):
            raise CopyDataDisconnected("WM_COPYDATA bridge window closed")
        return 0

    def close(self):
        window, self._window = self._window, None
        target = self._peer_hwnd
        if window is not None:
            if target and window.hwnd and _is_window(target):
                with suppress(Exception):
                    _send_copydata(target, window.hwnd, _MAGIC_DISCONNECT, b"", 200)
            window.close()


class CopyDataListener(_CopyDataStream):
    """External Windows bridge hidden window accepting one QMT client at a time."""

    def __init__(self, window_name, *, send_timeout_ms=DEFAULT_SEND_TIMEOUT_MS,
                 max_buffer_bytes=_DEFAULT_MAX_BUFFER_BYTES):
        super().__init__(send_timeout_ms=send_timeout_ms, max_buffer_bytes=max_buffer_bytes)
        self.connected = False
        self._pending_peer = None
        self._pending_rx = bytearray()
        self._mutex = None
        title = local_window_title(window_name)
        digest = hashlib.sha256(title.encode("utf-8")).hexdigest()
        ct.set_last_error(0)
        mutex = _create_mutex(None, False, "Local\\bigqmt.copydata." + digest)
        error = ct.get_last_error()
        if not mutex:
            raise ct.WinError(error)
        if error == _ERROR_ALREADY_EXISTS or _find_window(BRIDGE_WINDOW_CLASS, title):
            _close_handle(mutex)
            raise OSError("WM_COPYDATA bridge window already exists for %s" % title)
        self._mutex = mutex
        try:
            self._window = _HiddenMessageWindow(
                BRIDGE_WINDOW_CLASS,
                title,
                self._on_data,
                self._on_disconnect,
                thread_name="bigqmt-copydata-bridge-wnd",
            )
        except Exception:
            self._close_mutex()
            raise

    def _on_data(self, sender, payload):
        if not sender or not _is_window(sender):
            return _REPLY_REJECTED
        with self._lock:
            if self._peer_hwnd is None:
                if len(payload) > self._max_buffer_bytes:
                    return _REPLY_FULL
                self._peer_hwnd = sender
                self._peer_disconnected = False
                if payload:
                    self._rx.extend(payload)
                return _REPLY_OK
            if sender == self._peer_hwnd:
                if self._peer_disconnected:
                    return _REPLY_REJECTED
                if len(self._rx) + len(payload) > self._max_buffer_bytes:
                    return _REPLY_FULL
                if payload:
                    self._rx.extend(payload)
                return _REPLY_OK
            if not self._peer_disconnected and _is_window(self._peer_hwnd):
                return _REPLY_BUSY
            self._peer_disconnected = True
            if (self._pending_peer is None
                    or self._pending_peer == sender
                    or not _is_window(self._pending_peer)):
                if self._pending_peer != sender:
                    self._pending_peer = sender
                    self._pending_rx.clear()
                if len(self._pending_rx) + len(payload) > self._max_buffer_bytes:
                    return _REPLY_FULL
                if payload:
                    self._pending_rx.extend(payload)
                return _REPLY_OK
            return _REPLY_BUSY

    def _on_disconnect(self, sender):
        with self._lock:
            if sender == self._peer_hwnd:
                self._peer_disconnected = True

    def accept(self):
        with self._lock:
            if self._peer_hwnd is None and self._pending_peer is not None:
                if _is_window(self._pending_peer):
                    self._peer_hwnd = self._pending_peer
                    self._rx = bytearray(self._pending_rx)
                    self._peer_disconnected = False
                self._pending_peer = None
                self._pending_rx.clear()
            self.connected = self._peer_hwnd is not None
            return self.connected

    @property
    def in_waiting(self):
        with self._lock:
            target = self._peer_hwnd
            if target is None:
                return 0
            if self._rx and not self._peer_disconnected:
                return len(self._rx)
            disconnected = self._peer_disconnected
        if disconnected or not _is_window(target):
            with self._lock:
                self._peer_disconnected = True
            raise CopyDataDisconnected("WM_COPYDATA client disconnected")
        return 0

    def disconnect(self):
        with self._lock:
            self._peer_hwnd = None
            self._peer_disconnected = False
            self._rx.clear()
            self.connected = False

    def _close_mutex(self):
        mutex, self._mutex = self._mutex, None
        if mutex:
            _close_handle(mutex)

    def close(self):
        window, self._window = self._window, None
        with self._lock:
            target = self._peer_hwnd
            self._peer_hwnd = None
            self._peer_disconnected = True
            self._pending_peer = None
            self._rx.clear()
            self._pending_rx.clear()
            self.connected = False
        if window is not None:
            if target and window.hwnd and _is_window(target):
                with suppress(Exception):
                    _send_copydata(target, window.hwnd, _MAGIC_DISCONNECT, b"", 200)
            window.close()
        self._close_mutex()
