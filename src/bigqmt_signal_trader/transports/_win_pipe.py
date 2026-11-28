"""Nonblocking byte streams over Windows named pipe device handles only."""

import ctypes as ct
from ctypes import wintypes as wt

from .base import LocalIpcChannel, TransportError
from .pipe_transport import local_pipe_name


_k = ct.WinDLL("kernel32", use_last_error=True)
_DWORD, _BOOL, _HANDLE, _PTR = wt.DWORD, wt.BOOL, wt.HANDLE, ct.c_void_p
_INVALID = _PTR(-1).value
_NOWAIT = 1  # byte read mode, return immediately when empty or full
_DISCONNECTED = {109, 232, 233}  # broken / closing / not connected


def _bind(library, name, result, *args):
    fn = getattr(library, name)
    fn.restype, fn.argtypes = result, args
    return fn


_close = _bind(_k, "CloseHandle", _BOOL, _HANDLE)
_open = _bind(_k, "CreateFileW", _HANDLE, wt.LPCWSTR, _DWORD, _DWORD, _PTR,
              _DWORD, _DWORD, _HANDLE)
_mode = _bind(_k, "SetNamedPipeHandleState", _BOOL, _HANDLE, _PTR, _PTR, _PTR)
_peek = _bind(_k, "PeekNamedPipe", _BOOL, _HANDLE, _PTR, _DWORD, _PTR, _PTR, _PTR)
_read = _bind(_k, "ReadFile", _BOOL, _HANDLE, _PTR, _DWORD, _PTR, _PTR)
_write = _bind(_k, "WriteFile", _BOOL, _HANDLE, _PTR, _DWORD, _PTR, _PTR)


class PipeDisconnected(OSError):
    """The connected peer closed its pipe handle."""


def _raise_io_error():
    error = ct.get_last_error()
    if error in _DISCONNECTED:
        raise PipeDisconnected("Named pipe peer disconnected")
    raise ct.WinError(error)


class _PipeStream(LocalIpcChannel):
    @property
    def in_waiting(self):
        available = _DWORD()
        if not _peek(self._handle, None, 0, None, ct.byref(available), None):
            _raise_io_error()
        return available.value

    def read(self, size):
        data = ct.create_string_buffer(size)
        count = _DWORD()
        if not _read(self._handle, data, size, ct.byref(count), None):
            _raise_io_error()
        return data.raw[:count.value]

    def write(self, data):
        data = bytes(data)
        count = _DWORD()
        if not _write(self._handle, data, len(data), ct.byref(count), None):
            _raise_io_error()
        return count.value

    def close(self):
        handle, self._handle = self._handle, None
        if handle is not None:
            _close(handle)


class PipeClient(_PipeStream):
    def __init__(self, name):
        # SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION: a server may
        # identify the client, but cannot impersonate it to access resources.
        handle = _open(local_pipe_name(name), 0xC0000000, 0, None, 3, 0x00110000, None)
        if handle == _INVALID:
            raise TransportError("Cannot open local named pipe; start the Windows bridge first") from ct.WinError(
                ct.get_last_error())
        self._handle = handle
        mode = _DWORD(_NOWAIT)
        if not _mode(handle, ct.byref(mode), None, None):
            error = ct.WinError(ct.get_last_error())
            self.close()
            raise error


class _SecurityAttributes(ct.Structure):
    _fields_ = [("length", _DWORD), ("descriptor", _PTR), ("inherit", _BOOL)]


def _create_listener(name):
    # These APIs are used exclusively in the external bridge process. Grant
    # access only to its Windows user (not Everyone or anonymous readers).
    advapi = ct.WinDLL("advapi32", use_last_error=True)
    current_process = _bind(_k, "GetCurrentProcess", _HANDLE)
    free = _bind(_k, "LocalFree", _PTR, _PTR)
    open_token = _bind(advapi, "OpenProcessToken", _BOOL, _HANDLE, _DWORD, _PTR)
    token_info = _bind(advapi, "GetTokenInformation", _BOOL, _HANDLE, ct.c_int, _PTR, _DWORD, _PTR)
    sid_text = _bind(advapi, "ConvertSidToStringSidW", _BOOL, _PTR, _PTR)
    make_sd = _bind(advapi, "ConvertStringSecurityDescriptorToSecurityDescriptorW",
                    _BOOL, wt.LPCWSTR, _DWORD, _PTR, _PTR)
    create = _bind(_k, "CreateNamedPipeW", _HANDLE, wt.LPCWSTR, _DWORD, _DWORD,
                   _DWORD, _DWORD, _DWORD, _DWORD, _PTR)
    token = _HANDLE()
    if not open_token(current_process(), 8, ct.byref(token)):  # TOKEN_QUERY
        raise ct.WinError(ct.get_last_error())
    try:
        size = _DWORD()
        token_info(token, 1, None, 0, ct.byref(size))  # TokenUser
        if not size.value:
            raise ct.WinError(ct.get_last_error())
        info = ct.create_string_buffer(size.value)
        if not token_info(token, 1, info, size, ct.byref(size)):
            raise ct.WinError(ct.get_last_error())
        sid = ct.cast(info, ct.POINTER(_PTR))[0]  # TOKEN_USER starts with a SID pointer
        text = _PTR()
        if not sid_text(sid, ct.byref(text)):
            raise ct.WinError(ct.get_last_error())
        try:
            sddl = "D:P(A;;GA;;;%s)" % ct.wstring_at(text)
        finally:
            free(text)
    finally:
        _close(token)
    descriptor = _PTR()
    if not make_sd(sddl, 1, ct.byref(descriptor), None):
        raise ct.WinError(ct.get_last_error())
    try:
        security = _SecurityAttributes(ct.sizeof(_SecurityAttributes), descriptor, False)
        # Duplex, first and only instance, byte stream, no remote SMB clients.
        handle = create(name, 0x00080003, _NOWAIT | 8, 1, 65536, 65536, 0, ct.byref(security))
        if handle == _INVALID:
            raise ct.WinError(ct.get_last_error())
        return handle
    finally:
        free(descriptor)


class PipeListener(_PipeStream):
    def __init__(self, name):
        self.connected = False
        self._connect = _bind(_k, "ConnectNamedPipe", _BOOL, _HANDLE, _PTR)
        self._disconnect = _bind(_k, "DisconnectNamedPipe", _BOOL, _HANDLE)
        self._handle = _create_listener(local_pipe_name(name))

    def accept(self):
        if self.connected:
            return True
        if self._connect(self._handle, None):
            # In PIPE_NOWAIT, the first call after disconnect can succeed
            # simply because the instance has been made available again.
            return False
        error = ct.get_last_error()
        if error == 535:  # ERROR_PIPE_CONNECTED: client won the connect race
            self.connected = True
        elif error in _DISCONNECTED:
            self.disconnect()
        elif error != 536:  # ERROR_PIPE_LISTENING
            raise ct.WinError(error)
        return self.connected

    def disconnect(self):
        self.connected = False
        # Discard queued bytes instead of waiting for the old client to drain.
        if not self._disconnect(self._handle) and ct.get_last_error() not in _DISCONNECTED:
            raise ct.WinError(ct.get_last_error())
