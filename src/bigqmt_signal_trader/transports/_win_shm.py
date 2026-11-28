"""Windows session-local IPC handles for the pagefile-backed SHM transport.

Imported only when a Windows endpoint opens, so other transports do not
require ctypes or load the Win32 functions.
"""

import ctypes as ct
from contextlib import contextmanager
from ctypes import wintypes as wt

from .base import TransportError


_kernel = ct.WinDLL("kernel32", use_last_error=True)


def _bind(name, result, *arguments):
    func = getattr(_kernel, name)
    func.restype = result
    func.argtypes = arguments
    return func


_HANDLE, _DWORD, _TEXT, _BOOL, _PTR = wt.HANDLE, wt.DWORD, wt.LPCWSTR, wt.BOOL, ct.c_void_p
_close_handle = _bind("CloseHandle", _BOOL, _HANDLE)
_unmap = _bind("UnmapViewOfFile", _BOOL, _PTR)
_wait = _bind("WaitForSingleObject", _DWORD, _HANDLE, _DWORD)
_release = _bind("ReleaseMutex", _BOOL, _HANDLE)
_signal = _bind("SetEvent", _BOOL, _HANDLE)
_map_view = _bind("MapViewOfFile", _PTR, _HANDLE, _DWORD, _DWORD, _DWORD, ct.c_size_t)
_create_mutex = _bind("CreateMutexW", _HANDLE, _PTR, _BOOL, _TEXT)
_create_event = _bind("CreateEventW", _HANDLE, _PTR, _BOOL, _BOOL, _TEXT)
_create_mapping = _bind("CreateFileMappingW", _HANDLE, _HANDLE, _PTR, _DWORD, _DWORD, _DWORD, _TEXT)
_open_mutex = _bind("OpenMutexW", _HANDLE, _DWORD, _BOOL, _TEXT)
_open_event = _bind("OpenEventW", _HANDLE, _DWORD, _BOOL, _TEXT)
_open_mapping = _bind("OpenFileMappingW", _HANDLE, _DWORD, _BOOL, _TEXT)
_MAP_ACCESS = 0x0006  # FILE_MAP_READ | FILE_MAP_WRITE
_MUTEX_ACCESS = 0x100001  # SYNCHRONIZE | MUTEX_MODIFY_STATE
_EVENT_ACCESS = 0x100002  # SYNCHRONIZE | EVENT_MODIFY_STATE


class WindowsShm:
    def __init__(self, name, size, *, create):
        self._handles = []
        self._address = None
        self.buffer = None
        try:
            if create:
                self.guard = self._create_handle(_create_mutex, None, True, name + ".guard")
                try:
                    self.caller = self._create_handle(_create_mutex, None, False, name + ".caller")
                    self.ready = self._create_handle(_create_event, None, False, False, name + ".reply")
                    # INVALID_HANDLE_VALUE + PAGE_READWRITE: no backing file is opened.
                    section = self._create_handle(
                        _create_mapping, _PTR(-1), None, 0x04, 0, size, name + ".section")
                    self._map(section, size)
                except BaseException:
                    _release(self.guard)
                    raise
                # The caller initializes the header while still owning guard.
            else:
                section = self._take_handle(_open_mapping(_MAP_ACCESS, False, name + ".section"))
                self.guard = self._take_handle(_open_mutex(_MUTEX_ACCESS, False, name + ".guard"))
                self.caller = self._take_handle(_open_mutex(_MUTEX_ACCESS, False, name + ".caller"))
                self.ready = self._take_handle(_open_event(_EVENT_ACCESS, False, name + ".reply"))
                self._map(section, size)
        except BaseException:
            self.close()
            raise

    def _create_handle(self, create, *args):
        ct.set_last_error(0)
        return self._take_handle(create(*args), create=True)

    def _take_handle(self, handle, *, create=False):
        error = ct.get_last_error()
        if not handle:
            if not create and error == 2:  # ERROR_FILE_NOT_FOUND
                raise TransportError("SHM server is not running in this Windows session")
            raise ct.WinError(error)
        self._handles.append(handle)
        if create and error == 183:  # ERROR_ALREADY_EXISTS
            raise TransportError("SHM endpoint already exists; stop the other server and clients")
        return handle

    def _map(self, handle, size):
        self._address = _map_view(handle, _MAP_ACCESS, 0, 0, size)
        if not self._address:
            raise TransportError("Cannot map SHM view; check both endpoints' sizes") from ct.WinError(
                ct.get_last_error())
        self.buffer = (ct.c_char * size).from_address(self._address)

    def release_initial_guard(self):
        if not _release(self.guard):
            raise ct.WinError(ct.get_last_error())

    def wait(self, handle, millis):
        result = _wait(handle, millis)
        if result in (0, 0x80, 0x102):  # signaled, abandoned mutex, timeout
            return result
        raise ct.WinError(ct.get_last_error())

    @contextmanager
    def locked(self, handle, millis):
        result = self.wait(handle, millis)
        try:
            yield result
        finally:
            if result != 0x102 and not _release(handle):
                raise ct.WinError(ct.get_last_error())

    def notify_reply(self):
        if not _signal(self.ready):
            raise ct.WinError(ct.get_last_error())

    def close(self):
        self.buffer = None
        if self._address:
            _unmap(self._address)
            self._address = None
        while self._handles:
            _close_handle(self._handles.pop())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
