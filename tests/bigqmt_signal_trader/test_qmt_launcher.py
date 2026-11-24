# coding: utf-8
"""qmt_launcher tests (issue #45).

The property that matters most is directory scoping: this machine runs several
QMT installs side by side, so a name-only match would stop the wrong account's
terminal. Process enumeration is stubbed so the tests never touch real ones.
"""

import os
import sys
import unittest
from types import SimpleNamespace

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import qmt_launcher
from bigqmt_signal_trader.qmt_launcher import (
    QmtLauncherError,
    close_qmt,
    find_qmt_processes,
    is_qmt_running,
    resolve_install_dir,
)


LEMO = os.path.normpath("D:/qmt_lemo/bin.x64")
OTHER = os.path.normpath("D:/qmt_other/bin.x64")

FAKE_PROCESSES = [
    (100, "XtItClient.exe", os.path.join(LEMO, "XtItClient.exe")),
    (101, "miniquote.exe", os.path.join(LEMO, "miniquote.exe")),
    (200, "XtItClient.exe", os.path.join(OTHER, "XtItClient.exe")),  # another account
    (300, "notepad.exe", os.path.join(LEMO, "notepad.exe")),         # not ours
    (400, "XtItClient.exe", ""),                                     # unreadable path
]


class ProcessStub(object):
    """Patch process enumeration and record what gets terminated."""

    def __init__(self, processes=None):
        self.processes = list(FAKE_PROCESSES if processes is None else processes)
        self.terminated = []
        self._orig_iter = None
        self._orig_term = None
        self._orig_isdir = None

    def __enter__(self):
        self._orig_iter = qmt_launcher._iter_processes
        self._orig_term = qmt_launcher._terminate
        self._orig_isdir = os.path.isdir

        def _terminate(pid, force=False):
            self.terminated.append((pid, force))
            self.processes = [p for p in self.processes if p[0] != pid]
            return True

        qmt_launcher._iter_processes = lambda: list(self.processes)
        qmt_launcher._terminate = _terminate
        os.path.isdir = lambda p: True
        return self

    def __exit__(self, *exc):
        qmt_launcher._iter_processes = self._orig_iter
        qmt_launcher._terminate = self._orig_term
        os.path.isdir = self._orig_isdir
        return False


class ResolveInstallDirTest(unittest.TestCase):
    def test_accepts_root_bin_and_exe_paths(self):
        expected = os.path.abspath("D:/qmt_lemo/bin.x64")
        orig = os.path.isdir
        os.path.isdir = lambda p: os.path.basename(os.path.normpath(p)).lower() == "bin.x64"
        try:
            for given in ("D:/qmt_lemo", "D:/qmt_lemo/bin.x64",
                          "D:/qmt_lemo/bin.x64/XtItClient.exe"):
                self.assertEqual(os.path.normpath(resolve_install_dir(given)), expected, given)
        finally:
            os.path.isdir = orig

    def test_strips_surrounding_quotes(self):
        self.assertTrue(resolve_install_dir('"D:/qmt_lemo/bin.x64"').endswith("bin.x64"))

    def test_empty_is_rejected(self):
        for value in ("", None, "   "):
            with self.assertRaises(QmtLauncherError):
                resolve_install_dir(value)


class FindProcessesTest(unittest.TestCase):
    def test_only_matches_the_requested_install(self):
        with ProcessStub():
            pids = sorted(p[0] for p in find_qmt_processes("D:/qmt_lemo"))
        self.assertEqual(pids, [100, 101])  # not 200 (other install)

    def test_other_install_is_found_independently(self):
        with ProcessStub():
            pids = sorted(p[0] for p in find_qmt_processes("D:/qmt_other"))
        self.assertEqual(pids, [200])

    def test_ignores_unrelated_process_names(self):
        with ProcessStub():
            names = [p[1] for p in find_qmt_processes("D:/qmt_lemo")]
        self.assertNotIn("notepad.exe", names)

    def test_skips_processes_with_no_readable_path(self):
        """Matching a QMT name with an unknown path would be a coin flip on
        which install it belongs to -- skip rather than guess."""
        with ProcessStub():
            pids = [p[0] for p in find_qmt_processes("D:/qmt_lemo")]
        self.assertNotIn(400, pids)

    def test_is_qmt_running_reflects_scope(self):
        with ProcessStub():
            self.assertTrue(is_qmt_running("D:/qmt_lemo"))
            self.assertFalse(is_qmt_running("D:/qmt_unused"))


class CloseQmtTest(unittest.TestCase):
    def test_closes_only_the_requested_install(self):
        with ProcessStub() as stub:
            closed = close_qmt("D:/qmt_lemo")
        self.assertEqual(closed, 2)
        self.assertEqual(sorted(pid for pid, _ in stub.terminated), [100, 101])
        self.assertNotIn(200, [pid for pid, _ in stub.terminated])

    def test_terminates_gracefully_before_forcing(self):
        """A hard kill skips the terminal's data flush and truncates the
        local K-line store."""
        with ProcessStub() as stub:
            close_qmt("D:/qmt_lemo")
        self.assertTrue(all(force is False for _, force in stub.terminated))

    def test_escalates_to_force_when_terminate_is_ignored(self):
        with ProcessStub() as stub:
            def _stubborn(pid, force=False):
                stub.terminated.append((pid, force))
                if force:
                    stub.processes = [p for p in stub.processes if p[0] != pid]
                return True

            qmt_launcher._terminate = _stubborn
            close_qmt("D:/qmt_lemo", timeout_seconds=6.0, force_after_seconds=1.0)

        self.assertTrue(any(force for _, force in stub.terminated))

    def test_no_processes_is_not_an_error(self):
        with ProcessStub(processes=[]):
            self.assertEqual(close_qmt("D:/qmt_lemo"), 0)


class ReadinessTest(unittest.TestCase):
    def test_wait_until_ready_raises_on_timeout(self):
        """A scheduled restart must fail loudly, not hand the next step a
        terminal that never came up."""
        orig = qmt_launcher.port_is_listening
        qmt_launcher.port_is_listening = lambda *a, **k: False
        try:
            with self.assertRaises(QmtLauncherError):
                qmt_launcher.wait_until_ready(port=1, timeout_seconds=0.3, poll_interval=0.1)
        finally:
            qmt_launcher.port_is_listening = orig

    def test_wait_until_ready_returns_elapsed_seconds(self):
        orig = qmt_launcher.port_is_listening
        qmt_launcher.port_is_listening = lambda *a, **k: True
        try:
            self.assertIsInstance(
                qmt_launcher.wait_until_ready(port=1, timeout_seconds=5.0), float)
        finally:
            qmt_launcher.port_is_listening = orig


@pytest.mark.parametrize("action", ["open", "restart"])
def test_login_credentials_fall_back_to_dotenv(tmp_path, monkeypatch, action):
    (tmp_path / ".env").write_text(
        "BIGQMT_LOGIN_USER=file-user\nBIGQMT_LOGIN_PASSWORD='literal${PASSWORD}'\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BIGQMT_LOGIN_USER", "process-user")
    monkeypatch.delenv("BIGQMT_LOGIN_PASSWORD", raising=False)
    received = {}

    def launch(install_dir, **kwargs):
        received.update(kwargs["credentials"])

    monkeypatch.setattr(qmt_launcher, f"{action}_qmt", launch)
    assert qmt_launcher.main([action, "--dir", "test-qmt", "--mode", "login"]) == 0
    assert received == {"user": "process-user", "password": "literal${PASSWORD}"}


def test_login_uses_versioned_qt_window_owned_by_spawned_terminal(monkeypatch):
    import ctypes

    inspected = []

    def inspect_style(handle, flag):
        inspected.append(handle)
        return 0x960b0000

    def enum_windows(callback, results):
        for handle in (30, 40):
            callback(handle, results)

    monkeypatch.setattr(ctypes, "windll", SimpleNamespace(
        user32=SimpleNamespace(SetProcessDPIAware=lambda: None)), raising=False)
    monkeypatch.setitem(sys.modules, "win32con", SimpleNamespace(GWL_STYLE=-16))
    monkeypatch.setitem(sys.modules, "win32gui", SimpleNamespace(
        IsWindowVisible=lambda handle: True,
        GetWindowText=lambda handle: "Test QMT",
        GetClassName=lambda handle: "Qt5152QWindowIcon",
        EnumWindows=enum_windows,
        GetWindowLong=inspect_style,
    ))
    monkeypatch.setitem(sys.modules, "win32process", SimpleNamespace(
        GetWindowThreadProcessId=lambda handle: (1, handle * 10)))
    monkeypatch.setattr(qmt_launcher.os.path, "isdir", lambda path: True)
    monkeypatch.setattr(qmt_launcher.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(qmt_launcher, "_spawn", lambda *args, **kwargs: SimpleNamespace(pid=400))

    qmt_launcher.open_qmt("D:/qmt", mode="login", wait_ready=False,
                          credentials={"user": "test", "password": "test"})

    assert inspected == [40]


class OpenQmtTest(unittest.TestCase):
    def _stub_spawn(self):
        calls = []
        orig_spawn = qmt_launcher._spawn
        orig_ready = qmt_launcher.wait_until_ready

        def spawn(cmd, cwd=None, shell=False):
            calls.append((cmd, cwd, shell))
            return SimpleNamespace(pid=100)

        qmt_launcher._spawn = spawn
        qmt_launcher.wait_until_ready = lambda *a, **k: 1.0
        return calls, orig_spawn, orig_ready

    def test_linkmini_mode_passes_the_passwordless_flag(self):
        calls, orig_spawn, orig_ready = self._stub_spawn()
        orig_isdir, orig_isfile = os.path.isdir, os.path.isfile
        os.path.isdir = lambda p: True
        os.path.isfile = lambda p: p.lower().endswith("xtminiqmt.exe")
        try:
            qmt_launcher.open_qmt("D:/qmt_lemo", mode="linkmini")
        finally:
            qmt_launcher._spawn, qmt_launcher.wait_until_ready = orig_spawn, orig_ready
            os.path.isdir, os.path.isfile = orig_isdir, orig_isfile

        self.assertEqual(len(calls), 1)
        self.assertIn("linkMini", calls[0][0])

    def test_login_mode_without_credentials_is_rejected(self):
        calls, orig_spawn, orig_ready = self._stub_spawn()
        orig_isdir, orig_isfile = os.path.isdir, os.path.isfile
        os.path.isdir = lambda p: True
        os.path.isfile = lambda p: True
        try:
            with self.assertRaises(QmtLauncherError):
                qmt_launcher.open_qmt("D:/qmt_lemo", mode="login", credentials={})
        finally:
            qmt_launcher._spawn, qmt_launcher.wait_until_ready = orig_spawn, orig_ready
            os.path.isdir, os.path.isfile = orig_isdir, orig_isfile

    def test_unknown_mode_is_rejected(self):
        orig = os.path.isdir
        os.path.isdir = lambda p: True
        try:
            with self.assertRaises(QmtLauncherError):
                qmt_launcher.open_qmt("D:/qmt_lemo", mode="teleport")
        finally:
            os.path.isdir = orig

    def test_bat_mode_requires_an_existing_bat(self):
        orig = os.path.isdir
        os.path.isdir = lambda p: True
        try:
            with self.assertRaises(QmtLauncherError):
                qmt_launcher.open_qmt("D:/qmt_lemo", mode="bat", bat_path="D:/nope.bat")
        finally:
            os.path.isdir = orig


class LoginWindowDetectionTest(unittest.TestCase):
    def test_known_styles_do_not_depend_on_dpi_or_window_size(self):
        self.assertEqual(qmt_launcher.classify_qmt_window(0x96000000), "login")
        self.assertEqual(qmt_launcher.classify_qmt_window(0x960b0000), "main")
        self.assertEqual(qmt_launcher.classify_qmt_window(0x970b0000), "main")
        self.assertEqual(qmt_launcher.classify_qmt_window(0xb60b0000), "main")
        self.assertEqual(qmt_launcher.classify_qmt_window(0x96000000 - 2**32), "login")

    def test_unknown_styles_cannot_be_treated_as_login_or_main(self):
        for style in (None, "bad", 0, 0x90000000, 0x96080000, 0x960f0000, 0xd6000000):
            self.assertEqual(qmt_launcher.classify_qmt_window(style), "unknown", style)

    def test_completion_waits_through_login_unknown_and_missing_windows(self):
        handles = iter((10, 0, 30, 20))
        styles = {10: 0x96000000, 30: 0, 20: 0x960b0000}
        result = qmt_launcher._wait_for_main_window(
            lambda: next(handles),
            lambda h: qmt_launcher.classify_qmt_window(styles[h]),
            timeout_seconds=1.0, poll_interval=0.0,
        )
        self.assertEqual(result, 20)

    def test_completion_rejects_persistent_login_and_unknown_window(self):
        for style in (0x96000000, 0):
            result = qmt_launcher._wait_for_main_window(
                lambda: 10,
                lambda h: qmt_launcher.classify_qmt_window(style),
                timeout_seconds=0.0, poll_interval=0.0,
            )
            self.assertIsNone(result)


def test_elevation_required_error_is_reported_without_traceback(monkeypatch, capsys):
    def boom(*_args, **_kwargs):
        exc = OSError(740, "请求的操作需要提升")
        exc.winerror = 740
        raise exc

    monkeypatch.setattr(qmt_launcher, "open_qmt", boom)

    assert qmt_launcher.main(["open", "--dir", "test-qmt", "--no-wait"]) == 3
    assert "管理员权限" in capsys.readouterr().err


if __name__ == "__main__":
    unittest.main()
