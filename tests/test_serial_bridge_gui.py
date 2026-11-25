"""串口工作台进程监控测试，使用模拟窗口和桥接进程。"""

import io
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("tkinter")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import serial_bridge_gui as gui


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(gui.SerialControllerApp, "_build_ui", lambda self: None)
    monkeypatch.setattr(gui.SerialControllerApp, "_refresh_state", lambda self: None)
    monkeypatch.setattr(gui.threading, "Thread", Mock())
    config = SimpleNamespace(port="COM11", python="", env_dir=".", vk_url="valkey://localhost:6379",
                             qmt_root="qmt")
    controller = gui.SerialControllerApp(Mock(), config)
    gui.threading.Thread.reset_mock()
    controller.hint_var = Mock()
    controller.stop_qmt_on_stop = Mock()
    controller.stop_qmt_on_stop.get.return_value = False
    return controller


def test_watchdog_checks_every_twenty_seconds_and_only_restarts_exited_bridge(app):
    app.root.after.assert_any_call(20_000, app._check_bridge_process)
    app._check_bridge_process()
    gui.threading.Thread.assert_not_called()

    app.bridge_proc = Mock()
    app.bridge_proc.poll.return_value = None
    app._bridge_watchdog_active = True
    app._check_bridge_process()
    gui.threading.Thread.assert_not_called()

    app.bridge_proc.poll.return_value = 0
    app._check_bridge_process()
    gui.threading.Thread.assert_called_once_with(
        target=app._start_worker, kwargs={"bridge_only": True}, daemon=True)
    app._check_bridge_process()
    assert gui.threading.Thread.call_count == 1
    app.root.after.assert_called_with(20_000, app._check_bridge_process)


@pytest.mark.parametrize("action", ["_on_stop", "_on_restart", "_on_close"])
def test_intentional_shutdown_disarms_bridge_watchdog(app, action):
    app._bridge_watchdog_active = True
    getattr(app, action)()
    gui.threading.Thread.reset_mock()
    app.root.after.reset_mock()

    assert not app._bridge_watchdog_active
    app.busy = False
    app._check_bridge_process()
    gui.threading.Thread.assert_not_called()
    if action == "_on_close":
        app.root.after.assert_not_called()


def test_recovery_retries_and_only_starts_the_bridge(app, monkeypatch):
    command = ["python", "-m", "bigqmt_signal_trader.serial_vk_bridge"]
    config_env = {"PYTHONIOENCODING": "utf-8"}
    running = Mock(pid=42)
    running.poll.return_value = None
    popen = Mock(return_value=running)
    occupants = Mock(side_effect=[[{"pid": 7}], []])
    monkeypatch.setattr(gui.core, "find_bridge_processes", occupants)
    monkeypatch.setattr(gui.core, "resolve_python", lambda _: (["python"], "python"))
    monkeypatch.setattr(gui.core, "bridge_command", lambda *_: command)
    monkeypatch.setattr(gui.core, "child_env", lambda _: config_env)
    monkeypatch.setattr(gui.subprocess, "Popen", popen)
    app._handle_qmt_after_bridge = Mock()
    app._post = Mock()
    gui.threading.Thread.return_value.start.side_effect = app.bridge_ready.set

    app._start_worker()
    assert app._bridge_watchdog_active
    app._handle_qmt_after_bridge.assert_called_once_with(running)
    popen.reset_mock()
    app._handle_qmt_after_bridge.reset_mock()
    running.poll.return_value = 1

    app._start_worker(bridge_only=True)
    app._post.assert_any_call("hint", "桥接自动重启失败，下一次检查时重试。")
    assert app._bridge_watchdog_active
    assert not app.busy
    popen.assert_not_called()

    replacement = Mock(pid=43)
    replacement.poll.return_value = None
    popen.return_value = replacement
    app._start_worker(bridge_only=True)
    popen.assert_called_once()
    assert popen.call_args.args == (command,)
    assert popen.call_args.kwargs["env"] == config_env
    assert app.bridge_proc is replacement
    assert app.bridge_state == "running"
    app._handle_qmt_after_bridge.assert_not_called()


def test_old_bridge_cannot_reset_replacement_session(app):
    old = Mock()
    old.stdout = io.BytesIO((gui.BRIDGE_READY_MARK + "\n").encode("utf-8"))
    old.wait.return_value = 1
    app.bridge_proc = Mock()
    app.bridge_state = "running"
    app._bridge_watchdog_active = True
    app.session_ready = True

    app._pump_bridge(old)
    app._on_bridge_exit(old, 1)

    assert not app.bridge_ready.is_set()
    assert not app.bridge_exited.is_set()
    assert app.session_ready
    assert app.bridge_state == "running"


def test_settings_saves_vk_timeout_and_passes_it_to_bridge(app, monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    app.config = gui.core.Config(env_file=str(env_file))
    app.config_var = Mock()
    monkeypatch.setattr(gui.messagebox, "showwarning", Mock())
    dialog = SimpleNamespace(
        app=app, destroy=Mock(),
        _vk_fields=lambda: ("valkey://127.0.0.1:6379/5", "127.0.0.1", 6379, 5, "", ""),
    )
    fields = {
        "port": "COM11", "baudrate": "921600", "max_frame": "8388608",
        "account": "test-account", "qmt_dir": str(tmp_path), "qmt_mode": "exe",
        "qmt_exe": "", "login_user": "", "login_password": "", "pubsub": False,
        "vk_timeout": "7.5",
    }
    for name, value in fields.items():
        setattr(dialog, name, Mock(get=Mock(return_value=value)))

    gui.SettingsDialog._on_save(dialog)

    assert app.config.vk_timeout == 7.5
    assert app.config.dotenv["BIGQMT_SERIAL_VK_TIMEOUT"] == "7.5"
    reloaded = gui.core.load_config(env_file=str(env_file))
    assert reloaded.vk_timeout == 7.5
    command = gui.core.bridge_command(["python"], reloaded)
    assert command[command.index("--vk-timeout") + 1] == "7.5"
    assert "超时 7.5 秒" in app.config_var.set.call_args.args[0]
