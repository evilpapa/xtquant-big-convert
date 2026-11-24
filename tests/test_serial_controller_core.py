# coding: utf-8
"""串口桥接图形控制台的无界面逻辑测试。"""

import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import serial_controller_core as core


def test_parse_env_file_handles_quotes_comments_and_export(tmp_path):
    path = tmp_path / ".env"
    path.write_text(
        "# comment\n"
        "export BIGQMT_ACCOUNT_ID=123456\n"
        "BIGQMT_QMT_DIR=\"D:/qmt\"  # trailing comment\n"
        "BIGQMT_LOGIN_PASSWORD='literal${PASSWORD}'\n"
        "BIGQMT_SERIAL_PORT=COM12\n",
        encoding="utf-8")

    values = core.parse_env_file(str(path))

    assert values["BIGQMT_ACCOUNT_ID"] == "123456"
    assert values["BIGQMT_QMT_DIR"] == "D:/qmt"
    assert values["BIGQMT_LOGIN_PASSWORD"] == "literal${PASSWORD}"
    assert values["BIGQMT_SERIAL_PORT"] == "COM12"


def test_decode_log_line_falls_back_to_gbk_for_mixed_qmt_logs():
    assert core.decode_log_line("中文".encode("utf-8")) == "中文"
    assert core.decode_log_line("统计窗口".encode("gbk")) == "统计窗口"
    assert core.decode_log_line(b"bad:\xff")  # 不抛异常


def test_decode_log_line_keeps_valid_utf8_when_the_tail_is_truncated():
    # UTF-8 被截断后剩下的字节恰好是合法 GBK，不能因此整行按 GBK 解。
    raw = "，统计窗".encode("utf-8") + "口".encode("utf-8")[:2] + b"?10s"
    raw.decode("gbk")  # 前提：这串字节确实能被 GBK 解码

    decoded = core.decode_log_line(raw)

    assert decoded == "，统计窗?10s"
    assert "\ufffd" not in decoded
    assert "缁" not in decoded


def test_decode_log_line_keeps_qmt_truncation_marker():
    raw = "adjust 节拍：ticks=104，统计窗".encode("utf-8") + \
        "口".encode("utf-8")[:2] + b"?10s"

    decoded = core.decode_log_line(raw)

    assert decoded == "adjust 节拍：ticks=104，统计窗?10s"


def test_bridge_detection_ignores_shell_processes():
    assert core._is_python_or_bridge_name("python.exe")
    assert core._is_python_or_bridge_name("python3.12.exe")
    assert core._is_python_or_bridge_name("bigqmt-serial-bridge.exe")
    assert not core._is_python_or_bridge_name("bash.exe")
    assert not core._is_python_or_bridge_name("powershell.exe")

    cmdline = ["python.exe", "-m", "bigqmt_signal_trader.serial_valkey_bridge",
               "--port", "COM11"]
    assert core._is_bridge_command(cmdline)
    assert core._command_line_port(cmdline) == "COM11"
    assert core._command_line_port(["--port=COM12"]) == "COM12"


def test_find_bridge_processes_scopes_to_the_configured_port(monkeypatch):
    monkeypatch.setattr(core, "_bridge_processes_psutil", lambda: [
        {"pid": 101, "port": "COM11", "cmdline": "a"},
        {"pid": 102, "port": "", "cmdline": "default COM11"},
        {"pid": 103, "port": "COM12", "cmdline": "other"},
    ])

    found = core.find_bridge_processes("COM11")

    assert [item["pid"] for item in found] == [101, 102]
    assert core.find_bridge_processes("COM12")[0]["pid"] == 103
    assert core.find_bridge_processes("COM99") == []


def test_normalize_qmt_root_accepts_root_bin_and_exe(tmp_path):
    root = tmp_path / "qmt"
    bin_dir = root / "bin.x64"
    bin_dir.mkdir(parents=True)
    exe = bin_dir / "XtItClient.exe"
    exe.write_bytes(b"")

    assert os.path.normcase(core.normalize_qmt_root(str(root))) == os.path.normcase(str(root))
    assert os.path.normcase(core.normalize_qmt_root(str(bin_dir))) == os.path.normcase(str(root))
    assert os.path.normcase(core.normalize_qmt_root(str(exe))) == os.path.normcase(str(root))


def test_valkey_url_from_qmt_config_dict(tmp_path):
    path = tmp_path / "bigqmt_signal_trader_qmt_config.py"
    path.write_text(
        "BIGQMT_CONFIG = {\n"
        "    'host': '192.168.2.112',\n"
        "    'port': 6379,\n"
        "    'db': 5,\n"
        "    'username': '',\n"
        "    'password': '',\n"
        "}\n",
        encoding="utf-8")

    assert core.valkey_url_from_config(str(path)) == "valkey://192.168.2.112:6379/5"


def test_load_config_reads_explicit_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "BIGQMT_ACCOUNT_ID=123456\n"
        "BIGQMT_QMT_DIR=D:/qmt\n"
        "BIGQMT_SERIAL_PORT=COM12\n"
        "BIGQMT_SERIAL_BAUDRATE=115200\n"
        "BIGQMT_SERIAL_VALKEY_URL=valkey://127.0.0.1:6379/7\n"
        "BIGQMT_QMT_MODE=login\n",
        encoding="utf-8")

    config = core.load_config(env_file=str(env_file))

    assert config.env_file == str(env_file)
    assert config.account == "123456"
    assert config.qmt_root == os.path.abspath("D:/qmt")
    assert config.port == "COM12"
    assert config.baudrate == 115200
    assert config.valkey_url == "valkey://127.0.0.1:6379/7"
    assert config.qmt_mode == "login"


def test_load_config_rejects_a_missing_explicit_env_file(tmp_path):
    try:
        core.load_config(env_file=str(tmp_path / "missing.env"))
    except core.ConfigError as exc:
        assert "missing.env" in str(exc)
    else:
        raise AssertionError("missing explicit .env must be reported")


def test_load_config_uses_running_qmt_when_dir_is_unset(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("BIGQMT_ACCOUNT_ID=123456\n", encoding="utf-8")
    monkeypatch.delenv("BIGQMT_QMT_DIR", raising=False)
    monkeypatch.setattr(core, "detect_running_qmt_root", lambda: "D:/running_qmt")

    config = core.load_config(env_file=str(env_file))

    assert config.qmt_root == os.path.abspath("D:/running_qmt")


def test_bridge_command_carries_no_login_password(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "BIGQMT_ACCOUNT_ID=123456\n"
        "BIGQMT_LOGIN_USER=123456\n"
        "BIGQMT_LOGIN_PASSWORD=secret-value\n",
        encoding="utf-8")
    config = core.load_config(env_file=str(env_file))
    command = core.bridge_command(["python.exe"], config)

    assert "--account-id" in command
    assert "secret-value" not in command
    assert command[:3] == ["python.exe", "-m",
                           "bigqmt_signal_trader.serial_valkey_bridge"]


def test_child_env_passes_login_credentials_from_dotenv(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "BIGQMT_LOGIN_USER=from-file\n"
        "BIGQMT_LOGIN_PASSWORD=file-password\n",
        encoding="utf-8")
    monkeypatch.delenv("BIGQMT_LOGIN_USER", raising=False)
    monkeypatch.delenv("BIGQMT_LOGIN_PASSWORD", raising=False)
    config = core.load_config(env_file=str(env_file))

    env = core.child_env(config)

    assert env["BIGQMT_LOGIN_USER"] == "from-file"
    assert env["BIGQMT_LOGIN_PASSWORD"] == "file-password"
    assert env["PYTHONIOENCODING"] == "utf-8"


def test_wrap_executable_runs_bat_through_comspec(monkeypatch):
    monkeypatch.setenv("ComSpec", "cmd.exe")

    assert core.wrap_executable("C:/tools/python.bat") == [
        "cmd.exe", "/c", "C:/tools/python.bat"]
    assert core.wrap_executable("C:/tools/python.exe") == ["C:/tools/python.exe"]


def test_log_tailer_reads_initial_tail_and_follows(tmp_path):
    path = tmp_path / "XtClient_FormulaOutput_20260101.log"
    path.write_bytes("第一行\n".encode("utf-8") + "第二行\n".encode("gbk"))
    tailer = core.LogTailer(initial_bytes=64 * 1024)

    assert tailer.switch(str(path)) == ["第一行", "第二行"]
    assert tailer.poll() == []

    with open(path, "ab") as handle:
        handle.write("third\n".encode("utf-8"))
    assert tailer.poll() == ["third"]
    tailer.close()


def test_log_tailer_reopens_after_truncation(tmp_path):
    path = tmp_path / "log.txt"
    path.write_text("one\ntwo\n", encoding="utf-8")
    tailer = core.LogTailer()
    assert tailer.switch(str(path)) == ["one", "two"]

    path.write_text("new\n", encoding="utf-8")
    assert tailer.poll() == ["new"]
    tailer.close()


def test_qmt_log_path_uses_local_date(tmp_path):
    path = core.qmt_log_path(str(tmp_path), day="20261124")
    assert path == os.path.join(str(tmp_path), "userdata", "log",
                               "XtClient_FormulaOutput_20261124.log")
    assert core.qmt_log_path("") == ""


def test_qmt_requires_elevation_reads_the_manifest(tmp_path):
    elevated = tmp_path / "elevated.exe"
    elevated.write_bytes(
        b"<requestedExecutionLevel level='requireAdministrator' uiAccess='false'/>")
    normal = tmp_path / "normal.exe"
    normal.write_bytes(b"<requestedExecutionLevel level='asInvoker'/>")

    assert core.qmt_requires_elevation(str(elevated)) is True
    assert core.qmt_requires_elevation(str(normal)) is False
    assert core.qmt_requires_elevation(str(tmp_path / "missing.exe")) is False
    assert core.qmt_requires_elevation("") is False


def test_is_admin_returns_a_bool():
    assert isinstance(core.is_admin(), bool)


def test_relaunch_as_admin_returns_false_when_not_windows(monkeypatch):
    monkeypatch.setattr(core, "IS_WINDOWS", False)

    assert core.relaunch_as_admin(["--autostart"]) is False


def test_format_env_value_quotes_only_when_needed():
    assert core.format_env_value("plain") == "plain"
    assert core.format_env_value("") == ""
    assert core.format_env_value("has space") == '"has space"'
    assert core.format_env_value("has#hash") == '"has#hash"'
    assert core.format_env_value('quote"inside') == '"quote\\"inside"'


def test_update_env_file_round_trips_passwords_and_keeps_comments(tmp_path):
    path = tmp_path / ".env"
    path.write_text(
        "# 保留注释\n"
        "BIGQMT_ACCOUNT_ID=old\n"
        "BIGQMT_OBSOLETE=1\n",
        encoding="utf-8")

    core.update_env_file(
        str(path),
        {
            "BIGQMT_ACCOUNT_ID": "123456",
            "BIGQMT_LOGIN_PASSWORD": 'p@ss word#1"\\',
            "BIGQMT_LOGIN_USER": "",
        },
        remove_keys=("BIGQMT_OBSOLETE",))

    text = path.read_text(encoding="utf-8")
    assert "# 保留注释" in text
    assert "BIGQMT_OBSOLETE" not in text
    values = core.parse_env_file(str(path))
    assert values["BIGQMT_ACCOUNT_ID"] == "123456"
    assert values["BIGQMT_LOGIN_PASSWORD"] == 'p@ss word#1"\\'
    assert values["BIGQMT_LOGIN_USER"] == ""


def test_build_and_parse_valkey_url():
    url = core.build_valkey_url("192.168.2.112", 6379, 5, "user", "p@ss word")
    assert url == "valkey://user:p%40ss%20word@192.168.2.112:6379/5"
    assert core.parse_valkey_url(url) == {
        "host": "192.168.2.112", "port": 6379, "db": 5,
        "username": "user", "password": "p@ss word",
    }
    assert core.build_valkey_url("127.0.0.1:6380", 6379, 0) == "valkey://127.0.0.1:6380/0"
    assert core.build_valkey_url("") == ""


def test_load_config_reads_valkey_parts_and_login_credentials(tmp_path, monkeypatch):
    for key in ("BIGQMT_VALKEY_HOST", "BIGQMT_VALKEY_PORT", "BIGQMT_VALKEY_DB",
                "BIGQMT_VALKEY_USERNAME", "BIGQMT_VALKEY_PASSWORD",
                "BIGQMT_SERIAL_VALKEY_URL", "BIGQMT_VALKEY_URL",
                "BIGQMT_LOGIN_USER", "BIGQMT_LOGIN_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "BIGQMT_ACCOUNT_ID=123456\n"
        "BIGQMT_VALKEY_HOST=10.0.0.5\n"
        "BIGQMT_VALKEY_PORT=6380\n"
        "BIGQMT_VALKEY_DB=3\n"
        "BIGQMT_VALKEY_USERNAME=valkey-user\n"
        "BIGQMT_VALKEY_PASSWORD=valkey-pass\n"
        "BIGQMT_LOGIN_USER=login-user\n"
        "BIGQMT_LOGIN_PASSWORD=login-pass\n",
        encoding="utf-8")

    config = core.load_config(env_file=str(env_file))

    assert config.valkey_url == "valkey://valkey-user:valkey-pass@10.0.0.5:6380/3"
    assert (config.valkey_host, config.valkey_port, config.valkey_db) == ("10.0.0.5", 6380, 3)
    assert config.valkey_username == "valkey-user"
    assert config.valkey_password == "valkey-pass"
    assert config.login_credentials() == ("login-user", "login-pass")
