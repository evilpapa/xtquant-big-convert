# coding: utf-8
"""local_serial_settings 供外部脚本读取 .env 中的账号、Valkey、QMT 目录和登录凭据。"""

import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.local_env import local_serial_settings


def test_reads_qmt_dir_for_the_serial_bridge_launcher(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "BIGQMT_ACCOUNT_ID=123456\nBIGQMT_QMT_DIR=D:/qmt\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    settings = local_serial_settings()

    assert settings["account_id"] == "123456"
    assert settings["qmt_dir"] == "D:/qmt"


def test_qmt_dir_defaults_to_empty(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("BIGQMT_ACCOUNT_ID=123456\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert local_serial_settings()["qmt_dir"] == ""


def test_qmt_dir_survives_a_missing_dotenv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert local_serial_settings()["qmt_dir"] == ""


def test_reads_login_credentials_from_dotenv(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "BIGQMT_LOGIN_USER=123456\nBIGQMT_LOGIN_PASSWORD='literal${PASSWORD}'\n",
        encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    settings = local_serial_settings()

    assert settings["login_user"] == "123456"
    assert settings["login_password"] == "literal${PASSWORD}"


def test_login_credentials_default_to_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    settings = local_serial_settings()

    assert settings["login_user"] == ""
    assert settings["login_password"] == ""
