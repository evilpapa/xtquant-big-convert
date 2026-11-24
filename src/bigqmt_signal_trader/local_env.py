"""Read local configuration for tools running outside QMT."""

def read_local_env():
    """Read BIGQMT_* assignments from the working directory's .env."""
    try:
        stream = open(".env", encoding="utf-8-sig")
    except FileNotFoundError:
        return {}

    with stream:
        from dotenv import dotenv_values

        return {
            key: value
            for key, value in dotenv_values(stream=stream, interpolate=False).items()
            if key.startswith("BIGQMT_") and value is not None
        }


def local_serial_settings():
    values = read_local_env()
    return {
        "account_id": values.get("BIGQMT_ACCOUNT_ID", "").strip(),
        "valkey_url": values.get("BIGQMT_SERIAL_VALKEY_URL") or values.get("BIGQMT_VALKEY_URL"),
        "qmt_dir": (values.get("BIGQMT_QMT_DIR") or "").strip(),
        # -QmtMode login 的备选凭据；环境变量优先，qmt_launcher 只在环境变量为空时读这里。
        "login_user": (values.get("BIGQMT_LOGIN_USER") or "").strip(),
        "login_password": values.get("BIGQMT_LOGIN_PASSWORD") or "",
    }
