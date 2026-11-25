# coding: utf-8
"""Big QMT 串口桥接控制台的无界面逻辑。

GUI（tools/serial_bridge_gui.py）和打包后的 exe 共用这里的配置解析、子进程命令、
QMT 日志解码与 tail。模块不依赖 tkinter，便于单元测试。

约束：这些代码全部在 QMT 进程之外运行；QMT 进程内的策略仍受仓库 AGENTS.md 的
文件 / Socket 限制。
"""

import ast
import datetime as _dt
import os
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import quote, unquote, urlsplit

IS_WINDOWS = os.name == "nt"
CREATE_NO_WINDOW = 0x08000000 if IS_WINDOWS else 0
CREATE_NEW_PROCESS_GROUP = 0x00000200 if IS_WINDOWS else 0

DEFAULT_VK_URL = "valkey://127.0.0.1:6379/5"
DEFAULT_VK_TIMEOUT = 5.0
DEFAULT_QMT_EXE = "XtItClient.exe"
DEFAULT_PORT = "COM11"
DEFAULT_BAUDRATE = 921600
DEFAULT_MAX_FRAME_BYTES = 8388608
MAX_DISPLAY_CHARS = 4000

QMT_PROCESS_NAMES = ("XtItClient.exe", "XtMiniQmt.exe")
_QMT_ALL_PROCESS_NAMES = QMT_PROCESS_NAMES + (
    "miniquote.exe", "BrokerProxy.exe", "minibroker.exe")
_TRUTHY = frozenset(("1", "true", "yes", "y", "on"))
_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")
_INLINE_COMMENT = re.compile(r"\s+#")


class ConfigError(RuntimeError):
    """配置缺失或无效；GUI 直接展示给用户。"""


# ------------------------------------------------------------------ encoding
def decode_text(data):
    """解码子进程输出：优先 UTF-8，失败按 GBK（Windows 控制台常见）。"""
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("gbk", errors="replace")


def _utf8_sequence_length(byte):
    if byte < 0x80:
        return 1
    if 0xC2 <= byte <= 0xDF:
        return 2
    if 0xE0 <= byte <= 0xEF:
        return 3
    if 0xF0 <= byte <= 0xF4:
        return 4
    return 0


def _decode_mixed(raw):
    """一行里 UTF-8 / GBK 混排或被截断时，尽量保住可读部分。"""
    out = []
    index = 0
    length = len(raw)
    while index < length:
        size = _utf8_sequence_length(raw[index])
        end = index + size
        if size and end <= length:
            try:
                out.append(raw[index:end].decode("utf-8"))
                index = end
                continue
            except UnicodeDecodeError:
                pass
        start = index
        index += 1
        while index < length:
            size = _utf8_sequence_length(raw[index])
            if size and index + size <= length:
                try:
                    raw[index:index + size].decode("utf-8")
                except UnicodeDecodeError:
                    index += 1
                    continue
                break
            index += 1
        out.append(raw[start:index].decode("utf-8", errors="replace"))
    return "".join(out)


def _utf8_invalid_bytes(raw):
    """统计一行里无法按 UTF-8 解出的字节数。"""
    total = len(raw)
    valid = 0
    index = 0
    while index < total:
        size = _utf8_sequence_length(raw[index])
        end = index + size
        if size and end <= total:
            try:
                raw[index:end].decode("utf-8")
                valid += size
                index = end
                continue
            except UnicodeDecodeError:
                pass
        index += 1
    return total - valid


def decode_log_line(raw):
    """解码 QMT 日志的一行。

    ``XtClient_FormulaOutput_*.log`` 会把 QMT 自己的 GBK 文本和策略打印的
    UTF-8 文本混在同一个文件里，还会截断多字节字符。坑在于：UTF-8 中文被截断
    后剩下的字节往往仍是合法的 GBK，所以不能简单地“UTF-8 失败就整行 GBK”。

    顺序是：

    1. 整行 UTF-8（纯策略输出、ASCII）；
    2. 如果只有少量字节不合法、且 UTF-8 容错结果里仍有中文，说明这是一行被
       截断的 UTF-8（常见：QMT 把 ``统计窗口`` 的最后一个字切掉），丢掉截断的
       字节后返回其余中文（QMT 通常已经在截断处留下 ``?``）；
    3. 整行 GBK（QMT 自己的中文日志）；
    4. 两者都失败时逐段解码，尽量保住可读部分。
    """
    if isinstance(raw, str):
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    if _utf8_invalid_bytes(raw) <= 6:
        candidate = raw.decode("utf-8", errors="ignore")
        cjk = sum(1 for char in candidate if "\u4e00" <= char <= "\u9fff")
        if cjk >= 1:
            return candidate
    try:
        return raw.decode("gbk")
    except UnicodeDecodeError:
        return _decode_mixed(raw)


def clip_for_display(text, limit=MAX_DISPLAY_CHARS):
    """单行过长（下载几千只股票的参数）时截断，避免 Text 控件卡死。"""
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + "…（已截断 %d 字符）" % (len(text) - limit)


# ------------------------------------------------------------------ .env file
_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", '"': '"', "\\": "\\"}


def _unescape_double(text):
    out = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            out.append(_ESCAPES.get(text[index + 1], text[index + 1]))
            index += 2
        else:
            out.append(char)
            index += 1
    return "".join(out)


def _strip_value(value):
    value = value.strip()
    if value[:1] == '"':
        out = []
        index = 1
        while index < len(value):
            char = value[index]
            if char == "\\" and index + 1 < len(value):
                out.append(value[index:index + 2])
                index += 2
                continue
            if char == '"':
                return _unescape_double("".join(out))
            out.append(char)
            index += 1
        return _unescape_double("".join(out))
    if value[:1] == "'":
        end = value.rfind("'")
        if end > 0:
            return value[1:end]
    match = _INLINE_COMMENT.search(value)
    if match:
        value = value[:match.start()].rstrip()
    return value


def format_env_value(value):
    """把值写成 .env 形式；需要时用双引号并转义。"""
    text = "" if value is None else str(value)
    if text == "":
        return ""
    if (re.search(r"[\s#\"'\\]", text) or text != text.strip()
            or "\n" in text or "\r" in text or "\t" in text):
        escaped = (text.replace("\\", "\\\\")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("\t", "\\t")
                   .replace('"', '\\"'))
        return '"%s"' % escaped
    return text


_KEY_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def update_env_file(path, values, remove_keys=()):
    """就地更新 .env，保留注释和无关键；不存在的键追加到末尾。"""
    path = os.path.abspath(path)
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    try:
        with open(path, encoding="utf-8-sig") as handle:
            lines = handle.read().splitlines()
    except OSError:
        lines = []
    wanted = {key: format_env_value(value) for key, value in values.items()}
    removed = set(remove_keys)
    result = []
    written = set()
    for line in lines:
        match = _KEY_LINE.match(line)
        key = match.group(1) if match else None
        if key in wanted:
            result.append("%s=%s" % (key, wanted[key]))
            written.add(key)
        elif key in removed:
            continue
        else:
            result.append(line)
    for key, value in wanted.items():
        if key not in written:
            result.append("%s=%s" % (key, value))
    text = "\n".join(result)
    if text:
        text += "\n"
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    os.replace(tmp_path, path)
    return path


def default_env_path():
    """没有现成 .env 时，设置面板把新文件写到哪里。"""
    if getattr(sys, "frozen", False):
        return os.path.join(app_dir(), ".env")
    return os.path.join(os.getcwd(), ".env")


# ------------------------------------------------------------------ elevation
_ELEVATION_CACHE = {}


def is_admin():
    """当前进程是否已提升权限。"""
    if not IS_WINDOWS:
        return False
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def qmt_requires_elevation(exe_path):
    """读 PE manifest，判断 QMT 是否声明了 requireAdministrator。

    XtItClient.exe（国金 2.1.x）的 manifest 是 ``requireAdministrator``，
    非提升进程直接 CreateProcess 会得到 WinError 740。
    """
    path = str(exe_path or "")
    if not path or not os.path.isfile(path):
        return False
    try:
        stat = os.stat(path)
    except OSError:
        return False
    key = (os.path.abspath(path), stat.st_size, int(stat.st_mtime))
    if key in _ELEVATION_CACHE:
        return _ELEVATION_CACHE[key]
    marker = b"requireAdministrator"
    found = False
    try:
        with open(path, "rb") as handle:
            tail = b""
            while True:
                chunk = handle.read(2 * 1024 * 1024)
                if not chunk:
                    break
                if marker in tail + chunk:
                    found = True
                    break
                tail = chunk[-64:]
    except OSError:
        return False
    _ELEVATION_CACHE[key] = found
    return found


def relaunch_as_admin(extra_args=None, cwd=None):
    """通过 ShellExecuteW(runas) 以管理员身份重新启动本程序。

    ``extra_args`` 追加到当前命令行末尾；GUI 内部的
    ``--autostart/--restart-qmt/--keep-qmt`` 会先从原命令行移除，避免状态重复。
    成功返回 True（UAC 已确认，新进程已经启动）；用户取消或失败返回 False。
    """
    if not IS_WINDOWS:
        return False
    extra = list(extra_args or [])
    internal = ("--autostart", "--restart-qmt", "--keep-qmt", "--no-auto-restart")
    base = [arg for arg in sys.argv[1:] if arg not in internal]
    if getattr(sys, "frozen", False):
        executable = sys.executable
        params = base + extra
    else:
        executable = sys.executable
        params = [os.path.abspath(sys.argv[0])] + base + extra
    command_line = subprocess.list2cmdline(params)
    try:
        import ctypes
        result = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", executable, command_line, cwd or os.getcwd(), 1)
    except Exception:
        return False
    return int(result) > 32


def parse_env_file(path):
    """读取最小可用的 .env：KEY=VALUE、``export``、引号和行内注释。

    不实现变量插值——与 ``python-dotenv`` 的 ``interpolate=False`` 保持一致，
    避免密码里的 ``$`` 被意外展开。
    """
    values = {}
    if not path:
        return values
    try:
        with open(path, encoding="utf-8-sig") as handle:
            lines = handle.read().splitlines()
    except OSError:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if not match:
            continue
        values[match.group(1)] = _strip_value(match.group(2))
    return values


def _first_existing(paths, kind=os.path.isfile):
    seen = set()
    for path in paths:
        if not path:
            continue
        key = os.path.normcase(os.path.abspath(path))
        if key in seen:
            continue
        seen.add(key)
        if kind(path):
            return path
    return ""


def app_dir():
    """exe 所在目录；源码运行时是 tools/ 目录。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def find_env_file(explicit=None):
    candidates = [explicit, os.environ.get("BIGQMT_ENV_FILE")]
    base = app_dir()
    candidates += [
        os.path.join(base, ".env"),
        os.path.join(os.path.dirname(base), ".env"),  # dist/ 的上一级 = 仓库根目录
        os.path.join(os.getcwd(), ".env"),
    ]
    return _first_existing(candidates)


# ------------------------------------------------------------------ QMT config
def read_qmt_config_dict(path):
    """用 AST 取 ``BIGQMT_CONFIG`` 字面量，不执行 QMT 侧的配置文件。"""
    try:
        with open(path, encoding="utf-8-sig") as handle:
            source = handle.read()
    except OSError:
        return {}
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError:
        return {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "BIGQMT_CONFIG"
                   for target in node.targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, SyntaxError):
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def vk_url_from_config(path):
    config = read_qmt_config_dict(path)
    if not config:
        return ""
    url = str(config.get("url") or "").strip()
    if url:
        return url
    host = str(config.get("host") or "").strip()
    if not host:
        return ""
    return build_vk_url(host, config.get("port") or 6379,
                            config.get("db") or 0,
                            config.get("username") or "",
                            config.get("password") or "")


def build_vk_url(host, port=6379, db=0, username="", password=""):
    """由地址 / 端口 / DB / 凭据拼连接串；允许直接传入完整 URL。"""
    host = str(host or "").strip()
    if not host:
        return ""
    if "://" in host:
        return host
    if not re.search(r":\d+$", host):
        host = "%s:%s" % (host, int(port or 6379))
    auth = ""
    if password:
        auth = "%s:%s@" % (quote(str(username or ""), safe=""),
                           quote(str(password), safe=""))
    elif username:
        auth = "%s@" % quote(str(username), safe="")
    try:
        db = int(db or 0)
    except (TypeError, ValueError):
        db = 0
    return "valkey://%s%s/%d" % (auth, host, db)


def parse_vk_url(url):
    """把连接串拆成设置面板的字段。无法解析时返回默认值。"""
    result = {"host": "127.0.0.1", "port": 6379, "db": 0,
              "username": "", "password": ""}
    try:
        parts = urlsplit(str(url or ""))
        hostname = parts.hostname
    except ValueError:
        return result
    if not hostname:
        return result
    result["host"] = hostname
    try:
        result["port"] = int(parts.port or 6379)
    except ValueError:
        result["port"] = 6379
    db_text = (parts.path or "/0").strip("/") or "0"
    try:
        result["db"] = int(db_text)
    except ValueError:
        result["db"] = 0
    result["username"] = unquote(parts.username or "")
    result["password"] = unquote(parts.password or "")
    return result


def find_qmt_config_file(qmt_dir="", env_dir=""):
    candidates = []
    for base in (env_dir, app_dir(), os.path.dirname(app_dir()), os.getcwd()):
        if base:
            candidates.append(os.path.join(base, "bigqmt_signal_trader_qmt_config.py"))
    if qmt_dir:
        candidates.append(os.path.join(
            normalize_qmt_root(qmt_dir), "python", "bigqmt_signal_trader_qmt_config.py"))
    return _first_existing(candidates)


# -------------------------------------------------------------------- config
def _as_bool(value, default=False):
    if value is None or value == "":
        return default
    return str(value).strip().lower() in _TRUTHY


def _as_int(value, name):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        raise ConfigError("%s 不是有效整数：%r" % (name, value))


def parse_vk_timeout(value):
    message = "VK 桥接超时必须为大于 0 且不超过 60 的秒数"
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        raise ValueError(message) from None
    if not 0 < seconds <= 60:
        raise ValueError(message)
    return seconds


def normalize_qmt_root(path):
    """把安装根目录、bin.x64 或 exe 路径统一成安装根目录。"""
    if not path:
        return ""
    text = str(path).strip().strip('"').strip("'")
    if not text:
        return ""
    absolute = os.path.abspath(os.path.expanduser(text))
    if os.path.isfile(absolute) or absolute.lower().endswith(".exe"):
        absolute = os.path.dirname(absolute)
    if os.path.basename(absolute).lower() == "bin.x64":
        absolute = os.path.dirname(absolute)
    return absolute


class Config(object):
    """控制台运行所需的全部配置。凭据只放在内存和子进程环境里。"""

    def __init__(self, **values):
        self.env_file = ""
        self.env_dir = os.getcwd()
        self.account = ""
        self.vk_url = DEFAULT_VK_URL
        self.vk_host = "127.0.0.1"
        self.vk_port = 6379
        self.vk_db = 0
        self.vk_username = ""
        self.vk_password = ""
        self.vk_timeout = DEFAULT_VK_TIMEOUT
        self.qmt_root = ""
        self.qmt_exe = DEFAULT_QMT_EXE
        self.qmt_mode = "exe"
        self.port = DEFAULT_PORT
        self.baudrate = DEFAULT_BAUDRATE
        self.max_frame_bytes = DEFAULT_MAX_FRAME_BYTES
        self.pubsub = False
        self.python = ""
        self.login_user = ""
        self.login_password = ""
        self.dotenv = {}
        self.local_config = ""
        self.__dict__.update(values)

    @property
    def qmt_bin_dir(self):
        return os.path.join(self.qmt_root, "bin.x64") if self.qmt_root else ""

    @property
    def qmt_log_dir(self):
        return os.path.join(self.qmt_root, "userdata", "log") if self.qmt_root else ""

    def summary(self):
        return "账号 %s  |  串口 %s@%s  |  VK %s（超时 %s 秒）  |  QMT %s" % (
            self.account or "(未配置)", self.port, self.baudrate,
            redact_url(self.vk_url), self.vk_timeout, self.qmt_root or "(未配置)")

    def login_credentials(self):
        return self.login_user, self.login_password


def redact_url(url):
    return re.sub(r"//[^/@]*@", "//***@", str(url or ""))


def load_config(overrides=None, env_file=None):
    overrides = dict(overrides or {})
    if env_file:
        explicit = os.path.abspath(os.path.expanduser(env_file))
        if not os.path.isfile(explicit):
            raise ConfigError("找不到 .env：%s" % env_file)
        env_path = explicit
    else:
        env_path = find_env_file()
    values = parse_env_file(env_path)
    env_dir = os.path.dirname(env_path) if env_path else os.getcwd()

    def pick(override_key, env_key, default=""):
        value = overrides.get(override_key)
        if value not in (None, ""):
            return str(value).strip()
        return str(values.get(env_key) or os.environ.get(env_key) or default).strip()

    account = pick("account", "BIGQMT_ACCOUNT_ID")
    qmt_dir = pick("qmt_dir", "BIGQMT_QMT_DIR")
    vk_url = pick("vk_url", "BIGQMT_SERIAL_VK_URL")
    if not vk_url:
        vk_url = pick("vk_url", "BIGQMT_VK_URL")
    vk_host = pick("vk_host", "BIGQMT_VK_HOST")
    vk_port = pick("vk_port", "BIGQMT_VK_PORT", "6379") or "6379"
    vk_db = pick("vk_db", "BIGQMT_VK_DB", "0") or "0"
    vk_username = pick("vk_username", "BIGQMT_VK_USERNAME")
    vk_password = pick("vk_password", "BIGQMT_VK_PASSWORD")
    try:
        vk_timeout = parse_vk_timeout(
            pick("vk_timeout", "BIGQMT_SERIAL_VK_TIMEOUT", str(DEFAULT_VK_TIMEOUT)))
    except ValueError as exc:
        raise ConfigError("BIGQMT_SERIAL_VK_TIMEOUT：%s" % exc) from exc
    if not vk_url and vk_host:
        vk_url = build_vk_url(vk_host, vk_port, vk_db,
                                      vk_username, vk_password)
    port = pick("port", "BIGQMT_SERIAL_PORT", DEFAULT_PORT) or DEFAULT_PORT
    baudrate = pick("baudrate", "BIGQMT_SERIAL_BAUDRATE", str(DEFAULT_BAUDRATE))
    max_frame = pick("max_frame_bytes", "BIGQMT_SERIAL_MAX_FRAME_BYTES",
                     str(DEFAULT_MAX_FRAME_BYTES))
    pubsub_raw = overrides.get("pubsub")
    if pubsub_raw is None:
        pubsub_raw = values.get("BIGQMT_SERIAL_VK_PUBSUB",
                                os.environ.get("BIGQMT_SERIAL_VK_PUBSUB"))
    qmt_mode = pick("qmt_mode", "BIGQMT_QMT_MODE", "exe").lower()
    qmt_exe = pick("qmt_exe", "BIGQMT_QMT_EXE", DEFAULT_QMT_EXE) or DEFAULT_QMT_EXE

    if qmt_mode not in ("exe", "login"):
        raise ConfigError("QMT 启动方式只支持 exe / login，当前为 %r" % qmt_mode)

    qmt_root = normalize_qmt_root(qmt_dir)
    if not qmt_root:
        qmt_root = normalize_qmt_root(detect_running_qmt_root())

    local_config = find_qmt_config_file(qmt_root, env_dir)
    if not vk_url and local_config:
        vk_url = vk_url_from_config(local_config)
    if not vk_url:
        vk_url = DEFAULT_VK_URL
    vk_parts = parse_vk_url(vk_url)

    if overrides.get("login_password") is not None:
        login_password = str(overrides["login_password"])
    else:
        login_password = os.environ.get("BIGQMT_LOGIN_PASSWORD")
        if login_password is None:
            login_password = values.get("BIGQMT_LOGIN_PASSWORD", "")
    login_user = (overrides.get("login_user")
                  or os.environ.get("BIGQMT_LOGIN_USER")
                  or values.get("BIGQMT_LOGIN_USER") or "").strip()

    python = overrides.get("python") or os.environ.get("BIGQMT_PYTHON") or ""

    return Config(
        env_file=env_path,
        env_dir=env_dir,
        account=account,
        vk_url=vk_url,
        vk_host=vk_parts["host"],
        vk_port=vk_parts["port"],
        vk_db=vk_parts["db"],
        vk_username=vk_parts["username"],
        vk_password=vk_parts["password"],
        vk_timeout=vk_timeout,
        qmt_root=qmt_root,
        qmt_exe=qmt_exe,
        qmt_mode=qmt_mode,
        port=port,
        baudrate=_as_int(baudrate, "BIGQMT_SERIAL_BAUDRATE"),
        max_frame_bytes=_as_int(max_frame, "BIGQMT_SERIAL_MAX_FRAME_BYTES"),
        pubsub=_as_bool(pubsub_raw),
        python=python,
        login_user=login_user,
        login_password=login_password,
        dotenv=values,
        local_config=local_config,
    )


# --------------------------------------------------------------- processes
def _list_processes_psutil():
    try:
        import psutil
    except ImportError:
        return None
    found = []
    wanted = set(name.lower() for name in _QMT_ALL_PROCESS_NAMES)
    for proc in psutil.process_iter(["pid", "name", "exe"]):
        try:
            info = proc.info
            name = str(info.get("name") or "")
            exe = str(info.get("exe") or "")
            if name.lower() in wanted and exe:
                found.append((int(info["pid"]), name, exe))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except Exception:
            continue
    return found


def _list_processes_wmic():
    try:
        completed = subprocess.run(
            ["wmic", "process", "get", "Name,ExecutablePath,ProcessId", "/format:csv"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=15, creationflags=CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError):
        return []
    found = []
    wanted = set(name.lower() for name in _QMT_ALL_PROCESS_NAMES)
    for row in decode_text(completed.stdout).splitlines():
        parts = [part.strip() for part in row.split(",")]
        # CSV 列顺序：Node,ExecutablePath,Name,ProcessId
        if len(parts) < 4:
            continue
        exe, name, pid_text = parts[1], parts[2], parts[3]
        if name.lower() not in wanted or not exe:
            continue
        try:
            found.append((int(pid_text), name, exe))
        except ValueError:
            continue
    return found


def list_qmt_processes():
    found = _list_processes_psutil()
    if found is None:
        found = _list_processes_wmic()
    return found


def running_qmt_processes(qmt_root):
    """只返回指定安装目录下的 QMT 进程，避免误伤同机其他账号。"""
    if not qmt_root:
        return []
    target = os.path.normcase(os.path.join(normalize_qmt_root(qmt_root), "bin.x64"))
    result = []
    for pid, name, exe in list_qmt_processes():
        if os.path.normcase(os.path.dirname(exe)) == target:
            result.append((pid, name, exe))
    return result


def detect_running_qmt_root():
    for _pid, _name, exe in list_qmt_processes():
        root = normalize_qmt_root(exe)
        if root:
            return root
    return ""


# ---------------------------------------------------------- serial bridge
_BRIDGE_MARKERS = (
    "bigqmt_signal_trader.serial_vk_bridge",
    "bigqmt_signal_trader.serial_valkey_bridge",
    "bigqmt-serial-bridge",
)


def _is_bridge_command(cmdline):
    text = cmdline if isinstance(cmdline, str) else " ".join(str(part) for part in (cmdline or []))
    return any(marker in text for marker in _BRIDGE_MARKERS)


def _is_python_or_bridge_name(name):
    """只认 python.exe / pythonw.exe / bigqmt-serial-bridge.exe，
    避免把命令行里碰巧包含模块名的 bash、编辑器或 grep 当成桥接进程。"""
    lower = str(name or "").lower()
    return lower.startswith("python") or lower.startswith("bigqmt-serial-bridge")


def _command_line_port(cmdline):
    parts = [str(part) for part in (cmdline or [])]
    for index, part in enumerate(parts):
        if part == "--port" and index + 1 < len(parts):
            return parts[index + 1]
        if part.startswith("--port="):
            return part.split("=", 1)[1]
    return ""


def _bridge_processes_psutil():
    try:
        import psutil
    except ImportError:
        return None
    found = []
    current = os.getpid()
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            info = proc.info
            pid = int(info["pid"])
            if pid == current:
                continue
            cmdline = list(info.get("cmdline") or [])
            if (_is_python_or_bridge_name(info.get("name"))
                    and _is_bridge_command(cmdline)):
                found.append({"pid": pid, "port": _command_line_port(cmdline),
                              "cmdline": " ".join(cmdline)})
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except Exception:
            continue
    return found


def _bridge_processes_wmic():
    try:
        completed = subprocess.run(
            ["wmic", "process", "get", "Name,CommandLine,ProcessId", "/format:list"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=15, creationflags=CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError):
        return []
    found = []
    block = {}
    for line in decode_text(completed.stdout).splitlines() + [""]:
        line = line.strip()
        if not line:
            if block:
                if (_is_python_or_bridge_name(block.get("Name"))
                        and _is_bridge_command(block.get("CommandLine", ""))):
                    try:
                        pid = int(block.get("ProcessId", "0"))
                    except ValueError:
                        pid = 0
                    if pid and pid != os.getpid():
                        found.append({"pid": pid,
                                      "port": _command_line_port(block.get("CommandLine", "").split()),
                                      "cmdline": block.get("CommandLine", "")})
                block = {}
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            block[key] = value
    return found


def find_bridge_processes(port=None):
    """找出占用指定串口的 serial_vk_bridge 进程（当前进程除外）。"""
    found = _bridge_processes_psutil()
    if found is None:
        found = _bridge_processes_wmic()
    if not port:
        return found
    wanted = str(port).strip().lower()
    scoped = []
    for item in found:
        item_port = (item.get("port") or "").lower()
        if item_port == wanted:
            scoped.append(item)
        elif not item_port and wanted == DEFAULT_PORT.lower():
            # 命令行没写 --port 时，桥接使用默认 COM11。
            scoped.append(item)
    return scoped


def terminate_bridge_processes(processes, timeout=8.0):
    """停止外部桥接进程；返回已处理的 PID 列表。"""
    pids = []
    for item in processes or []:
        try:
            pid = int(item["pid"])
        except (KeyError, TypeError, ValueError):
            continue
        if pid != os.getpid() and pid not in pids:
            pids.append(pid)
    if not pids:
        return []
    try:
        import psutil
    except ImportError:
        for pid in pids:
            subprocess.run(["taskkill", "/pid", str(pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           creationflags=CREATE_NO_WINDOW)
            subprocess.run(["taskkill", "/pid", str(pid), "/f"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           creationflags=CREATE_NO_WINDOW)
        return pids

    targets = []
    for pid in pids:
        try:
            targets.append(psutil.Process(pid))
        except psutil.Error:
            continue
    for proc in targets:
        try:
            proc.terminate()
        except psutil.Error:
            pass

    def alive():
        result = []
        for proc in targets:
            try:
                if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                    result.append(proc)
            except psutil.Error:
                pass
        return result

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not alive():
            return pids
        time.sleep(0.2)
    for proc in alive():
        try:
            proc.kill()
        except psutil.Error:
            pass
    return pids


# ---------------------------------------------------------------- commands
def wrap_executable(path):
    """``python`` 可能是 .bat shim（pyenv / conda），需要经 cmd.exe 启动。"""
    path = str(path)
    if path.lower().endswith((".bat", ".cmd")):
        comspec = os.environ.get("ComSpec") or "cmd.exe"
        return [comspec, "/c", path]
    return [path]


def resolve_python(explicit=None):
    """找到一个能 ``import bigqmt_signal_trader`` 的解释器。

    返回 ``(command_prefix, description)``。GUI 打包成 exe 后 ``sys.executable``
    是 exe 本身，不能用来 ``-m``，所以必须找外部 Python。
    """
    candidates = []
    for candidate in (explicit, os.environ.get("BIGQMT_PYTHON"),
                      None if getattr(sys, "frozen", False) else sys.executable,
                      shutil.which("python.exe"), shutil.which("python"),
                      shutil.which("python3.exe"), shutil.which("python3")):
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    if not candidates:
        raise ConfigError(
            "找不到 Python。请安装 xtquant-big-convert，并设置 BIGQMT_PYTHON "
            "或用 --python 指定解释器。")
    errors = []
    for candidate in candidates:
        command = wrap_executable(candidate)
        try:
            completed = subprocess.run(
                command + ["-c", "import bigqmt_signal_trader"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=30, creationflags=CREATE_NO_WINDOW)
        except (OSError, subprocess.SubprocessError) as exc:
            errors.append("%s: %s" % (candidate, exc))
            continue
        if completed.returncode == 0:
            return command, candidate
        errors.append("%s: %s" % (candidate, decode_text(completed.stderr).strip()))
    raise ConfigError(
        "找不到可用的 Python（需要已安装 xtquant-big-convert）。尝试过：\n"
        + "\n".join(errors[:4]))


def child_env(config):
    """给桥接 / qmt_launcher 子进程的环境：.env 补齐，显式环境变量优先。"""
    env = os.environ.copy()
    for key in ("BIGQMT_ACCOUNT_ID", "BIGQMT_QMT_DIR", "BIGQMT_LOGIN_USER",
                "BIGQMT_LOGIN_PASSWORD", "BIGQMT_SERIAL_VK_URL",
                "BIGQMT_VK_URL", "BIGQMT_QMT_MODE", "BIGQMT_QMT_EXE"):
        if not env.get(key) and config.dotenv.get(key):
            env[key] = config.dotenv[key]
    if config.account:
        env.setdefault("BIGQMT_ACCOUNT_ID", config.account)
    if config.login_user:
        env.setdefault("BIGQMT_LOGIN_USER", config.login_user)
    if config.login_password:
        env.setdefault("BIGQMT_LOGIN_PASSWORD", config.login_password)
    env["BIGQMT_SERIAL_VK_URL"] = config.vk_url
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    return env


def bridge_command(python_command, config):
    command = list(python_command) + [
        "-m", "bigqmt_signal_trader.serial_vk_bridge",
        "--port", config.port,
        "--baudrate", str(config.baudrate),
        "--account-id", config.account,
        "--max-frame-bytes", str(config.max_frame_bytes),
        "--vk-url", config.vk_url,
        "--vk-timeout", str(config.vk_timeout),
    ]
    if config.pubsub:
        command.append("--vk-pubsub")
    return command


def qmt_launcher_command(python_command, action, config, extra=None):
    command = list(python_command) + [
        "-m", "bigqmt_signal_trader.qmt_launcher", action,
        "--dir", config.qmt_root,
    ]
    if extra:
        command.extend(extra)
    return command


def run_capture(command, cwd=None, env=None, timeout=180):
    """运行一个短命令并返回 ``(returncode, text)``；超时返回 124。"""
    try:
        completed = subprocess.run(
            command, cwd=cwd, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=timeout,
            creationflags=CREATE_NO_WINDOW)
    except subprocess.TimeoutExpired as exc:
        output = exc.output or b""
        return 124, decode_text(output) + "\n[命令超时 %.0f 秒]" % timeout
    except OSError as exc:
        return 1, str(exc)
    return completed.returncode, decode_text(completed.stdout)


# ------------------------------------------------------------------ log tail
class LogTailer(object):
    """按行 follow 一个文本日志；文件截断/重建时自动从头读。"""

    def __init__(self, initial_bytes=256 * 1024):
        self.initial_bytes = int(initial_bytes)
        self.path = ""
        self._handle = None
        self._pending = b""

    def switch(self, path, from_start=False):
        """切换文件。返回初始要显示的行（from_start=True 时为空）。"""
        self.close()
        self.path = path or ""
        self._pending = b""
        if not self.path or not os.path.isfile(self.path):
            return []
        try:
            self._handle = open(self.path, "rb")
        except OSError:
            self._handle = None
            return []
        if from_start:
            return []
        try:
            size = os.path.getsize(self.path)
            start = max(0, size - self.initial_bytes)
            self._handle.seek(start)
            data = self._handle.read() or b""
            self._handle.seek(size)
        except OSError:
            self.close()
            return []
        if start and b"\n" in data:
            data = data.split(b"\n", 1)[1]  # 丢掉被截断的首行
        return self._split(data)

    def poll(self):
        """返回自上次调用以来的新行。"""
        if not self.path:
            return []
        if self._handle is None:
            if not os.path.isfile(self.path):
                return []
            try:
                self._handle = open(self.path, "rb")
                self._pending = b""
                return self._split(self._handle.read() or b"")
            except OSError:
                return []
        try:
            size = os.path.getsize(self.path)
            if size < self._handle.tell():
                # 被截断或重写：重新打开，从头开始。
                self._handle.close()
                self._handle = open(self.path, "rb")
                self._pending = b""
            data = self._handle.read() or b""
        except OSError:
            self.close()
            return []
        return self._split(data)

    def _split(self, data):
        if not data:
            return []
        buffer = self._pending + data
        parts = buffer.split(b"\n")
        self._pending = parts.pop()
        return [decode_log_line(part.rstrip(b"\r")) for part in parts]

    def close(self):
        if self._handle is not None:
            try:
                self._handle.close()
            except OSError:
                pass
        self._handle = None


def qmt_log_path(qmt_root, day=None):
    """``<QMT>/userdata/log/XtClient_FormulaOutput_YYYYMMDD.log``。"""
    if not qmt_root:
        return ""
    day = day or _dt.date.today().strftime("%Y%m%d")
    return os.path.join(normalize_qmt_root(qmt_root), "userdata", "log",
                        "XtClient_FormulaOutput_%s.log" % day)
