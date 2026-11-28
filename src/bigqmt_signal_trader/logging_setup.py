"""大 QMT 桥接的文件日志和标准输出。

内嵌单文件策略使用标准输出。外部进程的文件日志午夜轮转，默认保留 7 天，
可通过 BIGQMT_LOG_RETENTION_DAYS 调整。
BIGQMT_LOG_ENABLED 和 BIGQMT_LOG_TO_STDOUT 分别控制日志启用和标准输出。
"""

import datetime as _dt
import os
import sys
import time
import traceback

try:
    import logging
    import logging.handlers
except ImportError:
    # 部分券商的 Python 环境缺少 logging，使用简化的日志实现。
    logging = None

_LOGGER_NAME = "bigqmt"
_initialized = False
_EMBEDDED_STRATEGY = "__bigqmt_load_local_module" in globals()
_qmt_stdout_only = False


def use_qmt_stdout():
    """Use a console-only logger in this strategy process, without touching files."""
    global _qmt_stdout_only
    _qmt_stdout_only = True


def qmt_stdout_only():
    return _EMBEDDED_STRATEGY or _qmt_stdout_only


def _env_bool(name, default=True):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _resolve_log_dir():
    """查找可写的日志目录，优先使用 QMT 的 python 目录，其次使用用户缓存。"""
    if _EMBEDDED_STRATEGY or _qmt_stdout_only:
        return None
    candidates = []
    for entry in sys.path:
        try:
            if entry and entry.endswith(r"\python") and os.path.isdir(entry):
                candidates.append(entry)
        except Exception:
            continue
    candidates.append(os.path.join(os.path.expanduser("~"), ".cache", "bigqmt"))
    for base in candidates:
        try:
            path = os.path.join(base, "logs")
            os.makedirs(path, exist_ok=True)
            # 检查目录是否可写。
            probe = os.path.join(path, ".write_test")
            with open(probe, "w"):
                pass
            try:
                os.remove(probe)
            except Exception:
                pass
            return path
        except Exception:
            continue
    return None


if logging is not None:
    class _SafeStreamHandler(logging.Handler):
        """用 print 输出日志，忽略输出失败。"""

        def emit(self, record):
            try:
                print(self.format(record))
            except Exception:
                pass


class _FallbackLogger:
    """标准库缺少 logging 时使用的简化日志实现。"""

    def __init__(self, name):
        self.name = name

    def _write(self, level, message, args, exception_text=None):
        try:
            rendered = str(message) % args if args else str(message)
        except Exception:
            rendered = "%s %s" % (message, args)
        if exception_text:
            rendered = "%s\n%s" % (rendered, exception_text)
        line = "%s [%s] [%s] %s" % (
            _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), level, self.name, rendered,
        )
        if _env_bool("BIGQMT_LOG_ENABLED", True):
            log_dir = _resolve_log_dir()
            if log_dir is not None:
                try:
                    with open(os.path.join(log_dir, "bigqmt.log"), "a") as log_file:
                        log_file.write(line + "\n")
                except Exception:
                    pass
        if _env_bool("BIGQMT_LOG_TO_STDOUT", True):
            try:
                print(line)
            except Exception:
                pass

    def debug(self, message, *args):
        self._write("DEBUG", message, args)

    def info(self, message, *args):
        self._write("INFO", message, args)

    def warning(self, message, *args):
        self._write("WARNING", message, args)

    def error(self, message, *args):
        self._write("ERROR", message, args)

    def exception(self, message, *args):
        exc = sys.exception()
        details = str(exc) if _qmt_stdout_only else traceback.format_exc()
        self._write("ERROR", message, args, details)


def _cleanup_old_logs(log_dir, retention_days):
    """启动时清理过期日志，补充 TimedRotatingFileHandler 在轮转时的清理。"""
    try:
        cutoff = time.time() - retention_days * 86400
        for name in os.listdir(log_dir):
            if not (name.startswith("bigqmt") and name.endswith(".log") or ".log." in name):
                continue
            path = os.path.join(log_dir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                continue
    except Exception:
        pass


def _detach_existing_handlers(logger):
    """移除并关闭上一轮加载留下的处理器（#139）。

    策略重载会重置模块状态，logging 注册表仍保留旧处理器。
    移除处理器可避免重复写入，关闭句柄可避免 Windows 日志轮转失败。
    """
    for handler in list(getattr(logger, "handlers", [])):
        try:
            logger.removeHandler(handler)
        except Exception:
            pass
        try:
            handler.close()
        except Exception:
            pass


def _log_file_name():
    """按账号或进程号命名日志，减少 Windows 上多个进程争用文件（#139、#144）。

    按账号命名时，重启后可继续轮转同一文件。BIGQMT_LOG_NAME 可指定文件名。
    """
    if explicit := str(os.environ.get("BIGQMT_LOG_NAME") or "").strip():
        return explicit
    account = str(os.environ.get("BIGQMT_ACCOUNT_ID") or "").strip()
    tag = "".join(ch for ch in account if ch.isalnum()) or ("pid%d" % os.getpid())
    return "bigqmt-%s.log" % tag


def _tolerant_rotator(source, dest):
    """轮转时尝试重命名，文件被占用时跳过。"""
    try:
        if os.path.exists(source):
            os.replace(source, dest)
    except Exception:
        pass


def _setup():
    global _initialized
    if _initialized:
        return
    _initialized = True
    if logging is None:
        return
    logger = logging.getLogger(_LOGGER_NAME)
    _detach_existing_handlers(logger)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not _env_bool("BIGQMT_LOG_ENABLED", True):
        logger.addHandler(logging.NullHandler())
        return

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 文件日志在午夜轮转，默认保留 7 天。
    log_dir = _resolve_log_dir()
    if log_dir is not None:
        try:
            fname = os.path.join(log_dir, _log_file_name())
            file_handler = logging.handlers.TimedRotatingFileHandler(
                fname,
                when="midnight",
                interval=1,
                backupCount=int(os.environ.get("BIGQMT_LOG_RETENTION_DAYS", 7)),
                encoding="utf-8",
                utc=False,
            )
            file_handler.rotator = _tolerant_rotator
            file_handler.setFormatter(fmt)
            logger.addHandler(file_handler)
            _cleanup_old_logs(log_dir, int(os.environ.get("BIGQMT_LOG_RETENTION_DAYS", 7)))
        except Exception:
            pass

    # 同时输出到标准输出。
    if _env_bool("BIGQMT_LOG_TO_STDOUT", True):
        stream = _SafeStreamHandler()
        stream.setLevel(logging.INFO)
        stream.setFormatter(fmt)
        logger.addHandler(stream)

    if not logger.handlers:
        logger.addHandler(logging.NullHandler())


def get_logger(name=""):
    """返回 bigqmt 下的模块日志记录器，例如 bigqmt.rpc。"""
    suffix = str(name or "").strip(".")
    full = _LOGGER_NAME if not suffix else "%s.%s" % (_LOGGER_NAME, suffix)
    if _qmt_stdout_only:
        return _FallbackLogger(full)
    _setup()
    if logging is None:
        return _FallbackLogger(full)
    return logging.getLogger(full)


def log_file_path():
    """Return the current log file path (or None if file logging is off)."""
    if _qmt_stdout_only:
        return None
    _setup()
    log_dir = _resolve_log_dir()
    if log_dir is None:
        return None
    return os.path.join(log_dir, "bigqmt.log")
