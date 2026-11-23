import builtins
from pathlib import Path

import pytest


@pytest.mark.parametrize("embedded", [False, True])
def test_logging_fallback_when_python_omits_logging(monkeypatch, tmp_path, capsys, embedded):
    source_path = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    original_import = builtins.__import__

    def import_without_logging(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "logging" or name.startswith("logging."):
            raise ImportError("broker python omits logging")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", "1")
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "1" if embedded else "0")
    namespace = {
        "__name__": "bigqmt_logging_fallback_test",
        "__builtins__": dict(vars(builtins), __import__=import_without_logging),
    }
    if embedded:
        namespace["__bigqmt_load_local_module"] = object()
    exec(compile(source_path.read_bytes(), str(source_path), "exec"), namespace)

    if embedded:
        def reject_io(*args, **kwargs):
            pytest.fail("embedded logging attempted filesystem I/O")

        with monkeypatch.context() as guard:
            guard.setattr(namespace["os"], "makedirs", reject_io)
            guard.setitem(namespace["__builtins__"], "open", reject_io)
            namespace["get_logger"]("rpc").error("startup failed: %s", "missing module")
            assert namespace["log_file_path"]() is None
        assert "[bigqmt.rpc] startup failed: missing module" in capsys.readouterr().out
        return

    logger = namespace["get_logger"]("rpc")
    logger.error("startup failed: %s", "missing module")

    log_path = Path(namespace["log_file_path"]())
    assert "[bigqmt.rpc] startup failed: missing module" in log_path.read_text()
