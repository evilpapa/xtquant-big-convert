"""The single-file QMT builds must actually build and load (issues #56, #76).

Some brokers' QMT sandboxes reject external imports and file loading outright,
so the only way in is one self-contained strategy file. tools/build_single_file
and tools/build_no_vk_single_file_flat generate those.

The flat builder execs every embedded module inside a ``def _mod_N():`` body.
That is precisely the shape that made ``from xtquant.xtconstant import *`` a
SyntaxError in issue #76 -- a bug the entire rest of the suite stayed green
through, because nothing else compiles a module inside a function. These tests
exist so the single-file builds are checked rather than silently rotting: they
run the real builders and compile what comes out.
"""

import io
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = os.path.join(ROOT, "tools")

BUILDERS = {
    "valkey": ("build_single_file.py", "utf-8"),
}

_built = {}


def build(kind):
    """Run a builder once per session and return (path, source)."""
    if kind in _built:
        return _built[kind]
    script, encoding = BUILDERS[kind]
    handle, out_path = tempfile.mkstemp(suffix="_%s.py" % kind)
    os.close(handle)
    env = dict(os.environ, BIGQMT_BUILD_OUT=out_path)
    completed = subprocess.run(
        [sys.executable, os.path.join(TOOLS, script)],
        cwd=TOOLS, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if completed.returncode != 0:
        raise AssertionError("%s failed:\n%s"
                             % (script, completed.stdout.decode("utf-8", "replace")))
    with io.open(out_path, encoding=encoding) as stream:
        source = stream.read()
    _built[kind] = (out_path, source)
    return _built[kind]


class BuildsSucceedTest(unittest.TestCase):
    def test_vk_build_compiles(self):
        path, source = build("valkey")
        encoding_line, generated_at, _ = source.split("\n", 2)
        self.assertEqual(encoding_line, "#coding:gbk")
        self.assertRegex(
            generated_at,
            r"^# Generated at: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$",
        )
        with open(path, "rb") as stream:
            compile(stream.read(), path, "exec")

    pass

    pass

    def test_no_star_imports_survive_into_either_build(self):
        for kind in BUILDERS:
            _path, source = build(kind)
            offenders = re.findall(r"^\s*from \S+ import \*\s*$", source, re.M)
            self.assertEqual(offenders, [], "%s build: %s" % (kind, offenders))


class ConfigBlockTest(unittest.TestCase):
    """The template config ships to users; a contributor's own settings must
    not ride along. The flat builder arrived carrying a real account id and
    rpc_allow_order_methods=True."""

    def test_account_id_is_read_when_the_strategy_runs(self):
        for kind in BUILDERS:
            path, source = build(kind)
            config = compile(source.split("_EMBEDDED_SOURCES = {", 1)[0], path, "exec")
            for value in (None, "  ", "  test-account  "):
                with self.subTest(kind=kind, value=value), patch.dict(os.environ):
                    if value is None:
                        os.environ.pop("BIGQMT_ACCOUNT_ID", None)
                    else:
                        os.environ["BIGQMT_ACCOUNT_ID"] = value
                    namespace = {}
                    if value and value.strip():
                        exec(config, namespace)
                    else:
                        with self.assertWarnsRegex(RuntimeWarning, "BIGQMT_ACCOUNT_ID"):
                            exec(config, namespace)
                    self.assertEqual(namespace["BIGQMT_ACCOUNT_ID"], (value or "").strip())

    def test_no_bare_digit_account_id(self):
        for kind in BUILDERS:
            _path, source = build(kind)
            leaked = re.findall(r'BIGQMT_ACCOUNT_ID = "(\d{4,})"', source)
            self.assertEqual(leaked, [], "%s build leaked an account id" % kind)

    def test_order_rpc_is_off_by_default(self):
        """Remote order/cancel must be opt-in, matching the example config."""
        for kind in BUILDERS:
            _path, source = build(kind)
            self.assertIn('"rpc_allow_order_methods": False,', source, kind)
            self.assertNotIn('"rpc_allow_order_methods": True,', source, kind)

    def test_background_threads_stay_off(self):
        """get_trade_detail_data returns empty off the main strategy thread."""
        for kind in BUILDERS:
            _path, source = build(kind)
            self.assertIn('"rpc_background_threads": False,', source, kind)

    def test_account_type_is_carried_through(self):
        """Added in #68; the templates arrived without it."""
        for kind in BUILDERS:
            _path, source = build(kind)
            self.assertIn("BIGQMT_ACCOUNT_TYPE", source, kind)

    def test_template_config_matches_the_documented_example(self):
        """Drift between the two silently gives single-file users a different
        default than everyone else."""
        example = os.path.join(
            ROOT, "src", "bigqmt_signal_trader_qmt_config.example.py")
        with io.open(example, encoding="utf-8") as stream:
            example_source = stream.read()
        keys = re.findall(r'^\s{4}"([a-z_]+)":', example_source, re.M)
        self.assertTrue(keys, "no keys parsed from the example config")

        for kind in BUILDERS:
            _path, source = build(kind)
            missing = [key for key in keys if '"%s":' % key not in source]
            self.assertEqual(missing, [], "%s build missing %s" % (kind, missing))


@pytest.mark.parametrize(("setting", "expected"), [
    ('" TrUe " # explicit opt-in', True),
    ("0", False),
    ("invalid", False),
])
def test_builder_bakes_only_the_order_switch_from_local_env(tmp_path, setting, expected):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        f"BIGQMT_RPC_ALLOW_ORDER_METHODS={setting}\n"
        "BIGQMT_VK_PASSWORD=fixture-secret-not-for-strategy\n",
        encoding="utf-8-sig",
    )
    artifact = tmp_path / "strategy.py"
    subprocess.run(
        [sys.executable, os.path.join(TOOLS, "build_single_file.py")],
        cwd=tmp_path,
        env=dict(os.environ, BIGQMT_BUILD_OUT=str(artifact)),
        check=True,
        capture_output=True,
    )
    source = artifact.read_text(encoding="ascii")
    assert f'"rpc_allow_order_methods": {expected},' in source
    assert "fixture-secret-not-for-strategy" not in source
    assert "__RPC_ALLOW_ORDER_METHODS__" not in source


class EmbeddedContentTest(unittest.TestCase):
    def test_both_builds_embed_the_whole_package(self):
        sys.path.insert(0, TOOLS)
        import build_single_file

        package = os.path.join(ROOT, "src", "bigqmt_signal_trader")
        expected = sum(
            1
            for directory, dirs, files in os.walk(package)
            for name in files
            if name.endswith(".py")
            and "__pycache__" not in directory
            and name not in build_single_file.EXCLUDED_MODULES
        )
        for kind in BUILDERS:
            _path, source = build(kind)
            found = len(re.findall(r'"bigqmt_signal_trader/[\w/]+\.py"', source))
            self.assertGreaterEqual(
                found, expected, "%s build embedded %d of %d package modules"
                % (kind, found, expected))

    def test_setup_tooling_is_kept_out_of_the_strategy_builds(self):
        """init_config imports subprocess and getpass. QMT's whitelist has
        already rejected socket here, so setup tooling has no business riding
        along in a file that runs inside the sandbox."""
        for kind in BUILDERS:
            _path, source = build(kind)
            self.assertNotIn('"bigqmt_signal_trader/init_config.py"', source, kind)

    def test_excluding_a_module_something_imports_fails_the_build(self):
        """An earlier attempt at trimming this build excluded a module that was
        imported at the top level of another, and it died on load instead of at
        build time."""
        sys.path.insert(0, TOOLS)
        import build_single_file

        top_dir, package_dir = build_single_file.resolve_source_dirs()
        with self.assertRaises(SystemExit):
            build_single_file.collect_package(
                package_dir, top_dir, excluded=("vk_common.py",))

    pass


class SimulatorTest(unittest.TestCase):
    def run_simulator(self, *args):
        return subprocess.run(
            [sys.executable, os.path.join(TOOLS, "simulate_qmt.py"), *args],
            cwd=ROOT,
            env=dict(os.environ, BIGQMT_ACCOUNT_ID="ignored", BIGQMT_VK_PORT="invalid"),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8", timeout=15,
        )

    def test_simulator_builds_and_runs_lifecycle_and_serial_demo(self):
        completed = self.run_simulator()
        self.assertEqual(completed.returncode, 0, completed.stdout)
        for check in ("ping", "get_asset", "get_full_tick", "order_callback", "deal_callback"):
            self.assertIn(f"{check} OK", completed.stdout)
        self.assertIn("策略已停止", completed.stdout)

    def test_simulator_sends_rpc_to_an_existing_artifact(self):
        path, _source = build("valkey")
        completed = self.run_simulator(
            path, "--rpc", "get_full_tick", "--params", '{"codes":["000001.SZ"]}')
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertIn('"lastPrice": 10.0', completed.stdout)

    def test_simulator_reports_io_even_when_strategy_catches_it(self):
        _path, source = build("valkey")
        probe = """
import socket
for attempt in (lambda: open('unexpected-file'), socket.socket):
    try:
        attempt()
    except OSError:
        pass
"""
        with tempfile.NamedTemporaryFile(suffix=".py", delete_on_close=False) as artifact:
            artifact.close()
            Path(artifact.name).write_text(source + probe, encoding="utf-8")
            completed = self.run_simulator(artifact.name, "--rpc", "ping")
        self.assertEqual(completed.returncode, 1, completed.stdout)
        self.assertIn("检测到策略尝试外部 I/O", completed.stdout)
        self.assertIn("  open ", completed.stdout)
        self.assertIn("  socket.__new__ ", completed.stdout)

    def test_serial_startup_failure_does_not_fall_back_to_external_io(self):
        _path, source = build("valkey")
        probe = """
import serial
def unavailable_port(**options):
    raise OSError('simulated port unavailable')
serial.Serial = unavailable_port
"""
        with tempfile.NamedTemporaryFile(suffix=".py", delete_on_close=False) as artifact:
            artifact.close()
            Path(artifact.name).write_text(source + probe, encoding="utf-8")
            completed = self.run_simulator(artifact.name, "--timeout", "0.2")
        self.assertEqual(completed.returncode, 1, completed.stdout)
        self.assertIn("simulated port unavailable", completed.stdout)
        self.assertNotIn("检测到策略尝试外部 I/O", completed.stdout)


def tearDownModule():
    for path, _source in _built.values():
        try:
            os.remove(path)
        except OSError:
            pass


if __name__ == "__main__":
    unittest.main()
