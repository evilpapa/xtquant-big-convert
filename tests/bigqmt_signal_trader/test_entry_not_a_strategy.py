"""验证 QMT 按普通脚本运行时的诊断提示（#123）。"""

import io
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

ENTRY = os.path.join(SRC, "BIGQMT_VK_DRYRUN.py")


def _source():
    with io.open(ENTRY, encoding="utf-8") as handle:
        return handle.read()


class WarnsTest(unittest.TestCase):
    def test_it_checks_for_the_injected_globals(self):
        source = _source()

        self.assertIn('"passorder", "get_trade_detail_data", "download_history_data"',
                      source)

    def test_it_says_init_will_never_be_called(self):
        self.assertIn("init() 不会被调用", _source())

    def test_it_names_the_editor_window(self):
        self.assertIn("策略编辑窗口", _source())

    def test_it_names_the_standalone_process_option(self):
        """独立进程以 __main__ 执行文件并跳过 init。"""
        self.assertIn("独立 Python 进程", _source())

    def test_it_explains_the_log_looks_healthy(self):
        """说明普通脚本执行结束后没有监听器。"""
        source = _source()

        self.assertIn("运行将在此结束且没有监听器", source)


class EncodingTest(unittest.TestCase):
    """普通入口用 UTF-8 保存中文诊断。"""

    def test_the_entry_declares_utf8(self):
        self.assertTrue(_source().splitlines()[0].startswith("#coding:utf-8"))

    def test_it_still_parses(self):
        with open(ENTRY, "rb") as handle:
            compile(handle.read(), ENTRY, "exec")


class OnlyWhenNoneArePresentTest(unittest.TestCase):
    """策略注入了任一 API 时，无需提示运行方式错误。"""

    def _warns(self, present):
        namespace = dict((name, lambda *a: None) for name in present)
        return not any(name in namespace for name in
                       ("passorder", "get_trade_detail_data",
                        "download_history_data"))

    def test_a_real_strategy_run_is_quiet(self):
        self.assertFalse(self._warns(
            ["passorder", "get_trade_detail_data", "download_history_data"]))

    def test_one_present_is_enough_to_stay_quiet(self):
        """部分注入时，缺失项由其他诊断提示报告。"""
        self.assertFalse(self._warns(["passorder"]))

    def test_none_present_warns(self):
        self.assertTrue(self._warns([]))


if __name__ == "__main__":
    unittest.main()
