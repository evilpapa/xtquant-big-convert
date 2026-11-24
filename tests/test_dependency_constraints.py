# coding: utf-8
"""Check the VK dependency and the QMT compatibility documentation."""
import io
import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYPROJECT = os.path.join(ROOT, "pyproject.toml")
README = os.path.join(ROOT, "docs", "USER_GUIDE.md")


def _read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


class VkUpperBoundTest(unittest.TestCase):
    def test_the_vk_extra_has_an_upper_bound(self):
        line = [l for l in _read(PYPROJECT).splitlines()
                if l.strip().startswith("valkey = [")]
        self.assertTrue(line, "pyproject 里找不到 VK extra")
        self.assertRegex(line[0], r'"valkey>=\d+(?:\.\d+)*,<\d+"')

    def test_the_reason_is_written_down_next_to_the_constraint(self):
        """光有数字没有理由，下一个人会以为是随手写的然后删掉。"""
        text = _read(PYPROJECT)
        head = text.split("valkey = [")[0][-800:]
        self.assertIn("#245", head, "约束旁边没写为什么")


class ReadmeDocumentsTheConstraintsTest(unittest.TestCase):
    """详细文档 2026-09-23 起从 README 迁入 docs/USER_GUIDE.md（README 只留
    简介与免责声明），钉的内容跟着文档走。"""

    def test_python_minimum_is_312_in_package_and_guide(self):
        self.assertIn('requires-python = ">=3.12"', _read(PYPROJECT))
        text = _read(README)
        self.assertIn("客户端、QMT 策略和独立串口桥接都需要 Python 3.12", text)


if __name__ == "__main__":
    unittest.main()
