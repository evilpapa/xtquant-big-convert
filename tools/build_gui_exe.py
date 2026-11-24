# coding: utf-8
"""把 tools/serial_bridge_gui.py 打包成单文件 Windows exe。

    python -m pip install pyinstaller
    python tools/build_gui_exe.py

产物：dist/BigQMTSerialController.exe

exe 只是前端：serial_vk_bridge 和 qmt_launcher 仍由外部 Python 执行
（BIGQMT_PYTHON / PATH 里的 python，且该环境已安装 xtquant-big-convert）。
"""

import os
import subprocess
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "serial_bridge_gui.py")
NAME = "BigQMTSerialController"
DIST_DIR = os.path.join(ROOT, "dist")
BUILD_DIR = os.path.join(ROOT, "build", "pyinstaller")


def main():
    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print("缺少 pyinstaller：python -m pip install pyinstaller", file=sys.stderr)
        return 1

    command = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm", "--clean",
        "--onefile", "--windowed",
        "--name", NAME,
        "--paths", os.path.join(ROOT, "tools"),
        # psutil 让 QMT 进程检测在 wmic 被移除的 Windows 上也能用；
        # 构建环境没装时 PyInstaller 只会告警，回退到 wmic。
        "--hidden-import", "psutil",
        "--distpath", DIST_DIR,
        "--workpath", BUILD_DIR,
        "--specpath", os.path.join(ROOT, "build"),
        SCRIPT,
    ]
    print("运行：" + subprocess.list2cmdline(command))
    completed = subprocess.run(command, cwd=ROOT)
    if completed.returncode != 0:
        return completed.returncode
    target = os.path.join(DIST_DIR, NAME + ".exe")
    print("\n已生成：%s" % target)
    print("把 exe 放在仓库根目录或 dist/ 下时，会自动读取上一级 / 同级的 .env。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
