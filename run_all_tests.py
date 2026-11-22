# coding: utf-8
"""按模块执行测试并输出汇总。默认跳过需要运行 QMT 和 Valkey 的在线测试。

用法
    python run_all_tests.py              # 所有离线测试（默认）
    python run_all_tests.py -v           # 详细输出
    python run_all_tests.py --live       # 同时运行在线 RPC 测试
    python run_all_tests.py --group signal_trader   # 只运行指定组
    python run_all_tests.py --group backtest
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")

# 测试分组（组名、路径、是否需要在线服务）
GROUPS = [
    ("signal_trader", [os.path.join("tests", "bigqmt_signal_trader")], False),
    ("backtest", [os.path.join("tests", "bigqmt_backtest")], False),
]
LIVE_GROUP = ("live_api", ["test_all_apis.py"], True)


def run_group(name, paths, verbose):
    """运行一组 pytest 测试，返回结果计数、耗时、输出和退出码。"""
    cmd = [sys.executable, "-m", "pytest"] + paths + ["-q" if not verbose else "-v"]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    elapsed = time.time() - t0
    out = (proc.stdout or "") + (proc.stderr or "")
    # 解析 pytest 的英文汇总，例如 "290 passed in 8.5s" / "1 failed, 289 passed, 3 skipped"
    passed = failed = skipped = 0
    for line in out.splitlines():
        line = line.strip()
        if not any(tok in line for tok in ("passed", "failed", "skipped", "error")):
            continue
        for part in line.split(","):
            words = part.strip().split()
            if len(words) >= 2 and words[0].isdigit():
                num = int(words[0])
                if words[1].startswith("passed"):
                    passed += num
                elif words[1].startswith("failed") or words[1].startswith("error"):
                    failed += num
                elif words[1].startswith("skipped"):
                    skipped += num
    return passed, failed, skipped, elapsed, out, proc.returncode


def main():
    parser = argparse.ArgumentParser(description="运行大 QMT 桥接测试")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--live", action="store_true", help="同时运行在线 RPC 测试（需启动 QMT）")
    parser.add_argument("--group", help="只运行指定组（signal_trader/backtest/live_api）")
    args = parser.parse_args()

    groups = list(GROUPS)
    if args.live:
        groups.append(LIVE_GROUP)
    if args.group:
        groups = [g for g in groups if g[0] == args.group]
        if not groups:
            print("未知分组 %s（可选：%s）" % (args.group, ", ".join(g[0] for g in GROUPS + [LIVE_GROUP])))
            return 1

    print("=" * 70)
    print("大 QMT 桥接全量测试")
    print("=" * 70)

    total_passed = total_failed = total_skipped = 0
    total_time = 0.0
    failed_groups = []
    for name, paths, needs_live in groups:
        print("\n--- %s ---" % name)
        passed, failed, skipped, elapsed, out, rc = run_group(name, paths, args.verbose)
        total_passed += passed
        total_failed += failed
        total_skipped += skipped
        total_time += elapsed
        status = "通过" if failed == 0 and rc == 0 else "失败"
        print("  %s: %d 项通过，%d 项失败，%d 项跳过（%.1f 秒）" % (status, passed, failed, skipped, elapsed))
        if failed or rc != 0:
            failed_groups.append(name)
            if not args.verbose:
                # 输出末尾的失败详情。
                tail = "\n".join(out.splitlines()[-20:])
                print(tail)

    print("\n" + "=" * 70)
    print("=== 汇总 ===")
    print("通过 %d / 失败 %d / 跳过 %d / 总计 %d" % (total_passed, total_failed, total_skipped, total_passed + total_failed + total_skipped))
    print("总耗时 %.1fs" % total_time)
    if failed_groups:
        print("失败分组: %s" % ", ".join(failed_groups))
        return 1
    print("全部通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
