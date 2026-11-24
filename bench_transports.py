# coding: utf-8
"""测量 VK RPC 的端到端延迟。"""
import argparse
import os
import statistics
import time
import uuid

from bigqmt_signal_trader.adapters.vk_common import build_vk_client
from bigqmt_signal_trader.vk_rpc import call_vk_rpc


def _load_vk_config():
    """从客户端配置或环境变量读取连接参数。"""
    try:
        from bigqmt_signal_trader.xtquant_compat import load_client_config

        config = load_client_config().get("vk_config") or {}
    except Exception:
        config = {}
    return dict(config, socket_timeout=8)


VK_CONFIG = _load_vk_config()
ACCOUNT = os.environ.get("BIGQMT_ACCOUNT_ID", "")


def _stats(name, lats):
    lats = sorted(lats)
    n = len(lats)
    print(
        "%-8s 样本=%d  最小=%.2f  p50=%.2f  p90=%.2f  p99=%.2f  最大=%.2f  平均=%.2f 毫秒"
        % (
            name,
            n,
            min(lats),
            statistics.median(lats),
            lats[int(n * 0.9)],
            lats[int(n * 0.99)] if n > 1 else lats[-1],
            max(lats),
            statistics.mean(lats),
        )
    )


def bench_vk(n):
    r = build_vk_client(VK_CONFIG)
    # 预热并检查连通性
    try:
        call_vk_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
    except Exception as e:
        print("无法连接 VK，跳过该项测试：%s" % e)
        return
    lats = []
    for _ in range(n):
        t0 = time.time()
        call_vk_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
        lats.append((time.time() - t0) * 1000)
    _stats("VK", lats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--count", type=int, default=100, help="每种传输方式的请求次数")
    ap.add_argument("--skip-vk", action="store_true", help="跳过 VK 测试")
    args = ap.parse_args()
    print("=== 传输延迟对比（每项 %d 次） ===" % args.count)
    if not args.skip_vk:
        bench_vk(args.count)


if __name__ == "__main__":
    main()
