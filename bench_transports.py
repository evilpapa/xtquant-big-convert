# coding: utf-8
"""测量 Valkey RPC 的端到端延迟。"""
import argparse
import os
import statistics
import time
import uuid

from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client
from bigqmt_signal_trader.valkey_rpc import call_valkey_rpc


def _load_valkey_config():
    """从客户端配置或环境变量读取连接参数。"""
    try:
        from bigqmt_signal_trader.xtquant_compat import load_client_config

        config = load_client_config().get("valkey_config") or {}
    except Exception:
        config = {}
    return dict(config, socket_timeout=8)


VALKEY = _load_valkey_config()
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


def bench_valkey(n):
    r = build_valkey_client(VALKEY)
    # 预热并检查连通性
    try:
        call_valkey_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
    except Exception as e:
        print("无法连接 Valkey，跳过该项测试：%s" % e)
        return
    lats = []
    for _ in range(n):
        t0 = time.time()
        call_valkey_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
        lats.append((time.time() - t0) * 1000)
    _stats("valkey", lats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--count", type=int, default=100, help="每种传输方式的请求次数")
    ap.add_argument("--skip-valkey", action="store_true", help="跳过 Valkey 测试")
    args = ap.parse_args()
    print("=== 传输延迟对比（每项 %d 次） ===" % args.count)
    if not args.skip_valkey:
        bench_valkey(args.count)


if __name__ == "__main__":
    main()
