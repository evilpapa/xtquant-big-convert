# coding: utf-8
"""Measure end-to-end Valkey RPC latency."""
import argparse
import os
import statistics
import time
import uuid

from bigqmt_signal_trader.adapters.valkey_common import build_valkey_client
from bigqmt_signal_trader.valkey_rpc import call_valkey_rpc


def _load_valkey_config():
    """Pull connection details from the local client config or env vars."""
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
        "%-8s n=%d  min=%.2f  p50=%.2f  p90=%.2f  p99=%.2f  max=%.2f  avg=%.2f ms"
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
    # warmup + connectivity
    try:
        call_valkey_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
    except Exception as e:
        print("Valkey server not reachable, skipping valkey leg: %s" % e)
        return
    lats = []
    for _ in range(n):
        t0 = time.time()
        call_valkey_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
        lats.append((time.time() - t0) * 1000)
    _stats("valkey", lats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--count", type=int, default=100, help="requests per transport")
    ap.add_argument("--skip-valkey", action="store_true", help="skip the valkey leg")
    args = ap.parse_args()
    print("=== transport latency comparison (n=%d each) ===" % args.count)
    if not args.skip_valkey:
        bench_valkey(args.count)


if __name__ == "__main__":
    main()
