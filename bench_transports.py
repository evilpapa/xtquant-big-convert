# coding: utf-8
"""Measure end-to-end Redis RPC latency."""
import argparse
import os
import statistics
import time
import uuid

import redis

from bigqmt_signal_trader.redis_rpc import call_redis_rpc


def _load_redis_config():
    """Pull connection details from the local client config or env vars."""
    try:
        from bigqmt_signal_trader.xtquant_compat import load_client_config

        cfg = load_client_config()
        rc = dict(cfg.get("redis_config") or {})
        rc.setdefault("host", os.environ.get("BIGQMT_REDIS_HOST", "127.0.0.1"))
        rc.setdefault("port", int(os.environ.get("BIGQMT_REDIS_PORT", "6379")))
        rc.setdefault("db", int(os.environ.get("BIGQMT_REDIS_DB", "5")))
        return {
            "host": rc.get("host"),
            "port": int(rc.get("port")),
            "db": int(rc.get("db")),
            "username": rc.get("username") or None,
            "password": rc.get("password") or None,
            "socket_timeout": 8,
        }
    except Exception:
        return {
            "host": os.environ.get("BIGQMT_REDIS_HOST", "127.0.0.1"),
            "port": int(os.environ.get("BIGQMT_REDIS_PORT", "6379")),
            "db": int(os.environ.get("BIGQMT_REDIS_DB", "5")),
            "socket_timeout": 8,
        }


REDIS = _load_redis_config()
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


def bench_redis(n):
    r = redis.Redis(**REDIS)
    # warmup + connectivity
    try:
        call_redis_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
    except Exception as e:
        print("Redis server not reachable, skipping redis leg: %s" % e)
        return
    lats = []
    for _ in range(n):
        t0 = time.time()
        call_redis_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
        lats.append((time.time() - t0) * 1000)
    _stats("redis", lats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--count", type=int, default=100, help="requests per transport")
    ap.add_argument("--skip-redis", action="store_true", help="skip the redis leg")
    args = ap.parse_args()
    print("=== transport latency comparison (n=%d each) ===" % args.count)
    if not args.skip_redis:
        bench_redis(args.count)


if __name__ == "__main__":
    main()
