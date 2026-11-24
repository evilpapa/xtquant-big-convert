# coding: utf-8
"""大 QMT VK RPC 延迟测试。

分别测量 ping 和 get_full_tick，观察传输与行情接口的耗时。
"""
import os
import statistics
import time

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


ACCOUNT = os.environ.get("BIGQMT_ACCOUNT_ID", "")
VK_CONFIG = _load_vk_config()


def bench(r, method, params, n=20, timeout=6):
    lats = []
    errors = 0
    for i in range(n):
        t0 = time.time()
        try:
            resp = call_vk_rpc(r, ACCOUNT, method, params, timeout_seconds=timeout)
            dt = (time.time() - t0) * 1000
            if resp.get("ok"):
                lats.append(dt)
            else:
                errors += 1
                if errors <= 2:
                    print("  %s 第 %d 次返回错误：%s" % (method, i, resp.get("error", "")[:120]))
        except Exception as e:
            errors += 1
            if errors <= 2:
                print("  %s 第 %d 次抛出异常：%s" % (method, i, e))
    if not lats:
        print("%-18s: 全部失败（%d 次错误）" % (method, errors))
        return
    lats.sort()
    p50 = statistics.median(lats)
    p95 = lats[int(len(lats) * 0.95)] if len(lats) >= 20 else lats[-1]
    print(
        "%-18s: 样本=%d 成功=%d 失败=%d  最小=%.0f  p50=%.0f  p95=%.0f  最大=%.0f  平均=%.0f 毫秒"
        % (
            method,
            len(lats),
            len(lats),
            errors,
            min(lats),
            p50,
            p95,
            max(lats),
            statistics.mean(lats),
        )
    )


def main():
    r = build_vk_client(VK_CONFIG)
    # 预热连接
    try:
        call_vk_rpc(r, ACCOUNT, "ping", {}, timeout_seconds=6)
        print("ping 预热成功\n")
    except Exception as e:
        print("预热失败：%s\n" % e)
        return

    print("=== 延迟测试（每项调用 20 次） ===")
    bench(r, "ping", {}, n=20)
    bench(r, "get_full_tick", {"codes": ["000001.SZ"]}, n=20)
    bench(r, "get_full_tick", {"codes": ["000001.SZ", "600000.SH", "000333.SZ"]}, n=20)
    bench(r, "get_instrument", {"code": "000001.SZ"}, n=20)


if __name__ == "__main__":
    main()
