# coding: utf-8
"""订阅中际旭创 (300308.SZ) 行情，用于实测 QMT 桥。

用法：
    # 在仓库根目录运行，.env 里的 VK 配置会被自动读取
    python scripts/subscribe_300308.py

    # 也可以手动指定 VK 地址
    BIGQMT_VK_URL=valkey://192.168.2.112:6379/5 python scripts/subscribe_300308.py

脚本会：
  1. 通过 RPC 探活（ping）
  2. 拉取一次 300308.SZ 的合约详情和最新快照
  3. 订阅 300308.SZ 全推行情，持续打印盘口变化
  4. 每 30 秒打印一次汇总（最新价、涨跌幅、成交额）
  5. Ctrl+C 退订并退出
"""

import os
import sys
import signal
import time
from datetime import datetime

# 从仓库运行时，将 src 加入模块搜索路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

STOCK_CODE = "300308.SZ"
STOCK_NAME = "中际旭创"

# ── 状态 ──────────────────────────────────────────────────────────
_last_print = {}       # 上一次打印的关键字段，用于去重
_tick_count = 0        # 收到的推送次数
_first_tick_ts = None  # 首次推送时间


def _fmt_price(p):
    """格式化价格，None / 0 显示为 '-'。"""
    if p is None or p == 0:
        return "-"
    return f"{p:.2f}"


def _fmt_volume(v):
    """成交量/额用万或亿做单位。"""
    if v is None:
        return "-"
    if abs(v) >= 1e8:
        return f"{v / 1e8:.2f}亿"
    if abs(v) >= 1e4:
        return f"{v / 1e4:.1f}万"
    return str(int(v))


def on_quote(datas):
    """全推回调：只关注 300308.SZ。"""
    global _tick_count, _first_tick_ts, _last_print

    tick = datas.get(STOCK_CODE)
    if tick is None:
        return

    _tick_count += 1
    if _first_tick_ts is None:
        _first_tick_ts = time.time()

    last_price = tick.get("lastPrice", 0)
    last_close = tick.get("lastClose", 0)
    volume = tick.get("volume", 0)        # 手
    amount = tick.get("amount", 0)        # 元
    high = tick.get("high", 0)
    low = tick.get("low", 0)
    bid1 = (tick.get("bidPrice") or [0])[0]
    ask1 = (tick.get("askPrice") or [0])[0]
    bid1v = (tick.get("bidVol") or [0])[0]
    ask1v = (tick.get("askVol") or [0])[0]

    # 去重：盘口没变就不打印
    key_fields = (last_price, volume, bid1, ask1)
    if key_fields == _last_print.get("key"):
        return
    _last_print["key"] = key_fields

    chg_pct = ((last_price - last_close) / last_close * 100) if last_close else 0
    ts = tick.get("stime", "") or datetime.now().strftime("%H:%M:%S")

    print(
        f"  {ts}  "
        f"最新 {_fmt_price(last_price)}  "
        f"涨跌 {chg_pct:+.2f}%  "
        f"高 {_fmt_price(high)} 低 {_fmt_price(low)}  "
        f"买一 {_fmt_price(bid1)}×{bid1v}  "
        f"卖一 {_fmt_price(ask1)}×{ask1v}  "
        f"成交额 {_fmt_volume(amount)}"
    )


def print_summary(xtdata):
    """每隔一段时间打印汇总。"""
    tick = (xtdata.get_full_tick([STOCK_CODE]) or {}).get(STOCK_CODE)
    if tick is None:
        print(f"\n── 汇总：{STOCK_CODE} 当前无行情 ──\n")
        return
    last_price = tick.get("lastPrice", 0)
    last_close = tick.get("lastClose", 0)
    chg_pct = ((last_price - last_close) / last_close * 100) if last_close else 0
    volume = tick.get("volume", 0)
    amount = tick.get("amount", 0)
    print(
        f"\n── 汇总 {datetime.now().strftime('%H:%M:%S')} ──"
        f"  {STOCK_NAME}({STOCK_CODE})"
        f"  最新 {_fmt_price(last_price)}  涨跌 {chg_pct:+.2f}%"
        f"  成交量 {_fmt_volume(volume * 100)}股"
        f"  成交额 {_fmt_volume(amount)}"
        f"  推送 {_tick_count} 次\n"
    )


def main():
    import bigqmt_signal_trader.xtquant_compat as compat

    print(f"{'=' * 68}")
    print(f"  {STOCK_NAME}({STOCK_CODE}) 行情订阅 — QMT 桥实测")
    print(f"{'=' * 68}\n")

    # 1. 初始化客户端（自动读取 .env 或环境变量中的 VK 配置）
    compat.configure()
    client = compat.get_default_client()
    xtdata = compat.xtdata

    print(f"[连接] 账号 {client.account_id}  传输 {client.transport_name}")

    # 2. 探活
    print("[探活] ping ...", end=" ", flush=True)
    try:
        pong = client.call("ping", {})
        server_time = pong.get("server_time", "?")
        print(f"OK  服务端时间 {server_time}")
    except Exception as e:
        print(f"FAIL: {e}")
        print("\n请确认 QMT 策略已启动、VK 可达。")
        sys.exit(1)

    # 3. 合约详情
    print(f"\n[合约] {STOCK_CODE} ...", end=" ", flush=True)
    try:
        detail = xtdata.get_instrument_detail(STOCK_CODE)
        if detail:
            print(f"{detail.get('InstrumentName', '?')}  上市 {detail.get('OpenDate', '?')}")
        else:
            print("无详情（可能终端未订阅该品种）")
    except Exception as e:
        print(f"异常: {e}")

    # 4. 拉一次最新快照
    print(f"\n[快照] get_full_tick ...", end=" ", flush=True)
    try:
        snapshot = xtdata.get_full_tick([STOCK_CODE])
        tick = (snapshot or {}).get(STOCK_CODE)
        if tick:
            print(f"最新 {_fmt_price(tick.get('lastPrice'))}  昨收 {_fmt_price(tick.get('lastClose'))}")
        else:
            print("无数据（非交易时段可能为空）")
    except Exception as e:
        print(f"异常: {e}")

    # 5. 订阅全推
    print(f"\n[订阅] subscribe_whole_quote(['{STOCK_CODE}']) ...")
    try:
        seq = xtdata.subscribe_whole_quote([STOCK_CODE], callback=on_quote)
        print(f"[订阅] 成功，订阅号 seq={seq}")
    except Exception as e:
        print(f"[订阅] 失败: {e}")
        print("回退到轮询模式 ...\n")
        seq = None

    print(f"\n{'─' * 68}")
    print(f"  等待行情推送中 ... Ctrl+C 退出")
    print(f"{'─' * 68}\n")

    # 6. 阻塞等待推送，每 30 秒打印汇总
    stop = False

    def _on_signal(sig, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    SUMMARY_INTERVAL = 30
    last_summary = time.time()

    try:
        while not stop:
            now = time.time()
            # 有订阅时依赖推送回调；没有订阅时轮询快照
            if seq is None:
                try:
                    snapshot = xtdata.get_full_tick([STOCK_CODE])
                    if snapshot:
                        on_quote(snapshot)
                except Exception:
                    pass
            if now - last_summary >= SUMMARY_INTERVAL:
                try:
                    print_summary(xtdata)
                except Exception:
                    pass
                last_summary = now
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    # 7. 退订
    print(f"\n\n[退出] 正在退订 ...")
    if seq is not None:
        try:
            xtdata.unsubscribe_quote(seq)
            print(f"[退出] 已退订 seq={seq}")
        except Exception as e:
            print(f"[退出] 退订异常: {e}")

    elapsed = time.time() - (_first_tick_ts or time.time())
    rate = _tick_count / elapsed if elapsed > 0 else 0
    print(f"[统计] 共收到 {_tick_count} 次推送，运行 {elapsed:.0f}s，平均 {rate:.1f} 次/秒")
    print("再见！")


if __name__ == "__main__":
    main()
