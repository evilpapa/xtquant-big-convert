# coding: utf-8
"""大 QMT VK 桥接命令行客户端（江海证券部署）。

在外部 Python 环境运行，通过 xtquant_big_convert RPC 查询资产、持仓、委托、
成交和行情，也可提交委托和撤单。用法示例：

    python qmt_cli.py discover
    python qmt_cli.py ping
    python qmt_cli.py asset
    python qmt_cli.py positions
    python qmt_cli.py orders
    python qmt_cli.py trades
    python qmt_cli.py tick 000001.SZ 600519.SH
    python qmt_cli.py kline 000001.SZ --period 1d --count 5
    python qmt_cli.py buy 000001.SZ 100 10.50
    python qmt_cli.py sell 000001.SZ 100 11.00
    python qmt_cli.py cancel 1234
    python qmt_cli.py watch

账号依次来自 --account、本地 .env、环境变量或客户端配置模块。
`discover` 在 VK 中查找已注册的账号。
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import bigqmt_signal_trader_client_config as cfg  # noqa: E402
except ImportError:  # 配置尚未生成，例如部署前运行 discover。
    cfg = None

from bigqmt_signal_trader.xtquant_compat import (  # noqa: E402
    BigQmtRpcClient,
    BigQmtXtTrader,
    BigQmtXtData,
)
from bigqmt_signal_trader.adapters.vk_common import build_vk_client, local_vk_config
from bigqmt_signal_trader.local_env import read_local_env
from xtquant import xtconstant  # noqa: E402
from xtquant.xttype import StockAccount  # noqa: E402


def account_id(args):
    value = (
        getattr(args, "account", "")
        or read_local_env().get("BIGQMT_ACCOUNT_ID", "").strip()
        or os.environ.get("BIGQMT_ACCOUNT_ID", "")
        or getattr(cfg, "BIGQMT_ACCOUNT_ID", "")
    )
    value = str(value or "").strip()
    if not value:
        sys.exit(
            "资金账号为空。请传入 --account，或在 .env、环境变量或客户端配置模块中设置 BIGQMT_ACCOUNT_ID。"
        )
    return value


def raw_vk():
    client = BigQmtRpcClient()
    config = local_vk_config(cfg)
    config.update(client.vk_config, url=client._vk_url)
    return build_vk_client(config, environ={})


def make_trader(acc_id):
    return BigQmtXtTrader(account_id=acc_id)


def make_data(acc_id):
    return BigQmtXtData(BigQmtRpcClient(account_id=acc_id))


def as_stock_account(acc_id):
    account_type = str(read_local_env().get("BIGQMT_ACCOUNT_TYPE")
                       or getattr(cfg, "BIGQMT_ACCOUNT_TYPE", None) or "STOCK")
    return StockAccount(acc_id, account_type)


def fmt_row(obj, fields):
    out = {}
    for name in fields:
        value = getattr(obj, name, None)
        if isinstance(value, float):
            value = round(value, 4)
        out[name] = value
    return out


def cmd_discover(args):
    r = raw_vk()
    keys = [k.decode() if isinstance(k, bytes) else k for k in r.keys("bigqmt:*")]
    if not keys:
        print("VK 数据库 %s 中没有 bigqmt:* 键，请确认 QMT 策略是否正在运行。"
              % r.connection_pool.connection_kwargs.get("db"))
        return
    accounts = set()
    for key in keys:
        for prefix in ("bigqmt:positions:", "bigqmt:rpc:req:", "bigqmt:order_events:", "bigqmt:trade_events:"):
            if key.startswith(prefix):
                accounts.add(key[len(prefix):])
    print("VK 键：")
    for key in sorted(keys):
        print("  ", key)
    print("发现的账号：", sorted(accounts) or "（暂无）")


def cmd_ping(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    started = time.time()
    pong = trader.client.call("ping")
    elapsed_ms = (time.time() - started) * 1000.0
    print("ping 成功，耗时 %.1f 毫秒" % elapsed_ms)
    print(json.dumps(pong, ensure_ascii=False, indent=2, default=str))


def cmd_asset(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    asset = trader.query_stock_asset(as_stock_account(acc_id))
    if asset is None:
        print("未查询到账号资产（query_stock_asset 返回 None）")
        return
    print(json.dumps(fmt_row(asset, [
        "account_id", "account_type", "cash", "frozen_cash",
        "market_value", "total_asset",
    ]), ensure_ascii=False, indent=2, default=str))


def cmd_positions(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    positions = trader.query_stock_positions(as_stock_account(acc_id)) or []
    print("持仓：%d 条" % len(positions))
    for pos in positions:
        print(json.dumps(fmt_row(pos, [
            "stock_code", "volume", "can_use_volume", "open_price",
            "market_value", "on_road_volume", "yesterday_volume",
        ]), ensure_ascii=False, default=str))


def cmd_orders(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    orders = trader.query_stock_orders(
        as_stock_account(acc_id), cancelable_only=args.cancelable
    ) or []
    print("委托：%d 条" % len(orders))
    for order in orders:
        print(json.dumps(fmt_row(order, [
            "order_id", "stock_code", "order_type", "order_volume",
            "price", "traded_volume", "order_status", "strategy_name",
            "order_time", "order_remark",
        ]), ensure_ascii=False, default=str))


def cmd_trades(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    trades = trader.query_stock_trades(as_stock_account(acc_id)) or []
    print("成交：%d 条" % len(trades))
    for trade in trades:
        print(json.dumps(fmt_row(trade, [
            "stock_code", "order_type", "traded_volume", "traded_price",
            "traded_amount", "traded_time", "order_id",
        ]), ensure_ascii=False, default=str))


def cmd_tick(args):
    acc_id = account_id(args)
    data = make_data(acc_id)
    ticks = data.get_full_tick(args.codes) or {}
    for code in args.codes:
        tick = ticks.get(code)
        if not tick:
            print("%s：暂无行情" % code)
            continue
        print(json.dumps({
            "code": code,
            "lastPrice": tick.get("lastPrice"),
            "time": tick.get("time"),
            "bidPrice": tick.get("bidPrice"),
            "askPrice": tick.get("askPrice"),
            "pvolume": tick.get("pvolume"),
        }, ensure_ascii=False, default=str))


def cmd_kline(args):
    acc_id = account_id(args)
    data = make_data(acc_id)
    result = data.get_market_data_ex(
        ["open", "high", "low", "close", "volume", "amount"],
        [args.code], period=args.period, count=args.count,
        dividend_type="none", fill_data=False,
    ) or {}
    df = result.get(args.code)
    if df is None or len(df) == 0:
        print("未查到 %s 的 K 线，请检查 QMT 是否已下载相应数据。" % args.code)
        return
    print(df.to_string())


def cmd_order(args, order_type):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    account = as_stock_account(acc_id)
    if trader.connect() != 0:
        sys.exit("连接失败")
    price_type = xtconstant.FIX_PRICE if args.price > 0 else xtconstant.LATEST_PRICE
    order_id = trader.order_stock(
        account, args.code, order_type, args.volume, price_type,
        args.price, "bigqmt_cli", args.remark or "cli-order",
    )
    if isinstance(order_id, int) and order_id == -1:
        sys.exit("委托被拒绝（order_stock 返回 -1）")
    print("委托已提交，委托号=%s" % order_id)


def cmd_cancel(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    account = as_stock_account(acc_id)
    result = trader.cancel_order_stock(account, args.order_id)
    print("撤单返回值：%s（0 表示成功，-1 表示失败）" % result)


class _Printer:
    def on_disconnected(self):
        print("[event] 连接已断开")

    def on_stock_order(self, order):
        print("[order] %s" % json.dumps(fmt_row(order, [
            "order_id", "stock_code", "order_type", "order_volume",
            "price", "traded_volume", "order_status",
        ]), ensure_ascii=False, default=str))

    def on_stock_trade(self, trade):
        print("[trade] %s" % json.dumps(fmt_row(trade, [
            "stock_code", "order_type", "traded_volume", "traded_price",
        ]), ensure_ascii=False, default=str))

    def on_order_error(self, error):
        print("[order_error] %s" % json.dumps(fmt_row(error, [
            "order_id", "error_id", "error_msg",
        ]), ensure_ascii=False, default=str))

    def on_cancel_error(self, error):
        print("[cancel_error] %s" % json.dumps(fmt_row(error, [
            "order_id", "error_id", "error_msg",
        ]), ensure_ascii=False, default=str))

    def on_order_stock_async_response(self, response):
        print("[async_response] %s" % json.dumps(fmt_row(response, [
            "seq", "order_id",
        ]), ensure_ascii=False, default=str))

    def on_account_status(self, status):
        print("[account_status] %s" % json.dumps(fmt_row(status, [
            "account_id", "account_type", "status",
        ]), ensure_ascii=False, default=str))


def cmd_watch(args):
    acc_id = account_id(args)
    trader = make_trader(acc_id)
    account = as_stock_account(acc_id)
    trader.register_callback(_Printer())
    trader.start()
    if trader.connect() != 0:
        sys.exit("连接失败")
    trader.subscribe(account)
    print("正在监听账号 %s 的交易回报，按 Ctrl+C 停止。" % acc_id)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n监听已停止。")


def main(argv=None):
    parser = argparse.ArgumentParser(description="大 QMT VK 桥接命令行工具")
    parser.add_argument("--account", default="", help="指定资金账号，覆盖默认配置")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("discover", help="在 VK 中查找已注册的账号")
    sub.add_parser("ping", help="测试 RPC 连通性")
    sub.add_parser("asset", help="查询账号资产")
    sub.add_parser("positions", help="查询持仓")
    p_orders = sub.add_parser("orders", help="查询委托")
    p_orders.add_argument("--cancelable", action="store_true")
    sub.add_parser("trades", help="查询成交")
    p_tick = sub.add_parser("tick", help="查询行情快照")
    p_tick.add_argument("codes", nargs="+")
    p_kline = sub.add_parser("kline", help="查询 QMT 本地 K 线")
    p_kline.add_argument("code")
    p_kline.add_argument("--period", default="1d")
    p_kline.add_argument("--count", type=int, default=10)
    for name, order_type in (("buy", xtconstant.STOCK_BUY), ("sell", xtconstant.STOCK_SELL)):
        p = sub.add_parser(name, help="提交买入委托" if name == "buy" else "提交卖出委托")
        p.add_argument("code")
        p.add_argument("volume", type=int)
        p.add_argument("price", type=float, help="0 表示按最新价委托")
        p.add_argument("--remark", default="")
        p.set_defaults(order_type=order_type)
    p_cancel = sub.add_parser("cancel", help="撤销委托")
    p_cancel.add_argument("order_id")
    sub.add_parser("watch", help="监听委托和成交回报")

    args = parser.parse_args(argv)
    handlers = {
        "discover": cmd_discover,
        "ping": cmd_ping,
        "asset": cmd_asset,
        "positions": cmd_positions,
        "orders": cmd_orders,
        "trades": cmd_trades,
        "tick": cmd_tick,
        "kline": cmd_kline,
        "buy": lambda a: cmd_order(a, a.order_type),
        "sell": lambda a: cmd_order(a, a.order_type),
        "cancel": cmd_cancel,
        "watch": cmd_watch,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
