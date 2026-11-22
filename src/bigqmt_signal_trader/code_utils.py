"""证券代码标准化和委托数量处理。"""

import re


_DIGIT_CODE_RE = re.compile(r"^\d{6}$")

# 表示整个交易所而非某一只具体标的的交易所标识。
#
# get_markets() 返回的交易所代码中包括股票市场，因此将它们放在独立集合中。
# 期货交易所另列集合，因为期货交易所只交易期货：交易所标识本身已经说明了
# 标的类型，因此无需缩小范围，也无需调用方额外指定（issues #95、#104）。
STOCK_MARKET_CODES = frozenset({"SH", "SZ", "BJ", "HK"})
OPTION_MARKET_CODES = frozenset({"SHO", "SZO"})
FUTURES_MARKET_CODES = frozenset({"IF", "SF", "DF", "ZF", "INE", "GF"})
EXCHANGE_TOKENS = STOCK_MARKET_CODES | OPTION_MARKET_CODES | FUTURES_MARKET_CODES


# QMT ContextInfo 对所有标的类型都使用交易所特定后缀。
_QMT_SUFFIXES = (
    ".SH", ".SZ", ".BJ", ".HK",            # 股票交易所
    ".HGT", ".SGT",                        # 港股通（沪/深）
    ".SHO", ".SZO",                        # 期权交易所（8 位代码）
    ".SF", ".DF", ".IF", ".ZF", ".INE", ".GF",    # 期货交易所
)
# 期货代码区分大小写：各交易所使用自己的书写规则，两种写法不可互换；例如
# “rb2401.SF”（SHFE，交易所标识前为小写）与 QMT 中的“RB2401.SF”是两个不同字符串，
# 而郑商所的代码应写作大写的“AP401.ZF”。因此，将小写期货代码统一转为大写后，
# 会生成 QMT 无法识别的代码，只会返回空行情，而不会报错（issue #95）。
#
# issue #68 已通过绕过此函数修复 POSITION 行；其他所有路径（委托、行情、全量
# tick 缓存、风控等）仍会经过此处，并会将代码转换为大写。
_CASE_SENSITIVE_SUFFIXES = frozenset({".SF", ".DF", ".IF", ".ZF", ".INE", ".GF"})


# 可转债 / 可交换债的代码段。以下两项都依赖该规则：
#   1. 裸 6 位码推断市场 —— 沪市转债 110/111/113 是 "1" 开头，落不进 ("5","6")
#      那条规则，会被判到深市；
#   2. 最小申报量 —— 债券论「张」，10 张起，不是 100。
#
#   SH  110/111/113 可转债、118 科创转债、132 可交换债
#   SZ  12x 可转债与可交换债
_BOND_SH_PREFIXES = ("110", "111", "113", "118", "132")
_BOND_SZ_PREFIXES = ("12",)
BOND_LOT = 10          # 债券最小申报数量（张）
DEFAULT_LOT = 100      # 股票/基金最小申报数量（股/份）
STAR_LOT = 200         # 科创板（688/689）


def is_bond_code(stock_code):
    """是不是可转债/可交换债。只看代码段，不需要联网。"""
    text = str(stock_code or "").strip().upper()
    if not text:
        return False
    if "." in text:
        pure, _, suffix = text.partition(".")
    elif text[:2] in ("SH", "SZ") and text[2:].isdigit():
        pure, suffix = text[2:], text[:2]
    else:
        pure, suffix = text, ""
    if not pure.isdigit():
        return False
    if suffix == "SH":
        return pure.startswith(_BOND_SH_PREFIXES)
    if suffix == "SZ":
        return pure.startswith(_BOND_SZ_PREFIXES)
    if suffix:
        return False
    # 裸码：两个市场的债券段互不重叠，直接一起判
    return pure.startswith(_BOND_SH_PREFIXES) or pure.startswith(_BOND_SZ_PREFIXES)


def normalize_stock_code(code):
    raw = str(code or "").strip()
    if not raw:
        return ""
    text = raw.upper()
    # 如果代码已经包含可识别的后缀，则直接原样传递；
    # 这些是无需标准化的原生 ContextInfo 代码。
    for suffix in _QMT_SUFFIXES:
        if text.endswith(suffix):
            prefix = text[:-len(suffix)]
            if prefix and (prefix.isdigit() or prefix[0].isalpha()):
                if suffix in _CASE_SENSITIVE_SUFFIXES:
                    # 规范化后缀，保留调用方传入的标的代码原样。
                    return raw[:-len(suffix)] + suffix
                return text
    # 6 位股票代码的原始处理逻辑
    if text.startswith("SH") and _DIGIT_CODE_RE.match(text[2:]):
        return f"{text[2:]}.SH"
    if text.startswith("SZ") and _DIGIT_CODE_RE.match(text[2:]):
        return f"{text[2:]}.SZ"
    if text.endswith(".SH") or text.endswith(".SZ"):
        prefix = text[:6]
        if _DIGIT_CODE_RE.match(prefix):
            return text
    if _DIGIT_CODE_RE.match(text):
        # 沪市转债 110/111/113/118/132 是 "1" 开头，不加这一条会被判到深市
        if text.startswith(_BOND_SH_PREFIXES) or text.startswith(("5", "6")):
            market = "SH"
        else:
            market = "SZ"
        return f"{text}.{market}"
    raise ValueError(f"invalid stock code: {code}")


def min_lot(stock_code):
    """最小申报数量。

    债券论「张」，10 张起 —— 之前一律按 100 算，于是 round_buy_volume 会把
    一手转债 (10 张) 变成 `(10 // 100) * 100 == 0`，单子直接废掉。
    """
    normalized = normalize_stock_code(stock_code)
    if is_bond_code(normalized):
        return BOND_LOT
    pure = normalized.split(".")[0]
    return STAR_LOT if pure.startswith("688") else DEFAULT_LOT


def round_buy_volume(stock_code, amount):
    lot = min_lot(stock_code)
    value = int(amount or 0)
    if value <= 0:
        return 0
    return (value // lot) * lot


def round_sell_volume(stock_code, amount, sell_all=False):
    value = int(amount or 0)
    if value <= 0:
        return 0
    if sell_all:
        return value
    lot = min_lot(stock_code)
    return (value // lot) * lot
