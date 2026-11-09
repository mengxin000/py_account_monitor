"""Presentation-only funding breakdown; keep the existing P&L accounting."""
from collections import Counter

EXCHANGE_NAMES = {"binance": "Binance", "hyperliquid": "Hyperliquid"}
MARKET_NAMES = {"spot": "现货", "pm_margin": "PM现货", "pm_um": "PM UM", "usdm": "USDM", "perp": "永续"}


def configured_markets(config):
    explicit = getattr(config, "report_markets", ())
    if explicit:
        return list(explicit)
    markets = []
    for mode in (getattr(config, "spot_mode", "none"), getattr(config, "futures_mode", "none")):
        if mode != "none":
            markets.append(("binance", mode))
    if getattr(config, "hyperliquid", {}).get("enabled", False):
        markets.append(("hyperliquid", "perp"))
    return markets


def funding_source(row):
    exchange = str(row.get("exchange", "")).lower()
    scope = str(row.get("accountScope", "")).lower()
    if exchange == "hyperliquid" or scope == "hyperliquid" or str(row.get("source", "")).startswith("hyperliquid"):
        return ("hyperliquid", "perp")
    market = {"um": "pm_um", "usdm": "usdm", "pm_margin": "pm_margin", "spot": "spot"}.get(scope)
    if market:
        return (exchange or "binance", market)
    # Old Binance income journals predate the source metadata.
    value = row.get("data", row)
    if exchange == "binance" or value.get("incomeType") == "FUNDING_FEE":
        return ("binance", "")
    return (exchange, scope)


def funding_breakdown(rows, markets):
    markets = list(markets)
    totals = dict.fromkeys(markets, 0.0)
    for row in rows:
        value = row.get("data", row)
        try:
            amount = float(value.get("income", 0) or 0)
        except (TypeError, ValueError):
            continue
        # Raw ACCOUNT_UPDATE callbacks are audit records, not a second income.
        if "income" not in value:
            continue
        key = funding_source(row)
        if key[0] == "binance" and not key[1]:
            candidates = [m for m in markets if m[0] == "binance" and m[1] in {"usdm", "pm_um"}]
            if len(candidates) == 1:
                key = candidates[0]
        totals[key] = totals.get(key, 0.0) + amount
    counts = Counter(exchange for exchange, _ in totals)
    result = []
    for (exchange, market), amount in totals.items():
        label = EXCHANGE_NAMES.get(exchange, exchange or "未归属")
        if counts[exchange] > 1:
            suffix = MARKET_NAMES.get(market, market or "未归属市场")
            label += (" " if suffix[0].isascii() else "") + suffix
        result.append({"label": label + "资费", "income": amount})
    return result
