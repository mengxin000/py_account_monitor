import math
import re
from dataclasses import asdict
try:
    from ...core.models import MatchOrder
    from ...core.order_linking import reference_sid_from_cloid
except ImportError:
    from core.models import MatchOrder
    from core.order_linking import reference_sid_from_cloid


def symbol_for(coin, config):
    mapping = config.get("symbol_map", {})
    if coin in mapping:
        symbol = str(mapping[coin]).upper()
    else:
        base = coin[4:] if coin.lower().startswith("xyz:") else coin
        if not re.fullmatch(r"[A-Za-z0-9]+", base):
            raise ValueError(f"hyperliquid.symbol_map required for {coin}")
        symbol = base.upper() + "USDC"
    if not re.fullmatch(r"[A-Z0-9]+(?:USDT|USDC)", symbol):
        raise ValueError(f"invalid Hyperliquid canonical symbol {symbol}")
    return symbol


def rates(config):
    result = {"USDT": 1.0, "USDC": float(config.get("usdc_to_usdt", 1.0))}
    result.update({k: float(v) for k, v in config.get("token_rates", {}).items()})
    if any(not math.isfinite(v) or v <= 0 for v in result.values()):
        raise ValueError("Hyperliquid token conversion rates must be finite and positive")
    return result


def normalize_fill(fill, account_id, config, cloid=""):
    coin = str(fill["coin"])
    symbol = symbol_for(coin, config)
    rate = rates(config)
    quote = "USDT" if symbol.endswith("USDT") else "USDC"
    asset = fill.get("feeToken", "USDC")
    if asset not in rate:
        raise ValueError(f"missing fee conversion rate for {asset}")
    px, qty, fee = float(fill["px"]), float(fill["sz"]), float(fill["fee"])
    if not all(math.isfinite(x) for x in (px, qty, fee)) or px <= 0 or qty <= 0:
        raise ValueError("invalid Hyperliquid executed price/quantity/fee")
    if fill["side"] not in {"B", "A"}:
        raise ValueError("unknown Hyperliquid side")
    stamp = int(fill["time"])
    order = MatchOrder(
        id=str(fill.get("cloid") or cloid or ""), system_id=str(fill["oid"]),
        side="BUY" if fill["side"] == "B" else "SELL", status="FILLED",
        price=px * rate[quote], quantity=qty, fee=fee * rate[asset],
        time_ms=stamp, fill_time=stamp, symbol=symbol, native_symbol=coin,
        market_type="futures", exchange="hyperliquid", account_id=account_id,
        account_scope="hyperliquid", execution_id=f"{stamp}:{fill['tid']}",
        fee_currency=asset, leg=config.get("leg", "hedge"),
        reference_sid=reference_sid_from_cloid(str(fill.get("cloid") or cloid or "")) or "")
    return asdict(order)


def normalize_order(update, config):
    o = update["order"]
    status = str(update["status"])
    return {"exchange": "hyperliquid", "s": o["coin"].upper(),
            "nativeSymbol": o["coin"], "i": str(o["oid"]), "c": o.get("cloid") or "",
            "S": "BUY" if o["side"] == "B" else "SELL", "p": o["limitPx"],
            "q": o.get("origSz", o["sz"]),
            "z": float(o.get("origSz", o["sz"])) - float(o["sz"]),
            "X": "NEW" if status == "open" else "FILLED" if status == "filled" else "CANCELED",
            "T": update.get("statusTimestamp", o.get("timestamp", 0))}
