"""Pure adapter for REST individual fills. Never mix with WS cumulative orders.

Collection remains raw/isolated until the main service explicitly opts in.
Only spot/margin and base-denominated linear contracts are supported here.
"""
from dataclasses import asdict
from decimal import Decimal
import math
import re
try:
    from ...core.models import MatchOrder
except ImportError:
    from core.models import MatchOrder


def number(value, name, *, positive=False):
    result = Decimal(str(value))
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f"invalid OKX {name}")
    return result


def normalize_fill(fill, account_id, instrument, valuation_rates, *, symbol_map=None):
    """Return MatchOrder fields. Rates are explicit USDT per unit of currency."""
    inst_id = str(fill.get("instId", ""))
    inst_type = fill.get("instType")
    if not inst_id or instrument.get("instId") != inst_id or instrument.get("instType") != inst_type:
        raise ValueError("OKX instrument metadata does not match fill")
    parts = inst_id.split("-")
    if len(parts) < 2:
        raise ValueError("invalid OKX instrument name")
    base, quote = parts[:2]
    if quote not in {"USDT", "USDC"}:
        raise ValueError("OKX matching currently supports USDT/USDC quotes only")
    qty = number(fill["fillSz"], "fillSz", positive=True)
    if inst_type in {"SWAP", "FUTURES"}:
        if instrument.get("ctType") != "linear" or instrument.get("ctValCcy") != base:
            raise ValueError("OKX inverse/non-base contracts are not supported by linear matching")
        multiplier = number(instrument.get("ctMult") or "1", "ctMult", positive=True)
        if multiplier != 1:
            raise ValueError("OKX non-unit ctMult requires an explicit valuation policy")
        qty *= number(instrument["ctVal"], "ctVal", positive=True)
    elif inst_type not in {"SPOT", "MARGIN"}:
        raise ValueError("unsupported OKX instrument type")
    price = number(fill["fillPx"], "fillPx", positive=True)
    def rate(currency):
        if currency == "USDT":
            return Decimal(1)
        if currency not in valuation_rates:
            raise ValueError(f"missing OKX conversion rate for {currency}")
        return number(valuation_rates[currency], "conversion rate", positive=True)
    fee_native = number(fill["fee"], "fee")
    fee_currency = str(fill.get("feeCcy", ""))
    # A base-currency commission can use this fill's actual price; a third
    # currency needs its own supplied conversion rate, never the traded price.
    fee_rate = price * rate(quote) if fee_currency == base else rate(fee_currency) if fee_native else Decimal(1)
    side = str(fill.get("side", "")).upper()
    if side not in {"BUY", "SELL"} or not fill.get("ordId") or not fill.get("billId"):
        raise ValueError("OKX fill requires side, ordId and billId")
    stamp = int(fill.get("fillTime") or fill["ts"])
    symbol = (symbol_map or {}).get(inst_id, base + quote)
    if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]+(?:USDT|USDC)", symbol):
        raise ValueError("invalid OKX canonical symbol")
    result = MatchOrder(id=str(fill.get("clOrdId") or ""), system_id=str(fill["ordId"]),
        side=side, status="FILLED", price=float(price * rate(quote)), quantity=float(qty),
        fee=float(-fee_native * fee_rate), time_ms=stamp, fill_time=stamp, symbol=symbol,
        market_type="spot" if inst_type in {"SPOT", "MARGIN"} else "futures",
        exchange="okx", account_id=account_id, account_scope=inst_type.lower(),
        execution_id="bill:"+str(fill["billId"]), native_symbol=inst_id, fee_currency=fee_currency)
    if not all(math.isfinite(value) for value in (result.price, result.quantity, result.fee)):
        raise ValueError("OKX converted value exceeds matching numeric range")
    return asdict(result)
