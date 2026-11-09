"""Binance-specific execution parsing, independent of the matching engine."""
import time
from typing import Any, Mapping
try:
    from ...core.models import MatchOrder
except ImportError:
    from core.models import MatchOrder

def normalize_order(event: Mapping[str, Any]) -> MatchOrder:
    """Build a MatchOrder from Binance PM/Futures or Spot order payload."""
    if isinstance(event.get("_normalizedOrder"), Mapping):
        return MatchOrder(**event["_normalizedOrder"])
    order = event.get("o") if isinstance(event.get("o"), Mapping) else event
    event_type = str(event.get("e", event.get("eventType", "")))
    def value(*names: str, default: Any = "") -> Any:
        for name in names:
            if name in order and order[name] is not None:
                return order[name]
        return default

    raw_fee = float(value("n", "commission", default=0) or 0)
    spot_N = str(value("N", default=""))
    # Spot executionReport commissions are reported in the traded asset;
    # convert them to quote currency with the actual fill price. Futures
    # ORDER_TRADE_UPDATE commissions are already quoted in USDT/USDC.
    if event_type == "executionReport" and spot_N not in ("USDT", "USDC", ""):
        fill_price = float(value("L", "lastExecutedPrice", "ap", "avgPrice", "p", "price", default=0) or 0)
        fee = raw_fee * fill_price
    else:
        fee = raw_fee

    quantity = float(value("z", "executedQty", "q", "quantity", default=0) or 0)
    price = 0.0
    # L is the LAST execution price and is normally zero on a Spot cancel.
    # Z / z is the cumulative executed VWAP, including partial fills.
    if event_type.lower() == "executionreport" and quantity > 0:
        quote_amount = float(value("Z", "cummulativeQuoteQty", "cumulativeQuoteQty", default=0) or 0)
        if quote_amount > 0:
            price = quote_amount / quantity
    if price <= 0:
        for name in ("ap", "avgPrice", "L", "lastExecutedPrice"):
            candidate = float(value(name, default=0) or 0)
            if candidate > 0:
                price = candidate
                break

    symbol = str(value("s", "symbol", default="") or "").upper()
    conversion = event.get("_valuationRates", {})
    quote_rate = float(conversion.get("USDC", 1)) if symbol.endswith("USDC") else 1.0
    fee_rate = float(conversion.get(spot_N, 1)) if spot_N in ("USDT", "USDC") else quote_rate
    return MatchOrder(
        exchange=str(event.get("exchange", "binance")),
        account_id=str(event.get("accountId", "")),
        account_scope=str(event.get("accountScope", "unknown")),
        id=str(value("c", "clientOrderId", default="")),
        system_id=str(value("i", "orderId", default="")),
        side=str(value("S", "side", default="")),
        status=str(value("X", "x", "status", default="")),
        # p is a limit price, never a substitute for a missing fill price.
        price=price * quote_rate,
        quantity=quantity,
        fee=fee * fee_rate,
        time_ms=int(value("E", "T", "time", default=int(time.time() * 1000)) or 0),
        fill_time=int(value("T", "transactTime", default=0) or 0),
        symbol=str(value("s", "symbol", default="") or "").upper(),
        market_type="spot" if event_type.lower() == "executionreport" else ("futures" if event_type.upper() == "ORDER_TRADE_UPDATE" else ""),
    )

