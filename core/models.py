"""Exchange-neutral executed order fragment used by both replay and live views."""
import time
from dataclasses import dataclass, field


@dataclass
class MatchOrder:
    id: str
    system_id: str
    side: str
    status: str
    price: float
    quantity: float
    fee: float = 0.0
    flage: int = 0
    time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    fill_time: int = 0
    symbol: str = ""
    market_type: str = ""
    exchange: str = "binance"
    account_id: str = ""
    account_scope: str = "unknown"
    execution_id: str = ""  # Hyperliquid emits individual fills, not cumulative z.
    native_symbol: str = ""
    fee_currency: str = "USDT"
    leg: str = ""
    reference_sid: str = ""  # Explicit hedge target; never replaces own CID/SID.

    @property
    def identity(self):
        return (self.exchange, self.account_id, self.account_scope, self.market_type,
                self.symbol, self.system_id, self.execution_id)


def callback_event(record):
    """Compatibility view for journal/volume/order UI; original data stays intact."""
    n = record.get("normalizedOrder")
    if not isinstance(n, dict):
        event = record.get("data", record)
        if record.get("valuationRates"):
            event = {**event, "_valuationRates": record["valuationRates"]}
        return event
    return {"e": "MONITOR_EXECUTION", "E": n["time_ms"], "T": n["time_ms"],
            "s": n["symbol"], "c": n["id"], "i": n["system_id"],
            "S": n["side"], "X": n["status"], "x": "TRADE",
            "z": n["quantity"], "l": n["quantity"], "q": n["quantity"],
            "L": n["price"], "p": n["price"], "n": record.get("commission", n["fee"]),
            "N": n.get("fee_currency", "USDC"), "t": n.get("execution_id"),
            "_normalizedOrder": n}
