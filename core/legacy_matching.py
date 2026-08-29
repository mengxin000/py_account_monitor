"""Python translation of account_zdl/Strategy/strategy.cpp matching rules.

This module intentionally keeps the old semantics: client/system-id substring
matching, a 10-second pending timeout, BUY/SELL FIFO exposure queues, partial
quantity consumption, proportional fees and the original abs(profit) < 2
guard.  It is a calculation core; Excel and email output are layered on top.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping


def is_matching_order(
    current_id: str,
    system_id: str,
    match_system_id: str,
    match_client_id: str,
) -> bool:
    """Exact equivalent of C++ ``isMatchingOrder``."""
    if (match_system_id and match_system_id in current_id) or (
        system_id and system_id in match_client_id
    ):
        return True
    if (match_client_id and match_client_id in current_id) or (
        current_id and current_id in match_client_id
    ):
        return True
    return False


@dataclass
class MatchOrder:
    id: str
    system_id: str
    side: str
    status: str
    price: float
    quantity: float
    fee: float = 0.0
    flage: int = 0  # spelling retained for showData compatibility
    time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    fill_time: int = 0
    symbol: str = ""
    market_type: str = ""  # spot (executionReport) or futures (ORDER_TRADE_UPDATE)


@dataclass(frozen=True)
class MatchRecord:
    current_id: str
    current_system_id: str
    current_side: str
    match_id: str
    match_system_id: str
    match_side: str
    quantity: float
    current_price: float
    match_price: float
    current_fee: float
    match_fee: float
    profit: float
    offset: float
    event_time_ms: int
    current_symbol: str = ""
    match_symbol: str = ""


@dataclass(frozen=True)
class ExposureMatch:
    buy_id: str
    sell_id: str
    quantity: float
    buy_amount: float
    sell_amount: float
    buy_fee: float
    sell_fee: float
    profit_delta: float
    symbol: str = ""


class LegacyMatcher:
    """Stateful matcher preserving the old C++ queue and timeout behavior."""

    def __init__(
        self,
        *,
        timeout_ms: int = 10_000,
        on_match: Callable[[MatchRecord], Any] | None = None,
    ) -> None:
        self.timeout_ms = timeout_ms
        self.match_orders: list[MatchOrder] = []
        self.no_save_orders: list[MatchOrder] = []
        self.buy_not_match_orders: list[MatchOrder] = []
        self.sell_not_match_orders: list[MatchOrder] = []
        self.daily_profit = 0.0
        self.deal_profit = 0.0
        self.not_match_number_daily = 0
        self.match_number_daily = 0
        self.records: list[MatchRecord] = []
        self.exposure_records: list[ExposureMatch] = []
        self.on_match = on_match
        self._lock = threading.RLock()

    @staticmethod
    def from_order_event(event: Mapping[str, Any]) -> MatchOrder:
        """Build a MatchOrder from Binance PM/Futures or Spot order payload."""
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

        return MatchOrder(
            id=str(value("c", "clientOrderId", default="")),
            system_id=str(value("i", "orderId", default="")),
            side=str(value("S", "side", default="")),
            status=str(value("X", "x", "status", default="")),
            # For a completed fill, prefer the accumulated average execution
            # price; Spot executionReport commonly exposes only ``L`` (last
            # executed price), while ``p`` is merely the submitted limit price.
            price=float(value("ap", "avgPrice", "L", "lastExecutedPrice", "p", "price", default=0) or 0),
            quantity=float(value("z", "executedQty", "q", "quantity", default=0) or 0),
            fee=fee,
            time_ms=int(value("E", "T", "time", default=int(time.time() * 1000)) or 0),
            fill_time=int(value("T", "transactTime", default=0) or 0),
            symbol=str(value("s", "symbol", default="") or "").upper(),
            market_type="spot" if event_type.lower() == "executionreport" else ("futures" if event_type.upper() == "ORDER_TRADE_UPDATE" else ""),
        )

    def enqueue(self, order: MatchOrder) -> None:
        with self._lock:
            self.match_orders.append(order)

    def ingest(self, current: MatchOrder, *, extra_fee: float = 0.0) -> list[MatchRecord]:
        """Process one callback using the old partial-fee accumulation rules.

        ``PARTIALLY_FILLED`` callbacks are accumulated in ``no_save_orders``;
        on the terminal callback their accumulated fees are added exactly once.
        """
        with self._lock:
            if current.status == "PARTIALLY_FILLED":
                for old in self.no_save_orders:
                    if old.system_id == current.system_id:
                        old.status = current.status
                        old.quantity = current.quantity
                        old.price = current.price
                        old.fee += current.fee
                        old.time_ms = current.time_ms
                        return []
                self.no_save_orders.append(current)
                return []
            if current.status not in {"FILLED", "PARTIALLY_CANCELED", "CANCELED"}:
                return []
            if current.status == "CANCELED" and current.quantity <= 1e-8:
                return []
            accumulated = 0.0
            for index, old in enumerate(self.no_save_orders):
                if old.system_id == current.system_id:
                    accumulated = old.fee
                    self.no_save_orders.pop(index)
                    break
            return self.process_completed(current, extra_fee=extra_fee + accumulated)

    def process_completed(self, current: MatchOrder, *, extra_fee: float = 0.0) -> list[MatchRecord]:
        """Match an incoming (later) order against queued (earlier) match orders.

        Naming follows the report contract: the queued order is the *matching
        order* (``current_*`` fields), while the incoming order is the
        *matched order* (``match_*`` fields).  The local ``current`` argument
        is retained for API compatibility and means the incoming event only.
        """
        output: list[MatchRecord] = []
        with self._lock:
            order_qty = current.quantity
            flag = False
            consumed_previous = False
            index = 0
            while index < len(self.match_orders):
                queued_order = self.match_orders[index]
                if not is_matching_order(current.id, current.system_id, queued_order.system_id, queued_order.id):
                    index += 1
                    continue
                old_fee = 0.0 if queued_order.flage == 1 else queued_order.fee
                current_fee = 0.0 if consumed_previous else current.fee
                min_qty = min(order_qty, queued_order.quantity)
                sell_price = queued_order.price if queued_order.side == "SELL" else current.price
                buy_price = queued_order.price if queued_order.side == "BUY" else current.price
                profit = (sell_price - buy_price) * min_qty - current_fee - old_fee - extra_fee
                # Cross-market spread ratio: futures price is the denominator.  current->hedge
                if queued_order.market_type == "futures" and current.market_type == "spot":
                    futures_price, spot_price = queued_order.price, current.price
                elif queued_order.market_type == "spot" and current.market_type == "futures":
                    futures_price, spot_price = current.price, queued_order.price
                else:
                    futures_price, spot_price = sell_price, buy_price
                offset = (futures_price - spot_price) / futures_price if futures_price else 0.0
                record = MatchRecord(
                    current_id=queued_order.id,
                    current_system_id=queued_order.system_id,
                    current_side=queued_order.side,
                    match_id=current.id,
                    match_system_id=current.system_id,
                    match_side=current.side,
                    quantity=min_qty,
                    current_price=queued_order.price,
                    match_price=current.price,
                    current_fee=old_fee,
                    match_fee=current_fee + extra_fee,
                    profit=profit,
                    offset=offset,
                    event_time_ms=current.time_ms,
                    current_symbol=queued_order.symbol,
                    match_symbol=current.symbol,
                )
                output.append(record)
                self.records.append(record)
                self.daily_profit += profit
                self.deal_profit += profit
                self.match_number_daily += 1
                if self.on_match:
                    self.on_match(record)

                if queued_order.quantity > order_qty - 1e-8:
                    if abs(order_qty - queued_order.quantity) < 1e-8:
                        self.match_orders.pop(index)
                    else:
                        queued_order.quantity -= order_qty
                        queued_order.flage = 1
                    flag = True
                    break
                order_qty -= queued_order.quantity
                consumed_previous = True
                self.match_orders.pop(index)

            # Exact old fallback: update same system id, or enqueue residual.
            if not flag:
                updated = False
                for old in self.match_orders:
                    if old.system_id == current.system_id:
                        old.status = current.status
                        old.quantity = current.quantity
                        old.price = current.price
                        old.fee = current.fee
                        old.time_ms = current.time_ms
                        updated = True
                        break
                if not updated:
                    current.quantity = order_qty
                    current.flage = 1 if consumed_previous else current.flage
                    self.match_orders.append(current)
        return output

    def find_not_match_order(self, *, now_ms: int | None = None) -> list[MatchOrder]:
        """Move pending orders older than ten seconds to exposure FIFO queues."""
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        moved: list[MatchOrder] = []
        with self._lock:
            index = 0
            while index < len(self.match_orders):
                order = self.match_orders[index]
                if now - order.time_ms <= self.timeout_ms:
                    index += 1
                    continue
                moved.append(order)
                if order.side == "BUY":
                    self.buy_not_match_orders.append(order)
                else:
                    self.sell_not_match_orders.append(order)
                self.not_match_number_daily += 1
                self.match_orders.pop(index)
        return moved

    def handle_not_match_order(self) -> list[ExposureMatch]:
        """Exact BUY/SELL FIFO exposure reconciliation from the C++ code."""
        result: list[ExposureMatch] = []
        with self._lock:
            if not self.buy_not_match_orders or not self.sell_not_match_orders:
                return result
            exposure_symbol = self.buy_not_match_orders[0].symbol or self.sell_not_match_orders[0].symbol
            buy_total = sum(o.quantity for o in self.buy_not_match_orders)
            sell_total = sum(o.quantity for o in self.sell_not_match_orders)
            remain_buy = min(buy_total, sell_total)
            remain_sell = remain_buy
            buy_sum = sell_sum = buy_fee = sell_fee = 0.0
            while remain_buy > 1e-8 and self.buy_not_match_orders:
                buy = self.buy_not_match_orders[0]
                if buy.quantity < 1e-8:
                    self.buy_not_match_orders.pop(0)
                    continue
                qty = min(buy.quantity, remain_buy)
                buy_sum += buy.price * qty
                if buy.flage == 0:
                    buy_fee += buy.fee * (qty / buy.quantity)
                buy.quantity -= qty
                remain_buy -= qty
                if buy.quantity < 1e-8:
                    self.buy_not_match_orders.pop(0)
            while remain_sell > 1e-8 and self.sell_not_match_orders:
                sell = self.sell_not_match_orders[0]
                if sell.quantity < 1e-8:
                    self.sell_not_match_orders.pop(0)
                    continue
                qty = min(sell.quantity, remain_sell)
                sell_sum += sell.price * qty
                if sell.flage == 0:
                    sell_fee += sell.fee * (qty / sell.quantity)
                sell.quantity -= qty
                remain_sell -= qty
                if sell.quantity < 1e-8:
                    self.sell_not_match_orders.pop(0)
            delta = sell_sum - buy_sum - buy_fee - sell_fee
            if abs(delta) < 2:
                self.deal_profit += delta
            self.exposure_records.append(
                ExposureMatch("", "", min(buy_total, sell_total), buy_sum, sell_sum, buy_fee, sell_fee, delta, exposure_symbol)
            )
            result.extend(self.exposure_records[-1:])
        return result
