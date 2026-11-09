
from __future__ import annotations

import threading
import time
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping
from .models import MatchOrder
from .order_linking import is_matching_order, orders_linked, decimal_prefixes


def quoted_spread_from_client_id(client_id: str, market_type: str) -> float | None:
    if market_type != "futures":
        return None
    parts = client_id.rsplit("_", 1)
    if len(parts) != 2:
        return None
    try:
        return int(parts[1]) / 100_000.0
    except ValueError:
        return None


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
    quoted_spread: float | None = None
    quoted_spread_side: str = ""
    current_exchange: str = "binance"
    match_exchange: str = "binance"


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

    def __init__(
        self,
        *,
        timeout_ms: int = 600_000,
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
        self._execution_ids = set()
        self._binance_ids = set()
        self._hyperliquid_ids = set()
        self._hyperliquid_prefixes = {}

    @staticmethod
    def from_order_event(event: Mapping[str, Any]) -> MatchOrder:
        """Backward-compatible entry point for a normalized or Binance order."""
        if isinstance(event.get("_normalizedOrder"), Mapping):
            return MatchOrder(**event["_normalizedOrder"])
        try:
            from ..collectors.binance.orders import normalize_order
        except ImportError:
            from collectors.binance.orders import normalize_order
        return normalize_order(event)

    def enqueue(self, order: MatchOrder) -> None:
        with self._lock:
            self.match_orders.append(order)

    def ingest(self, current: MatchOrder, *, extra_fee: float = 0.0) -> list[MatchRecord]:
        """Process one callback using the old partial-fee accumulation rules.

        ``PARTIALLY_FILLED`` callbacks are accumulated in ``no_save_orders``;
        on the terminal callback their accumulated fees are added exactly once.
        """
        with self._lock:
            if current.exchange == "hyperliquid":
                if current.identity in self._execution_ids:
                    return []
                if current.id not in self._hyperliquid_ids:
                    prefixes = decimal_prefixes(current.id)
                    if len(self._binance_ids.intersection(prefixes)) > 1:
                        raise ValueError(f"ambiguous Binance orderId prefix in Hyperliquid cloid {current.id}")
                    for prefix in prefixes:
                        self._hyperliquid_prefixes.setdefault(prefix, set()).add(current.id)
                    self._hyperliquid_ids.add(current.id)
            elif current.exchange == "binance":
                if current.system_id not in self._binance_ids:
                    self._binance_ids.add(current.system_id)
                    for cloid in self._hyperliquid_prefixes.get(current.system_id, ()):
                        if len(self._binance_ids.intersection(decimal_prefixes(cloid))) > 1:
                            raise ValueError(f"ambiguous Binance orderId prefix in Hyperliquid cloid {cloid}")
            if current.status == "PARTIALLY_FILLED":
                for old in self.no_save_orders:
                    if old.identity == current.identity:
                        old.status = current.status
                        old.quantity = current.quantity
                        if current.price > 0:
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
                if old.identity == current.identity:
                    if current.price <= 0 and abs(current.quantity - old.quantity) <= 1e-8:
                        current.price = old.price
                    accumulated = old.fee
                    self.no_save_orders.pop(index)
                    break
            # Binance's ``n`` is the commission of this individual fill while
            # ``z`` is cumulative executed quantity.  Carry the sum of every
            # partial-fill commission into the terminal order instead of
            # leaving only the last callback's commission on a residual.
            current.fee += accumulated
            result = self.process_completed(current, extra_fee=extra_fee)
            if current.exchange == "hyperliquid":
                self._execution_ids.add(current.identity)
            return result

    def process_completed(self, current: MatchOrder, *, extra_fee: float = 0.0) -> list[MatchRecord]:
        """Match an incoming (later) order against queued (earlier) match orders.

        Naming follows the report contract: the queued order is the *matching
        order* (``current_*`` fields), while the incoming order is the
        *matched order* (``match_*`` fields).  The local ``current`` argument
        is retained for API compatibility and means the incoming event only.
        """
        output: list[MatchRecord] = []
        with self._lock:
            if current.quantity > 1e-8 and (not math.isfinite(current.price) or current.price <= 0):
                raise ValueError(
                    f"missing executed price: symbol={current.symbol} order_id={current.system_id} "
                    f"status={current.status} quantity={current.quantity}"
                )
            order_qty = current.quantity
            current_fee_remaining = current.fee + extra_fee
            flag = False
            consumed_previous = False
            index = 0
            while index < len(self.match_orders):
                queued_order = self.match_orders[index]
                if not orders_linked(current, queued_order):
                    index += 1
                    continue
                min_qty = min(order_qty, queued_order.quantity)
                queued_qty_before = queued_order.quantity
                current_qty_before = order_qty
                # ``fee`` always represents the commission still attached to
                # the remaining quantity. Allocate it proportionally whenever
                # only part of an order is consumed.
                old_fee = queued_order.fee * (min_qty / queued_qty_before) if queued_qty_before else 0.0
                current_fee = current_fee_remaining * (min_qty / current_qty_before) if current_qty_before else 0.0
                sell_price = queued_order.price if queued_order.side == "SELL" else current.price
                buy_price = queued_order.price if queued_order.side == "BUY" else current.price
                profit = (sell_price - buy_price) * min_qty - current_fee - old_fee
                # Cross-market spread ratio: futures price is the denominator.  current->hedge
                if queued_order.market_type == "futures" and current.market_type == "spot":
                    futures_price, spot_price = queued_order.price, current.price
                elif queued_order.market_type == "spot" and current.market_type == "futures":
                    futures_price, spot_price = current.price, queued_order.price
                else:
                    futures_price, spot_price = sell_price, buy_price
                    # For perp/perp across venues use Binance minus Hyperliquid.
                    if {current.exchange, queued_order.exchange} == {"binance", "hyperliquid"}:
                        futures_price = current.price if current.exchange == "binance" else queued_order.price
                        spot_price = current.price if current.exchange == "hyperliquid" else queued_order.price
                offset = (futures_price - spot_price) / futures_price if futures_price else 0.0
                quoted_spread = quoted_spread_from_client_id(
                    queued_order.id, queued_order.market_type
                )
                quoted_spread_side = queued_order.side if quoted_spread is not None else ""
                if quoted_spread is None:
                    quoted_spread = quoted_spread_from_client_id(current.id, current.market_type)
                    quoted_spread_side = current.side if quoted_spread is not None else ""
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
                    match_fee=current_fee,
                    profit=profit,
                    offset=offset,
                    event_time_ms=current.time_ms,
                    current_symbol=queued_order.symbol,
                    match_symbol=current.symbol,
                    quoted_spread=quoted_spread,
                    quoted_spread_side=quoted_spread_side,
                    current_exchange=queued_order.exchange,
                    match_exchange=current.exchange,
                )
                output.append(record)
                self.records.append(record)
                self.daily_profit += profit
                self.deal_profit += profit
                self.match_number_daily += 1
                if self.on_match:
                    self.on_match(record)

                queued_order.fee -= old_fee
                current_fee_remaining -= current_fee
                if queued_order.quantity > order_qty - 1e-8:
                    if abs(order_qty - queued_order.quantity) < 1e-8:
                        self.match_orders.pop(index)
                    else:
                        queued_order.quantity -= order_qty
                    order_qty = 0.0
                    flag = True
                    break
                order_qty -= queued_order.quantity
                consumed_previous = True
                self.match_orders.pop(index)

            # Exact old fallback: update same system id, or enqueue residual.
            if not flag:
                updated = False
                for old in self.match_orders:
                    if old.identity == current.identity:
                        old.status = current.status
                        old.quantity = current.quantity
                        old.price = current.price
                        old.fee = current_fee_remaining
                        old.time_ms = current.time_ms
                        updated = True
                        break
                if not updated:
                    current.quantity = order_qty
                    current.fee = current_fee_remaining
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
                allocated_fee = buy.fee * (qty / buy.quantity)
                buy_fee += allocated_fee
                buy.fee -= allocated_fee
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
                allocated_fee = sell.fee * (qty / sell.quantity)
                sell_fee += allocated_fee
                sell.fee -= allocated_fee
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
