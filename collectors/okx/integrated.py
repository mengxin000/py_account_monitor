"""Bridge native OKX collection into the common account journals.

Only REST individual fills enter matching. Orders pushes remain an audit
stream and wake reconciliation; they never contribute cumulative size/fees.
"""
import asyncio
import math
import time
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from .account import OKXCollector
from .normalize import normalize_fill
try:
    from ...core.jsonl_io import iter_jsonl
except ImportError:
    from core.jsonl_io import iter_jsonl


def valuation_rates(settings):
    rates = {"USDT": 1.0, "USDC": settings.get("usdc_to_usdt", 1.0),
             "USD": settings.get("usd_to_usdt", 1.0), **settings.get("token_rates", {})}
    for currency, value in rates.items():
        if not isinstance(currency, str) or not math.isfinite(float(value)) or float(value) <= 0:
            raise ValueError("OKX valuation rates must be finite and positive")
    if float(rates["USDT"]) != 1:
        raise ValueError("OKX USDT valuation rate must be 1")
    return rates


class IntegratedOKXCollector(OKXCollector):
    def __init__(self, config, store, settings, on_trade):
        # Keep the standalone audit layout; normalized fills/funding go to the
        # parent's shared store, never expose native cumulative orders to replay.
        super().__init__(replace(config, output_dir=store.root / "okx"))
        self.common_store, self.settings, self.on_trade = store, settings, on_trade
        self.rates = valuation_rates(settings)
        self._common_seen = {}
        self._common_lock = asyncio.Lock()
        self._instruments = {}
        self.report_error = "OKX initial reconciliation not complete"
        self.last_at = self.equity = self.mmr = None
        self._reconcile_lock = asyncio.Lock()

    async def _publish(self, filename, row, ms, event_id):
        # Use the SAME trading-day/path policy and lock as the other legs.
        now = datetime.fromtimestamp(ms / 1000)
        async with self._common_lock:
            path = self.common_store._path(filename, now)
            if path not in self._common_seen:
                def read_ids():
                    return {r.get("eventId") for _, _, r in iter_jsonl(path)
                            if r.get("exchange") == "okx"} if path.exists() else set()
                self._common_seen[path] = await asyncio.to_thread(read_ids)
                days = sorted({p.parent.name for p in self._common_seen})
                for p in list(self._common_seen):
                    if p.parent.name in days[:-2]:
                        del self._common_seen[p]
            if event_id in self._common_seen[path]:
                return False
            row["eventId"] = event_id
            await asyncio.to_thread(self.common_store.append, filename, row, now=now)
            self._common_seen[path].add(event_id)
            return True

    async def _instrument(self, fill):
        key = (fill["instType"], fill["instId"])
        if key not in self._instruments:
            rows = await self.rest.get("/api/v5/public/instruments",
                                      {"instType": key[0], "instId": key[1]}, private=False)
            for item in rows:
                self._instruments[(item["instType"], item["instId"])] = item
        if key not in self._instruments:
            raise ValueError(f"OKX instrument specification unavailable: {key}")
        return self._instruments[key]

    async def _unique(self, filename, row, ms, event_id):
        # Audit first. Retry publication even if that audit row already exists
        # (crash between the two writes); common journal has its own durable ID.
        added = await super()._unique(filename, row, ms, event_id)
        if row["recordType"] == "fill_history":
            fill = dict(row["data"])
            if "clOrdId" not in fill:
                orders = await self.rest.get("/api/v5/trade/order",
                                            {"instId": fill["instId"], "ordId": fill["ordId"]})
                if len(orders) != 1 or str(orders[0].get("ordId")) != str(fill["ordId"]):
                    raise ValueError("OKX fill order identity unavailable")
                fill["clOrdId"] = orders[0].get("clOrdId", "")
            normalized = normalize_fill(fill, self.config.account_id, await self._instrument(fill),
                                        self.rates, symbol_map=self.settings.get("symbol_map"))
            result = {**row, "recordType": "trade_callback", "matchingEnabled": True,
                      "accountScope": normalized["account_scope"], "symbol": normalized["symbol"],
                      "normalizedOrder": normalized, "commission": str(-Decimal(fill["fee"])),
                      "nativePrice": fill["fillPx"], "valuationRates": self.rates}
            # Prices are already converted. Do not apply valuationRates twice
            # in downstream compatibility consumers.
            result.pop("valuationRates")
            if await self._publish("trade_callbacks.jsonl", result, ms, event_id):
                self.on_trade()
        elif row["recordType"] == "funding_bill":
            bill = row["data"]
            inst_type = str(bill.get("instType") or ("SWAP" if str(bill.get("instId", "")).endswith("-SWAP") else ""))
            if inst_type not in self.config.inst_types:
                return added
            currency = bill["ccy"]
            if currency not in self.rates:
                raise ValueError(f"missing OKX funding conversion rate for {currency}")
            income = float(Decimal(bill["pnl"]) * Decimal(str(self.rates[currency])))
            if not math.isfinite(income):
                raise ValueError("non-finite OKX funding income")
            result = {**row, "recordType": "funding_income", "accountScope": inst_type.lower(),
                      "raw": bill, "data": {"income": income, "asset": "USDT", "time": ms,
                      "symbol": bill.get("instId"), "tranId": event_id, "incomeType": "FUNDING_FEE"}}
            if await self._publish("funding.jsonl", result, ms, event_id):
                self.logger.info("OKX funding collected instrument=%s income=%s", bill.get("instId"), income)
        return added

    async def message(self, packet, received_us):
        await super().message(packet, received_us)
        row = self.envelope("private_callback", packet, received_us // 1000,
                            received_us=received_us, channel=packet.get("arg", {}).get("channel"))
        await asyncio.to_thread(self.common_store.append, "all_callbacks.jsonl", row,
                                now=datetime.fromtimestamp(received_us / 1_000_000))
        if packet.get("arg", {}).get("channel") == "orders" and any(
            Decimal(order.get("accFillSz") or "0") > 0 for order in packet.get("data", [])
        ):
            self.reconcile_requested.set()

    async def reconcile(self):
        async with self._reconcile_lock:
            try:
                await super().reconcile()
                self.report_error = None
            except Exception as exc:
                self.report_error = f"{type(exc).__name__}: {exc}"
                raise

    async def _reconcile_loop(self, stop):
        # A busy orders channel must not turn recovery into a REST flood.
        while not stop.is_set():
            self.reconcile_requested.clear()
            try:
                await self.reconcile()
            except (OSError, ValueError):
                raise
            except Exception as exc:
                self.logger.warning("OKX reconciliation failed error=%s", exc)
            try:
                await asyncio.wait_for(stop.wait(), 5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await asyncio.wait_for(self.reconcile_requested.wait(), self.config.reconcile_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def equity_snapshot(self):
        try:
            await self.collect_account()
            balance = self.last_account["data"]["balance"][0]
            equity = float(Decimal(balance["totalEq"]) * Decimal(str(self.rates["USD"])))
            if not math.isfinite(equity):
                raise ValueError("OKX totalEq unavailable or non-finite")
            ratio = balance.get("mgnRatio")
            self.mmr = float(ratio) if ratio not in (None, "") else None
            if self.mmr is not None and not math.isfinite(self.mmr):
                self.mmr = None
            self.equity, self.last_at = equity, datetime.now()
            return {"equity": equity, "mmr": self.mmr, "raw": self.last_account}
        except Exception:
            self.equity = None
            raise

    async def run(self, stop):
        # Main monitor owns equity polling; do not start a second account loop.
        self.logger.info("OKX integrated collector starting inst_types=%s", self.config.inst_types)
        tasks = [asyncio.create_task(self.stream.run(stop)),
                 asyncio.create_task(self._reconcile_loop(stop)), asyncio.create_task(stop.wait())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            if stop.is_set():
                await tasks[0]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.rest.close()
            self.logger.info("OKX integrated collector stopped")
