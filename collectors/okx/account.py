"""Collect native OKX events; intentionally do not build MatchOrder objects."""
import asyncio
import logging
import time
from datetime import datetime
from decimal import Decimal

try:
    from ...connections.okx import OKXRestClient, OKXUserStream
    from ...core.jsonl_io import iter_jsonl
except ImportError:
    from connections.okx import OKXRestClient, OKXUserStream
    from core.jsonl_io import iter_jsonl
from .journal import CHINA, OKXJournal, day_start_ms, trading_day


class OKXCollector:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(f"account_monitor.{config.account_id}.okx")
        self.store = OKXJournal(config.output_dir)
        self.rest = OKXRestClient(config.credentials, base_url=config.rest_base_url,
                                  proxy=config.proxy, simulated=config.simulated)
        self.reconcile_requested = asyncio.Event()
        self.stream = OKXUserStream(self.rest, self.message, on_connect=self.reconcile_requested.set,
                                    url=config.websocket_url, logger=self.logger)
        self.last_account = None
        self.last_rest_at = None
        self.last_error = None
        self._seen = {}
        self._write_lock = asyncio.Lock()
        self._history_end = {}  # Per market; advanced only after every page is durable.
        self._instrument_day = None
        self._reconciled = False

    def envelope(self, kind, data, ms, *, source="okx_stream", received_us=None, channel=None):
        received_us = received_us or time.time_ns() // 1000
        return {"exchange": "okx", "source": source, "accountScope": "okx",
                "accountId": self.config.account_id, "recordType": kind,
                "eventType": channel or kind, "eventTimeMs": int(ms),
                "receivedTimeUs": received_us,
                "receivedTime": datetime.fromtimestamp(received_us / 1_000_000, CHINA).isoformat(timespec="microseconds"),
                "matchingEnabled": False, "data": data}

    async def _unique(self, filename, row, ms, event_id):
        async with self._write_lock:
            key = (trading_day(ms), filename)
            if key not in self._seen:
                path = self.store.path(filename, ms)
                def read_ids():
                    return {record.get("eventId") for _, _, record in iter_jsonl(path)} if path.exists() else set()
                self._seen[key] = await asyncio.to_thread(read_ids)
                days = sorted({k[0] for k in self._seen})
                for old in days[:-2]:
                    for entry in [k for k in self._seen if k[0] == old and k != key]:
                        self._seen.pop(entry)
            if event_id in self._seen[key]:
                return False
            row["eventId"] = event_id
            await asyncio.to_thread(self.store.append, filename, row, ms)
            self._seen[key].add(event_id)
            return True

    async def message(self, packet, received_us):
        channel = packet.get("arg", {}).get("channel")
        rows = packet.get("data")
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError("invalid OKX channel data")
        ms = received_us // 1000
        envelope = self.envelope("private_callback", packet, ms, received_us=received_us, channel=channel)
        server_times = [int(row.get("uTime") or row.get("ts")) for row in rows if row.get("uTime") or row.get("ts")]
        envelope["eventTimeMs"] = max(server_times) if server_times else None
        await asyncio.to_thread(self.store.append, "all_callbacks.jsonl", envelope, ms)
        if channel != "orders":
            return
        for order in rows:
            if order.get("instType") not in self.config.inst_types:
                continue
            # An orders push describes cumulative order state, NOT one fill.
            # Preserve partially-filled cancels as well; no fee/quantity conversion.
            if Decimal(order.get("accFillSz") or "0") <= 0:
                continue
            stamp = int(order.get("fillTime") or order.get("uTime") or ms)
            row = self.envelope("trade_callback", order, stamp, received_us=received_us, channel="orders")
            row.update(symbol=order.get("instId"), quantityUnit="contracts" if order.get("instType") in {"SWAP", "FUTURES", "OPTION"} else "base")
            event_id = ":".join(str(order.get(k, "")) for k in
                                ("instType", "instId", "ordId", "tradeId", "state", "accFillSz", "uTime"))
            await self._unique("trade_callbacks.jsonl", row, stamp, "order:" + event_id)

    async def collect_account(self):
        # Full raw balance/positions snapshots, no invented USD->USDT conversion,
        # no summing totalEq twice for spot and derivatives in one trading account.
        balance = await self.rest.get("/api/v5/account/balance")
        positions = await self.rest.get("/api/v5/account/positions")
        if len(balance) != 1 or not balance[0].get("uTime"):
            raise ValueError("invalid OKX account balance snapshot")
        ms = int(self.rest.now_ms())
        state = self.envelope("account_snapshot", {"balance": balance, "positions": positions}, ms, source="okx_rest")
        state["eventTimeMs"] = int(balance[0]["uTime"])
        state["equityCurrency"] = "USD"  # OKX totalEq, not an assumed stablecoin amount.
        await asyncio.to_thread(self.store.snapshot, "account.json", state, ms)
        if self.last_rest_at is None or self.last_error:
            self.logger.info("OKX account REST ready")
        self.last_account, self.last_rest_at, self.last_error = state, ms, None

    async def reconcile(self):
        await self.rest.sync_time()
        end = int(self.rest.now_ms())
        today = day_start_ms(end)
        count = 0
        for inst_type in self.config.inst_types:
            begin = max(today - 86_400_000, self._history_end.get(inst_type, today) - 60_000)
            async for rows in self.rest.pages("/api/v5/trade/fills-history",
                    {"instType": inst_type, "begin": begin, "end": end}):
                for fill in rows:
                    stamp = int(fill.get("fillTime") or fill["ts"])
                    if not fill.get("billId"):
                        raise ValueError("OKX fill has no billId; refusing unsafe dedup")
                    row = self.envelope("fill_history", fill, stamp, source="okx_rest")
                    row["symbol"] = fill.get("instId")
                    count += await self._unique("fills.jsonl", row, stamp, "bill:" + str(fill["billId"]))
            # Separate REST fill ledger; never add it to WS cumulative orders.
            self._history_end[inst_type] = end
        pending = []
        for inst_type in self.config.inst_types:
            async for rows in self.rest.pages("/api/v5/trade/orders-pending", {"instType": inst_type}, cursor_key="ordId"):
                pending.extend(rows)
        await asyncio.to_thread(self.store.snapshot, "open_orders.json",
                                self.envelope("open_orders", pending, end, source="okx_rest"), end)
        # Funding bill records are preserved in native currency/sign. They do
        # not enter existing P&L until valuation/matching is agreed separately.
        funding_begin = max(today - 86_400_000, self._history_end.get("funding", today) - 60_000)
        async for rows in self.rest.pages("/api/v5/account/bills", {"type": "8", "begin": funding_begin, "end": end}):
            for bill in rows:
                if str(bill.get("subType")) not in {"173", "174"}:
                    continue
                stamp = int(bill["ts"])
                if not bill.get("billId"):
                    raise ValueError("OKX funding bill has no billId")
                await self._unique("funding.jsonl", self.envelope("funding_bill", bill, stamp, source="okx_rest"),
                                   stamp, "bill:" + str(bill["billId"]))
        self._history_end["funding"] = end
        day = trading_day(end)
        if self._instrument_day != day:
            instruments = {}
            for inst_type in self.config.inst_types:
                instruments[inst_type] = await self.rest.get("/api/v5/public/instruments", {"instType": inst_type}, private=False)
            await asyncio.to_thread(self.store.snapshot, "instruments.json", instruments, end)
            self._instrument_day = day
        if count or not self._reconciled:
            self.logger.info("OKX reconciliation complete new_fills=%d open_orders=%d", count, len(pending))
        self._reconciled = True

    async def _account_loop(self, stop):
        while not stop.is_set():
            try:
                await self.collect_account()
            except (OSError, ValueError):
                # OSError includes disk failures: never silently run without storage.
                raise
            except Exception as exc:
                self.last_error = type(exc).__name__ + ": " + str(exc)
                self.logger.warning("OKX account poll failed error=%s", self.last_error)
            try:
                await asyncio.wait_for(stop.wait(), self.config.rest_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def _reconcile_loop(self, stop):
        while not stop.is_set():
            self.reconcile_requested.clear()
            try:
                await self.reconcile()
            except (OSError, ValueError):
                raise
            except Exception as exc:
                self.logger.warning("OKX reconciliation failed error=%s", str(exc))
            # Includes reconnect signals; one reconciliation at a time.
            try:
                await asyncio.wait_for(self.reconcile_requested.wait(), self.config.reconcile_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def run(self, stop):
        self.logger.info("OKX collector starting inst_types=%s output=%s matching=disabled", self.config.inst_types, self.store.root)
        tasks = [asyncio.create_task(self.stream.run(stop)), asyncio.create_task(self._account_loop(stop)),
                 asyncio.create_task(self._reconcile_loop(stop)), asyncio.create_task(stop.wait())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            if stop.is_set():
                await tasks[0]  # Drain accepted WS messages on graceful shutdown.
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.rest.close()
            self.logger.info("OKX collector stopped")
