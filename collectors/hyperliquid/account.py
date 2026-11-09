"""Durable fills/funding and unified-account valuation; read-only endpoints."""
import asyncio
import json
import logging
import math
import time
from datetime import datetime, timedelta
try:
    from ...connections.hyperliquid.rest import HyperliquidRestClient
    from ...connections.hyperliquid.user_stream import HyperliquidUserStream
    from ...core.jsonl_io import iter_jsonl
except ImportError:
    from connections.hyperliquid.rest import HyperliquidRestClient
    from connections.hyperliquid.user_stream import HyperliquidUserStream
    from core.jsonl_io import iter_jsonl
from .normalize import normalize_fill, normalize_order, symbol_for, rates


class HyperliquidCollector:
    def __init__(self, account_id, store, config, on_trade, on_error, proxy=None):
        self.account_id, self.store, self.config = account_id, store, config
        self.address = config["address"]
        self.logger = logging.getLogger(f"account_monitor.{account_id}.hyperliquid")
        self.rest = HyperliquidRestClient(config.get("rest_base_url", "https://api.hyperliquid.xyz"), proxy)
        self.on_trade = on_trade
        self.seen = {}
        self.cloids = {}
        self.lock = asyncio.Lock()
        self.metadata = None
        self.metadata_at = 0
        self.perp_states = None
        self.perp_states_at = 0
        self.last_at = None
        self.equity = None
        self.last_orders = []
        self.on_market = None
        self.stream = HyperliquidUserStream(self.address, self.message, on_connect=self.reconcile,
            on_error=on_error, proxy=proxy, logger=self.logger,
            url=config.get("websocket_url", "wss://api.hyperliquid.xyz/ws"))

    @staticmethod
    def day_at(ms):
        now = datetime.fromtimestamp(ms / 1000)
        if (now.hour, now.minute) < (9, 30):
            now -= timedelta(days=1)
        return now.strftime("%Y%m%d")

    def keys(self, ms):
        day = self.day_at(ms)
        if day not in self.seen:
            keys = set()
            for filename in ("trade_callbacks.jsonl", "funding.jsonl"):
                path = self.store.root / day / filename
                if path.exists():
                    for _, _, row in iter_jsonl(path):
                        if row.get("exchange") == "hyperliquid" and row.get("eventId"):
                            keys.add(row["eventId"])
            self.seen[day] = keys
            # Keep recent trading days only; older snapshots can be re-read from disk.
            for old in sorted(self.seen)[:-2]:
                self.seen.pop(old, None)
            return keys
        return self.seen[day]

    def envelope(self, kind, data, ms, received_us=None):
        return {"exchange": "hyperliquid", "source": "hyperliquid_stream",
                "accountScope": "hyperliquid", "accountId": self.account_id,
                "recordType": kind, "eventType": data.get("channel", "userFills"),
                "eventTimeMs": ms, "receivedTimeUs": received_us or time.time_ns() // 1000,
                "receivedTime": datetime.now().isoformat(timespec="microseconds"), "data": data}

    async def fill(self, fill, received_us=None, *, source="hyperliquid_stream", is_snapshot=False):
        # Spot fills are not part of this perpetual leg.
        if str(fill["coin"]).startswith("@") or "/" in str(fill["coin"]):
            return
        ms = int(fill["time"])
        event_id = f"fill:{fill['coin']}:{fill['oid']}:{ms}:{fill['tid']}"
        async with self.lock:
            seen = self.keys(ms)
            if event_id in seen:
                return
            oid = str(fill["oid"])
            cloid = fill.get("cloid") or self.cloids.get(oid)
            if not cloid and oid not in self.cloids:
                response = await self.rest.info("orderStatus", user=self.address, oid=int(oid))
                if response.get("status") == "order":
                    cloid = response["order"]["order"].get("cloid")
                self.cloids[oid] = cloid or ""
                if not cloid:
                    self.logger.warning("Hyperliquid fill has no cloid; left for Exposure oid=%s", oid)
            normalized = normalize_fill(fill, self.account_id, self.config, cloid)
            row = self.envelope("trade_callback", {"channel": "userFills", "fill": fill}, ms, received_us)
            row.update(eventId=event_id, symbol=normalized["symbol"], normalizedOrder=normalized,
                       commission=fill["fee"], nativePrice=fill["px"], source=source, isSnapshot=is_snapshot)
            now = datetime.fromtimestamp(ms / 1000)
            await asyncio.to_thread(self.store.append, "all_callbacks.jsonl", row, now=now)
            await asyncio.to_thread(self.store.append, "trade_callbacks.jsonl", row, now=now)
            seen.add(event_id)  # A failed append must never acknowledge the fill.
            self.on_trade()
            if self.on_market and not is_snapshot and source == "hyperliquid_stream" and abs(time.time() * 1000 - ms) < 15_000:
                self.on_market({"e": "HYPERLIQUID_ORDER", "s": fill["coin"].upper(),
                    "i": oid, "X": "EXECUTION_ONLY", "x": "TRADE", "l": fill["sz"],
                    "T": ms, "t": fill["tid"], "_accountScope": "hyperliquid"})

    async def funding(self, item, received_us=None, *, source="hyperliquid_stream"):
        delta = item.get("delta", item)
        ms = int(item["time"])
        # REST and WS describe the same hourly payment; use one common identity.
        event_id = f"funding:{ms}:{delta['coin']}"
        async with self.lock:
            seen = self.keys(ms)
            if event_id in seen:
                return
            row = self.envelope("funding_income", item, ms, received_us)
            income = float(delta["usdc"]) * rates(self.config)["USDC"]
            if not math.isfinite(income):
                raise ValueError("invalid Hyperliquid funding")
            row.update(eventId=event_id, raw=item, source=source, eventType="userFundings",
                       data={"income": income, "asset": "USDT", "time": ms,
                             "symbol": symbol_for(delta["coin"], self.config), "tranId": event_id})
            await asyncio.to_thread(self.store.append, "funding.jsonl", row, now=datetime.fromtimestamp(ms / 1000))
            seen.add(event_id)
            self.logger.info("Hyperliquid funding collected coin=%s income=%s", delta["coin"], income)

    async def message(self, packet, received_us):
        channel, data = packet["channel"], packet["data"]
        if channel == "allDexsClearinghouseState":
            self.perp_states = dict(data["clearinghouseStates"])
            self.perp_states_at = time.monotonic()
        elif channel == "userFills":
            for fill in data.get("fills", []):
                await self.fill(fill, received_us, is_snapshot=bool(data.get("isSnapshot", False)))
        elif channel == "userFundings":
            for item in data.get("fundings", []):
                await self.funding(item, received_us)
        elif channel == "orderUpdates":
            for update in data:
                o = update["order"]
                if str(o["coin"]).startswith("@") or "/" in str(o["coin"]):
                    continue
                self.cloids[str(o["oid"])] = o.get("cloid") or ""
                if len(self.cloids) > 50000:
                    self.cloids.pop(next(iter(self.cloids)))
                ms = int(update.get("statusTimestamp", o["timestamp"]))
                row = self.envelope("order_callback", update, ms, received_us)
                row["eventType"] = "orderUpdates"
                row["orderView"] = normalize_order(update, self.config)
                row["symbol"] = row["orderView"]["s"]
                await asyncio.to_thread(self.store.append, "all_callbacks.jsonl", row, now=datetime.fromtimestamp(ms / 1000))
                if self.on_market:
                    self.on_market({**row["orderView"], "e": "HYPERLIQUID_ORDER", "_accountScope": "hyperliquid"})

    async def reconcile(self):
        # Populate cloid in one request before falling back to per-order lookup.
        # userFills itself does not guarantee a cloid field.
        historical = await self.rest.info("historicalOrders", user=self.address)
        if not isinstance(historical, list):
            raise ValueError("invalid Hyperliquid historicalOrders response")
        for item in historical:
            order = item["order"]
            self.cloids[str(order["oid"])] = order.get("cloid") or ""
        now = datetime.now()
        start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if now < start:
            start -= timedelta(days=1)
        begin, end = int(start.timestamp() * 1000), int(now.timestamp() * 1000)
        async for rows in self.rest.history("userFillsByTime", self.address, begin, end):
            for row in sorted(rows, key=lambda x: (x["time"], x["tid"])):
                await self.fill(row, source="hyperliquid_rest", is_snapshot=True)
        async for rows in self.rest.history("userFunding", self.address, begin, end):
            for row in rows:
                await self.funding(row, source="hyperliquid_rest")
        self.logger.info("Hyperliquid fills/funding reconciled from trading-day 09:30")

    async def equity_snapshot(self):
        if self.metadata is None or time.monotonic() - self.metadata_at > 3600:
            mode = await self.rest.info("userAbstraction", user=self.address)
            if mode != "unifiedAccount":
                raise ValueError(f"expected Hyperliquid unifiedAccount, got {mode}")
            meta, dexs, spot_meta = await asyncio.gather(
                self.rest.info("meta"), self.rest.info("perpDexs"), self.rest.info("spotMeta"))
            dex_meta = {"": meta}
            for dex in dexs:
                if dex and dex.get("name"):
                    dex_meta[dex["name"]] = await self.rest.info("meta", dex=dex["name"])
            self.metadata = (dex_meta, spot_meta)
            self.metadata_at = time.monotonic()
        dex_meta, spot_meta = self.metadata
        spot = await self.rest.info("spotClearinghouseState", user=self.address)
        conversion = rates(self.config)
        balances = spot.get("balances")
        if not isinstance(balances, list):
            raise ValueError("Hyperliquid unified balances unavailable")
        tokens = {int(t["index"]): t["name"] for t in spot_meta["tokens"]}
        total = 0.0
        for row in balances:
            amount = float(row["total"])
            if not amount:
                continue
            coin = row["coin"]
            if coin not in conversion:
                # Obtain token prices from the spot universe, never a perpetual mark.
                contexts = await self.rest.info("spotMetaAndAssetCtxs")
                for pair, ctx in zip(contexts[0]["universe"], contexts[1]):
                    base, quote = (tokens.get(int(i)) for i in pair["tokens"])
                    if base and quote in conversion and ctx.get("markPx") is not None:
                        conversion[base] = float(ctx["markPx"]) * conversion[quote]
                if coin not in conversion:
                    raise ValueError(f"cannot value unified-account asset {coin}")
            total += amount * conversion[coin]
        positions = []
        max_ratio = 0.0
        maintenance_by_token, isolated_by_token = {}, {}
        if self.perp_states is None or time.monotonic() - self.perp_states_at > 30:
            semaphore = asyncio.Semaphore(4)
            async def query(dex):
                async with semaphore:
                    return dex, await self.rest.info("clearinghouseState", user=self.address, dex=dex)
            self.perp_states = dict(await asyncio.gather(*(query(dex) for dex in dex_meta)))
            self.perp_states_at = time.monotonic()
        for dex, meta in dex_meta.items():
            state = self.perp_states.get(dex, {"assetPositions":[], "crossMaintenanceMarginUsed":0})
            collateral = tokens.get(int(meta.get("collateralToken", 0)), "USDC")
            if collateral not in conversion:
                raise ValueError(f"missing collateral conversion rate for {collateral}")
            isolated = 0.0
            for row in state["assetPositions"]:
                p = row["position"]
                total += float(p["unrealizedPnl"]) * conversion[collateral]
                if p.get("leverage", {}).get("type") == "isolated":
                    isolated += float(p["marginUsed"])
                positions.append({**p, "dex": dex})
            balance = sum(float(b["total"]) for b in balances if b["coin"] == collateral)
            available = balance - isolated
            maintenance = float(state["crossMaintenanceMarginUsed"])
            maintenance_by_token[collateral] = maintenance_by_token.get(collateral, 0) + maintenance
            isolated_by_token[collateral] = isolated_by_token.get(collateral, 0) + isolated
        for collateral, maintenance in maintenance_by_token.items():
            balance = sum(float(b["total"]) for b in balances if b["coin"] == collateral)
            available = balance - isolated_by_token[collateral]
            if available > 0:
                max_ratio = max(max_ratio, maintenance / available * 100)
            elif maintenance > 0:
                max_ratio = 100.0
        if not math.isfinite(total):
            raise ValueError("Hyperliquid equity non-finite")
        self.last_at, self.equity = datetime.now(), total
        return {"equity": total, "positions": positions, "balances": balances,
                "mmr": max_ratio, "mode": "unifiedAccount", "usdcToUsdt": conversion["USDC"]}

    async def open_orders(self):
        # Read all deployed perpetual DEXs; Spot orders are excluded.
        if self.metadata is None:
            raise ValueError("Hyperliquid metadata not ready")
        output = []
        for dex in self.metadata[0]:
            rows = await self.rest.info("frontendOpenOrders", user=self.address, dex=dex)
            for row in rows:
                if str(row["coin"]).startswith("@") or "/" in str(row["coin"]):
                    continue
                output.append({"symbol": row["coin"].upper(), "orderId": str(row["oid"]),
                    "clientOrderId": row.get("cloid") or "", "side": "BUY" if row["side"] == "B" else "SELL",
                    "price": row["limitPx"], "origQty": row["sz"], "executedQty": 0})
        return output

    async def funding_loop(self, stop):
        while not stop.is_set():
            now = datetime.now()
            target = now.replace(minute=0, second=5, microsecond=0) + timedelta(hours=1)
            try:
                await asyncio.wait_for(stop.wait(), (target - now).total_seconds())
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.reconcile()
            except Exception:
                self.logger.exception("Hyperliquid hourly reconciliation failed")
