"""Shared on-demand bookTicker streams and fill-triggered quote windows."""
from __future__ import annotations

import asyncio
import logging
import re
import time
import json
import queue as ipc_queue
import uuid
from .market_ingress import stamp, launch, take_batch
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

try:
    from ..storage.market_store import MarketStore
except ImportError:
    from storage.market_store import MarketStore

LOGGER = logging.getLogger("binance_market_collector")
try:
    from ..connections.diagnostics import connection_error
except ImportError:
    from connections.diagnostics import connection_error


@dataclass
class StreamState:
    quotes: deque
    orders: set = field(default_factory=set)
    idle_since: float = 0
    until: float = 0
    since: float = float("inf")
    written: int = 0
    sequence: int = 0
    task: asyncio.Task | None = None


class MarketCollector:
    def __init__(self, root: Path, settings: dict | None = None):
        config = settings or {}
        self.enabled = bool(config.get("enabled", True))
        self.proxy = config.get("proxy")
        self.connection_states = {}
        self.raw_sink = None
        self.ingress_capacity = max(1, int(config.get("ingress_queue_max", 50000)))
        self.before = max(0, float(config.get("buffer_seconds", 30)))
        self.after = max(0, float(config.get("post_fill_seconds", 10)))
        self.idle = max(1, float(config.get("unsubscribe_idle_seconds", 60)))
        self.capacity = max(1, int(config.get("buffer_max_records", 10000)))
        self.store = MarketStore(root, int(config.get("retention_days", 3)))
        self.states: dict[tuple[str, str], StreamState] = {}
        self.revisions: dict[tuple[str, str], int] = {}
        # Order/window lifecycle records are never evicted.  Quotes are a
        # lossy, bounded stream: if disk falls behind, discard the oldest
        # quote so a new quote and critical records can continue through.
        self.critical_queue = asyncio.Queue()
        self.market_queue = asyncio.Queue(maxsize=int(config.get("market_queue_max", 50000)))
        self.queue = self.market_queue  # compatibility for offline callers
        self.dropped_quotes = 0
        self.stopping = False
        self.connect_lock = asyncio.Lock()
        self.session = None
        self.market_tasks: dict[str, asyncio.Task] = {}

    def emit(self, key, record, metadata=False):
        item = (*key, record, metadata)
        if metadata:
            # This queue is intentionally unbounded: a lifecycle event is
            # more important than bounded memory while the writer catches up.
            self.critical_queue.put_nowait(item)
            return True
        try:
            self.market_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            try:
                self.market_queue.get_nowait()
                self.market_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self.market_queue.put_nowait(item)
            self.dropped_quotes += 1
            if self.dropped_quotes == 1 or self.dropped_quotes % 1000 == 0:
                LOGGER.warning("market archive quote queue full; oldest quotes evicted count=%d", self.dropped_quotes)
                self.critical_queue.put_nowait((*key, {"kind":"archive_overflow", "receivedTimeMs":time.time_ns()//1_000_000, "droppedTotal":self.dropped_quotes}, True))
            return True

    def ensure(self, key, now):
        if key not in self.states:
            self.states[key] = StreamState(deque(maxlen=self.capacity), idle_since=now)
        return self.states[key]

    def trim(self, state, now):
        while state.quotes and state.quotes[0]["receivedTimeMs"] < (now - self.before) * 1000:
            state.quotes.popleft()

    def order_event(self, account, event):
        if not self.enabled or self.stopping:
            return
        event_type = event.get("e")
        if event_type == "ORDER_TRADE_UPDATE":
            if event.get("fs", "UM") != "UM":
                return
            market, order = "futures", event.get("o", {})
        elif event_type == "executionReport":
            market, order = "spot", event
        else:
            return
        symbol = str(order.get("s", "")).upper()
        if not re.fullmatch(r"[A-Z0-9_]+", symbol):
            return
        now = time.time()
        key = market, symbol
        state = self.ensure(key, now)
        revision_key = account, market
        self.revisions[revision_key] = self.revisions.get(revision_key, 0) + 1
        order_id = str(order.get("i", order.get("c", "")))
        scope = event.get("_accountScope")
        identity = account, f"{scope}:{order_id}" if scope else order_id
        if order.get("X") in {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}:
            state.orders.add(identity)
        else:
            state.orders.discard(identity)
        if state.orders:
            state.idle_since = 0
        elif not state.idle_since:
            state.idle_since = now
        if order.get("x") == "TRADE" and float(order.get("l", 0) or 0) > 0:
            self.trim(state, now)
            state.since = min(state.since, now - self.before) if now - self.before <= state.until else now - self.before
            state.until = max(state.until, now + self.after)
            self.emit(key, {
                "kind": "fill_window", "receivedTimeMs": int(now * 1000),
                "accountId": account, "orderId": order.get("i"), "tradeId": order.get("t"),
                "eventTimeMs": order.get("T", event.get("T")),
                "requestedStartMs": int((now - self.before) * 1000),
                "requestedEndMs": int((now + self.after) * 1000),
                "availableStartMs": state.quotes[0]["receivedTimeMs"] if state.quotes else None,
                "timeBasis": "local_receive_time",
            }, True)
            for quote in state.quotes:
                if quote["sequence"] > state.written:
                    if not self.emit(key, quote):
                        break
                    state.written = quote["sequence"]

    def quote_event(self, key, event, now=None, envelope=None):
        now = time.time() if now is None else now
        state = self.states[key]
        if not all(field in event for field in ("b", "B", "a", "A")):
            return
        state.sequence += 1
        quote = {"receivedTimeMs": int(now * 1000), "eventTimeMs": event.get("E"),
                 "transactionTimeMs": event.get("T"), "updateId": event.get("u"),
                 "sequence": state.sequence, "bidPrice": event["b"], "bidQty": event["B"],
                 "askPrice": event["a"], "askQty": event["A"]}
        if envelope is not None:
            quote.update(envelope)
            quote["receivedTimeMs"] = envelope["receivedTimeUs"] // 1000
            quote["processedMonoNs"] = time.perf_counter_ns()
        self.trim(state, now)
        state.quotes.append(quote)
        if state.since <= now <= state.until and self.emit(key, quote):
            state.written = state.sequence

    def ingest_raw(self, market, envelope):
        if "metadata" in envelope:
            record = envelope["metadata"]
            kind = record.get("kind")
            if kind in {"connected", "disconnected"}:
                self.connection_states[market] = "CONNECTED" if kind == "connected" else "RECONNECTING"
                LOGGER.log(logging.WARNING if kind == "disconnected" else logging.INFO,
                           "market receiver market=%s state=%s error=%s", market, kind, record.get("error", ""))
            self.emit((market, envelope["symbol"]), record, True)
            return
        try:
            payload = json.loads(envelope["rawPayload"])
            event = payload.get("data", payload)
            key = market, str(event.get("s", "")).upper()
            if key in self.states:
                self.quote_event(key, event, envelope["receivedTimeUs"] / 1_000_000, envelope)
        except (ValueError, TypeError, AttributeError):
            LOGGER.warning("invalid market payload market=%s connection=%s sequence=%s", market, envelope.get("connectionId"), envelope.get("receiveSequence"))

    async def isolated_stream(self, market):
        process, commands, output, stop, drops = launch(market, self.store.root, self.proxy, self.ingress_capacity)
        desired_previous = None
        last_drops = 0
        self.connection_states[market] = "CONNECTING"
        try:
            while not self.stopping:
                desired = sorted(symbol for m, symbol in self.states if m == market)
                if desired != desired_previous:
                    try:
                        commands.put_nowait(desired)
                        desired_previous = desired
                    except ipc_queue.Full:
                        pass
                for record in await asyncio.to_thread(take_batch, output):
                    self.ingest_raw(market, record)
                if drops.value != last_drops:
                    last_drops = drops.value
                    LOGGER.warning("market ingress overflow market=%s dropped_total=%s", market, last_drops)
                    for symbol in desired:
                        self.emit((market,symbol), {"kind":"ingress_overflow", "receivedTimeMs":time.time_ns()//1_000_000, "droppedTotal":last_drops}, True)
                if not process.is_alive():
                    raise RuntimeError(f"market receiver exited code={process.exitcode}")
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("isolated market receiver failed market=%s", market)
            await asyncio.sleep(3)
        finally:
            stop.set()
            deadline = time.monotonic() + 3
            while process.is_alive() and time.monotonic() < deadline:
                for record in await asyncio.to_thread(take_batch, output):
                    self.ingest_raw(market, record)
            forced = process.is_alive()
            if forced:
                process.terminate()
                LOGGER.warning("market receiver forced shutdown market=%s; queued records may be lost", market)
            await asyncio.to_thread(process.join, 2)
            if not forced:
                for record in await asyncio.to_thread(take_batch, output):
                    self.ingest_raw(market, record)
            for channel in (commands, output):
                channel.cancel_join_thread()
                channel.close()
            process.close()
            self.connection_states[market] = "IDLE"

    async def stream(self, market):
        """Maintain one dynamically subscribed bookTicker socket per market."""
        url = "wss://stream.binance.com:9443/ws" if market == "spot" else "wss://fstream.binance.com/ws"
        delay = 1
        request_id = 0
        while not self.stopping:
            try:
                # Pace reconnect storms below the public connection attempt limit.
                async with self.connect_lock:
                    await asyncio.sleep(1.1)
                async with self.session.ws_connect(url, heartbeat=20, proxy=self.proxy) as ws:
                    connection_id = uuid.uuid4().hex
                    receive_sequence = 0
                    self.connection_states[market] = "CONNECTED"
                    subscribed: set[str] = set()
                    pending: dict[int, tuple[str, set[str]]] = {}
                    desired = {symbol for current_market, symbol in self.states if current_market == market}
                    LOGGER.info("market combined stream connected market=%s symbols=%d", market, len(desired))
                    for symbol in desired:
                        self.emit(
                            (market, symbol),
                            {"kind": "connected", "receivedTimeMs": int(time.time()*1000), "combined": True},
                            True,
                        )
                    delay = 1
                    while not self.stopping:
                        desired = {symbol for current_market, symbol in self.states if current_market == market}
                        pending_additions = {
                            symbol for method, symbols in pending.values() if method == "SUBSCRIBE" for symbol in symbols
                        }
                        pending_removals = {
                            symbol for method, symbols in pending.values() if method == "UNSUBSCRIBE" for symbol in symbols
                        }
                        additions = sorted(desired - subscribed - pending_additions)
                        removals = sorted(subscribed - desired - pending_removals)
                        if additions:
                            request_id += 1
                            await ws.send_json({
                                "method": "SUBSCRIBE",
                                "params": [f"{symbol.lower()}@bookTicker" for symbol in additions],
                                "id": request_id,
                            })
                            pending[request_id] = ("SUBSCRIBE", set(additions))
                        if removals:
                            request_id += 1
                            await ws.send_json({
                                "method": "UNSUBSCRIBE",
                                "params": [f"{symbol.lower()}@bookTicker" for symbol in removals],
                                "id": request_id,
                            })
                            pending[request_id] = ("UNSUBSCRIBE", set(removals))
                        try:
                            msg = await ws.receive(timeout=0.5)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            receive_sequence += 1
                            envelope = stamp(msg.data, connection_id, receive_sequence)
                            if self.raw_sink is not None:
                                self.raw_sink(envelope)
                            payload = msg.json()
                            response_id = payload.get("id")
                            if response_id in pending and "result" in payload:
                                method, symbols = pending.pop(response_id)
                                if payload.get("result") is not None:
                                    LOGGER.warning("market subscription rejected market=%s response=%s", market, payload)
                                    continue
                                if method == "SUBSCRIBE":
                                    subscribed.update(symbols)
                                    LOGGER.info("market subscriptions added market=%s symbols=%s", market, ",".join(sorted(symbols)))
                                    now_ms = int(time.time() * 1000)
                                    for symbol in symbols:
                                        self.emit(
                                            (market, symbol),
                                            {"kind": "subscribed", "receivedTimeMs": now_ms, "combined": True},
                                            True,
                                        )
                                else:
                                    subscribed.difference_update(symbols)
                                    LOGGER.info("market subscriptions removed market=%s symbols=%s", market, ",".join(sorted(symbols)))
                                continue
                            if "code" in payload and "msg" in payload:
                                pending.pop(response_id, None)
                                LOGGER.warning("market subscription error market=%s response=%s", market, payload)
                                continue
                            if payload.get("data", {}).get("e") == "serverShutdown" or payload.get("e") == "serverShutdown":
                                raise ConnectionError("market server shutdown")
                            event = payload.get("data", payload)
                            symbol = str(event.get("s", "")).upper()
                            key = (market, symbol)
                            if key in self.states:
                                self.quote_event(key, event, now=envelope["receivedTimeUs"] / 1_000_000, envelope=envelope)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(str(ws.exception()))
                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                            raise ConnectionError("market websocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connection_states[market] = "RECONNECTING"
                reason = connection_error(exc, url)
                LOGGER.warning("market combined stream disconnected market=%s error=%s", market, reason)
                symbols = sorted(symbol for current_market, symbol in self.states if current_market == market)
                for symbol in symbols:
                    self.emit((market, symbol), {"kind": "disconnected", "receivedTimeMs": int(time.time()*1000), "error": reason, "combined": True}, True)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def reconcile(self, monitors):
        while not self.stopping:
            for monitor in monitors:
                for market, path in (("spot", "/papi/v1/margin/openOrders"), ("futures", "/papi/v1/um/openOrders")):
                    account = monitor.config.account_id
                    revision = self.revisions.get((account, market), 0)
                    try:
                        # Portfolio-Margin openOrders supports an omitted symbol and
                        # returns orders for all symbols. This is both more complete
                        # and cheaper than reconstructing a symbol list from JSONL.
                        response = await monitor.rest.get(path, params=None, signed=True)
                        rows = response.get("data") if isinstance(response, dict) else response
                        if not isinstance(rows, list):
                            raise ValueError("openOrders response is not a list")
                        scoped = hasattr(monitor, "spot_stream")
                        scope = "pm_margin" if market == "spot" else "um"
                        orders = [{**row, "_accountScope": scope} for row in rows] if scoped else rows
                        spot_stream = getattr(monitor, "spot_stream", None)
                        if market == "spot" and spot_stream is not None:
                            spot_response = await spot_stream.rest.get("/api/v3/openOrders", signed=True)
                            spot_rows = spot_response.get("data") if isinstance(spot_response, dict) else spot_response
                            if not isinstance(spot_rows, list):
                                raise ValueError("Spot openOrders response is not a list")
                            orders = [*orders, *({**row, "_accountScope": "spot"} for row in spot_rows)]
                        # Don't overwrite a callback that arrived while REST was in flight.
                        if self.revisions.get((account, market), 0) != revision:
                            continue
                        now = time.time()
                        for key, state in self.states.items():
                            if key[0] == market:
                                state.orders = {x for x in state.orders if x[0] != account}
                        for order in orders:
                            symbol = str(order["symbol"]).upper()
                            if not re.fullmatch(r"[A-Z0-9_]+", symbol):
                                continue
                            state = self.ensure((market, symbol), now)
                            order_id = str(order["orderId"])
                            scope = order.get("_accountScope")
                            state.orders.add((account, f"{scope}:{order_id}" if scope else order_id))
                        for key, state in self.states.items():
                            if key[0] == market:
                                if state.orders:
                                    state.idle_since = 0
                                elif not state.idle_since:
                                    state.idle_since = now
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        LOGGER.warning("open orders reconciliation failed account=%s market=%s", account, market, exc_info=True)
            await asyncio.sleep(60)

    async def writer(self):
        last_maintenance = 0
        while True:
            item = None
            queue = None
            try:
                item = self.critical_queue.get_nowait()
                queue = self.critical_queue
            except asyncio.QueueEmpty:
                if self.stopping and self.market_queue.empty():
                    return
                try:
                    item = await asyncio.wait_for(self.market_queue.get(), timeout=60)
                    queue = self.market_queue
                except asyncio.TimeoutError:
                    item = None
            if item is None and queue is self.market_queue:
                queue.task_done()
                if self.stopping and self.critical_queue.empty() and self.market_queue.empty():
                    return
                continue
            if item is None:
                try:
                    await asyncio.to_thread(self.store.maintain)
                except Exception:
                    LOGGER.exception("market archive maintenance failed")
                if self.stopping and self.critical_queue.empty() and self.market_queue.empty():
                    return
                continue
            try:
                batch = [item]
                for _ in range(255):
                    try:
                        following = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if following is not None:
                        batch.append(following)
                    queue.task_done()
                await asyncio.to_thread(self.store.write_batch, batch)
                if time.monotonic() - last_maintenance > 60:
                    await asyncio.to_thread(self.store.maintain)
                    last_maintenance = time.monotonic()
            except Exception:
                LOGGER.exception("market archive write/maintenance failed")
            finally:
                queue.task_done()

    async def run(self, monitors):
        if not self.enabled:
            return
        writer = asyncio.create_task(self.writer())
        reconciler = asyncio.create_task(self.reconcile(monitors))
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15), trust_env=True) as self.session:
                while not self.stopping:
                    now = time.time()
                    for key, state in list(self.states.items()):
                        self.trim(state, now)
                        if not state.orders and state.idle_since and now - state.idle_since >= self.idle and now > state.until:
                            if state.task:
                                state.task.cancel()
                                await asyncio.gather(state.task, return_exceptions=True)
                                state.task = None
                            # A private callback may have arrived during close.
                            if state.orders or time.time() <= state.until or time.time() - state.idle_since < self.idle:
                                continue
                            self.emit(key, {"kind": "unsubscribed", "receivedTimeMs": int(now*1000)}, True)
                            del self.states[key]
                            LOGGER.info("market unsubscribed market=%s symbol=%s", *key)
                    for market in ("spot", "futures"):
                        active = any(current_market == market for current_market, _ in self.states)
                        task = self.market_tasks.get(market)
                        if active and (task is None or task.done()):
                            self.market_tasks[market] = asyncio.create_task(self.isolated_stream(market))
                        elif not active and task is not None:
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                            self.market_tasks.pop(market, None)
                    await asyncio.sleep(0.2)
        finally:
            self.stopping = True
            reconciler.cancel()
            tasks = list(self.market_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(reconciler, *tasks, return_exceptions=True)
            for key in self.states:
                self.emit(key, {"kind": "collector_stopped", "receivedTimeMs": int(time.time()*1000)}, True)
            # Wake a writer that is waiting on an empty market queue. The
            # writer drains critical records before returning.
            await self.market_queue.put(None)
            await writer
            self.states.clear()
