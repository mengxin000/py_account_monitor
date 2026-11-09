from __future__ import annotations
import asyncio
import json
import logging
import time
import aiohttp


class HyperliquidUserStream:
    def __init__(self, address, on_message, *, on_connect=None, on_error=None,
                 url="wss://api.hyperliquid.xyz/ws", proxy=None, logger=None):
        self.address, self.on_message = address, on_message
        self.on_connect, self.on_error = on_connect, on_error
        self.url, self.proxy = url, proxy
        self.logger = logger or logging.getLogger(__name__)
        self.connected = False
        self.received_events = 0

    async def run(self, stop):
        while not stop.is_set():
            tasks = []
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
                async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                    async with session.ws_connect(self.url, heartbeat=20, proxy=self.proxy) as ws:
                        self.received_events = 0
                        feeds = {"userFills", "orderUpdates", "userFundings", "allDexsClearinghouseState"}
                        for kind in sorted(feeds):
                            subscription = {"type": kind, "user": self.address}
                            if kind == "userFills":
                                subscription["aggregateByTime"] = False
                            await ws.send_json({"method": "subscribe", "subscription": subscription})
                        queue = asyncio.Queue(4096)  # Critical events: bounded backpressure, no drops.
                        acknowledged = set()
                        deadline = time.monotonic() + 20

                        async def receive():
                            next_ping = time.monotonic() + 20
                            last_message = time.monotonic()
                            while not stop.is_set():
                                if time.monotonic() >= next_ping:
                                    await ws.send_json({"method": "ping"})
                                    next_ping = time.monotonic() + 20
                                if time.monotonic() - last_message > 60:
                                    raise TimeoutError("Hyperliquid server heartbeat missing")
                                if acknowledged != feeds and time.monotonic() > deadline:
                                    raise TimeoutError("Hyperliquid subscription acknowledgment missing")
                                try:
                                    message = await ws.receive(timeout=1)
                                except asyncio.TimeoutError:
                                    continue
                                if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                    raise ConnectionError(f"Hyperliquid WebSocket closed code={ws.close_code}")
                                last_message = time.monotonic()
                                received_us = time.time_ns() // 1000
                                payload = json.loads(message.data)
                                # Control acknowledgments must not wait behind a large
                                # historical fill snapshot or slow disk/order lookups.
                                if payload.get("channel") == "subscriptionResponse":
                                    ack = payload.get("data", {}).get("subscription", {})
                                    acknowledged.add(ack.get("type"))
                                    if acknowledged == feeds and not self.connected:
                                        self.connected = True
                                        self.logger.info("Hyperliquid user subscriptions ready")
                                    continue
                                if payload.get("channel") == "pong":
                                    continue
                                await queue.put((payload, received_us))

                        async def consume():
                            while True:
                                payload, received_us = await queue.get()
                                channel = payload.get("channel")
                                if channel == "error":
                                    raise ValueError("Hyperliquid subscription error")
                                if channel == "subscriptionResponse":
                                    ack = payload.get("data", {}).get("subscription", {})
                                    acknowledged.add(ack.get("type"))
                                    if acknowledged == feeds:
                                        self.connected = True
                                        self.logger.info("Hyperliquid user subscriptions ready")
                                elif channel in feeds:
                                    await self.on_message(payload, received_us)
                                    self.received_events += 1

                        tasks = [asyncio.create_task(receive()), asyncio.create_task(consume()),
                                 asyncio.create_task(stop.wait())]
                        if self.on_connect:
                            tasks.append(asyncio.create_task(self.on_connect()))
                        self.logger.info("Hyperliquid user WebSocket connected; awaiting subscription acknowledgments")
                        while not stop.is_set():
                            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                            if stop.is_set():
                                break
                            for task in done:
                                await task
                                tasks.remove(task)
                            if len(tasks) < 3:
                                raise ConnectionError("Hyperliquid stream worker stopped")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Hyperliquid user stream reconnecting type=%s reason=%s events=%d",
                                    type(exc).__name__, exc, self.received_events)
                if self.on_error:
                    await self.on_error({"source": "hyperliquid_stream", "error": f"{type(exc).__name__}: {exc}"})
            finally:
                self.connected = False
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await asyncio.wait_for(stop.wait(), 3)
            except asyncio.TimeoutError:
                pass
