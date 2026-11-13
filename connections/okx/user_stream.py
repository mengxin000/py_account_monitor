"""Login, acknowledged subscriptions, text heartbeat and supervised reconnect."""
import asyncio
import json
import logging
import time
from urllib.parse import urlsplit

import aiohttp

from .rest import OKXError


class OKXUserStream:
    def __init__(self, rest, on_message, *, on_connect=None, logger=None,
                 url="wss://ws.okx.com:8443/ws/v5/private", queue_size=4096):
        parsed = urlsplit(url)
        if parsed.scheme != "wss" or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
            raise ValueError("OKX WebSocket URL must use wss")
        self.rest, self.url, self.on_message = rest, url, on_message
        self.on_connect = on_connect
        self.logger = logger or logging.getLogger(__name__)
        self.connected = False
        self.state = "DISCONNECTED"
        self.received_events = 0
        self.last_error = None
        self.queue = asyncio.Queue(queue_size)
        self.subscriptions = [dict(channel="orders", instType="ANY"),
                              dict(channel="account"), dict(channel="positions", instType="ANY")]

    @staticmethod
    def _key(arg):
        return arg.get("channel"), arg.get("instType", "")

    async def _connection(self, stop):
        self.state = "CONNECTING"
        await self.rest.sync_time()
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.ws_connect(self.url, proxy=self.rest.proxy, autoping=True) as ws:
                self.state = "AUTHENTICATING"
                await ws.send_json(self.rest.credentials.login(str(int(self.rest.now_ms()/1000))))
                required = {self._key(arg) for arg in self.subscriptions}
                acknowledged = set()
                deadline = time.monotonic() + 20
                last_rx = time.monotonic()
                ping_at = None
                while not stop.is_set():
                    now = time.monotonic()
                    if not self.connected and now > deadline:
                        raise TimeoutError("OKX login/subscription acknowledgement timeout")
                    if ping_at is not None and now - ping_at > 10:
                        raise TimeoutError("OKX pong timeout")
                    if now - last_rx > 15 and ping_at is None:
                        await ws.send_str("ping")
                        ping_at = now
                    try:
                        message = await ws.receive(timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    received_us = time.time_ns() // 1000
                    last_rx = time.monotonic()
                    if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        raise ConnectionError(f"OKX WebSocket closed code={ws.close_code}")
                    if message.data in ("pong", b"pong"):
                        ping_at = None
                        continue
                    packet = json.loads(message.data)
                    if not isinstance(packet, dict):
                        raise ValueError("invalid OKX WebSocket packet")
                    event = packet.get("event")
                    if event in {"error", "channel-conn-count-error", "notice"}:
                        raise OKXError("websocket", packet.get("code", event))
                    if event == "login":
                        if str(packet.get("code")) != "0":
                            raise OKXError("websocket/login", packet.get("code", "rejected"))
                        self.logger.info("OKX private login accepted")
                        self.state = "SUBSCRIBING"
                        await ws.send_json({"op": "subscribe", "args": self.subscriptions})
                    elif event == "subscribe":
                        if str(packet.get("code", "0")) != "0":
                            raise OKXError("websocket/subscribe", packet["code"])
                        acknowledged.add(self._key(packet.get("arg", {})))
                        if required <= acknowledged and not self.connected:
                            self.connected = True
                            self.state = "CONNECTED"
                            self.last_error = None
                            self.logger.info("OKX private subscriptions ready channels=orders,account,positions")
                            if self.on_connect:
                                self.on_connect()  # Signal reconciliation; never block receive on REST.
                    elif "data" in packet and packet.get("arg", {}).get("channel") in {"orders", "account", "positions"}:
                        # Timestamp captured on receive, before queueing/writing.
                        await self.queue.put((packet, received_us))
                        self.received_events += 1

    async def _reconnect(self, stop):
        delay = 3
        while not stop.is_set():
            try:
                await self._connection(stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                was_connected = self.connected
                self.last_error = str(exc) if isinstance(exc, OKXError) else type(exc).__name__
                self.logger.warning("OKX private disconnected stage=%s error=%s received=%d retry=%ss",
                                    self.state, self.last_error, self.received_events, delay)
                if was_connected:
                    delay = 3
            finally:
                self.connected = False
                self.state = "DISCONNECTED"
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)

    async def _consume(self):
        while True:
            packet, received_us = await self.queue.get()
            try:
                await self.on_message(packet, received_us)
            finally:
                self.queue.task_done()

    async def run(self, stop):
        receiver = asyncio.create_task(self._reconnect(stop))
        consumer = asyncio.create_task(self._consume())
        stopper = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait([receiver, consumer, stopper], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task  # A failed disk consumer is fatal, not a quiet reconnect.
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
            if not consumer.done():
                await asyncio.wait_for(self.queue.join(), 30)
        finally:
            for task in (receiver, consumer, stopper):
                task.cancel()
            await asyncio.gather(receiver, consumer, stopper, return_exceptions=True)
            self.connected = False
            self.state = "STOPPED"
