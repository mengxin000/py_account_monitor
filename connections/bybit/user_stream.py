"""Authenticated Bybit V5 private stream with reconnect and raw packet delivery."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time

import aiohttp


class BybitPrivateStream:
    ACK_TIMEOUT = 20.0
    PING_INTERVAL = 20.0
    PONG_TIMEOUT = 10.0

    def __init__(self, credentials, on_message, *, categories=("spot", "linear"), url="wss://stream.bybit.com/v5/private", proxy=None, logger=None):
        if not url.startswith("wss://"):
            raise ValueError("Bybit private stream URL must use wss")
        self.credentials, self.on_message = credentials, on_message
        self.categories = tuple(dict.fromkeys(categories))
        self.url, self.proxy = url, proxy
        self.logger = logger or logging.getLogger(__name__)
        self.connected = False
        self.state = "DISCONNECTED"
        self.received_events = 0
        self.last_error = None
        self._queue = asyncio.Queue(maxsize=4096)

    def _auth_message(self):
        expires = int(time.time() * 1000) + 10_000
        signature = hmac.new(self.credentials.secret_key.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256).hexdigest()
        return {"op": "auth", "args": [self.credentials.api_key, expires, signature]}

    async def _connection(self, stop):
        self.state = "CONNECTING"
        loop = asyncio.get_running_loop()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15), trust_env=True) as session:
            async with session.ws_connect(self.url, proxy=self.proxy) as ws:
                self.state = "AUTHENTICATING"
                ack_deadline = loop.time() + self.ACK_TIMEOUT
                await asyncio.wait_for(ws.send_json(self._auth_message()), self.ACK_TIMEOUT)
                next_ping = pong_deadline = None
                topics = ["wallet"]
                for category in self.categories:
                    topics.extend((f"order.{category}", f"execution.{category}"))
                    if category != "spot":
                        topics.append(f"position.{category}")

                while not stop.is_set():
                    now = loop.time()
                    if ack_deadline is not None and now >= ack_deadline:
                        raise TimeoutError(f"Bybit private ACK timeout stage={self.state}")
                    if pong_deadline is not None and now >= pong_deadline:
                        raise TimeoutError("Bybit private pong timeout")
                    if next_ping is not None and pong_deadline is None and now >= next_ping:
                        next_ping = now + self.PING_INTERVAL
                        pong_deadline = now + self.PONG_TIMEOUT
                        await asyncio.wait_for(ws.send_json({"op": "ping"}), self.PONG_TIMEOUT)
                        now = loop.time()
                    heartbeat_deadline = pong_deadline if pong_deadline is not None else next_ping
                    deadline = ack_deadline if ack_deadline is not None else heartbeat_deadline
                    try:
                        message = await asyncio.wait_for(ws.receive(), max(0, deadline - now))
                    except asyncio.TimeoutError:
                        continue  # Re-check timers even when the server sends nothing.
                    received_us = time.time_ns() // 1000
                    if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError(f"Bybit WebSocket closed code={ws.close_code}")
                    if message.type != aiohttp.WSMsgType.TEXT:
                        continue
                    packet = json.loads(message.data)
                    if not isinstance(packet, dict):
                        continue
                    op = packet.get("op")
                    if op == "auth" and self.state == "AUTHENTICATING":
                        if packet.get("success") is not True:
                            raise RuntimeError("Bybit private authentication rejected")
                        self.state = "SUBSCRIBING"
                        ack_deadline = loop.time() + self.ACK_TIMEOUT
                        await asyncio.wait_for(ws.send_json({"op": "subscribe", "args": topics}), self.ACK_TIMEOUT)
                    elif op == "subscribe" and self.state == "SUBSCRIBING":
                        if packet.get("success") is not True:
                            raise RuntimeError("Bybit private subscription rejected")
                        self.connected, self.state, self.last_error = True, "CONNECTED", None
                        ack_deadline = None
                        next_ping = loop.time() + self.PING_INTERVAL
                        self.logger.info("Bybit private subscriptions ready topics=%s", ",".join(topics))
                    elif op == "pong" or (op == "ping" and packet.get("ret_msg") == "pong"):
                        pong_deadline = None
                    elif op == "ping":
                        await asyncio.wait_for(ws.send_json({"op": "pong"}), self.PONG_TIMEOUT)
                    elif "topic" in packet and self.state in {"SUBSCRIBING", "CONNECTED"}:
                        # Do not drop private data while subscription ACK is in flight.
                        await self._queue.put((packet, received_us))
                        self.received_events += 1

    async def _reconnect(self, stop):
        delay = 3
        while not stop.is_set():
            try:
                await self._connection(stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = type(exc).__name__
                self.logger.warning("Bybit private stream disconnected state=%s error=%s events=%s retry=%ss", self.state, self.last_error, self.received_events, delay)
            finally:
                self.connected, self.state = False, "DISCONNECTED"
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)

    async def _consume(self):
        while True:
            packet, received_us = await self._queue.get()
            try:
                await self.on_message(packet, received_us)
            finally:
                self._queue.task_done()

    async def run(self, stop):
        receiver = asyncio.create_task(self._reconnect(stop))
        consumer = asyncio.create_task(self._consume())
        stopper = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait((receiver, consumer, stopper), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
            if not consumer.done():
                await asyncio.wait_for(self._queue.join(), 30)
        finally:
            for task in (receiver, consumer, stopper):
                task.cancel()
            await asyncio.gather(receiver, consumer, stopper, return_exceptions=True)
            self.connected, self.state = False, "STOPPED"
