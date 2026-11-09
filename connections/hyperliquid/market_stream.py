"""One dynamic public socket; timestamps raw frames before parsing."""
import asyncio
import json
import time
import uuid
import aiohttp
try:
    from ...collectors.market_ingress import stamp
except ImportError:
    from collectors.market_ingress import stamp


def native_coin(symbol):
    prefix, sep, suffix = symbol.partition(":")
    return prefix.lower() + sep + suffix if sep else symbol


async def market_stream(session, feed, desired, receive, running, proxy=None, on_open=None):
    """Caller owns reconnect policy and bounded outgoing queues."""
    async with session.ws_connect("wss://api.hyperliquid.xyz/ws", heartbeat=20, proxy=proxy) as ws:
        if on_open:
            on_open()
        subscribed = set()
        pending = {}
        connection, sequence = uuid.uuid4().hex, 0
        next_ping = time.monotonic() + 20
        last_message = time.monotonic()
        while running():
            wanted = set(desired())
            if pending and time.monotonic() - min(pending.values()) > 15:
                raise TimeoutError("Hyperliquid market subscription acknowledgment missing")
            for method, symbols in (("unsubscribe", subscribed - wanted), ("subscribe", wanted - subscribed)):
                for symbol in sorted(symbols):
                    await ws.send_json({"method": method, "subscription": {"type": feed, "coin": native_coin(symbol)}})
                    pending[(method, symbol)] = time.monotonic()
                    if method == "subscribe":
                        subscribed.add(symbol)
                    else:
                        subscribed.discard(symbol)
                    await asyncio.sleep(.05)
            if time.monotonic() >= next_ping:
                await ws.send_json({"method": "ping"})
                next_ping = time.monotonic() + 20
            if time.monotonic() - last_message > 60:
                raise TimeoutError("Hyperliquid public stream heartbeat missing")
            try:
                message = await ws.receive(timeout=.2)
            except asyncio.TimeoutError:
                continue
            sequence += 1
            last_message = time.monotonic()
            envelope = stamp(message.data, connection, sequence)
            if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                raise ConnectionError(f"Hyperliquid market closed code={ws.close_code}")
            receive(envelope)
            payload = json.loads(message.data)
            if payload.get("channel") == "error":
                raise ValueError("Hyperliquid market subscription rejected")
            if payload.get("channel") == "subscriptionResponse":
                data = payload["data"]
                pending.pop((data["method"], data["subscription"].get("coin", "").upper()), None)
