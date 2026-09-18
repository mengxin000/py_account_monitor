from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, MutableMapping, Sequence
from urllib.parse import urlencode

import aiohttp

Json = dict[str, Any]
MessageHandler = Callable[[Json], Any | Awaitable[Any]]


from .base import BinanceConnectionError, _call_handler

@dataclass(slots=True)
class BinanceMarketStream:
    """Reconnectable combined public market-data stream.

    ``streams`` are Binance names such as ``btcusdt@depth10@100ms`` and
    ``btcusdt@bookTicker``.  Events are passed to ``on_message`` unchanged,
    except that combined-stream wrappers are unwrapped to their ``data``.
    """

    streams: Sequence[str]
    websocket_url: str = "wss://data-stream.binance.vision/stream"
    reconnect_seconds: float = 3.0
    on_message: MessageHandler | None = None
    on_error: MessageHandler | None = None
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        query = urlencode({"streams": "/".join(s.lower() for s in self.streams)})
        url = f"{self.websocket_url}?{query}"
        while not stop_event.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        self.logger.info("market WebSocket connected streams=%s", ",".join(self.streams))
                        async for message in ws:
                            if stop_event.is_set():
                                break
                            if message.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(message.data)
                                if isinstance(payload, dict) and "data" in payload:
                                    payload = payload["data"]
                                await _call_handler(self.on_message, payload)
                            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                raise BinanceConnectionError("market WebSocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("market stream disconnected: %s", exc)
                await _call_handler(self.on_error, {"error": str(exc)})
                if not stop_event.is_set():
                    self.logger.info("market WebSocket reconnecting in %ss", self.reconnect_seconds)
                    await asyncio.sleep(self.reconnect_seconds)
