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


from .base import BinanceCredentials, BinanceConnectionError, UserStreamConfig, _call_handler
from .rest import BinanceRestClient
from ..diagnostics import connection_error

class BinanceUserDataStream:
    """Portfolio-Margin/spot/futures user-data stream with listen-key renewal."""

    def __init__(
        self,
        credentials: BinanceCredentials,
        *,
        config: UserStreamConfig | None = None,
        on_message: MessageHandler | None = None,
        on_error: MessageHandler | None = None,
        reconnect_seconds: float = 3.0,
        logger: logging.Logger | None = None,
        proxy: str | None = None,
    ) -> None:
        self.config = config or UserStreamConfig()
        self.proxy = proxy
        self.rest = BinanceRestClient(credentials, base_url=self.config.rest_base_url, logger=logger, proxy=proxy)
        self.on_message = on_message
        self.on_error = on_error
        self.reconnect_seconds = reconnect_seconds
        self.logger = logger or logging.getLogger(__name__)
        self.connected = False

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        while not stop_event.is_set():
            listen_key: str | None = None
            keepalive: asyncio.Task[None] | None = None
            try:
                listen_key = await self.rest.create_listen_key(self.config)
                self.logger.info("user stream listen key created")
                keepalive = asyncio.create_task(self._keepalive_loop(listen_key, stop_event))
                url = f"{self.config.websocket_base_url.rstrip('/')}/ws/{listen_key}"
                timeout = aiohttp.ClientTimeout(total=None)
                async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                    async with session.ws_connect(url, heartbeat=20, proxy=self.proxy) as ws:
                        self.logger.info("user WebSocket connected")
                        self.connected = True
                        receiver = asyncio.create_task(self._receive_loop(ws, stop_event))
                        stopper = asyncio.create_task(stop_event.wait())
                        try:
                            # A keepalive failure must be treated exactly like
                            # a socket failure. Previously this background task
                            # could die silently while the stale socket stayed
                            # open and stopped delivering account events.
                            done, _ = await asyncio.wait(
                                {receiver, keepalive, stopper},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            for task in done:
                                await task  # propagate keepalive/receiver error
                            if not stop_event.is_set():
                                raise BinanceConnectionError("user stream task stopped unexpectedly")
                        finally:
                            receiver.cancel()
                            stopper.cancel()
                            await asyncio.gather(receiver, stopper, return_exceptions=True)
            except asyncio.CancelledError:
                # The daemon cancels this task during Ctrl+C/restart.  Close
                # the REST client so aiohttp does not report unclosed sessions.
                raise
            except Exception as exc:
                self.connected = False
                reason = connection_error(exc, self.config.websocket_base_url)
                self.logger.warning("user stream disconnected: %s", reason)
                await _call_handler(self.on_error, {"error": reason, "source": "pm_stream"})
                if not stop_event.is_set():
                    self.logger.info("user WebSocket reconnecting in %ss", self.reconnect_seconds)
            finally:
                self.connected = False
                if keepalive:
                    keepalive.cancel()
                    await asyncio.gather(keepalive, return_exceptions=True)
                if listen_key:
                    try:
                        await self.rest.close_listen_key(self.config, listen_key)
                    except Exception:
                        pass
                await self.rest.close()
            if not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), self.reconnect_seconds)
                except asyncio.TimeoutError:
                    pass
        await self.rest.close()

    async def _keepalive_loop(self, listen_key: str, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.keepalive_seconds)
            except asyncio.TimeoutError:
                try:
                    await self.rest.keepalive_listen_key(self.config, listen_key)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise BinanceConnectionError(f"listen key keepalive failed: {exc}") from exc

    async def _receive_loop(self, ws: Any, stop_event: asyncio.Event) -> None:
        async for message in ws:
            if stop_event.is_set():
                return
            if message.type == aiohttp.WSMsgType.TEXT:
                payload = json.loads(message.data)
                event_type = str(payload.get("e", payload.get("eventType", ""))) if isinstance(payload, dict) else ""
                if event_type.lower() == "listenkeyexpired":
                    raise BinanceConnectionError("listen key expired")
                await _call_handler(self.on_message, payload)
            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                raise BinanceConnectionError("user WebSocket closed")
        if not stop_event.is_set():
            raise BinanceConnectionError("user WebSocket ended")
