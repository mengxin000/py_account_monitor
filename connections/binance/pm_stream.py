from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp

from .base import BinanceCredentials, BinanceConnectionError, MessageHandler, UserStreamConfig, _call_handler
from .rest import BinanceRestClient
from ..diagnostics import connection_error


class _UserStreamStateError(BinanceConnectionError):
    """Internal, safe-to-log lifecycle error; never includes keys or URLs."""


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
        self.received_events = 0

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        try:
            while not stop_event.is_set():
                stage = "listen_key"
                self.received_events = 0
                try:
                    listen_key = await self.rest.create_listen_key(self.config)
                    self.logger.info("user stream listen key obtained")
                    stage = "connect"
                    url = self.config.websocket_url(listen_key)
                    # Bound the HTTP upgrade; this does not impose an idle
                    # timeout on a legitimately quiet account's WebSocket.
                    timeout = aiohttp.ClientTimeout(total=15, sock_connect=15)
                    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                        async with session.ws_connect(url, heartbeat=20, proxy=self.proxy) as ws:
                            self.logger.info("user WebSocket connected")
                            self.connected = True
                            receiver = asyncio.create_task(self._receive_loop(ws, stop_event))
                            keepalive = asyncio.create_task(self._keepalive_loop(listen_key, stop_event))
                            stopper = asyncio.create_task(stop_event.wait())
                            tasks = (receiver, keepalive, stopper)
                            try:
                                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                                if stop_event.is_set():
                                    return
                                for task, task_stage in ((receiver, "receive"), (keepalive, "keepalive")):
                                    if task in done:
                                        stage = task_stage
                                        await task
                                raise _UserStreamStateError("user stream task stopped unexpectedly")
                            finally:
                                self.connected = False
                                # Cancel and observe every task before the
                                # local WebSocket/session is closed.
                                for task in tasks:
                                    task.cancel()
                                await asyncio.gather(*tasks, return_exceptions=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.connected = False
                    endpoint = (self.config.rest_base_url if stage in {"listen_key", "keepalive"}
                                else self.config.websocket_base_url)
                    reason = f"stage={stage} {connection_error(exc, endpoint)}"
                    if isinstance(exc, _UserStreamStateError):
                        reason += f" reason={exc}"
                    self.logger.warning("user stream disconnected: %s received_events=%d", reason, self.received_events)
                    try:
                        await _call_handler(self.on_error, {"error": reason, "source": self.config.source})
                    except asyncio.CancelledError:
                        raise
                    except Exception as handler_error:
                        self.logger.error("user stream error handler failed: %s", connection_error(handler_error, endpoint))
                    if not stop_event.is_set():
                        self.logger.info("user WebSocket reconnecting in %ss", self.reconnect_seconds)
                finally:
                    self.connected = False
                # listenKey is account-scoped and POST may have returned a
                # key shared with another process. Close our local socket,
                # never DELETE the account key during retry or shutdown.
                if not stop_event.is_set():
                    try:
                        await asyncio.wait_for(stop_event.wait(), self.reconnect_seconds)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self.connected = False
            await self.rest.close()

    @staticmethod
    def _check_server_error(payload: dict[str, Any]) -> None:
        error = payload.get("error")
        details = error if isinstance(error, dict) else payload
        code = details.get("code")
        status = payload.get("status")
        try:
            code = int(code) if code is not None else None
        except (ValueError, TypeError):
            code = None
        try:
            status = int(status) if status is not None else None
        except (ValueError, TypeError):
            status = None
        if (error is not None or (status is not None and status >= 400)
                or (code is not None and code != 0)):
            # Server messages may echo the listenKey or signed URL. Report
            # only numeric error codes/status, not the raw payload/msg.
            raise _UserStreamStateError(f"user stream server error api_code={code} http_status={status}")

    async def _keepalive_loop(self, listen_key: str, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.keepalive_seconds)
            except asyncio.TimeoutError:
                try:
                    result = await self.rest.keepalive_listen_key(self.config, listen_key)
                    if not isinstance(result, dict):
                        raise _UserStreamStateError("listen key renewal returned an invalid response")
                    self._check_server_error(result)
                    renewed_key = result.get("listenKey")
                    if self.config.listen_key_path == "/fapi/v1/listenKey" or "listenKey" in result:
                        if not isinstance(renewed_key, str) or not renewed_key:
                            raise _UserStreamStateError("listen key renewal returned no valid key")
                        if renewed_key != listen_key:
                            # Updating a variable cannot rebind the already
                            # open socket. Let run() re-obtain the active key
                            # and establish a new WebSocket subscription.
                            raise _UserStreamStateError("listen key changed during renewal; resubscription required")
                    self.logger.info("user stream listen key renewed")
                except asyncio.CancelledError:
                    raise
                except _UserStreamStateError:
                    raise
                except Exception as exc:
                    raise _UserStreamStateError("listen key keepalive failed") from exc

    async def _receive_loop(self, ws: Any, stop_event: asyncio.Event) -> None:
        async for message in ws:
            if stop_event.is_set():
                return
            if message.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                try:
                    payload = json.loads(message.data)
                except (ValueError, TypeError) as exc:
                    raise _UserStreamStateError("invalid user stream JSON") from exc
                if not isinstance(payload, dict):
                    raise _UserStreamStateError("invalid user stream payload type")
                self._check_server_error(payload)
                # USDⓈ-M private combined streams can wrap events as {stream, data}.
                if isinstance(payload, dict) and not payload.get("e") and isinstance(payload.get("data"), dict):
                    payload = payload["data"]
                    self._check_server_error(payload)
                event_type = str(payload.get("e") or payload.get("eventType") or "")
                if event_type.lower() in {"listenkeyexpired", "eventstreamterminated"}:
                    raise _UserStreamStateError("listen key expired or user stream terminated")
                if not event_type:
                    # Successful control replies are not account events.
                    if "id" in payload and "result" in payload:
                        continue
                    raise _UserStreamStateError("user stream response missing event type")
                await _call_handler(self.on_message, payload)
                self.received_events += 1
                if self.received_events == 1:
                    self.logger.info("user stream first event handled type=%s", event_type)
            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                reason = f"user WebSocket closed close_code={getattr(ws, 'close_code', None)}"
                socket_error = ws.exception() if message.type == aiohttp.WSMsgType.ERROR else None
                raise _UserStreamStateError(reason) from socket_error
        if not stop_event.is_set():
            raise _UserStreamStateError(f"user WebSocket ended close_code={getattr(ws, 'close_code', None)}")
