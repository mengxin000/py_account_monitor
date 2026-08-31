"""Binance REST/WebSocket connections for monitoring accounts.

The account email (for example ``binance-sub-02@example.com``) is metadata
used to identify a sub-account.  It is *not* sent as authentication.  The API
key must be created on the account whose data is being monitored (or be a
properly permitted parent/sub-account key).

The defaults target Binance Portfolio Margin (the unified account).  Spot and
USD-M Futures user streams can be enabled by changing ``UserStreamConfig``.
No trading endpoint is exposed by this module.
"""

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


class BinanceConnectionError(RuntimeError):
    """A transport, authentication, or Binance API error."""


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    secret_key: str
    subaccount_email: str | None = None
    # The API key should have USER_DATA/USER_STREAM only for this monitor.
    label: str | None = None


@dataclass(slots=True)
class UserStreamConfig:
    """Endpoints for a Binance user-data stream.

    Portfolio Margin defaults are ``POST /papi/v1/listenKey`` and
    ``wss://fstream.binance.com/pm``.  The endpoint is configurable because
    Binance product routes differ for spot and USD-M futures.
    """

    rest_base_url: str = "https://papi.binance.com"
    websocket_base_url: str = "wss://fstream.binance.com/pm"
    listen_key_path: str = "/papi/v1/listenKey"
    keepalive_seconds: float = 30 * 60

    @classmethod
    def spot(cls) -> "UserStreamConfig":
        return cls(
            rest_base_url="https://api.binance.com",
            websocket_base_url="wss://stream.binance.com:9443",
            listen_key_path="/api/v3/userDataStream",
        )

    @classmethod
    def usd_m_futures(cls) -> "UserStreamConfig":
        return cls(
            rest_base_url="https://fapi.binance.com",
            websocket_base_url="wss://fstream.binance.com/private",
            listen_key_path="/fapi/v1/listenKey",
        )


def _join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


async def _call_handler(handler: MessageHandler | None, payload: Json) -> None:
    if handler is None:
        return
    result = handler(payload)
    if inspect.isawaitable(result):
        await result


class BinanceRestClient:
    """Async REST client with Binance HMAC-SHA256 signed USER_DATA requests."""

    def __init__(
        self,
        credentials: BinanceCredentials,
        *,
        base_url: str = "https://papi.binance.com",
        time_base_url: str = "https://api.binance.com",
        timeout_seconds: float = 10.0,
        recv_window_ms: int = 5_000,
        session: aiohttp.ClientSession | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.credentials = credentials
        self.base_url = base_url.rstrip("/")
        self.time_base_url = time_base_url.rstrip("/")
        self.recv_window_ms = recv_window_ms
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session = session
        self._owns_session = session is None
        self._logger = logger or logging.getLogger(__name__)
        self._time_offset_ms = 0
        self._time_synced = False
        self._time_lock = asyncio.Lock()

    async def __aenter__(self) -> "BinanceRestClient":
        await self._get_session()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._owns_session = True
        return self._session

    @staticmethod
    def _query(params: Mapping[str, Any]) -> str:
        # Binance signs the exact URL-encoded parameter string.
        return urlencode([(k, v) for k, v in params.items() if v is not None], doseq=True)

    def _signed_params(self, params: Mapping[str, Any] | None) -> MutableMapping[str, Any]:
        values: MutableMapping[str, Any] = dict(params or {})
        values.setdefault("recvWindow", self.recv_window_ms)
        values["timestamp"] = int(time.time() * 1000) + self._time_offset_ms
        payload = self._query(values)
        values["signature"] = hmac.new(
            self.credentials.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return values

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        signed: bool = False,
        api_key: bool = False,
        retries: int = 2,
    ) -> Json:
        headers = {"Accept": "application/json"}
        if api_key or signed:
            headers["X-MBX-APIKEY"] = self.credentials.api_key
        url = _join_url(self.base_url, path)
        last_error: Exception | None = None
        time_resynced = False
        for attempt in range(retries + 1):
            try:
                if signed and not self._time_synced:
                    await self.sync_time()
                values = self._signed_params(params) if signed else dict(params or {})
                session = await self._get_session()
                async with session.request(method.upper(), url, params=values, headers=headers) as response:
                    raw = await response.text()
                    try:
                        payload = json.loads(raw) if raw else {}
                    except json.JSONDecodeError as exc:
                        raise BinanceConnectionError(
                            f"Binance returned non-JSON ({response.status}): {raw[:200]}"
                        ) from exc
                    if response.status >= 400:
                        # -1021 means the local clock is outside recvWindow.
                        # Re-sync once and retry with a newly signed payload.
                        if (
                            signed
                            and not time_resynced
                            and isinstance(payload, dict)
                            and payload.get("code") == -1021
                        ):
                            time_resynced = True
                            self._time_synced = False
                            await self.sync_time()
                            continue
                        raise BinanceConnectionError(
                            f"Binance {response.status} {method.upper()} {path}: {payload}"
                        )
                    if isinstance(payload, dict):
                        return payload
                    return {"data": payload}
            except (aiohttp.ClientError, asyncio.TimeoutError, BinanceConnectionError) as exc:
                last_error = exc
                # Do not retry API validation/authentication failures.
                if isinstance(exc, BinanceConnectionError) and "Binance 4" in str(exc):
                    break
                if attempt < retries:
                    await asyncio.sleep(min(2.0, 0.25 * (2**attempt)))
        raise BinanceConnectionError(f"REST request failed: {method.upper()} {path}") from last_error

    async def sync_time(self) -> int:
        """Synchronize timestamp signing with Binance server time.

        Binance rejects signed requests when the timestamp is ahead by more
        than the allowed window.  The offset is kept in memory and never
        changes the Windows system clock.
        """
        async with self._time_lock:
            if self._time_synced:
                return self._time_offset_ms
            session = await self._get_session()
            started = int(time.time() * 1000)
            url = _join_url(self.time_base_url, "/api/v3/time")
            try:
                async with session.get(url) as response:
                    payload = await response.json()
                received = int(time.time() * 1000)
                server_time = int(payload["serverTime"])
                # Estimate the midpoint of the request to reduce network-latency bias.
                midpoint = (started + received) // 2
                self._time_offset_ms = server_time - midpoint
                self._time_synced = True
                self._logger.info("Binance time offset synchronized: %d ms", self._time_offset_ms)
                return self._time_offset_ms
            except Exception as exc:
                self._logger.warning("Unable to synchronize Binance server time: %s", exc)
                # Keep running with local time; the API response will trigger
                # one more sync attempt if it returns -1021.
                # Do not mark the clock as synchronized on failure: transient
                # DNS/proxy/endpoint errors must be retried by the next
                # signed request instead of permanently poisoning this client.
                self._time_synced = False
                return self._time_offset_ms

    async def get(self, path: str, *, params: Mapping[str, Any] | None = None, signed: bool = False) -> Json:
        return await self.request("GET", path, params=params, signed=signed, api_key=signed)

    async def create_listen_key(self, config: UserStreamConfig) -> str:
        result = await self.request("POST", config.listen_key_path, api_key=True)
        listen_key = result.get("listenKey")
        if not isinstance(listen_key, str) or not listen_key:
            raise BinanceConnectionError(f"Binance did not return listenKey: {result}")
        return listen_key

    async def keepalive_listen_key(self, config: UserStreamConfig, listen_key: str) -> Json:
        return await self.request(
            "PUT", config.listen_key_path, params={"listenKey": listen_key}, api_key=True
        )

    async def close_listen_key(self, config: UserStreamConfig, listen_key: str) -> Json:
        return await self.request(
            "DELETE", config.listen_key_path, params={"listenKey": listen_key}, api_key=True
        )

    async def close(self) -> None:
        if self._session is not None and self._owns_session and not self._session.closed:
            await self._session.close()


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
    ) -> None:
        self.config = config or UserStreamConfig()
        self.rest = BinanceRestClient(credentials, base_url=self.config.rest_base_url, logger=logger)
        self.on_message = on_message
        self.on_error = on_error
        self.reconnect_seconds = reconnect_seconds
        self.logger = logger or logging.getLogger(__name__)

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
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        self.logger.info("user WebSocket connected")
                        receiver = asyncio.create_task(self._receive_loop(ws, stop_event))
                        try:
                            # A keepalive failure must be treated exactly like
                            # a socket failure. Previously this background task
                            # could die silently while the stale socket stayed
                            # open and stopped delivering account events.
                            done, _ = await asyncio.wait(
                                {receiver, keepalive},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            for task in done:
                                await task  # propagate keepalive/receiver error
                            if not stop_event.is_set():
                                raise BinanceConnectionError("user stream task stopped unexpectedly")
                        finally:
                            receiver.cancel()
                            await asyncio.gather(receiver, return_exceptions=True)
            except asyncio.CancelledError:
                # The daemon cancels this task during Ctrl+C/restart.  Close
                # the REST client so aiohttp does not report unclosed sessions.
                await self.rest.close()
                raise
            except Exception as exc:
                self.logger.warning("user stream disconnected: %s", exc)
                await _call_handler(self.on_error, {"error": str(exc)})
                if not stop_event.is_set():
                    self.logger.info("user WebSocket reconnecting in %ss", self.reconnect_seconds)
                    await asyncio.sleep(self.reconnect_seconds)
            finally:
                if keepalive:
                    keepalive.cancel()
                    await asyncio.gather(keepalive, return_exceptions=True)
                if listen_key:
                    try:
                        await self.rest.close_listen_key(self.config, listen_key)
                    except Exception:
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
