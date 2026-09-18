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


from .base import BinanceCredentials, BinanceConnectionError, UserStreamConfig, _join_url

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
        proxy: str | None = None,
    ) -> None:
        self.credentials = credentials
        self.proxy = proxy
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
            self._session = aiohttp.ClientSession(timeout=self._timeout, trust_env=True)
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
                async with session.request(method.upper(), url, params=values, headers=headers, proxy=self.proxy) as response:
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
                async with session.get(url, proxy=self.proxy) as response:
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
