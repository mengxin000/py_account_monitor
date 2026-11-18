"""Small read-only Bybit V5 REST client."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from urllib.parse import urlencode, urlsplit

import aiohttp
from yarl import URL


@dataclass(frozen=True)
class BybitCredentials:
    api_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self):
        if not self.api_key.strip() or not self.secret_key.strip():
            raise ValueError("Bybit api_key and secret_key are required")

    def sign(self, timestamp_ms: int, recv_window: int, payload: str = "") -> str:
        message = f"{timestamp_ms}{self.api_key}{recv_window}{payload}"
        return hmac.new(self.secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()


class BybitError(RuntimeError):
    def __init__(self, path: str, code: object, status: int | None = None):
        self.code, self.status = str(code), status
        super().__init__(f"Bybit request failed path={path} code={code} http={status}")


class BybitRestClient:
    def __init__(self, credentials: BybitCredentials, *, base_url="https://api.bybit.com", proxy=None, timeout=15):
        parsed = urlsplit(base_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
            raise ValueError("Bybit REST base_url must be an HTTPS origin")
        self.credentials, self.base_url = credentials, base_url.rstrip("/")
        self.proxy, self.timeout = proxy, timeout
        self.session = None
        self.recv_window = 5000

    async def get(self, path: str, params: dict | None = None):
        if not path.startswith("/v5/") or "?" in path or "#" in path:
            raise ValueError("Bybit API path must be a V5 path")
        params = {k: str(v) for k, v in (params or {}).items() if v is not None}
        query = urlencode(sorted(params.items()))
        timestamp = int(time.time() * 1000)
        signature = self.credentials.sign(timestamp, self.recv_window, query)
        headers = {"X-BAPI-API-KEY": self.credentials.api_key,
                   "X-BAPI-TIMESTAMP": str(timestamp),
                   "X-BAPI-RECV-WINDOW": str(self.recv_window),
                   "X-BAPI-SIGN": signature,
                   "X-BAPI-SIGN-TYPE": "2"}
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout), trust_env=True)
        # Keep aiohttp from re-quoting the exact bytes used in the signature.
        url = URL(self.base_url + path + ("?" + query if query else ""), encoded=True)
        async with self.session.get(url, headers=headers, proxy=self.proxy) as response:
            try:
                body = await response.json(content_type=None)
            except (aiohttp.ContentTypeError, json.JSONDecodeError):
                raise BybitError(path, "invalid_json", response.status)
            if response.status != 200 or not isinstance(body, dict) or body.get("retCode") != 0:
                code = body.get("retCode", response.status) if isinstance(body, dict) else response.status
                raise BybitError(path, code, response.status)
            return body.get("result", {})

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
