"""OKX V5 read-only client. No order, transfer or withdrawal methods."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit

import aiohttp
from yarl import URL


@dataclass(frozen=True)
class OKXCredentials:
    api_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    passphrase: str = field(repr=False)

    def __post_init__(self):
        if not all(isinstance(v, str) and v.strip() for v in
                   (self.api_key, self.secret_key, self.passphrase)):
            raise ValueError("OKX requires api_key, secret_key and passphrase")

    def sign(self, timestamp, method, request_path, body=""):
        message = f"{timestamp}{method.upper()}{request_path}{body}"
        return base64.b64encode(hmac.new(self.secret_key.encode(), message.encode(),
                                       hashlib.sha256).digest()).decode()

    def login(self, timestamp):
        return {"op": "login", "args": [{"apiKey": self.api_key,
                "passphrase": self.passphrase, "timestamp": timestamp,
                "sign": self.sign(timestamp, "GET", "/users/self/verify")}]}


class OKXError(RuntimeError):
    """Safe diagnostics: never include signed headers or arbitrary response text."""
    def __init__(self, path, code, status=None):
        self.code, self.status = str(code), status
        super().__init__(f"OKX request failed path={path} code={code} http={status}")


class OKXRestClient:
    def __init__(self, credentials, *, base_url="https://www.okx.com", proxy=None,
                 simulated=False, timeout=15):
        parsed = urlsplit(base_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
            raise ValueError("OKX REST base_url must be an HTTPS origin")
        if parsed.path not in ("", "/"):
            raise ValueError("OKX REST base_url must not contain an API path")
        self.credentials, self.base_url = credentials, base_url.rstrip("/")
        self.proxy, self.simulated, self.timeout = proxy, simulated, timeout
        self.session = None
        self.offset_ms = 0.0
        self.synced_at = 0.0
        self._sync_lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()
        self._next_request = 0.0

    def now_ms(self):
        return time.time() * 1000 + self.offset_ms

    async def _pace(self):
        # Shared by all paths for this account; at most four GETs per second.
        async with self._rate_lock:
            await asyncio.sleep(max(0, self._next_request - time.monotonic()))
            self._next_request = time.monotonic() + .25

    async def sync_time(self, *, force=False):
        async with self._sync_lock:
            if not force and self.synced_at and time.monotonic() - self.synced_at < 300:
                return
            await self._pace()
            start = time.time() * 1000
            rows = await self._request("/api/v5/public/time", {}, private=False, paced=True)
            end = time.time() * 1000
            if len(rows) != 1 or not str(rows[0].get("ts", "")).isdigit():
                raise OKXError("/api/v5/public/time", "invalid_server_time")
            if end - start > 5000:
                raise OKXError("/api/v5/public/time", "time_sample_too_slow")
            self.offset_ms = int(rows[0]["ts"]) - (start + end) / 2
            self.synced_at = time.monotonic()

    async def _request(self, path, params, *, private=True, paced=False):
        if not path.startswith("/api/v5/") or "?" in path or "#" in path:
            raise ValueError("invalid OKX API path")
        if not paced:
            await self._pace()
        query = urlencode({k: str(v) for k, v in params.items() if v is not None})
        request_path = path + ("?" + query if query else "")
        headers = {}
        if self.simulated:
            headers["x-simulated-trading"] = "1"
        if private:
            stamp = datetime.fromtimestamp(self.now_ms()/1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            headers.update({"OK-ACCESS-KEY": self.credentials.api_key,
                            "OK-ACCESS-PASSPHRASE": self.credentials.passphrase,
                            "OK-ACCESS-TIMESTAMP": stamp,
                            "OK-ACCESS-SIGN": self.credentials.sign(stamp, "GET", request_path)})
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout), trust_env=True)
        # Keep the signed query byte-for-byte; do not let the HTTP client re-encode it.
        async with self.session.get(URL(self.base_url + request_path, encoded=True),
                                    headers=headers, proxy=self.proxy, allow_redirects=False) as response:
            if response.status != 200:
                raise OKXError(path, "http_error", response.status)
            payload = await response.json()
            if not isinstance(payload, dict) or str(payload.get("code")) != "0":
                raise OKXError(path, payload.get("code", "invalid_response") if isinstance(payload, dict) else "invalid_response", response.status)
            rows = payload.get("data")
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise OKXError(path, "invalid_data", response.status)
            return rows

    async def get(self, path, params=None, *, private=True):
        for attempt in range(3):
            try:
                if private:
                    await self.sync_time()
                return await self._request(path, params or {}, private=private)
            except OKXError as exc:
                if exc.code == "50102" and private and attempt < 2:
                    await self.sync_time(force=True)
                    continue
                if attempt == 2 or not (exc.code == "50011" or exc.status == 429 or (exc.status or 0) >= 500):
                    raise
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 2:
                    raise OKXError(path, "transport_error") from None
            await asyncio.sleep(2 ** (attempt + 1))

    async def pages(self, path, params=None, *, cursor_key="billId"):
        """OKX after returns older records. Stop rather than silently truncate."""
        values = {**(params or {}), "limit": "100"}
        seen = set()
        for _ in range(10000):
            rows = await self.get(path, values)
            if not rows:
                return
            yield rows
            if len(rows) < 100:
                return
            cursor = str(rows[-1].get(cursor_key, ""))
            if not cursor or cursor in seen:
                raise OKXError(path, "pagination_stalled")
            seen.add(cursor)
            values["after"] = cursor
        raise OKXError(path, "pagination_limit_reached")

    async def close(self):
        if self.session is not None:
            await self.session.close()
