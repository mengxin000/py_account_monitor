from __future__ import annotations
import asyncio
import aiohttp


class HyperliquidRestClient:
    def __init__(self, base_url="https://api.hyperliquid.xyz", proxy=None):
        self.base_url = base_url.rstrip("/")
        self.proxy = proxy
        self.session = None

    async def info(self, kind, **params):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20), trust_env=True)
        for attempt in range(3):
            try:
                async with self.session.post(self.base_url + "/info", json={"type": kind, **params}, proxy=self.proxy) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if isinstance(data, dict) and (data.get("error") or data.get("status") == "err"):
                        raise ValueError(f"Hyperliquid info {kind} rejected")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 2:
                    raise
                await asyncio.sleep(1 + attempt)

    async def history(self, kind, user, start, end):
        """Inclusive time pagination; callers deduplicate timestamp boundaries."""
        cursor = start
        while cursor <= end:
            params = dict(user=user, startTime=cursor, endTime=end)
            if kind == "userFillsByTime":
                params["aggregateByTime"] = False
            rows = await self.info(kind, **params)
            if not isinstance(rows, list):
                raise ValueError(f"invalid Hyperliquid {kind} response")
            if not rows:
                return
            yield rows
            last = max(int(row["time"]) for row in rows)
            # Info's general time-range ceiling is 500; fills can return 2000.
            if len(rows) < 500:
                return
            if last <= cursor:
                raise ValueError("Hyperliquid history page saturated at one timestamp; reconciliation incomplete")
            cursor = last

    async def close(self):
        if self.session is not None:
            await self.session.close()
