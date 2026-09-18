"""Ordinary Spot private stream (WebSocket API signature subscription)."""
import asyncio
import hashlib
import hmac
import json
import logging
import time

import aiohttp

from .base import BinanceConnectionError, _call_handler
from .rest import BinanceRestClient
from ..diagnostics import connection_error


class BinanceSpotStream:
    def __init__(self, credentials, *, on_message=None, on_error=None, logger=None,
                 proxy=None, websocket_url="wss://ws-api.binance.com:443/ws-api/v3"):
        self.credentials = credentials
        self.on_message = on_message
        self.on_error = on_error
        self.logger = logger or logging.getLogger(__name__)
        self.proxy = proxy
        self.websocket_url = websocket_url
        self.rest = BinanceRestClient(credentials, base_url="https://api.binance.com", proxy=proxy)
        self.connected = False

    def subscription_request(self, timestamp):
        params = {"apiKey": self.credentials.api_key, "recvWindow": 5000, "timestamp": timestamp}
        payload = "&".join(f"{key}={params[key]}" for key in sorted(params))
        params["signature"] = hmac.new(self.credentials.secret_key.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return {"id": "subscribe", "method": "userDataStream.subscribe.signature", "params": params}

    async def _receive(self, ws):
        async for message in ws:
            if message.type != aiohttp.WSMsgType.TEXT:
                if message.type == aiohttp.WSMsgType.ERROR:
                    raise BinanceConnectionError("Spot WebSocket error")
                continue
            payload = json.loads(message.data)
            event = payload.get("event")
            if isinstance(event, dict):
                if event.get("e") == "eventStreamTerminated":
                    raise BinanceConnectionError("Spot subscription terminated")
                await _call_handler(self.on_message, event)
        raise BinanceConnectionError("Spot WebSocket ended")

    async def run(self, stop_event=None):
        stop_event = stop_event or asyncio.Event()
        delay = 3
        try:
            while not stop_event.is_set():
                try:
                    self.rest._time_synced = False
                    offset = await self.rest.sync_time()
                    if not self.rest._time_synced:
                        raise BinanceConnectionError("Spot time synchronization failed")
                    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
                    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                        async with session.ws_connect(self.websocket_url, heartbeat=20, proxy=self.proxy) as ws:
                            await ws.send_json(self.subscription_request(int(time.time()*1000) + offset))
                            reply = await asyncio.wait_for(ws.receive_json(), 15)
                            if reply.get("status") != 200 or "subscriptionId" not in reply.get("result", {}):
                                error = reply.get("error", {})
                                raise BinanceConnectionError(f"Spot subscription rejected status={reply.get('status')} code={error.get('code')}")
                            self.connected = True
                            delay = 3
                            self.logger.info("Spot private subscription connected")
                            receiver = asyncio.create_task(self._receive(ws))
                            stopper = asyncio.create_task(stop_event.wait())
                            try:
                                done, _ = await asyncio.wait({receiver, stopper}, return_when=asyncio.FIRST_COMPLETED)
                                if receiver in done:
                                    await receiver
                            finally:
                                receiver.cancel()
                                stopper.cancel()
                                await asyncio.gather(receiver, stopper, return_exceptions=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Transport exceptions can contain signed URLs: never log them verbatim.
                    reason = str(exc) if isinstance(exc, BinanceConnectionError) else connection_error(exc, self.websocket_url)
                    self.logger.warning("Spot private stream disconnected: %s; retry in %ss", reason, delay)
                    await _call_handler(self.on_error, {"error": reason, "source": "spot_stream"})
                    try:
                        await asyncio.wait_for(stop_event.wait(), delay)
                    except asyncio.TimeoutError:
                        pass
                    delay = min(60, delay * 2)
                finally:
                    self.connected = False
        finally:
            await self.rest.close()
