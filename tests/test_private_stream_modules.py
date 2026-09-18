import asyncio
import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from core.connection import BinanceCredentials
from connections.binance.spot_stream import BinanceSpotStream
from collectors.binance.normalize import source_metadata
from collectors.account_monitor import AccountMonitorConfig, BinanceAccountMonitor, load_config
from core.legacy_matching import LegacyMatcher


class PrivateStreamsTest(unittest.IsolatedAsyncioTestCase):
    def test_same_numeric_id_different_scope_does_not_merge_fees(self):
        matcher = LegacyMatcher()
        common = {"e": "executionReport", "s": "AAVEUSDT", "i": 1, "S": "BUY", "X": "PARTIALLY_FILLED", "z": "0.1", "n": "0.01", "N": "USDT", "L": "100"}
        matcher.ingest(matcher.from_order_event({**common, "accountScope": "spot"}))
        matcher.ingest(matcher.from_order_event({**common, "accountScope": "pm_margin"}))
        self.assertEqual(len(matcher.no_save_orders), 2)

    def test_signing(self):
        stream = BinanceSpotStream(BinanceCredentials("key", "secret"))
        request = stream.subscription_request(123)
        expected = hmac.new(b"secret", b"apiKey=key&recvWindow=5000&timestamp=123", hashlib.sha256).hexdigest()
        self.assertEqual(request["params"]["signature"], expected)
        self.assertEqual(request["method"], "userDataStream.subscribe.signature")

    def test_source_not_inferred_from_event_name(self):
        event = {"e": "executionReport"}
        self.assertEqual(source_metadata(event, "pm_stream")["accountScope"], "pm_margin")
        self.assertEqual(source_metadata(event, "spot_stream")["accountScope"], "spot")
        self.assertEqual(source_metadata(event, "unknown")["accountScope"], "unknown")
        self.assertEqual(source_metadata({"fs": "CM"}, "pm_stream")["accountScope"], "cm")

    async def test_both_sources_persist_without_mutating_raw(self):
        with tempfile.TemporaryDirectory() as temp:
            monitor = BinanceAccountMonitor(AccountMonitorConfig("test", BinanceCredentials("key", "secret"), Path(temp)))
            event = {"e": "executionReport", "s": "AAVEUSDT", "X": "FILLED", "z": "1"}
            await monitor._on_spot_event(event)
            await monitor._on_user_event(event)
            rows = [json.loads(line) for line in monitor.store._path("trade_callbacks.jsonl").read_text().splitlines()]
            self.assertEqual([row["accountScope"] for row in rows], ["spot", "pm_margin"])
            self.assertEqual(rows[0]["data"], event)
            self.assertNotIn("source", event)
            self.assertEqual(len(monitor.store._path("all_callbacks.jsonl").read_text().splitlines()), 2)

    def test_optional_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/"account.json"
            path.write_text(json.dumps({"api_key": "pm", "secret_key": "pmsecret", "spot": {"enabled": True, "api_key": "spot", "secret_key": "spotsecret"}}))
            config = load_config(path)
            self.assertEqual(config.spot_credentials.api_key, "spot")
            self.assertEqual(config.credentials.api_key, "pm")

    async def test_stop_before_start_makes_no_network_request(self):
        stream = BinanceSpotStream(BinanceCredentials("key", "secret"))
        stop = asyncio.Event()
        stop.set()
        with patch.object(stream.rest, "sync_time", new_callable=AsyncMock) as sync:
            await stream.run(stop)
            sync.assert_not_called()

    async def test_spot_subscription_ack_stop_and_cleanup(self):
        stream = BinanceSpotStream(BinanceCredentials("key", "secret"))
        stop = asyncio.Event()
        class Socket:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def send_json(self, request):
                self.request = request
            async def receive_json(self):
                stop.set()
                return {"status": 200, "result": {"subscriptionId": 0}}
        socket = Socket()
        class Session:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def ws_connect(self, *args, **kwargs): return socket
        async def sync():
            stream.rest._time_synced = True
            return 0
        async def receive(ws):
            await asyncio.Event().wait()
        with patch.object(stream.rest, "sync_time", side_effect=sync), \
             patch.object(stream.rest, "close", new_callable=AsyncMock) as close, \
             patch.object(stream, "_receive", side_effect=receive), \
             patch("connections.binance.spot_stream.aiohttp.ClientSession", return_value=Session()):
            await asyncio.wait_for(stream.run(stop), 1)
            close.assert_awaited_once()
        self.assertFalse(stream.connected)
        self.assertEqual(socket.request["method"], "userDataStream.subscribe.signature")

    async def test_subscription_rejection_reports_error_not_connected(self):
        stream = BinanceSpotStream(BinanceCredentials("key", "secret"))
        stop = asyncio.Event()
        errors = []
        async def error(event):
            errors.append(event)
            stop.set()
        stream.on_error = error
        async def sync():
            stream.rest._time_synced = True
            return 0
        class Socket:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def send_json(self, request): pass
            async def receive_json(self): return {"status": 401, "error": {"code": -2015}}
        class Session:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def ws_connect(self, *args, **kwargs): return Socket()
        with patch.object(stream.rest, "sync_time", side_effect=sync), \
             patch("connections.binance.spot_stream.aiohttp.ClientSession", return_value=Session()):
            await asyncio.wait_for(stream.run(stop), 1)
        self.assertIn("-2015", errors[0]["error"])
        self.assertFalse(stream.connected)
