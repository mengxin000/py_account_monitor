import asyncio
import gzip
import json
import tempfile
import unittest
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from collectors.market_collector import MarketCollector
from collectors.account_monitor import BinanceAccountMonitor, JsonlEventStore
from storage.market_store import MarketStore
from replay.batch_replay import replay_day
from reports.report_data import load_report_data


def order(status="NEW", execution="NEW", quantity="0", identity=1):
    return {"e": "ORDER_TRADE_UPDATE", "fs": "UM", "o": {
        "s": "AAVEUSDT", "i": identity, "c": "order-a", "X": status,
        "x": execution, "l": quantity, "t": identity, "S": "BUY",
        "z": quantity, "L": "100", "T": 100000,
    }}


class MarketTests(unittest.TestCase):
    def test_critical_records_are_separate_and_old_quotes_are_evicted(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = MarketCollector(Path(tmp), {"market_queue_max": 1})
            key = ("spot", "SUIUSDT")
            collector.emit(key, {"sequence": 1})
            collector.emit(key, {"sequence": 2})
            collector.emit(key, {"kind": "fill_window"}, True)
            self.assertEqual(collector.market_queue.qsize(), 1)
            self.assertEqual(collector.market_queue._queue[0][2]["sequence"], 2)
            self.assertEqual(collector.critical_queue.qsize(), 1)
            self.assertEqual(collector.dropped_quotes, 1)

    def test_shared_orders_window_union_capacity_and_cancel(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = MarketCollector(Path(tmp), {"buffer_max_records": 3})
            key = "futures", "AAVEUSDT"
            with patch("collectors.market_collector.time.time", return_value=100):
                collector.order_event("zdl", order())
                collector.order_event("mfx", order())
            state = collector.states[key]
            self.assertEqual(len(state.orders), 2)
            for index in range(5):
                collector.quote_event(key, {"u": index, "b": "100", "B": "1", "a": "101", "A": "2"}, 101+index)
            self.assertEqual(len(state.quotes), 3)
            with patch("collectors.market_collector.time.time", return_value=106):
                collector.order_event("zdl", order("FILLED", "TRADE", "1"))
            with patch("collectors.market_collector.time.time", return_value=107):
                collector.order_event("mfx", order("PARTIALLY_FILLED", "TRADE", "1"))
            quotes = [entry[2] for entry in list(collector.queue._queue) if not entry[3]]
            self.assertEqual(len(quotes), 3)  # overlapping lookbacks written once
            self.assertEqual(state.until, 117)
            self.assertEqual(len(state.orders), 1)
            with patch("collectors.market_collector.time.time", return_value=108):
                collector.order_event("mfx", order("CANCELED", "CANCELED"))
            self.assertFalse(state.orders)
            self.assertEqual(state.idle_since, 108)
            self.assertEqual(state.until, 117)  # cancel is not a new fill
            collector.trim(state, 200)
            self.assertFalse(state.quotes)

    def test_hour_compression_late_append_and_retention(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "raw"
            store = MarketStore(root)
            now = datetime(2026, 9, 10, 12)
            for days in (0, 1, 2, 3):
                moment = now - timedelta(days=days, hours=2)
                store.write("spot", "SUIUSDC", {"receivedTimeMs": int(moment.timestamp()*1000), "value": days})
            store.maintain(now)
            self.assertFalse((root / "20260907").exists())
            self.assertTrue((root / "20260908").exists())
            path = root / "20260910/spot/SUIUSDC/10.jsonl.gz"
            record = {"receivedTimeMs": int((now-timedelta(hours=2)).timestamp()*1000), "value": 4}
            store.write("spot", "SUIUSDC", record)
            with gzip.open(path, "rt") as stream:
                self.assertEqual(len(stream.readlines()), 2)

    def test_all_callbacks_excluded_from_account_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            day = Path(tmp)
            callback = json.dumps({"data": order("FILLED", "TRADE", "1")}) + "\n"
            (day / "all_callbacks.jsonl").write_text(callback)
            (day / "trade_callbacks.jsonl").write_text(callback)
            self.assertEqual(replay_day(day)["events"], 1)
            self.assertEqual(load_report_data(day, "zdl").fill_counts["AAVEUSDT"], 1)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_symbol_changes_use_subscribe_without_reconnecting(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = MarketCollector(Path(tmp))
            collector.ensure(("spot", "SUIUSDT"), 100)

            class FakeMessage:
                type = aiohttp.WSMsgType.TEXT
                def __init__(self, payload):
                    self.payload = payload
                def json(self):
                    return self.payload

            class FakeWebSocket:
                def __init__(self):
                    self.sent = []
                    self.receives = 0
                async def __aenter__(self):
                    return self
                async def __aexit__(self, *args):
                    return False
                async def send_json(self, payload):
                    self.sent.append(payload)
                async def receive(self, timeout=None):
                    self.receives += 1
                    if self.receives == 1:
                        collector.ensure(("spot", "AAVEUSDT"), 100)
                        return FakeMessage({"result": None, "id": 1})
                    collector.stopping = True
                    return FakeMessage({"result": None, "id": 2})

            websocket = FakeWebSocket()
            session = SimpleNamespace(ws_connect=lambda *args, **kwargs: websocket)
            collector.session = session
            await collector.stream("spot")
            self.assertEqual(len(websocket.sent), 2)
            self.assertEqual(websocket.sent[0]["params"], ["suiusdt@bookTicker"])
            self.assertEqual(websocket.sent[1]["params"], ["aaveusdt@bookTicker"])

    async def test_all_statuses_saved_but_only_fills_enter_trade_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            monitor = BinanceAccountMonitor.__new__(BinanceAccountMonitor)
            monitor.config = SimpleNamespace(account_id="zdl")
            monitor.store = JsonlEventStore(Path(tmp), "zdl")
            monitor.market_collector = None
            for event in [order(), order("CANCELED", "CANCELED"), order("FILLED", "TRADE", "1")]:
                await monitor._on_user_event(event)
            all_path = next(Path(tmp).glob("*/all_callbacks.jsonl"))
            self.assertEqual(len(all_path.read_text().splitlines()), 3)
            self.assertEqual(len(all_path.with_name("trade_callbacks.jsonl").read_text().splitlines()), 1)

    async def test_snapshot_uses_margin_symbol_and_unwraps_rest_arrays(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = MarketCollector(Path(tmp))
            calls = []
            async def get(path, params=None, signed=False):
                calls.append((path, params))
                return {"data": [{"symbol": "SUIUSDC" if "margin" in path else "SUIUSDT", "orderId": 7}]}
            monitor = SimpleNamespace(config=SimpleNamespace(account_id="zdl"), rest=SimpleNamespace(get=get))
            async def stop_sleep(seconds):
                collector.stopping = True
            with patch("collectors.market_collector.asyncio.sleep", side_effect=stop_sleep):
                await collector.reconcile([monitor])
            self.assertIsNone(calls[0][1])
            self.assertIsNone(calls[1][1])
            self.assertEqual(collector.states[("futures", "SUIUSDT")].orders, {("zdl", "7")})

    async def test_idle_stream_released_and_shutdown_drains_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = MarketCollector(Path(tmp), {"unsubscribe_idle_seconds": 1})
            state = collector.ensure(("spot", "SUIUSDC"), 1)
            async def fake_stream(key):
                await asyncio.Future()
            collector.stream = fake_stream
            task = asyncio.create_task(collector.run([]))
            await asyncio.sleep(0.3)
            self.assertFalse(collector.states)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertTrue(collector.queue.empty())
