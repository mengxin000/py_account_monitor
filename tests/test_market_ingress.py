import json
import multiprocessing
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from collectors.market_ingress import stamp, worker
from collectors.market_collector import MarketCollector
from storage.market_store import MarketStore


class IngressTests(unittest.TestCase):
    def test_stamp_preserves_payload_and_microseconds(self):
        raw = '{ "s": "SUIUSDT", "b":"1", "B":"2", "a":"3", "A":"4" }'
        with patch("collectors.market_ingress.time.time_ns", return_value=1789717935768123456), patch("collectors.market_ingress.time.perf_counter_ns", return_value=123):
            row = stamp(raw, "connection", 7)
        self.assertEqual(row["receivedTimeUs"],1789717935768123)
        self.assertEqual(row["rawPayload"],raw)
        self.assertEqual(row["receivedMonoNs"],123)

    def test_delayed_processing_keeps_capture_hour(self):
        with tempfile.TemporaryDirectory() as temp:
            collector = MarketCollector(Path(temp))
            key = "spot", "SUIUSDT"
            state = collector.ensure(key, 0)
            capture = datetime(2026,9,18,15,59,59).timestamp()
            state.since, state.until = capture-30, capture+10
            raw = '{"s":"SUIUSDT","b":"1","B":"2","a":"3","A":"4"}'
            envelope = {"rawPayload":raw,"receivedTimeUs":int(capture*1_000_000)+123456,"receivedMonoNs":1,"connectionId":"a","receiveSequence":1}
            with patch("collectors.market_collector.time.time",return_value=capture+100):
                collector.ingest_raw("spot",envelope)
            item = collector.market_queue.get_nowait()
            self.assertEqual(item[2]["receivedTimeUs"],envelope["receivedTimeUs"])
            collector.store.write_batch([item])
            path = Path(temp)/"20260918/spot/SUIUSDT/15.jsonl"
            saved = json.loads(path.read_text())
            self.assertEqual(saved["rawPayload"],raw)
            self.assertFalse(path.with_name("16.jsonl").exists())

    def test_spawn_shutdown_without_network(self):
        ctx = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as temp:
            commands, output = ctx.Queue(2),ctx.Queue(2)
            stop, drops = ctx.Event(),ctx.Value("q",0)
            stop.set()
            process = ctx.Process(target=worker,args=("spot",Path(temp),None,commands,output,stop,drops))
            process.start()
            process.join(10)
            try:
                self.assertFalse(process.is_alive())
                self.assertEqual(process.exitcode,0)
            finally:
                if process.is_alive(): process.terminate(); process.join()
                process.close()
                commands.close(); output.close()
