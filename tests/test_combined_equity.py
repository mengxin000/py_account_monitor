import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

from collectors.account_monitor import AccountMonitorConfig, BinanceAccountMonitor
from collectors.binance.equity import spot_equity
from core.connection import BinanceCredentials
from service.dashboard import number
from collectors.account_monitor import trading_day
from connections.diagnostics import connection_error


class EquityTests(unittest.IsolatedAsyncioTestCase):
    def test_boundary_and_sanitized_diagnostic(self):
        self.assertEqual(trading_day(datetime(2026,9,18,9,29,59)), "20260917")
        self.assertEqual(trading_day(datetime(2026,9,18,9,30)), "20260918")
        error = connection_error(OSError(10061, "secret?signature=SECRET"), "wss://example.com:9443/ws/SECRET")
        self.assertIn("10061", error)
        self.assertIn("9443", error)
        self.assertNotIn("SECRET", error)

    def test_locked_and_cross_quote(self):
        value, _ = spot_equity({"accountType":"SPOT", "balances":[{"asset":"USDC","free":"10","locked":"2"}]}, [{"symbol":"USDCUSDT","price":"0.99"}])
        self.assertAlmostEqual(value, 11.88)
        self.assertEqual(number(99999999.123, True), "99,999,999")

    def test_missing_price_is_not_zero(self):
        with self.assertRaises(ValueError):
            spot_equity({"accountType":"SPOT","balances":[{"asset":"ABC","free":"1","locked":"0"}]}, [])

    async def test_baseline_failure_restart_and_rollover(self):
        with tempfile.TemporaryDirectory() as temp:
            config = AccountMonitorConfig("test", BinanceCredentials("key","secret"), Path(temp), include_positions=False, include_balance=False)
            monitor = BinanceAccountMonitor(config)
            monitor.rest.get = AsyncMock(return_value={"actualEquity":"100"})
            async def spot(path, **kwargs):
                if path == "/api/v3/account":
                    return {"accountType":"SPOT","balances":[{"asset":"USDT","free":"20","locked":"5"}]}
                return {"data":[]}
            monitor.spot_stream.rest.get = AsyncMock(side_effect=spot)
            await monitor._collect_equity()
            self.assertEqual(monitor._baseline_equity, 125)
            restarted = BinanceAccountMonitor(config)
            self.assertEqual(restarted._baseline_equity,125)
            monitor.rest.get.return_value = {"actualEquity":"110"}
            await monitor._collect_equity()
            self.assertEqual(monitor.status()["actual_profit"],10)
            monitor.spot_stream.rest.get.side_effect = TimeoutError()
            await monitor._collect_equity()
            self.assertIsNone(monitor._total_equity)
            self.assertEqual(monitor._baseline_equity,125)
            row = json.loads(monitor.store._path("equity.json").read_text())
            self.assertIsNone(row["latestEquity"])
            monitor.spot_stream.rest.get.side_effect = spot
            monitor._baseline_day = "19990101"
            await monitor._collect_equity()
            self.assertEqual(monitor._baseline_equity,135)

    async def test_legacy_pm_baseline_not_mixed(self):
        with tempfile.TemporaryDirectory() as temp:
            config = AccountMonitorConfig("test",BinanceCredentials("key","secret"),Path(temp))
            monitor = BinanceAccountMonitor(config)
            old = {"baselineEquity":10,"baselineTime":datetime.now().isoformat()}
            monitor.store._path("equity.json").write_text(json.dumps(old))
            restarted = BinanceAccountMonitor(config)
            self.assertIsNone(restarted._baseline_equity)
            self.assertEqual(restarted._legacy_baseline,old)
