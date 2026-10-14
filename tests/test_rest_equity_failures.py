"""Offline regressions for rejected time responses and unavailable equity."""
import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from collectors.account_monitor import AccountMonitorConfig, BinanceAccountMonitor, trading_day
from connections.binance.base import BinanceCredentials, BinanceConnectionError
from connections.binance.rest import BinanceRestClient


class Response:
    def __init__(self, payload, status=200, error=None):
        self.payload, self.status, self.error = payload, status, error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self):
        if self.error is not None:
            raise self.error
        return self.payload


class TimeSyncTests(unittest.IsolatedAsyncioTestCase):
    def client(self, *responses):
        session = Mock(closed=False)
        session.get = Mock(side_effect=responses)
        return BinanceRestClient(BinanceCredentials("dummy-key", "dummy-secret"), session=session)

    async def test_success_uses_request_midpoint_and_caches(self):
        client = self.client(Response({"serverTime": 1500}))
        with patch("connections.binance.rest.time.time", side_effect=[1.0, 1.2]):
            self.assertEqual(await client.sync_time(), 400)
        self.assertTrue(client._time_synced)
        self.assertEqual(await client.sync_time(), 400)
        client._session.get.assert_called_once()

    async def test_rejected_http_response_has_status_and_numeric_code(self):
        client = self.client(Response({"code": 0, "msg": "SENSITIVE"}, 451))
        client._time_offset_ms = 123
        with self.assertLogs(client._logger, level="WARNING") as logs:
            self.assertEqual(await client.sync_time(), 123)
        text = " ".join(logs.output)
        self.assertIn("http_status=451", text)
        self.assertIn("api_code=0", text)
        self.assertNotIn("SENSITIVE", text)
        self.assertNotIn("dummy-secret", text)
        self.assertFalse(client._time_synced)

    async def test_error_payload_with_200_is_not_accepted(self):
        client = self.client(Response({"code": -1003, "serverTime": 1500}))
        with self.assertLogs(client._logger, level="WARNING") as logs:
            await client.sync_time()
        self.assertIn("api_code=-1003", " ".join(logs.output))
        self.assertFalse(client._time_synced)

    async def test_missing_and_invalid_fields_are_not_keyerrors(self):
        for payload in ({}, [], None, {"serverTime": None}, {"serverTime": True},
                        {"serverTime": "not-a-time"}, {"serverTime": 0},
                        {"serverTime": -1}, {"serverTime": 1.5}):
            with self.subTest(payload=payload):
                client = self.client(Response(payload))
                with self.assertLogs(client._logger, level="WARNING") as logs:
                    self.assertEqual(await client.sync_time(), 0)
                self.assertIn("serverTime", " ".join(logs.output))
                self.assertNotIn("KeyError", " ".join(logs.output))
                self.assertFalse(client._time_synced)

    async def test_html_rejection_preserves_http_status(self):
        client = self.client(Response(None, 451, ValueError("SENSITIVE HTML")))
        with self.assertLogs(client._logger, level="WARNING") as logs:
            await client.sync_time()
        self.assertIn("non-JSON http_status=451", " ".join(logs.output))
        self.assertNotIn("SENSITIVE", " ".join(logs.output))
        self.assertFalse(client._time_synced)

    async def test_network_timeout_logs_exception_type_and_retries_later(self):
        client = self.client(asyncio.TimeoutError(), Response({"serverTime": "1500"}))
        with self.assertLogs(client._logger, level="WARNING") as logs:
            await client.sync_time()
        self.assertIn("TimeoutError", " ".join(logs.output))
        self.assertFalse(client._time_synced)
        with patch("connections.binance.rest.time.time", side_effect=[1.0, 1.2]):
            self.assertEqual(await client.sync_time(), 400)
        self.assertTrue(client._time_synced)

    async def test_session_creation_failure_is_handled(self):
        client = self.client()
        client._get_session = AsyncMock(side_effect=OSError(10061, "SENSITIVE"))
        with self.assertLogs(client._logger, level="WARNING") as logs:
            self.assertEqual(await client.sync_time(), 0)
        self.assertIn("errno=10061", " ".join(logs.output))
        self.assertNotIn("SENSITIVE", " ".join(logs.output))

    async def test_cancellation_propagates(self):
        client = self.client(asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await client.sync_time()


class EquityFailureTests(unittest.IsolatedAsyncioTestCase):
    def monitor(self, spot="spot", futures="usdm"):
        # No runtime/config files or real accounts are touched by these tests.
        with patch.object(BinanceAccountMonitor, "_load_baseline"), \
                patch.object(BinanceAccountMonitor, "_load_funding_seen"):
            monitor = BinanceAccountMonitor(AccountMonitorConfig(
                "test", BinanceCredentials("dummy-key", "dummy-secret"), Path("unused"),
                spot_mode=spot, futures_mode=futures,
                usdm_credentials=BinanceCredentials("dummy-usdm", "dummy-secret"),
                include_positions=False, include_balance=False,
            ))
        monitor.store = Mock()
        if monitor.spot_stream:
            monitor.spot_stream.rest.get = AsyncMock(side_effect=self.spot)
        return monitor

    @staticmethod
    async def spot(path, **kwargs):
        if path == "/api/v3/account":
            return {"accountType": "SPOT", "balances": [{"asset": "USDT", "free": "10", "locked": "0"}]}
        return {"data": []}

    async def test_usdm_failure_still_persists_unavailable_equity(self):
        monitor = self.monitor()
        monitor.usdm_rest.get = AsyncMock(side_effect=BinanceConnectionError("Binance 451"))
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertIsNone(state["latestEquity"])
        self.assertIsNone(state["usdmEquity"])
        self.assertIsNone(state["baselineEquity"])
        self.assertEqual(state["spotEquity"], 10)
        self.assertEqual(state["latestPositions"], [])
        self.assertIn("usdm", state["errors"])

    async def test_existing_baseline_survives_failure_and_next_sample_recovers(self):
        monitor = self.monitor()
        positions = [{"symbol": "GRAMUSDT", "positionAmt": "17.9"}]
        monitor.usdm_rest.get = AsyncMock(return_value={"totalMarginBalance": "200", "positions": positions})
        await monitor._collect_equity()
        baseline_time = monitor._baseline_at
        monitor.usdm_rest.get.side_effect = TimeoutError()
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertEqual(state["baselineEquity"], 210)
        self.assertEqual(state["baselinePositions"], positions)
        self.assertIsNone(state["latestEquity"])
        monitor.usdm_rest.get.side_effect = None
        monitor.usdm_rest.get.return_value = {"totalMarginBalance": "205", "positions": positions}
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertEqual(state["latestEquity"], 215)
        self.assertEqual(state["baselineEquity"], 210)
        self.assertEqual(monitor._baseline_at, baseline_time)
        self.assertEqual(state["latestPositions"], positions)
        self.assertNotIn("usdm", state["errors"])

    async def test_both_components_failed_are_not_zero(self):
        monitor = self.monitor()
        monitor.usdm_rest.get = AsyncMock(side_effect=TimeoutError())
        monitor.spot_stream.rest.get = AsyncMock(side_effect=TimeoutError())
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertIsNone(state["latestEquity"])
        self.assertEqual(set(state["errors"]), {"usdm", "spot"})
        self.assertEqual(state["latestPositions"], [])

    async def test_pm_failure_and_spot_only_are_safe(self):
        for spot, futures in (("pm_margin", "pm_um"), ("spot", "none")):
            with self.subTest(spot=spot, futures=futures):
                monitor = self.monitor(spot, futures)
                if monitor.rest:
                    monitor.rest.get = AsyncMock(side_effect=TimeoutError())
                await monitor._collect_equity()
                state = monitor.store.write_equity_state.call_args.args[0]
                self.assertEqual(state["latestPositions"], [])
                self.assertEqual(state["latestEquity"], 10 if spot == "spot" else None)

    async def test_new_trading_day_waits_for_complete_equity_sample(self):
        monitor = self.monitor()
        monitor._baseline_day = "19990101"
        monitor._baseline_equity = 999
        monitor.usdm_rest.get = AsyncMock(side_effect=TimeoutError())
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertEqual(state["tradingDay"], trading_day())
        self.assertIsNone(state["baselineEquity"])
        monitor.usdm_rest.get.side_effect = None
        monitor.usdm_rest.get.return_value = {"totalMarginBalance": "200"}
        await monitor._collect_equity()
        state = monitor.store.write_equity_state.call_args.args[0]
        self.assertEqual(state["baselineEquity"], 210)


if __name__ == "__main__":
    unittest.main()
