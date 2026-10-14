"""Private-stream lifecycle regressions; every REST/socket operation is mocked."""
import asyncio
import json
import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp

from connections.binance.base import BinanceCredentials, BinanceConnectionError, UserStreamConfig
from connections.binance.pm_stream import BinanceUserDataStream
from connections.binance.rest import BinanceRestClient
from collectors.account_monitor import AccountMonitorConfig, BinanceAccountMonitor


FILL = {
    "e": "ORDER_TRADE_UPDATE", "E": 1791795605296,
    "o": {
        "s": "GRAMUSDT", "i": 95624106, "t": 9956855,
        "c": "test-order", "S": "BUY", "x": "TRADE", "X": "FILLED",
        "l": "17.9", "z": "17.9", "L": "1.440000",
        "n": "0.00103104", "N": "USDT",
    },
}


def message(payload, kind=aiohttp.WSMsgType.TEXT):
    return SimpleNamespace(type=kind, data=json.dumps(payload))


class Socket:
    def __init__(self, messages=(), error=None):
        self.messages = asyncio.Queue()
        for item in messages:
            self.messages.put_nowait(item)
        self.entered = asyncio.Event()
        self.closed = False
        self.close_code = 1006 if error else None
        self.error = error

    async def __aenter__(self):
        self.entered.set()
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Empty feeds stay connected, as a quiet production account would.
        return await self.messages.get()

    def exception(self):
        return self.error


class Session:
    def __init__(self, socket, options, urls):
        self.socket = socket
        self.options = options
        self.urls = urls
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def ws_connect(self, url, **options):
        self.urls.append(url)
        if isinstance(self.socket, Exception):
            raise self.socket
        return self.socket


class UserStreamLifecycleTest(unittest.IsolatedAsyncioTestCase):
    def make_stream(self, config=None):
        stream = BinanceUserDataStream(
            BinanceCredentials("offline-api", "offline-secret", key_type="hmac"),
            config=config or UserStreamConfig.usd_m_futures(),
            reconnect_seconds=0,
            logger=logging.getLogger("offline_user_stream_lifecycle"),
        )
        stream.rest.create_listen_key = AsyncMock(side_effect=["old-key", "new-key"])
        stream.rest.keepalive_listen_key = AsyncMock(return_value={"listenKey": "old-key"})
        stream.rest.close_listen_key = AsyncMock()
        stream.rest.close = AsyncMock()
        return stream

    async def recovery(self, first, *, renewal=None, error_handler_fails=False, persist=None):
        stream = self.make_stream()
        if renewal is not None:
            stream.config.keepalive_seconds = 0.001
            stream.rest.keepalive_listen_key = AsyncMock(side_effect=renewal)
        second = Socket([message(FILL)])
        sessions, urls, events, errors = [], [], [], []
        stop = asyncio.Event()

        def factory(**options):
            socket = (first, second)[len(sessions)]
            session = Session(socket, options, urls)
            sessions.append(session)
            return session

        async def on_message(event):
            if persist is not None:
                await persist(event)
            events.append(event)
            stop.set()

        async def on_error(event):
            self.assertFalse(stream.connected)
            errors.append(event)
            if error_handler_fails:
                raise RuntimeError("sensitive-handler-detail")

        stream.on_message, stream.on_error = on_message, on_error
        with self.assertLogs(stream.logger, level="INFO") as logs, \
             patch("connections.binance.pm_stream.aiohttp.ClientSession", side_effect=factory):
            await asyncio.wait_for(stream.run(stop), 2)

        self.assertEqual(events, [FILL])
        self.assertEqual(len(sessions), 2)
        self.assertTrue(all(session.closed for session in sessions))
        self.assertTrue(second.closed)
        if isinstance(first, Socket):
            self.assertTrue(first.closed)
        self.assertIn("listenKey=old-key", urls[0])
        self.assertIn("listenKeyExpired", urls[0])
        self.assertIn("listenKey=new-key", urls[1])
        self.assertEqual(stream.rest.create_listen_key.await_count, 2)
        stream.rest.close_listen_key.assert_not_awaited()
        stream.rest.close.assert_awaited_once()
        self.assertFalse(stream.connected)
        self.assertEqual(stream.received_events, 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["source"], "usdm_stream")
        text = "\n".join(logs.output)
        for secret in ("old-key", "new-key", "offline-api", "offline-secret", "sensitive-handler-detail"):
            self.assertNotIn(secret, text)
        return stream, errors[0]["error"], text

    async def test_server_error_reconnects_and_next_socket_receives_trade(self):
        for payload in (
            {"code": -1125, "msg": "old-key"},
            {"stream": "old-key", "data": {"code": -1125, "msg": "old-key"}},
            {"status": 400, "error": {"code": -1125, "msg": "old-key"}},
        ):
            with self.subTest(payload=payload):
                _, reason, _ = await self.recovery(Socket([message(payload)]))
                self.assertIn("stage=receive", reason)
                self.assertIn("api_code=-1125", reason)

    async def test_positive_protocol_error_is_not_treated_as_account_event(self):
        _, reason, _ = await self.recovery(Socket([message({"code": 2, "msg": "invalid subscription"})]))
        self.assertIn("api_code=2", reason)

    async def test_unrecognized_payload_reconnects_instead_of_staying_silently_connected(self):
        _, reason, _ = await self.recovery(Socket([message({"msg": "old-key"})]))
        self.assertIn("missing event type", reason)

    async def test_expiration_reconnects_without_a_transport_disconnect(self):
        for payload in (
            {"e": "listenKeyExpired", "listenKey": "old-key"},
            {"stream": "old-key", "data": {"e": "listenKeyExpired", "listenKey": "old-key"}},
        ):
            with self.subTest(payload=payload):
                _, reason, _ = await self.recovery(Socket([message(payload)]))
                self.assertIn("expired", reason)

    async def test_changed_renewal_key_rebinds_socket_instead_of_extending_stale_socket(self):
        stream, reason, _ = await self.recovery(Socket(), renewal=[{"listenKey": "new-key"}])
        self.assertIn("stage=keepalive", reason)
        self.assertIn("listen key changed", reason)
        self.assertEqual(stream.rest.keepalive_listen_key.call_args.args[1], "old-key")

    async def test_failed_or_invalid_renewal_always_leaves_old_connection(self):
        for response in (
            {}, {"listenKey": ""}, {"listenKey": 123},
            {"code": -1125},
            BinanceConnectionError("Binance 400 PUT /fapi/v1/listenKey: {'code': -1125}"),
        ):
            with self.subTest(response=response):
                _, reason, _ = await self.recovery(Socket(), renewal=[response])
                self.assertIn("stage=keepalive", reason)

    async def test_socket_error_reconnects_and_preserves_safe_close_code(self):
        socket = Socket(
            [SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=None)],
            error=TimeoutError("old-key"),
        )
        _, reason, _ = await self.recovery(socket)
        self.assertIn("close_code=1006", reason)

    async def test_connect_timeout_retries_without_deleting_account_key(self):
        _, reason, _ = await self.recovery(TimeoutError("old-key"))
        self.assertIn("stage=connect", reason)

    async def test_error_callback_failure_does_not_disable_reconnection(self):
        await self.recovery(Socket([message({"code": -1125})]), error_handler_fails=True)

    async def test_trade_after_reconnection_is_persisted_to_both_original_journals(self):
        for renewal in (False, True):
            with self.subTest(renewal=renewal), tempfile.TemporaryDirectory() as temp:
                monitor = BinanceAccountMonitor(AccountMonitorConfig(
                    "offline-dh", None, Path(temp), spot_mode="none", futures_mode="usdm",
                    usdm_credentials=BinanceCredentials("offline-api", "offline-secret", key_type="hmac"),
                ))
                if renewal:
                    await self.recovery(Socket(), renewal=[{"listenKey": "new-key"}], persist=monitor._on_usdm_event)
                else:
                    await self.recovery(Socket([message({"code": -1125})]), persist=monitor._on_usdm_event)
                for filename in ("all_callbacks.jsonl", "trade_callbacks.jsonl"):
                    rows = [json.loads(line) for line in monitor.store._path(filename).read_text().splitlines()]
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["accountScope"], "usdm")
                    self.assertEqual(rows[0]["symbol"], "GRAMUSDT")
                    self.assertEqual(rows[0]["data"], FILL)

    async def test_raw_combined_and_binary_fills_keep_original_fields(self):
        stream = self.make_stream()
        stop = asyncio.Event()
        received = []
        binary = message(FILL, aiohttp.WSMsgType.BINARY)
        binary.data = binary.data.encode()
        socket = Socket([
            message({"id": 1, "result": None}),
            message(FILL), message({"stream": "old-key", "data": FILL}), binary,
        ])

        async def callback(event):
            received.append(event)
            if len(received) == 3:
                stop.set()

        stream.on_message = callback
        task = asyncio.create_task(stream._receive_loop(socket, stop))
        await asyncio.wait_for(stop.wait(), 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(received, [FILL, FILL, FILL])
        self.assertEqual(stream.received_events, 3)

    async def test_normal_stop_and_cancellation_close_local_socket_only(self):
        for config in (UserStreamConfig(), UserStreamConfig.usd_m_futures()):
            for cancel in (False, True):
                with self.subTest(source=config.source, cancel=cancel):
                    stream = self.make_stream(config)
                    socket, urls = Socket(), []
                    session = Session(socket, {}, urls)
                    stop = asyncio.Event()
                    with patch("connections.binance.pm_stream.aiohttp.ClientSession", return_value=session):
                        task = asyncio.create_task(stream.run(stop))
                        await asyncio.wait_for(socket.entered.wait(), 1)
                        self.assertTrue(stream.connected)
                        if cancel:
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        else:
                            stop.set()
                            await asyncio.wait_for(task, 1)
                    self.assertTrue(socket.closed)
                    self.assertTrue(session.closed)
                    self.assertFalse(stream.connected)
                    stream.rest.close_listen_key.assert_not_awaited()
                    stream.rest.close.assert_awaited_once()

    async def test_stop_before_start_does_not_open_account_stream(self):
        stream = self.make_stream()
        stop = asyncio.Event()
        stop.set()
        with patch("connections.binance.pm_stream.aiohttp.ClientSession") as factory:
            await stream.run(stop)
        factory.assert_not_called()
        stream.rest.create_listen_key.assert_not_awaited()
        stream.rest.close_listen_key.assert_not_awaited()

    async def test_pm_empty_renewal_response_remains_compatible(self):
        config = UserStreamConfig(keepalive_seconds=0.001)
        stream = self.make_stream(config)
        stop = asyncio.Event()

        async def renew(*args):
            stop.set()
            return {}

        stream.rest.keepalive_listen_key = AsyncMock(side_effect=renew)
        await asyncio.wait_for(stream._keepalive_loop("old-key", stop), 1)
        stream.rest.keepalive_listen_key.assert_awaited_once()

    async def test_usdm_renewal_and_explicit_close_use_header_only_rest_requests(self):
        for config, params in (
            (UserStreamConfig.usd_m_futures(), None),
            (UserStreamConfig(), {"listenKey": "old-key"}),
            (UserStreamConfig.spot(), {"listenKey": "old-key"}),
        ):
            with self.subTest(path=config.listen_key_path):
                rest = BinanceRestClient(BinanceCredentials("key", "secret", key_type="hmac"))
                rest.request = AsyncMock(return_value={})
                await rest.keepalive_listen_key(config, "old-key")
                rest.request.assert_awaited_once_with("PUT", config.listen_key_path, params=params, api_key=True)
                rest.request.reset_mock()
                await rest.close_listen_key(config, "old-key")
                rest.request.assert_awaited_once_with("DELETE", config.listen_key_path, params=params, api_key=True)


if __name__ == "__main__":
    unittest.main()
