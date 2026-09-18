from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, MutableMapping, Sequence
from urllib.parse import urlencode

import aiohttp

Json = dict[str, Any]
MessageHandler = Callable[[Json], Any | Awaitable[Any]]


class BinanceConnectionError(RuntimeError):
    """A transport, authentication, or Binance API error."""


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    secret_key: str
    subaccount_email: str | None = None
    # The API key should have USER_DATA/USER_STREAM only for this monitor.
    label: str | None = None


@dataclass(slots=True)
class UserStreamConfig:
    """Endpoints for a Binance user-data stream.

    Portfolio Margin defaults are ``POST /papi/v1/listenKey`` and
    ``wss://fstream.binance.com/pm``.  The endpoint is configurable because
    Binance product routes differ for spot and USD-M futures.
    """

    rest_base_url: str = "https://papi.binance.com"
    websocket_base_url: str = "wss://fstream.binance.com/pm"
    listen_key_path: str = "/papi/v1/listenKey"
    keepalive_seconds: float = 30 * 60

    @classmethod
    def spot(cls) -> "UserStreamConfig":
        return cls(
            rest_base_url="https://api.binance.com",
            websocket_base_url="wss://stream.binance.com:9443",
            listen_key_path="/api/v3/userDataStream",
        )

    @classmethod
    def usd_m_futures(cls) -> "UserStreamConfig":
        return cls(
            rest_base_url="https://fapi.binance.com",
            websocket_base_url="wss://fstream.binance.com/private",
            listen_key_path="/fapi/v1/listenKey",
        )


def _join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


async def _call_handler(handler: MessageHandler | None, payload: Json) -> None:
    if handler is None:
        return
    result = handler(payload)
    if inspect.isawaitable(result):
        await result
