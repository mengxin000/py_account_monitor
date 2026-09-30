from __future__ import annotations

import asyncio
import base64
import binascii
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
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_der_private_key, load_pem_private_key

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
    key_type: str = "auto"
    _ed25519_private_key: Ed25519PrivateKey | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        key_type = self.key_type.strip().lower()
        if key_type not in {"auto", "hmac", "ed25519"}:
            raise ValueError("key_type must be 'auto', 'hmac', or 'ed25519'")

        private_key = None
        if key_type in {"auto", "ed25519"}:
            private_key = self._try_load_ed25519_private_key(self.secret_key)
            if key_type == "ed25519" and private_key is None:
                raise ValueError("Ed25519 secret_key must be a PKCS#8 PEM or base64-encoded DER private key")

        if private_key is not None:
            object.__setattr__(self, "key_type", "ed25519")
            object.__setattr__(self, "_ed25519_private_key", private_key)
        else:
            if key_type == "ed25519":  # defensive; validation above handles this
                raise ValueError("invalid Ed25519 private key")
            object.__setattr__(self, "key_type", "hmac")

    @staticmethod
    def _try_load_ed25519_private_key(value: str) -> Ed25519PrivateKey | None:
        raw = value.strip().encode("utf-8")
        try:
            if raw.startswith(b"-----BEGIN"):
                loaded = load_pem_private_key(raw, password=None)
            else:
                try:
                    key_bytes = base64.b64decode(b"".join(raw.split()), validate=True)
                except (ValueError, binascii.Error):
                    return None
                try:
                    loaded = load_der_private_key(key_bytes, password=None)
                except ValueError:
                    # Also accept a base64-encoded raw 32-byte Ed25519 seed.
                    if len(key_bytes) != 32:
                        return None
                    loaded = Ed25519PrivateKey.from_private_bytes(key_bytes)
        except (TypeError, ValueError):
            return None
        return loaded if isinstance(loaded, Ed25519PrivateKey) else None

    def sign(self, payload: str | bytes) -> str:
        """Sign Binance's canonical payload using this credential's key type.

        HMAC credentials return lowercase hex. Ed25519 credentials return
        base64-encoded signatures as required by Binance.
        """
        message = payload.encode("utf-8") if isinstance(payload, str) else payload
        if self.key_type == "ed25519":
            assert self._ed25519_private_key is not None
            return base64.b64encode(self._ed25519_private_key.sign(message)).decode("ascii")
        return hmac.new(self.secret_key.encode("utf-8"), message, hashlib.sha256).hexdigest()


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
    source: str = "pm_stream"
    private_events: tuple[str, ...] = ()

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
            source="usdm_stream",
            private_events=("ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE"),
        )

    def websocket_url(self, listen_key: str) -> str:
        if self.private_events:
            # Binance expects multiple private event names in one
            # slash-delimited `events` parameter, not repeated query keys.
            query = urlencode({"listenKey": listen_key})
            query += f"&events={'/'.join(self.private_events)}"
            return f"{self.websocket_base_url.rstrip('/')}/ws?{query}"
        return f"{self.websocket_base_url.rstrip('/')}/ws/{listen_key}"


def _join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


async def _call_handler(handler: MessageHandler | None, payload: Json) -> None:
    if handler is None:
        return
    result = handler(payload)
    if inspect.isawaitable(result):
        await result
