"""One Binance monitoring instance with independently selected product legs.

This module intentionally does not place orders.  It combines:

* signed REST snapshots for the selected PM, Spot, and/or USDⓈ-M accounts;
* separate PM, Spot, and USDⓈ-M private user-data streams as selected;
* JSONL persistence of order/trade callbacks, split by trading symbol.

The class is account-agnostic: three subaccounts can later run three
instances with different credentials and output directories.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import signal
import stat
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

try:  # package import
    from ..core.connection import (
        BinanceCredentials,
        BinanceRestClient,
        BinanceUserDataStream,
        UserStreamConfig,
    )
except ImportError:  # direct script execution from this directory
    from core.connection import (  # type: ignore[no-redef]
        BinanceCredentials,
        BinanceRestClient,
        BinanceUserDataStream,
        UserStreamConfig,
    )

LOGGER = logging.getLogger("binance_account_monitor")
try:
    from ..connections.binance.spot_stream import BinanceSpotStream
except ImportError:
    from connections.binance.spot_stream import BinanceSpotStream
from .binance.normalize import source_metadata
from .binance.equity import spot_equity
try:
    from ..connections.diagnostics import connection_error
except ImportError:
    from connections.diagnostics import connection_error
_SAFE_SYMBOL = re.compile(r"^[A-Z0-9_.-]+$")
FUNDING_POLL_TIMES = ((0, 5), (8, 5), (16, 5))


@dataclass(frozen=True, slots=True)
class AccountMonitorConfig:
    account_id: str
    credentials: BinanceCredentials | None
    output_dir: Path = Path("runtime")
    rest_interval_seconds: float = 5.0
    include_balance: bool = True
    balance_interval_seconds: float = 60.0
    rest_base_url: str = "https://papi.binance.com"
    include_positions: bool = True
    funding_interval_seconds: float = 60.0
    funding_income_path: str = "/papi/v1/um/income"
    spot_mode: str = "spot"  # spot | pm_margin | none
    futures_mode: str = "pm_um"  # pm_um | usdm | none
    usdm_credentials: BinanceCredentials | None = None
    spot_credentials: BinanceCredentials | None = None
    proxy: str | None = None
    pm_usd_to_usdt: float = 1.0


def trading_day(now: datetime | None = None) -> str:
    """C++ trading date: each report day starts at 09:30 local time."""
    current = now or datetime.now()
    if (current.hour, current.minute, current.second) < (9, 30, 0):
        current -= timedelta(days=1)
    return current.strftime("%Y%m%d")


def _date_folder(root: Path, now: datetime | None = None) -> Path:
    return root / trading_day(now)


def _event_symbol(event: Mapping[str, Any]) -> str | None:
    """Extract symbol from Portfolio Margin/Futures or Spot user events."""
    order = event.get("o")
    if isinstance(order, Mapping):
        symbol = order.get("s") or order.get("symbol")
        if symbol:
            return str(symbol).upper()
    symbol = event.get("s") or event.get("symbol")
    return str(symbol).upper() if symbol else None


def _is_trade_callback(event: Mapping[str, Any]) -> bool:
    """Return true for order/execution callbacks, including partial fills."""
    event_type = str(event.get("e", event.get("eventType", ""))).upper()
    return event_type in {"ORDER_TRADE_UPDATE", "EXECUTIONREPORT", "EXECUTION_REPORT"}


def _is_fill_callback(event: Mapping[str, Any]) -> bool:
    """Keep only callbacks needed by the old matching algorithm."""
    if not _is_trade_callback(event):
        return False
    order = event.get("o") if isinstance(event.get("o"), Mapping) else event
    status = str(order.get("X", order.get("status", ""))).upper()
    if status not in {"PARTIALLY_FILLED", "FILLED", "PARTIALLY_CANCELED", "CANCELED"}:
        return False
    if status == "CANCELED":
        try:
            return float(order.get("z", order.get("executedQty", 0)) or 0) > 1e-8
        except (TypeError, ValueError):
            return False
    return True


class JsonlEventStore:
    """Thread-safe append-only JSONL store with daily folders."""

    def __init__(self, root: Path, account_id: str) -> None:
        self.root = Path(root)
        self.account_id = account_id
        self._lock = threading.Lock()
        self._prepared_paths: set[Path] = set()

    def _path(self, filename: str, now: datetime | None = None) -> Path:
        folder = _date_folder(self.root, now)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / filename

    def append(self, filename: str, record: Mapping[str, Any], *, now: datetime | None = None) -> Path:
        path = self._path(filename, now)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            for attempt, delay in enumerate((0.0, 0.02, 0.05, 0.10, 0.20, 0.40), 1):
                try:
                    if path not in self._prepared_paths:
                        if path.exists() and path.stat().st_size:
                            with path.open("rb+") as binary_stream:
                                binary_stream.seek(-1, os.SEEK_END)
                                if binary_stream.read(1) not in {b"\n", b"\r"}:
                                    binary_stream.seek(0, os.SEEK_END)
                                    binary_stream.write(b"\n")
                                    binary_stream.flush()
                        self._prepared_paths.add(path)
                    with path.open("a", encoding="utf-8", newline="\n") as stream:
                        stream.write(line)
                        stream.flush()
                    break
                except PermissionError:
                    # Runtime files copied from another computer can retain
                    # the Windows ReadOnly attribute. These are service-owned
                    # append-only files, so restore write access automatically.
                    if path.exists() and not (path.stat().st_mode & stat.S_IWRITE):
                        path.chmod(path.stat().st_mode | stat.S_IWRITE)
                        LOGGER.warning(
                            "read-only runtime file made writable account=%s path=%s",
                            self.account_id,
                            path,
                        )
                        continue
                    if attempt == 6:
                        raise
                    time.sleep(delay)
        return path

    def write_equity_state(self, state: Mapping[str, Any], *, now: datetime | None = None) -> Path:
        path = self._path("equity.json", now)
        # Reports can briefly have equity.json open while the five-second REST
        # loop replaces it. Windows then raises WinError 5 for os.replace().
        # A unique temporary file also prevents collisions if two processes
        # overlap during a restart. Keep the old complete state until replace
        # succeeds, and retry only this short-lived sharing violation.
        temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        payload = json.dumps(state, ensure_ascii=False, indent=2)
        with self._lock:
            try:
                temp.write_text(payload, encoding="utf-8")
                for attempt, delay in enumerate((0.02, 0.05, 0.10, 0.20, 0.40), 1):
                    try:
                        os.replace(temp, path)
                        return path
                    except PermissionError:
                        if attempt == 5:
                            raise
                        time.sleep(delay)
            finally:
                try:
                    temp.unlink(missing_ok=True)
                except OSError:
                    pass
        return path

    def append_trade_event(self, event: Mapping[str, Any], *, source: str = "unknown") -> Path | None:
        symbol = _event_symbol(event)
        if not symbol:
            return None
        if not _SAFE_SYMBOL.fullmatch(symbol):
            raise ValueError(f"unsafe Binance symbol for filename: {symbol!r}")
        record = {
            **source_metadata(event, source),
            "recordType": "trade_callback",
            "accountId": self.account_id,
            "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
            "eventType": event.get("e", event.get("eventType")),
            "symbol": symbol,
            "data": event,
        }
        # One chronological source of truth per account/day. Pairing and
        # Exposure grouping are derived later by underlying asset.
        return self.append("trade_callbacks.jsonl", record)


class BinanceAccountMonitor:
    """Run one account's REST snapshot loop and private user stream."""

    def __init__(self, config: AccountMonitorConfig) -> None:
        self.market_collector = None
        if not math.isfinite(config.pm_usd_to_usdt) or config.pm_usd_to_usdt <= 0:
            raise ValueError("pm_usd_to_usdt must be finite and positive")
        self.config = config
        if config.spot_mode not in {"spot", "pm_margin", "none"}:
            raise ValueError(f"unsupported spot.mode: {config.spot_mode}")
        if config.futures_mode not in {"pm_um", "usdm", "none"}:
            raise ValueError(f"unsupported futures.mode: {config.futures_mode}")
        if config.spot_mode == "none" and config.futures_mode == "none":
            raise ValueError("at least one of spot.mode or futures.mode must be enabled")
        if config.futures_mode == "usdm" and config.usdm_credentials is None:
            raise ValueError("futures.mode=usdm requires usdm api_key and secret_key")
        self.uses_pm = config.spot_mode == "pm_margin" or config.futures_mode == "pm_um"
        self.store = JsonlEventStore(config.output_dir, config.account_id)
        self.stop_event = asyncio.Event()
        if self.uses_pm and config.credentials is None:
            raise ValueError("PM credentials are required for the selected spot/futures modes")
        if config.spot_mode == "spot" and (config.spot_credentials or config.credentials) is None:
            raise ValueError("spot.mode=spot requires Spot api_key and secret_key")
        self.rest = BinanceRestClient(config.credentials, base_url=config.rest_base_url,
                                      logger=LOGGER, proxy=config.proxy) if self.uses_pm else None
        self.user_stream = BinanceUserDataStream(
            config.credentials, config=UserStreamConfig(rest_base_url=config.rest_base_url),
            on_message=self._on_user_event, on_error=self._on_stream_error,
            logger=logging.getLogger(f"binance_account_monitor.{config.account_id}.pm"), proxy=config.proxy,
        ) if self.uses_pm else None
        self.usdm_rest = BinanceRestClient(config.usdm_credentials, base_url="https://fapi.binance.com",
                                           logger=LOGGER, proxy=config.proxy) if config.futures_mode == "usdm" else None
        self.usdm_stream = BinanceUserDataStream(
            config.usdm_credentials, config=UserStreamConfig.usd_m_futures(),
            on_message=self._on_usdm_event, on_error=self._on_stream_error,
            logger=logging.getLogger(f"binance_account_monitor.{config.account_id}.usdm"), proxy=config.proxy,
        ) if config.futures_mode == "usdm" else None
        self.spot_stream = BinanceSpotStream(
            config.spot_credentials or config.credentials,
            on_message=self._on_spot_event, on_error=self._on_stream_error,
            logger=logging.getLogger(f"binance_account_monitor.{config.account_id}.spot"),
            proxy=config.proxy,
        ) if config.spot_mode == "spot" else None
        self._last_account: dict[str, Any] = {}
        self._last_account_at: datetime | None = None
        self._last_trade_at: datetime | None = None
        self._last_error: str | None = None
        self._rest_errors = {}
        self._stream_errors = {}
        self._next_balance_at = 0.0
        self._spot_at = None
        self._spot_equity = None
        self._pm_equity = None
        self._usdm_equity = None
        self._total_equity = None
        self._baseline_components = None
        self._legacy_baseline = None
        self._equity_scope = f"spot={config.spot_mode};futures={config.futures_mode}:USDT:{config.pm_usd_to_usdt}"
        self._baseline_equity: float | None = None
        self._baseline_at: datetime | None = None
        self._baseline_day: str | None = None
        self._baseline_positions: Any = []
        self._last_funding_at: datetime | None = None
        # Funding REST results are queried repeatedly (and after restarts), so
        # keep the Binance transaction IDs in memory for de-duplication.
        self._funding_seen: set[str] = set()
        self._load_funding_seen()
        self._positions_warned = False
        self._funding_warned = False
        self._load_baseline()

    @staticmethod
    def _equity(account: Mapping[str, Any]) -> float | None:
        for key in ("actualEquity", "accountEquity"):
            try:
                if account.get(key) is not None:
                    return float(account[key])
            except (TypeError, ValueError):
                pass
        return None

    def _load_baseline(self) -> None:
        path = self.store._path("equity.json")
        if not path.exists():
            return
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
            if row.get("equityScope") != self._equity_scope:
                self._legacy_baseline = row
                LOGGER.warning("equity scope changed account=%s; new baseline required", self.config.account_id)
                self._baseline_day = trading_day()
                return
            self._baseline_components = row.get("baselineComponents")
            self._legacy_baseline = row.get("previousScopeBaseline")
            if row.get("baselineEquity") is not None:
                self._baseline_equity = float(row["baselineEquity"])
                self._baseline_at = datetime.fromisoformat(row["baselineTime"])
                self._baseline_day = str(row.get("tradingDay") or trading_day(self._baseline_at))
                self._baseline_positions = row.get("baselinePositions", [])
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            LOGGER.exception("failed to load 09:30 equity baseline account=%s", self.config.account_id)

    def _load_funding_seen(self) -> None:
        path = self.store._path("funding.jsonl")
        if not path.exists():
            return
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                row = json.loads(line)
                data = row.get("data", row)
                if row.get("recordType") == "funding_income" and isinstance(data, Mapping):
                    key = str(data.get("tranId") or data.get("id") or f"{data.get('time')}:{data.get('symbol')}:{data.get('income')}")
                    self._funding_seen.add(key)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            LOGGER.warning("failed to load funding de-duplication state account=%s", self.config.account_id, exc_info=True)

    async def _on_spot_event(self, event: dict[str, Any]) -> None:
        await self._on_user_event(event, source="spot_stream")

    async def _on_usdm_event(self, event: dict[str, Any]) -> None:
        await self._on_user_event(event, source="usdm_stream")

    async def _on_user_event(self, event: dict[str, Any], *, source: str = "pm_stream") -> None:
        metadata = source_metadata(event, source)
        if _is_trade_callback(event):
            try:
                self.store.append("all_callbacks.jsonl", {
                    **metadata,
                    "recordType": "order_callback", "accountId": self.config.account_id,
                    "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
                    "eventType": event.get("e"), "symbol": _event_symbol(event), "data": event,
                })
            except Exception:
                LOGGER.exception("all callbacks write failed account=%s", self.config.account_id)
            try:
                if self.market_collector is not None:
                    self.market_collector.order_event(self.config.account_id, {**event, "_accountScope": metadata["accountScope"]})
            except Exception:
                LOGGER.exception("market callback handling failed account=%s", self.config.account_id)
        # UM user streams emit ACCOUNT_UPDATE with reason FUNDING_FEE when a
        # funding settlement changes the account.  Keep the raw callback for
        # audit/replay; the authoritative amount is still collected by the
        # scheduled income REST query below.
        if str(event.get("e", "")).upper() == "ACCOUNT_UPDATE":
            account_update = event.get("a")
            if isinstance(account_update, Mapping) and str(account_update.get("m", "")) == "FUNDING_FEE":
                self.store.append("funding.jsonl", {
                    **metadata,
                    "recordType": "funding_callback",
                    "accountId": self.config.account_id,
                    "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
                    "data": event,
                })
                LOGGER.info("funding callback received account=%s", self.config.account_id)
        if _is_fill_callback(event):
            self._last_trade_at = datetime.now()
            path = self.store.append_trade_event(event, source=source)
            if path:
                LOGGER.debug("trade callback %s -> %s", _event_symbol(event), path)

    def _ensure_baseline(self, account: Mapping[str, Any]) -> None:
        equity = self._equity(account)
        now = datetime.now()
        if (now.hour, now.minute, now.second) < (9, 30, 0):
            return
        day = trading_day(now)
        # A long-running process crosses the 09:30 boundary without being
        # reconstructed, so explicitly rotate the in-memory baseline when the
        # trading day changes.
        if self._baseline_day != day:
            self._baseline_equity = None
            self._baseline_at = None
            self._baseline_positions = []
            self._baseline_day = day
        if equity is None or self._baseline_equity is not None:
            return
        self._baseline_equity = equity
        self._baseline_at = datetime.now()
        self._baseline_positions = account.get("positions", [])
        self._baseline_day = day
        LOGGER.info("equity baseline initialized account=%s trading_day=%s equity=%.10f", self.config.account_id, trading_day(), equity)

    async def _on_stream_error(self, event: dict[str, Any]) -> None:
        self._last_error = str(event.get("error", event))
        self._stream_errors[str(event.get("source", "pm_stream"))] = self._last_error

    async def _collect_equity(self):
        """One coherent sample; failed components never become zero equity."""
        now = datetime.now()
        day = trading_day(now)
        pm_account = None
        futures_account = None
        spot_value = 0.0 if self.config.spot_mode == "none" else None
        usdm_value = 0.0 if self.config.futures_mode != "usdm" else None
        assets = []
        pm_time = futures_time = spot_time = usdm_time = None
        if self.uses_pm:
            try:
                pm_account = await self.rest.get("/papi/v1/account", signed=True)
                if pm_account.get("actualEquity") is None or not math.isfinite(float(pm_account["actualEquity"])):
                    raise ValueError("PM actualEquity unavailable or non-finite")
                pm_time = datetime.now()
                self._pm_equity = float(pm_account["actualEquity"]) * self.config.pm_usd_to_usdt
                self._last_account = pm_account
                self._last_account_at = pm_time
                if self.config.futures_mode == "pm_um":
                    if self.config.include_positions:
                        try:
                            pm_account["positions"] = await self.rest.get("/papi/v1/um/positionRisk", signed=True)
                        except Exception:
                            LOGGER.debug("PM UM position snapshot unavailable account=%s", self.config.account_id)
                    if self.config.include_balance and time.monotonic() >= self._next_balance_at:
                        try:
                            pm_account["balance"] = await self.rest.get("/papi/v1/balance", signed=True)
                            self._next_balance_at = time.monotonic() + self.config.balance_interval_seconds
                        except Exception:
                            LOGGER.debug("PM balance snapshot unavailable account=%s", self.config.account_id)
                self._rest_errors.pop("pm", None)
            except Exception as exc:
                pm_account = None
                self._pm_equity = None
                self._rest_errors["pm"] = connection_error(exc, self.config.rest_base_url)
        else:
            self._rest_errors.pop("pm", None)
            self._pm_equity = None
        if self.config.futures_mode == "usdm":
            try:
                futures_account = await self.usdm_rest.get("/fapi/v3/account", signed=True)
                raw_equity = futures_account.get("totalMarginBalance")
                if raw_equity is None:
                    raise ValueError("USDⓈ-M totalMarginBalance unavailable")
                usdm_value = float(raw_equity)
                if not math.isfinite(usdm_value):
                    raise ValueError("USDⓈ-M equity non-finite")
                usdm_time = datetime.now()
                futures_time = usdm_time
                self._usdm_equity = usdm_value
                self._last_account = futures_account
                self._last_account_at = usdm_time
                if self.config.include_positions:
                    try:
                        futures_account["positions"] = await self.usdm_rest.get("/fapi/v3/positionRisk", signed=True)
                    except Exception:
                        LOGGER.debug("USDⓈ-M position snapshot unavailable account=%s", self.config.account_id)
                self._rest_errors.pop("usdm", None)
            except Exception as exc:
                futures_account = None
                usdm_value = None
                self._usdm_equity = None
                self._rest_errors["usdm"] = connection_error(exc, "https://fapi.binance.com")
        else:
            self._rest_errors.pop("usdm", None)
            self._usdm_equity = None
        if self.spot_stream is not None:
            try:
                spot = await self.spot_stream.rest.get("/api/v3/account", signed=True)
                tickers = await self.spot_stream.rest.get("/api/v3/ticker/price")
                spot_value, assets = spot_equity(spot, tickers.get("data", []))
                self._spot_at = spot_time = datetime.now()
                if self.config.futures_mode == "none" and not self.uses_pm:
                    self._last_account = {"actualEquity": spot_value}
                    self._last_account_at = spot_time
                self._rest_errors.pop("spot", None)
            except Exception as exc:
                self._rest_errors["spot"] = connection_error(exc, "https://api.binance.com")
                if isinstance(exc, ValueError):
                    self._rest_errors["spot"] = str(exc)
        else:
            self._rest_errors.pop("spot", None)
        self._spot_equity = spot_value if self.config.spot_mode == "spot" else None
        self._total_equity = None
        component_times = [at for at, value in ((pm_time, float(pm_account["actualEquity"]) * self.config.pm_usd_to_usdt if pm_account else None), (spot_time, spot_value), (usdm_time, usdm_value)) if at and value is not None]
        if component_times and (max(component_times) - min(component_times)).total_seconds() > max(30, self.config.rest_interval_seconds * 3):
            if spot_time:
                self._rest_errors["spot"] = "equity sample time skew too large"
                spot_value = None
            if usdm_time:
                self._rest_errors["usdm"] = "equity sample time skew too large"
                usdm_value = None
        if trading_day() != day:
            return  # Never mix samples spanning the 09:30 boundary.
        if self._baseline_day != day:
            self._baseline_equity = None
            self._baseline_at = None
            self._baseline_day = day
            self._baseline_components = None
            self._legacy_baseline = None
        pm = float(pm_account["actualEquity"]) * self.config.pm_usd_to_usdt if pm_account else (0.0 if not self.uses_pm else None)
        components = [value for value in (pm, spot_value, usdm_value) if value is not None]
        required_ready = ((not self.uses_pm or pm is not None)
                          and (self.config.spot_mode != "spot" or spot_value is not None)
                          and (self.config.futures_mode != "usdm" or usdm_value is not None))
        if required_ready and components:
            self._total_equity = sum(components)
            if self._baseline_equity is None:
                self._baseline_equity = self._total_equity
                self._baseline_at = datetime.now()
                self._baseline_components = {"pm": pm, "spot": self._spot_equity, "usdm": usdm_value,
                                            "pmTime": pm_time.isoformat() if pm_time else None,
                                            "spotTime": spot_time.isoformat() if spot_time else None,
                                            "usdmTime": usdm_time.isoformat() if usdm_time else None}
                baseline_account = futures_account if self.config.futures_mode == "usdm" else pm_account
                self._baseline_positions = baseline_account.get("positions", []) if baseline_account else []
                LOGGER.info("combined equity baseline initialized account=%s day=%s time=%s", self.config.account_id, day, self._baseline_at)
        state = {
            "schemaVersion": 2, "equityScope": self._equity_scope,
            "valuationCurrency": "USDT", "pmUsdToUsdt": self.config.pm_usd_to_usdt,
            "cashFlowAdjusted": False,
            "accountId": self.config.account_id, "tradingDay": day,
            "baselineEquity": self._baseline_equity,
            "baselineTime": self._baseline_at.isoformat(timespec="milliseconds") if self._baseline_at else None,
            "baselineComponents": self._baseline_components,
            "baselineSampleTime": self._baseline_at.isoformat() if self._baseline_at else None,
            "baselineKind": "first_complete_sample_of_scope",
            "previousScopeBaseline": self._legacy_baseline,
            "latestEquity": self._total_equity,
            "latestTime": datetime.now().isoformat(timespec="milliseconds"),
            "pmEquity": pm, "spotEquity": self._spot_equity,
            "pmTime": pm_time.isoformat() if pm_time else None,
            "spotTime": spot_time.isoformat() if spot_time else None,
            "usdmEquity": usdm_value,
            "usdmTime": usdm_time.isoformat() if usdm_time else None,
            "spotAssets": assets, "errors": dict(self._rest_errors),
            "baselinePositions": self._baseline_positions,
            "latestPositions": (futures_account if self.config.futures_mode == "usdm" else pm_account or {}).get("positions", []),
        }
        self.store.write_equity_state(state, now=now)

    async def _rest_loop(self) -> None:
        previous_errors = None
        while not self.stop_event.is_set():
            try:
                await self._collect_equity()
                errors = dict(self._rest_errors)
                if errors != previous_errors:
                    if errors:
                        LOGGER.warning("account REST state account=%s errors=%s", self.config.account_id, errors)
                    else:
                        LOGGER.info("account REST ready account=%s", self.config.account_id)
                    previous_errors = errors
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("equity persistence failed account=%s", self.config.account_id)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.rest_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def _funding_loop(self) -> None:
        """Collect settled funding income at the three C++ schedule times."""
        while not self.stop_event.is_set():
            now = datetime.now()
            candidates = [now.replace(hour=h, minute=m, second=5, microsecond=0) for h, m in FUNDING_POLL_TIMES]
            target = next((item for item in candidates if item > now), None)
            if target is None:
                tomorrow = now + timedelta(days=1)
                target = tomorrow.replace(hour=0, minute=5, second=5, microsecond=0)
            delay = max(0.0, (target - now).total_seconds())
            LOGGER.debug("next funding poll account=%s at=%s", self.config.account_id, target.isoformat())
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                if self.stop_event.is_set():
                    return
            except asyncio.TimeoutError:
                pass
            try:
                start = datetime.strptime(trading_day() + " 09:30", "%Y%m%d %H:%M")
                start_ms = int(start.timestamp() * 1000)
                end_ms = int(datetime.now().timestamp() * 1000)
                funding_client = self.usdm_rest if self.config.futures_mode == "usdm" else self.rest
                if funding_client is None or self.config.futures_mode == "none":
                    continue
                income_path = "/fapi/v1/income" if self.config.futures_mode == "usdm" else self.config.funding_income_path
                result = await funding_client.get(income_path, params={
                    "incomeType": "FUNDING_FEE",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                }, signed=True)
                LOGGER.debug("funding income queried account=%s start=%s end=%s", self.config.account_id, start.isoformat(), datetime.now().isoformat())
                rows = result if isinstance(result, list) else result.get("data", result.get("rows", []))
                if isinstance(rows, list):
                    for row in rows:
                        key = str(row.get("tranId") or row.get("id") or f"{row.get('time')}:{row.get('symbol')}:{row.get('income')}")
                        if key in self._funding_seen:
                            continue
                        self._funding_seen.add(key)
                        self.store.append("funding.jsonl", {
                            "exchange": "binance", "source": "rest",
                            "accountScope": "usdm" if self.config.futures_mode == "usdm" else "um",
                            "recordType": "funding_income",
                            "accountId": self.config.account_id,
                            "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
                            "data": row,
                        })
                        self._last_funding_at = datetime.now()
                        self._funding_warned = False
                        LOGGER.info("funding income collected account=%s income=%s asset=%s", self.config.account_id, row.get("income"), row.get("asset"))
            except asyncio.CancelledError:
                raise
            except Exception:
                if not self._funding_warned:
                    LOGGER.warning("funding income poll failed account=%s", self.config.account_id, exc_info=True)
                    self._funding_warned = True

    async def run(self) -> None:
        rest_task = asyncio.create_task(self._rest_loop())
        funding_task = asyncio.create_task(self._funding_loop())
        tasks = [rest_task, funding_task]
        if self.user_stream is not None:
            tasks.append(asyncio.create_task(self.user_stream.run(self.stop_event)))
        if self.usdm_stream is not None:
            tasks.append(asyncio.create_task(self.usdm_stream.run(self.stop_event)))
        if self.spot_stream is not None:
            tasks.append(asyncio.create_task(self.spot_stream.run(self.stop_event)))
        try:
            await asyncio.gather(*tasks)
        finally:
            self.stop_event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.rest is not None:
                await self.rest.close()
            if self.usdm_rest is not None:
                await self.usdm_rest.close()

    async def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        """Small read-only state snapshot used by the terminal dashboard."""
        account = self._last_account
        fresh = self._last_account_at is not None and (datetime.now() - self._last_account_at).total_seconds() < max(30, self.config.rest_interval_seconds * 3)
        def value(name: str) -> Any:
            return account.get(name, "-") if isinstance(account, dict) else "-"
        mmr = value("uniMMR")
        if mmr == "-" and account.get("totalMaintMargin") is not None and account.get("totalMarginBalance") is not None:
            try:
                margin_balance = float(account["totalMarginBalance"])
                mmr = float(account["totalMaintMargin"]) / margin_balance * 100 if margin_balance > 0 else None
            except (TypeError, ValueError):
                mmr = None
        return {
            "private_streams": {
                "pm": ("CONNECTED" if self.user_stream.connected else "CONNECTING") if self.user_stream else "DISABLED",
                "usdm": ("CONNECTED" if self.usdm_stream.connected else "CONNECTING") if self.usdm_stream else "DISABLED",
                "spot": ("CONNECTED" if self.spot_stream.connected else "CONNECTING") if self.spot_stream else "DISABLED",
            },
            "account_id": self.config.account_id,
            "pm_equity": self._pm_equity,
            "usdm_equity": self._usdm_equity,
            "spot_equity": self._spot_equity,
            "total_equity": self._total_equity if fresh else None,
            "actual_profit": self._total_equity - self._baseline_equity if fresh and self._total_equity is not None and self._baseline_equity is not None else None,
            "spot_at": self._spot_at,
            "rest_errors": dict(self._rest_errors),
            "stream_errors": {k:v for k,v in self._stream_errors.items()
                              if not ((k == "pm_stream" and self.user_stream and self.user_stream.connected)
                                      or (k == "usdm_stream" and self.usdm_stream and self.usdm_stream.connected)
                                      or (k == "spot_stream" and self.spot_stream and self.spot_stream.connected))},
            "account_equity": value("accountEquity"),
            "actual_equity": (float(account["actualEquity"]) * self.config.pm_usd_to_usdt
                              if account.get("actualEquity") is not None else
                              float(account["totalMarginBalance"]) if account.get("totalMarginBalance") is not None else None),
            "available": value("totalAvailableBalance"),
            "unimmr": mmr,
            "account_at": self._last_account_at,
            "trade_at": self._last_trade_at,
            "error": self._last_error,
            "baseline_equity": self._baseline_equity,
            "baseline_at": self._baseline_at,
            "funding_at": self._last_funding_at,
        }


def _credential_pair(
    *,
    api_key_env: str,
    secret_key_env: str,
    key_type_env: str,
    sources: list[Mapping[str, Any]],
    fallback: tuple[str | None, str | None, str] | None = None,
) -> tuple[str | None, str | None, str]:
    """Resolve a complete API-key/secret pair without mixing sources."""
    env_api = os.getenv(api_key_env)
    env_secret = os.getenv(secret_key_env)
    if env_api or env_secret:
        if not env_api or not env_secret:
            raise SystemExit(f"{api_key_env} and {secret_key_env} must be set together")
        return env_api, env_secret, os.getenv(key_type_env, "auto")

    for source in sources:
        api_key = source.get("api_key") or source.get("apiKey")
        secret_key = source.get("secret_key") or source.get("secKey")
        if api_key or secret_key:
            if not api_key or not secret_key:
                raise SystemExit(f"{api_key_env} and {secret_key_env} credentials must be configured as a pair")
            return str(api_key), str(secret_key), os.getenv(key_type_env) or str(source.get("key_type", "auto"))

    if fallback:
        return fallback[0], fallback[1], os.getenv(key_type_env) or fallback[2]
    return None, None, os.getenv(key_type_env, "auto")


def load_config(path: Path) -> AccountMonitorConfig:
    values = json.loads(path.read_text(encoding="utf-8"))
    credentials_file = values.get("credentials_file")
    credential_values: dict[str, Any] = {}
    if credentials_file:
        credential_path = (path.parent / credentials_file).resolve()
        credential_values = json.loads(credential_path.read_text(encoding="utf-8"))
    else:
        # Production account files may contain their own credentials so that
        # one account can be deployed without another nested config file.
        credential_values = values
    api_key, secret_key, key_type = _credential_pair(
        api_key_env="BINANCE_API_KEY",
        secret_key_env="BINANCE_SECRET_KEY",
        key_type_env="BINANCE_KEY_TYPE",
        sources=[credential_values],
    )
    account_id = str(values.get("account_id", "account"))
    credentials = None
    if api_key and secret_key:
        credentials = BinanceCredentials(
            api_key=api_key,
            secret_key=secret_key,
            subaccount_email=(os.getenv("BINANCE_SUBACCOUNT_EMAIL") or credential_values.get("subaccount_email") or values.get("subaccount_email")),
            label=account_id or credential_values.get("label"),
            key_type=str(key_type),
        )
    spot_values = values.get("spot", {})
    # Keep old account files working; new files should use explicit mode.
    spot_mode = str(spot_values.get("mode", "spot" if spot_values.get("enabled", True) else "none")).lower()
    spot_api_key, spot_secret_key, spot_key_type = _credential_pair(
        api_key_env="BINANCE_SPOT_API_KEY",
        secret_key_env="BINANCE_SPOT_SECRET_KEY",
        key_type_env="BINANCE_SPOT_KEY_TYPE",
        sources=[spot_values],
        fallback=(api_key, secret_key, key_type),
    )
    spot_credentials = None
    if spot_api_key and spot_secret_key:
        spot_credentials = BinanceCredentials(
            api_key=spot_api_key,
            secret_key=spot_secret_key,
            subaccount_email=(os.getenv("BINANCE_SPOT_SUBACCOUNT_EMAIL") or spot_values.get("subaccount_email") or values.get("subaccount_email")),
            label=f"{account_id}-spot",
            key_type=str(spot_key_type),
        )
    futures_values = values.get("futures", {})
    futures_mode = str(futures_values.get("mode", "pm_um")).lower()
    uses_pm = spot_mode == "pm_margin" or futures_mode == "pm_um"
    if credentials is None and uses_pm:
        raise SystemExit("Missing PM api_key/secret_key (top-level credentials or credentials_file)")
    if spot_mode == "spot" and spot_credentials is None:
        raise SystemExit("spot.mode=spot requires spot.api_key and spot.secret_key (or top-level credentials for backward compatibility)")
    usdm_values = values.get("usdm", {})
    usdm_api_key, usdm_secret_key, usdm_key_type = _credential_pair(
        api_key_env="BINANCE_USDM_API_KEY",
        secret_key_env="BINANCE_USDM_SECRET_KEY",
        key_type_env="BINANCE_USDM_KEY_TYPE",
        sources=[usdm_values, futures_values],
    )
    usdm_credentials = None
    if usdm_api_key and usdm_secret_key:
        usdm_credentials = BinanceCredentials(
            usdm_api_key, usdm_secret_key, label=f"{account_id}-usdm", key_type=str(usdm_key_type)
        )
    if futures_mode == "usdm" and usdm_credentials is None:
        raise SystemExit("futures.mode=usdm requires usdm.api_key and usdm.secret_key")
    output_dir = Path(values.get("output_dir", "runtime"))
    if not output_dir.is_absolute():
        # Account files live under config/, while runtime data belongs beside
        # config/ at the project root.
        output_dir = path.parent.parent / output_dir
    return AccountMonitorConfig(
        account_id=account_id,
        credentials=credentials,
        spot_mode=spot_mode,
        futures_mode=futures_mode,
        usdm_credentials=usdm_credentials,
        spot_credentials=spot_credentials,
        proxy=values.get("proxy"),
        pm_usd_to_usdt=float(values.get("pm_usd_to_usdt", 1.0)),
        output_dir=output_dir,
        rest_interval_seconds=float(values.get("rest_interval_seconds", 5.0)),
        include_balance=bool(values.get("include_balance", True)),
        balance_interval_seconds=float(values.get("balance_interval_seconds", 60.0)),
        rest_base_url=str(values.get("rest_base_url", "https://papi.binance.com")),
        include_positions=bool(values.get("include_positions", True)),
        funding_interval_seconds=float(values.get("funding_interval_seconds", 60.0)),
        funding_income_path=str(values.get("funding_income_path", "/papi/v1/um/income")),
    )


async def async_main(config_path: Path) -> None:
    config = load_config(config_path)
    monitor = BinanceAccountMonitor(config)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, monitor.stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows does not support add_signal_handler for all signals.
            pass
    LOGGER.info("starting account monitor: %s", config.account_id)
    await monitor.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one Binance account monitor")
    parser.add_argument("--config", type=Path, required=True, help="account JSON configuration")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(async_main(args.config.resolve()))


if __name__ == "__main__":
    main()
