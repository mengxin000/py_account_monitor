"""One Binance unified-account monitoring instance.

This module intentionally does not place orders.  It combines:

* a signed REST poll of ``GET /papi/v1/account`` every five seconds;
* one Portfolio Margin private user-data WebSocket;
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
    credentials: BinanceCredentials
    output_dir: Path = Path("runtime")
    rest_interval_seconds: float = 5.0
    include_balance: bool = True
    balance_interval_seconds: float = 60.0
    rest_base_url: str = "https://papi.binance.com"
    include_positions: bool = True
    funding_interval_seconds: float = 60.0
    funding_income_path: str = "/papi/v1/um/income"
    spot_enabled: bool = True
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
        self.store = JsonlEventStore(config.output_dir, config.account_id)
        self.stop_event = asyncio.Event()
        self.rest = BinanceRestClient(
            config.credentials,
            base_url=config.rest_base_url,
            logger=LOGGER,
            proxy=config.proxy,
        )
        self.user_stream = BinanceUserDataStream(
            config.credentials,
            config=UserStreamConfig(
                rest_base_url=config.rest_base_url,
            ),
            on_message=self._on_user_event,
            on_error=self._on_stream_error,
            logger=logging.getLogger(f"binance_account_monitor.{config.account_id}.pm"),
            proxy=config.proxy,
        )
        self.spot_stream = BinanceSpotStream(
            config.spot_credentials or config.credentials,
            on_message=self._on_spot_event, on_error=self._on_stream_error,
            logger=logging.getLogger(f"binance_account_monitor.{config.account_id}.spot"),
            proxy=config.proxy,
        ) if config.spot_enabled else None
        self._last_account: dict[str, Any] = {}
        self._last_account_at: datetime | None = None
        self._last_trade_at: datetime | None = None
        self._last_error: str | None = None
        self._rest_errors = {}
        self._stream_errors = {}
        self._next_balance_at = 0.0
        self._spot_at = None
        self._spot_equity = None
        self._total_equity = None
        self._baseline_components = None
        self._legacy_baseline = None
        self._equity_scope = f"{'pm+spot' if config.spot_enabled else 'pm'}:USDT:{config.pm_usd_to_usdt}"
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
        if str(event.get("e", "")) == "ACCOUNT_UPDATE":
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
        account = None
        spot_value = 0.0 if self.spot_stream is None else None
        assets = []
        pm_time = spot_time = None
        try:
            account = await self.rest.get("/papi/v1/account", signed=True)
            if account.get("actualEquity") is None:
                raise ValueError("PM actualEquity unavailable")
            if not math.isfinite(float(account["actualEquity"])):
                raise ValueError("PM actualEquity non-finite")
            self._last_account = account
            self._last_account_at = pm_time = datetime.now()
            self._rest_errors.pop("pm", None)
            if self.config.include_positions:
                try:
                    account["positions"] = await self.rest.get("/papi/v1/um/positionRisk", signed=True)
                except Exception:
                    LOGGER.debug("position snapshot unavailable account=%s", self.config.account_id)
            if self.config.include_balance and time.monotonic() >= self._next_balance_at:
                try:
                    account["balance"] = await self.rest.get("/papi/v1/balance", signed=True)
                    self._next_balance_at = time.monotonic() + self.config.balance_interval_seconds
                except Exception:
                    LOGGER.debug("balance snapshot unavailable account=%s", self.config.account_id)
        except Exception as exc:
            account = None
            self._rest_errors["pm"] = connection_error(exc, self.config.rest_base_url)
        if self.spot_stream is not None:
            try:
                spot = await self.spot_stream.rest.get("/api/v3/account", signed=True)
                tickers = await self.spot_stream.rest.get("/api/v3/ticker/price")
                spot_value, assets = spot_equity(spot, tickers.get("data", []))
                self._spot_at = spot_time = datetime.now()
                self._rest_errors.pop("spot", None)
            except Exception as exc:
                self._rest_errors["spot"] = connection_error(exc, "https://api.binance.com")
                if isinstance(exc, ValueError):
                    self._rest_errors["spot"] = str(exc)
        self._spot_equity = spot_value
        self._total_equity = None
        if pm_time and spot_time and abs((spot_time - pm_time).total_seconds()) > max(30, self.config.rest_interval_seconds * 3):
            self._rest_errors["spot"] = "equity sample time skew too large"
            spot_value = None
        if trading_day() != day:
            return  # Never mix samples spanning the 09:30 boundary.
        if self._baseline_day != day:
            self._baseline_equity = None
            self._baseline_at = None
            self._baseline_day = day
            self._baseline_components = None
            self._legacy_baseline = None
        pm = float(account["actualEquity"]) * self.config.pm_usd_to_usdt if account else None
        if pm is not None and spot_value is not None:
            self._total_equity = pm + spot_value
            if self._baseline_equity is None:
                self._baseline_equity = self._total_equity
                self._baseline_at = datetime.now()
                self._baseline_components = {"pm": pm, "spot": spot_value}
                self._baseline_components["pmTime"] = pm_time.isoformat() if pm_time else None
                self._baseline_components["spotTime"] = spot_time.isoformat() if spot_time else None
                self._baseline_positions = account.get("positions", [])
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
            "pmEquity": pm, "spotEquity": spot_value,
            "pmTime": pm_time.isoformat() if pm_time else None,
            "spotTime": spot_time.isoformat() if spot_time else None,
            "spotAssets": assets, "errors": dict(self._rest_errors),
            "baselinePositions": self._baseline_positions,
            "latestPositions": account.get("positions", []) if account else [],
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
                result = await self.rest.get(self.config.funding_income_path, params={
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
                            "exchange": "binance", "source": "rest", "accountScope": "um",
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
        user_task = asyncio.create_task(self.user_stream.run(self.stop_event))
        rest_task = asyncio.create_task(self._rest_loop())
        funding_task = asyncio.create_task(self._funding_loop())
        tasks = [user_task, rest_task, funding_task]
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
            await self.rest.close()

    async def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        """Small read-only state snapshot used by the terminal dashboard."""
        account = self._last_account
        fresh = self._last_account_at is not None and (datetime.now() - self._last_account_at).total_seconds() < max(30, self.config.rest_interval_seconds * 3)
        def value(name: str) -> Any:
            return account.get(name, "-") if isinstance(account, dict) else "-"
        return {
            "private_streams": {
                "pm": "CONNECTED" if self.user_stream.connected else "CONNECTING",
                "spot": ("CONNECTED" if self.spot_stream.connected else "CONNECTING") if self.spot_stream else "DISABLED",
            },
            "account_id": self.config.account_id,
            "spot_equity": self._spot_equity,
            "total_equity": self._total_equity if fresh else None,
            "actual_profit": self._total_equity - self._baseline_equity if fresh and self._total_equity is not None and self._baseline_equity is not None else None,
            "spot_at": self._spot_at,
            "rest_errors": dict(self._rest_errors),
            "stream_errors": {k:v for k,v in self._stream_errors.items() if not (self.user_stream.connected if k == "pm_stream" else self.spot_stream and self.spot_stream.connected)},
            "account_equity": value("accountEquity"),
            "actual_equity": float(account["actualEquity"]) * self.config.pm_usd_to_usdt if account.get("actualEquity") is not None else None,
            "available": value("totalAvailableBalance"),
            "unimmr": value("uniMMR"),
            "account_at": self._last_account_at,
            "trade_at": self._last_trade_at,
            "error": self._last_error,
            "baseline_equity": self._baseline_equity,
            "baseline_at": self._baseline_at,
            "funding_at": self._last_funding_at,
        }


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
    api_key = os.getenv("BINANCE_API_KEY") or credential_values.get("api_key") or credential_values.get("apiKey")
    secret_key = os.getenv("BINANCE_SECRET_KEY") or credential_values.get("secret_key") or credential_values.get("secKey")
    if not api_key or not secret_key:
        raise SystemExit("Missing BINANCE_API_KEY/BINANCE_SECRET_KEY or credentials_file")
    credentials = BinanceCredentials(
        api_key=api_key,
        secret_key=secret_key,
        subaccount_email=(
            os.getenv("BINANCE_SUBACCOUNT_EMAIL")
            or credential_values.get("subaccount_email")
            or values.get("subaccount_email")
        ),
        label=values.get("account_id") or credential_values.get("label"),
    )
    output_dir = Path(values.get("output_dir", "runtime"))
    if not output_dir.is_absolute():
        # Account files live under config/, while runtime data belongs beside
        # config/ at the project root.
        output_dir = path.parent.parent / output_dir
    spot_values = values.get("spot", {})
    spot_credentials = None
    if spot_values.get("api_key") or spot_values.get("secret_key"):
        if not spot_values.get("api_key") or not spot_values.get("secret_key"):
            raise ValueError("spot.api_key and spot.secret_key must be supplied together")
        spot_credentials = BinanceCredentials(spot_values["api_key"], spot_values["secret_key"], label=credentials.label)
    return AccountMonitorConfig(
        account_id=str(values.get("account_id", "account")),
        credentials=credentials,
        spot_enabled=bool(spot_values.get("enabled", True)),
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
