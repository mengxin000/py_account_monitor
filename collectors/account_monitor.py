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
import os
import re
import signal
import threading
import time
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

    def _path(self, filename: str, now: datetime | None = None) -> Path:
        folder = _date_folder(self.root, now)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / filename

    def append(self, filename: str, record: Mapping[str, Any], *, now: datetime | None = None) -> Path:
        path = self._path(filename, now)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            with path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.flush()
        return path

    def write_equity_state(self, state: Mapping[str, Any], *, now: datetime | None = None) -> Path:
        path = self._path("equity.json", now)
        temp = path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(path)
        return path

    def append_trade_event(self, event: Mapping[str, Any]) -> Path | None:
        symbol = _event_symbol(event)
        if not symbol:
            return None
        if not _SAFE_SYMBOL.fullmatch(symbol):
            raise ValueError(f"unsafe Binance symbol for filename: {symbol!r}")
        record = {
            "recordType": "trade_callback",
            "accountId": self.account_id,
            "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
            "eventType": event.get("e", event.get("eventType")),
            "symbol": symbol,
            "data": event,
        }
        return self.append(f"{symbol}.jsonl", record)


class BinanceAccountMonitor:
    """Run one account's REST snapshot loop and private user stream."""

    def __init__(self, config: AccountMonitorConfig) -> None:
        self.config = config
        self.store = JsonlEventStore(config.output_dir, config.account_id)
        self.stop_event = asyncio.Event()
        self.rest = BinanceRestClient(
            config.credentials,
            base_url=config.rest_base_url,
            logger=LOGGER,
        )
        self.user_stream = BinanceUserDataStream(
            config.credentials,
            config=UserStreamConfig(
                rest_base_url=config.rest_base_url,
            ),
            on_message=self._on_user_event,
            on_error=self._on_stream_error,
            logger=LOGGER,
        )
        self._last_account: dict[str, Any] = {}
        self._last_account_at: datetime | None = None
        self._last_trade_at: datetime | None = None
        self._last_error: str | None = None
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

    async def _on_user_event(self, event: dict[str, Any]) -> None:
        # UM user streams emit ACCOUNT_UPDATE with reason FUNDING_FEE when a
        # funding settlement changes the account.  Keep the raw callback for
        # audit/replay; the authoritative amount is still collected by the
        # scheduled income REST query below.
        if str(event.get("e", "")) == "ACCOUNT_UPDATE":
            account_update = event.get("a")
            if isinstance(account_update, Mapping) and str(account_update.get("m", "")) == "FUNDING_FEE":
                self.store.append("funding.jsonl", {
                    "recordType": "funding_callback",
                    "accountId": self.config.account_id,
                    "receivedTime": datetime.now().isoformat(timespec="milliseconds"),
                    "data": event,
                })
                LOGGER.info("funding callback received account=%s", self.config.account_id)
        if _is_fill_callback(event):
            self._last_trade_at = datetime.now()
            path = self.store.append_trade_event(event)
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
        LOGGER.warning("private stream error: %s", event)

    async def _rest_loop(self) -> None:
        next_balance_at = 0.0
        while not self.stop_event.is_set():
            started = int(time.time() * 1000)
            try:
                account = await self.rest.get("/papi/v1/account", signed=True)
                if self.config.include_positions:
                    try:
                        account["positions"] = await self.rest.get("/papi/v1/um/positionRisk", signed=True)
                        self._positions_warned = False
                    except Exception:
                        if not self._positions_warned:
                            LOGGER.warning("position snapshot unavailable account=%s", self.config.account_id, exc_info=True)
                            self._positions_warned = True
                now = time.monotonic()
                if self.config.include_balance and now >= next_balance_at:
                    try:
                        balance = await self.rest.get("/papi/v1/balance", signed=True)
                        account["balance"] = balance.get("data", balance)
                        next_balance_at = now + self.config.balance_interval_seconds
                    except Exception:
                        LOGGER.exception("account balance poll failed")
                self._ensure_baseline(account)
                equity = self._equity(account)
                if equity is not None:
                    self.store.write_equity_state({
                        "accountId": self.config.account_id,
                        "tradingDay": trading_day(),
                        "baselineEquity": self._baseline_equity,
                        "baselineTime": self._baseline_at.isoformat(timespec="milliseconds") if self._baseline_at else None,
                        "latestEquity": equity,
                        "latestTime": datetime.now().isoformat(timespec="milliseconds"),
                        "baselinePositions": getattr(self, "_baseline_positions", []),
                        "latestPositions": account.get("positions", []),
                    })
                self._last_account = account
                self._last_account_at = datetime.now()
                self._last_error = None
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("account information poll failed")
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.config.rest_interval_seconds
                )
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
        try:
            await asyncio.gather(user_task, rest_task, funding_task)
        finally:
            self.stop_event.set()
            for task in (user_task, rest_task, funding_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(user_task, rest_task, funding_task, return_exceptions=True)
            await self.rest.close()

    async def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        """Small read-only state snapshot used by the terminal dashboard."""
        account = self._last_account
        def value(name: str) -> Any:
            return account.get(name, "-") if isinstance(account, dict) else "-"
        return {
            "account_id": self.config.account_id,
            "account_equity": value("accountEquity"),
            "actual_equity": value("actualEquity"),
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
    return AccountMonitorConfig(
        account_id=str(values.get("account_id", "account")),
        credentials=credentials,
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
