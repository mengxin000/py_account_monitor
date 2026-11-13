"""Single-process Binance monitor for multiple subaccounts.

The daemon starts all account collectors, rotates JSONL folders at 09:30,
and periodically rebuilds one report per account before sending one email with
all Excel attachments.  No daily path editing or per-account process is needed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import tempfile
import threading
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from ..collectors.account_monitor import BinanceAccountMonitor, load_config, trading_day
    from ..config.settings import load_email_settings
    from ..reports.email_report import send_reports
    from ..reports.excel_report import build_workbook
    from ..reports.html_report import build_html
    from ..reports.report_data import load_report_data
    from ..reports.funding import configured_markets
    from ..replay.batch_replay import replay_day, load_underlying_alias, read_trade_sources
    from ..collectors.market_collector import MarketCollector
except ImportError:  # running from the project directory
    from collectors.account_monitor import BinanceAccountMonitor, load_config, trading_day
    from config.settings import load_email_settings
    from reports.email_report import send_reports
    from reports.excel_report import build_workbook
    from reports.html_report import build_html
    from reports.report_data import load_report_data
    from reports.funding import configured_markets
    from replay.batch_replay import replay_day, load_underlying_alias, read_trade_sources
    from collectors.market_collector import MarketCollector

LOGGER = logging.getLogger("binance_monitor_service")
# Both schedules run in worker threads. Hold through SMTP reading attachments,
# so another report cannot rewrite a workbook while it is being sent.
_REPORT_LOCK = threading.Lock()


def _validate_accounts(configs) -> None:
    ids, roots = set(), []
    for config in configs:
        account_id = config.account_id.casefold()
        if account_id in ids:
            raise ValueError(f"duplicate account_id: {config.account_id}")
        ids.add(account_id)
        root = config.output_dir.resolve()
        for previous in roots:
            if root == previous or root in previous.parents or previous in root.parents:
                raise ValueError(f"overlapping account runtime directories: {root} and {previous}")
        roots.append(root)


def _generate_and_send_reports(monitors, email_path, base, day, service_config=None, *, final=False):
    """Blocking report transaction; callers must run this outside the event loop."""
    with _REPORT_LOCK:
        attachments = []
        html_parts = [f"<h2>Binance账户监控报告 {day}</h2>"]
        aliases = load_underlying_alias()
        for monitor in monitors:
            try:
                okx = getattr(monitor, "okx", None)
                if okx and okx.report_error:
                    raise RuntimeError(f"OKX data reconciliation incomplete: {okx.report_error}")
                day_dir = monitor.store.root / day
                if not day_dir.exists():
                    continue
                sources = read_trade_sources(day_dir)
                replay_day(day_dir, aliases=aliases, trade_sources=sources,
                           matching_rules=getattr(monitor.config, "matching_rules", None))
                data = load_report_data(day_dir, monitor.config.account_id,
                                        aliases=aliases, trade_sources=sources)
                data.funding_markets = configured_markets(monitor.config)
                output_dir = base / "output" / monitor.config.account_id / day
                xlsx = build_workbook(data, output_dir / f"{monitor.config.account_id}_{day}.xlsx")
                html_path = build_html(data, output_dir / f"{monitor.config.account_id}_{day}.html")
                attachments.append(xlsx)
                html_parts.append(html_path.read_text(encoding="utf-8"))
                LOGGER.info("report generated account=%s final=%s xlsx=%s", monitor.config.account_id, final, xlsx)
            except Exception:
                LOGGER.exception("report failed for account %s final=%s", monitor.config.account_id, final)
        if email_path and email_path.exists() and attachments:
            try:
                settings = load_email_settings(email_path)
                subject = f"{settings.subject_prefix}_{day}" + ("_final" if final else "")
                count = _send_reports_with_snapshot(settings, subject, "\n".join(html_parts),
                                                    attachments, base, service_config)
                LOGGER.info("email sent successfully day=%s final=%s attachments=%d", day, final, count)
            except Exception:
                LOGGER.exception("email report failed day=%s final=%s attachments=%d", day, final, len(attachments))


async def _run_report_job(*args, **kwargs):
    # OKX's canonical input is REST fills, not cumulative WS orders. Reconcile
    # before freezing sources, including the independent 09:30 final report.
    for monitor in args[0]:
        okx = getattr(monitor, "okx", None)
        if okx:
            try:
                await okx.reconcile()
            except Exception:
                LOGGER.exception("OKX pre-report reconciliation failed account=%s", monitor.config.account_id)
    # Cancellation must not release ownership while a worker still writes files.
    worker = asyncio.create_task(asyncio.to_thread(_generate_and_send_reports, *args, **kwargs))
    try:
        await asyncio.shield(worker)
    except asyncio.CancelledError:
        await worker
        raise
    except Exception:
        LOGGER.exception("report job failed")


def _fmt(value: Any) -> str:
    if value in (None, "-"):
        return "-"
    try:
        return f"{float(value):,.4f}"
    except (TypeError, ValueError):
        return str(value)


def _age(moment: datetime | None) -> str:
    if moment is None:
        return "-"
    seconds = (datetime.now() - moment).total_seconds()
    return f"{seconds:.0f}s前" if seconds >= 0 else f"{-seconds:.0f}s后"


async def _dashboard(monitors: list[BinanceAccountMonitor], report_state: dict[str, datetime | None]) -> None:
    """Refresh a compact status view once per second without changing logic."""
    from .dashboard import render
    while True:
        print("\x1b[2J\x1b[H" + render(monitors, report_state), flush=True)
        await asyncio.sleep(1)


def _resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def _load_service(path: Path) -> tuple[list[Path], Path | None, float, float]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    base = path.parent
    account_files = [_resolve(base, str(item)) for item in data.get("accounts", [])]
    if not account_files:
        raise SystemExit("accounts.local.json must contain a non-empty 'accounts' list")
    email_file = data.get("email_file")
    email_path = _resolve(base, str(email_file)) if email_file else None
    return (
        account_files,
        email_path,
        float(data.get("report_interval_seconds", 1800)),
        float(data.get("first_report_delay_seconds", 180)),
    )


def _send_reports_with_snapshot(settings, subject: str, html_body: str, attachments: list[Path],
                                base: Path, service_config: dict[str, Any] | None = None) -> int:
    mail_attachments = list(attachments)
    config = service_config or {}
    if config.get("spot_futures_enable", False) is True:
        temporary_path = None
        try:
            source = _resolve(base, config["spot_futures_url"])
            destination = base / "output" / "spot_futures.json"
            destination.parent.mkdir(parents=True, exist_ok=True)
            # Replace atomically so overlapping scheduled emails never read a partial copy.
            with source.open("rb") as incoming:
                with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".spot_futures-",
                                                 suffix=".tmp", delete=False) as outgoing:
                    temporary_path = Path(outgoing.name)
                    shutil.copyfileobj(incoming, outgoing)
            temporary_path.replace(destination)
            mail_attachments.append(destination)
            LOGGER.info("spot futures attachment prepared source=%s output=%s", source, destination)
        except Exception:
            LOGGER.exception("spot futures attachment preparation failed; sending reports without it")
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    LOGGER.warning("could not remove temporary attachment path=%s", temporary_path, exc_info=True)
    send_reports(settings, subject, html_body, mail_attachments)
    return len(mail_attachments)


async def _report_loop(monitors: list[BinanceAccountMonitor], email_path: Path | None, interval: float, first_delay: float, base: Path, report_state: dict[str, datetime | None], service_config: dict[str, Any] | None = None) -> None:
    report_state["next_report"] = datetime.now() + timedelta(seconds=max(0.0, first_delay))
    await asyncio.sleep(max(0.0, first_delay))
    while True:
        day = trading_day()
        await _run_report_job(monitors, email_path, base, day, service_config)
        report_state["last_report"] = datetime.now()
        report_state["next_report"] = datetime.now() + timedelta(seconds=max(30.0, interval))
        await asyncio.sleep(max(30.0, interval))


async def _final_day_report_loop(monitors: list[BinanceAccountMonitor], email_path: Path | None, base: Path, service_config: dict[str, Any] | None = None) -> None:
    """Send the previous trading-day final report at each 09:30 boundary.

    This is intentionally independent from the regular half-hour loop.  A
    coincident regular report therefore remains a separate email.
    """
    while True:
        now = datetime.now()
        target = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep(max(0.0, (target - now).total_seconds()))
        day = (target - timedelta(days=1)).strftime("%Y%m%d")
        await _run_report_job(monitors, email_path, base, day, service_config, final=True)


async def run(service_path: Path) -> None:
    account_files, email_path, interval, first_delay = _load_service(service_path)
    service_config = json.loads(service_path.read_text(encoding="utf-8"))
    configs = [load_config(path) for path in account_files]
    _validate_accounts(configs)
    shared_proxy = service_config.get("proxy")
    if shared_proxy:
        configs = [replace(c, proxy=c.proxy or shared_proxy) for c in configs]
    monitors = [BinanceAccountMonitor(c) for c in configs]
    market_settings = dict(service_config.get("market_data", {}))
    proxies = {c.proxy for c in configs if c.proxy}
    if "proxy" not in market_settings:
        if shared_proxy:
            market_settings["proxy"] = shared_proxy
        elif len(proxies) == 1:
            market_settings["proxy"] = next(iter(proxies))
        elif len(proxies) > 1:
            raise ValueError("multiple account proxies: specify market_data.proxy for shared market data")
    market = MarketCollector(service_path.parent.parent / "runtime" / "raw", market_settings)
    for monitor in monitors:
        monitor.market_collector = market
        if monitor.hyperliquid:
            account_id = monitor.config.account_id
            monitor.hyperliquid.on_market = lambda event, account_id=account_id: market.order_event(account_id, event)
    report_state: dict[str, datetime | None] = {"last_report": None, "next_report": None}
    tasks = [asyncio.create_task(monitor.run()) for monitor in monitors]
    tasks.append(asyncio.create_task(market.run(monitors)))
    tasks.append(asyncio.create_task(_report_loop(monitors, email_path, interval, first_delay, service_path.parent.parent, report_state, service_config)))
    tasks.append(asyncio.create_task(_final_day_report_loop(monitors, email_path, service_path.parent.parent, service_config)))
    tasks.append(asyncio.create_task(_dashboard(monitors, report_state)))
    from .web_service import supervise
    tasks.append(asyncio.create_task(supervise(monitors, service_path.parent.parent, service_config.get("web", {}), market_settings.get("proxy"))))
    try:
        await asyncio.gather(*tasks)
    finally:
        for monitor in monitors:
            await monitor.stop()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all Binance account monitors")
    parser.add_argument("--config", type=Path, default=Path("config/accounts.local.json"))
    parser.add_argument("--log-level", default="WARNING", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args()
    project_root = args.config.resolve().parent.parent
    log_dir = project_root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root_logger = logging.getLogger()
    # Keep detailed lifecycle records in log/run.txt even when the console is
    # intentionally quiet for the dashboard.
    root_logger.setLevel(logging.INFO)
    log_path = log_dir / f"{trading_day()}run.txt"
    from .logging_support import TradingDayFileHandler
    file_handler = TradingDayFileHandler(log_dir)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, args.log_level))
    console_handler.setFormatter(formatter)
    # Dashboard owns stdout; diagnostics must not corrupt its redraw.
    LOGGER.info("monitor service starting config=%s", args.config.resolve())
    asyncio.run(run(args.config.resolve()))


if __name__ == "__main__":
    main()
