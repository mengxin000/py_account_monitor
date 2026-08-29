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
    from ..replay.batch_replay import replay_day
except ImportError:  # running from the project directory
    from collectors.account_monitor import BinanceAccountMonitor, load_config, trading_day
    from config.settings import load_email_settings
    from reports.email_report import send_reports
    from reports.excel_report import build_workbook
    from reports.html_report import build_html
    from reports.report_data import load_report_data
    from replay.batch_replay import replay_day

LOGGER = logging.getLogger("binance_monitor_service")


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
    while True:
        lines = [
            "Binance 多账户监控  |  Ctrl+C 停止  |  每 1 秒刷新",
            f"当前时间: {datetime.now():%Y-%m-%d %H:%M:%S}    下次报告: {_age(report_state.get('next_report'))}",
            "账户       REST账户权益       实际权益       可用余额       uniMMR       REST       成交回调       状态",
            "-" * 112,
        ]
        for monitor in monitors:
            status = monitor.status()
            error = status["error"]
            state = "ERROR" if error else "RUNNING"
            lines.append(
                f"{status['account_id']:<10}"
                f"{_fmt(status['account_equity']):>16}"
                f"{_fmt(status['actual_equity']):>15}"
                f"{_fmt(status['available']):>15}"
                f"{_fmt(status['unimmr']):>12}"
                f"{_age(status['account_at']):>10}"
                f"{_age(status['trade_at']):>12}"
                f"  {state}"
            )
            if error:
                lines.append(f"  最近错误: {error[:120]}")
        lines.append("")
        lines.append("数据目录: runtime/<account_id>/YYYYMMDD    报告目录: output/<account_id>/YYYYMMDD")
        print("\x1b[2J\x1b[H" + "\n".join(lines), end="", flush=True)
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


async def _report_loop(monitors: list[BinanceAccountMonitor], email_path: Path | None, interval: float, first_delay: float, base: Path, report_state: dict[str, datetime | None]) -> None:
    report_state["next_report"] = datetime.now() + timedelta(seconds=max(0.0, first_delay))
    await asyncio.sleep(max(0.0, first_delay))
    while True:
        day = trading_day()
        attachments: list[Path] = []
        html_parts: list[str] = [f"<h2>Binance账户监控报告 {day}</h2>"]
        for monitor in monitors:
            try:
                day_dir = monitor.store.root / day
                if not day_dir.exists():
                    continue
                replay_day(day_dir)
                data = load_report_data(day_dir, monitor.config.account_id)
                output_dir = base / "output" / monitor.config.account_id / day
                xlsx = build_workbook(data, output_dir / f"{monitor.config.account_id}_{day}.xlsx")
                html_path = build_html(data, output_dir / f"{monitor.config.account_id}_{day}.html")
                LOGGER.info("report generated account=%s xlsx=%s html=%s", monitor.config.account_id, xlsx, html_path)
                attachments.append(xlsx)
                html_parts.append(html_path.read_text(encoding="utf-8"))
            except Exception:
                LOGGER.exception("report failed for account %s", monitor.config.account_id)
        if email_path and email_path.exists() and attachments:
            try:
                settings = load_email_settings(email_path)
                # SMTP is blocking; keep the 1-second dashboard responsive
                # while the mail client performs TLS/login/retries.
                await asyncio.to_thread(
                    send_reports,
                    settings,
                    f"{settings.subject_prefix}_{day}",
                    "\n".join(html_parts),
                    attachments,
                )
                LOGGER.info("email sent successfully day=%s attachments=%d", day, len(attachments))
            except Exception:
                LOGGER.exception("email report failed day=%s attachments=%d", day, len(attachments))
        report_state["last_report"] = datetime.now()
        report_state["next_report"] = datetime.now() + timedelta(seconds=max(30.0, interval))
        await asyncio.sleep(max(30.0, interval))


async def _final_day_report_loop(monitors: list[BinanceAccountMonitor], email_path: Path | None, base: Path) -> None:
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
        attachments: list[Path] = []
        html_parts: list[str] = [f"<h2>Binance账户监控报告 {day}</h2>"]
        for monitor in monitors:
            try:
                day_dir = monitor.store.root / day
                if not day_dir.exists():
                    continue
                replay_day(day_dir)
                data = load_report_data(day_dir, monitor.config.account_id)
                output_dir = base / "output" / monitor.config.account_id / day
                xlsx = build_workbook(data, output_dir / f"{monitor.config.account_id}_{day}.xlsx")
                html_path = build_html(data, output_dir / f"{monitor.config.account_id}_{day}.html")
                attachments.append(xlsx)
                html_parts.append(html_path.read_text(encoding="utf-8"))
            except Exception:
                LOGGER.exception("final report failed for account %s", monitor.config.account_id)
        if email_path and email_path.exists() and attachments:
            try:
                settings = load_email_settings(email_path)
                await asyncio.to_thread(send_reports, settings, f"{settings.subject_prefix}_{day}_final", "\n".join(html_parts), attachments)
                LOGGER.info("final email sent successfully day=%s attachments=%d", day, len(attachments))
            except Exception:
                LOGGER.exception("final email report failed day=%s attachments=%d", day, len(attachments))


async def run(service_path: Path) -> None:
    account_files, email_path, interval, first_delay = _load_service(service_path)
    monitors = [BinanceAccountMonitor(load_config(path)) for path in account_files]
    report_state: dict[str, datetime | None] = {"last_report": None, "next_report": None}
    tasks = [asyncio.create_task(monitor.run()) for monitor in monitors]
    tasks.append(asyncio.create_task(_report_loop(monitors, email_path, interval, first_delay, service_path.parent.parent, report_state)))
    tasks.append(asyncio.create_task(_final_day_report_loop(monitors, email_path, service_path.parent.parent)))
    tasks.append(asyncio.create_task(_dashboard(monitors, report_state)))
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
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, args.log_level))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    LOGGER.info("monitor service starting config=%s", args.config.resolve())
    asyncio.run(run(args.config.resolve()))


if __name__ == "__main__":
    main()
