"""Rebuild Excel/HTML from one account trading-day JSONL and optionally email it."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from ..replay.batch_replay import replay_day
    from ..config.settings import load_report_settings
    from ..reports.email_report import send_report
    from ..reports.excel_report import build_workbook
    from ..reports.html_report import build_html
    from ..reports.report_data import load_report_data
except ImportError:  # running from the project directory
    from replay.batch_replay import replay_day
    from config.settings import load_report_settings
    from reports.email_report import send_report
    from reports.excel_report import build_workbook
    from reports.html_report import build_html
    from reports.report_data import load_report_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Binance account report")
    parser.add_argument("--config", type=Path, required=True, help="config/report.local.json")
    parser.add_argument("--send-email", action="store_true")
    args = parser.parse_args()
    settings = load_report_settings(args.config.resolve())
    replay_day(settings.day_dir)
    data = load_report_data(settings.day_dir, settings.account_id)
    xlsx = build_workbook(data, settings.output_dir / f"{settings.account_id}_{data.day}.xlsx")
    html_path = build_html(data, settings.output_dir / f"{settings.account_id}_{data.day}.html")
    if args.send_email:
        if settings.email is None:
            raise SystemExit("--send-email requires email_file in report config")
        send_report(settings.email, f"{settings.email.subject_prefix}_{data.day}", html_path.read_text(encoding="utf-8"), xlsx)
    print(json.dumps({"xlsx": str(xlsx), "html": str(html_path), "matches": len(data.matches), "unmatched": len(data.unmatched)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
