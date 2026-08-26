"""Replay one trading day's compact fill JSONL with the legacy matcher.

Run this before the half-hour email/report.  It reads all symbol files, does
not require persistent matcher memory, and deterministically rewrites the
day's ``matches.jsonl``.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from ..core.legacy_matching import LegacyMatcher
except ImportError:
    from core.legacy_matching import LegacyMatcher  # type: ignore[no-redef]


def _event_time(event: dict[str, Any], fallback: str) -> tuple[int, str]:
    order = event.get("o") if isinstance(event.get("o"), dict) else event
    value = order.get("T", order.get("E", event.get("E", 0)))
    try:
        return int(value or 0), fallback
    except (TypeError, ValueError):
        return 0, fallback


def replay_day(day_dir: Path) -> dict[str, Any]:
    events: list[tuple[int, str, dict[str, Any]]] = []
    for path in sorted(day_dir.glob("*.jsonl")):
        if path.name in {"account_info.jsonl", "equity.json", "funding.jsonl", "matches.jsonl", "unmatched.jsonl", "exposure_matches.jsonl"}:
            continue
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            record = json.loads(line)
            event = record.get("data", record)
            if not isinstance(event, dict):
                continue
            event_time, tie_breaker = _event_time(event, f"{path.name}:{line_no}")
            events.append((event_time, tie_breaker, event))
    events.sort(key=lambda item: (item[0], item[1]))

    # One matcher per symbol prevents a BUY/SELL from different trading pairs
    # entering the same FIFO or exposure queue.
    matchers: dict[str, LegacyMatcher] = {}
    for event_time, _, event in events:
        probe = LegacyMatcher()
        order = probe.from_order_event(event)
        symbol = order.symbol or str(event.get("s", event.get("symbol", ""))).upper() or "__UNKNOWN__"
        matcher = matchers.setdefault(symbol, LegacyMatcher())
        matcher.ingest(order)
        matcher.find_not_match_order(now_ms=event_time)

    if events:
        for matcher in matchers.values():
            matcher.find_not_match_order(now_ms=max(events[-1][0], int(time.time() * 1000)))
    pending: list[dict[str, Any]] = []
    exposure: list[Any] = []
    records: list[Any] = []
    buy_unmatched_count = sell_unmatched_count = 0
    deal_profit = 0.0
    for matcher in matchers.values():
        pending.extend({"kind": "pending", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.match_orders)
        pending.extend({"kind": "buy_exposure", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.buy_not_match_orders)
        pending.extend({"kind": "sell_exposure", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.sell_not_match_orders)
        buy_unmatched_count += len(matcher.buy_not_match_orders)
        sell_unmatched_count += len(matcher.sell_not_match_orders)
        records.extend(matcher.records)
        deal_profit += matcher.deal_profit

    matches_path = day_dir / "matches.jsonl"
    with matches_path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in sorted(records, key=lambda item: item.event_time_ms):
            stream.write(json.dumps(asdict(record), ensure_ascii=False, separators=(",", ":")) + "\n")
    unmatched_path = day_dir / "unmatched.jsonl"
    with unmatched_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in pending:
            stream.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
    for matcher in matchers.values():
        exposure.extend(matcher.handle_not_match_order())
    exposure_path = day_dir / "exposure_matches.jsonl"
    with exposure_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in exposure:
            stream.write(json.dumps(asdict(item), ensure_ascii=False, separators=(",", ":")) + "\n")
    return {
        "day": day_dir.name,
        "events": len(events),
        "matches": len(records),
        "dealProfit": deal_profit,
        "pending": sum(len(item.match_orders) for item in matchers.values()),
        "buyNotMatch": buy_unmatched_count,
        "sellNotMatch": sell_unmatched_count,
        "matchesFile": str(matches_path),
        "unmatchedFile": str(unmatched_path),
        "exposureFile": str(exposure_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay one day's Binance fill JSONL")
    parser.add_argument("--day-dir", type=Path, required=True, help="runtime/<account>/YYYYMMDD")
    args = parser.parse_args()
    result = replay_day(args.day_dir.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
