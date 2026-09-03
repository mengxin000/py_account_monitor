"""Replay one trading day's compact fill JSONL with the legacy matcher.

Run this before the half-hour email/report. It reads the account-wide callback
stream, groups orders by underlying asset, and deterministically rewrites the
derived ``matches/<base>.jsonl`` files.
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
    from ..core.jsonl_io import iter_jsonl
except ImportError:
    from core.legacy_matching import LegacyMatcher  # type: ignore[no-redef]
    from core.jsonl_io import iter_jsonl  # type: ignore[no-redef]


def _event_time(event: dict[str, Any]) -> int:
    order = event.get("o") if isinstance(event.get("o"), dict) else event
    value = order.get("T", order.get("E", event.get("E", 0)))
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _exposure_key(symbol: str) -> str:
    """Keep Exposure netting within one underlying, across USDT/USDC quotes."""
    normalized = symbol.strip().upper()
    for quote in ("USDT", "USDC"):
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return normalized[: -len(quote)]
    return normalized or "__UNKNOWN__"


def replay_day(day_dir: Path) -> dict[str, Any]:
    events: list[tuple[int, str, int, int, dict[str, Any]]] = []
    generated_files = {
        "account_info.jsonl",
        "funding.jsonl",
        "matches.jsonl",
        "unmatched.jsonl",
        "exposure_matches.jsonl",
        "exposure_remain.jsonl",
    }
    for path in sorted(day_dir.glob("*.jsonl")):
        # Replay only original per-symbol trade callback files. Feeding a
        # generated residual back into the matcher creates a phantom order
        # with no Binance client/system ID on every subsequent report run.
        if path.name in generated_files:
            continue
        for line_no, object_no, record in iter_jsonl(path):
            event = record.get("data", record)
            if not isinstance(event, dict):
                continue
            event_time = _event_time(event)
            events.append((event_time, path.name, line_no, object_no, event))
    # Do not combine the line number into a string tie-breaker: lexical order
    # places line 10 before line 9. Binance can emit PARTIALLY_FILLED and
    # FILLED callbacks with the same millisecond timestamp, so preserve their
    # numeric JSONL order to accumulate quantity and commission correctly.
    events.sort(key=lambda item: (item[0], item[1], item[2], item[3]))

    # One matcher per underlying: AAVEUSDT and AAVEUSDC share all direct and
    # Exposure queues, while an unrelated asset can never enter those queues.
    matchers: dict[str, LegacyMatcher] = {}
    for event_time, _, _, _, event in events:
        probe = LegacyMatcher()
        order = probe.from_order_event(event)
        matcher = matchers.setdefault(_exposure_key(order.symbol), LegacyMatcher())
        matcher.ingest(order)
        matcher.find_not_match_order(now_ms=event_time)

    if events:
        expiry_time = max(events[-1][0], int(time.time() * 1000))
        for matcher in matchers.values():
            matcher.find_not_match_order(now_ms=expiry_time)
    pending: list[dict[str, Any]] = []
    exposure_remain: list[dict[str, Any]] = []
    exposure: list[Any] = []
    records: list[Any] = []
    buy_unmatched_count = sell_unmatched_count = 0
    deal_profit = 0.0
    # unmatched.jsonl is an audit trail before Exposure reconciliation.
    records_by_base: dict[str, list[Any]] = {}
    for base, matcher in matchers.items():
        pending.extend({"kind": "pending", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.match_orders)
        pending.extend({"kind": "buy_exposure", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.buy_not_match_orders)
        pending.extend({"kind": "sell_exposure", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.sell_not_match_orders)
        records.extend(matcher.records)
        records_by_base[base] = list(matcher.records)

        exposure.extend(matcher.handle_not_match_order())
        exposure_remain.extend({"kind": "buy_exposure_remain", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.buy_not_match_orders)
        exposure_remain.extend({"kind": "sell_exposure_remain", "small_residual": abs(o.quantity) <= 1e-8, **asdict(o)} for o in matcher.sell_not_match_orders)
        buy_unmatched_count += len(matcher.buy_not_match_orders)
        sell_unmatched_count += len(matcher.sell_not_match_orders)
    deal_profit = sum(item.deal_profit for item in matchers.values())

    matches_dir = day_dir / "matches"
    matches_dir.mkdir(parents=True, exist_ok=True)
    expected_match_files = {f"{base}.jsonl" for base, rows in records_by_base.items() if rows}
    for stale_path in matches_dir.glob("*.jsonl"):
        if stale_path.name not in expected_match_files:
            stale_path.unlink()
    for base, base_records in records_by_base.items():
        if not base_records:
            continue
        with (matches_dir / f"{base}.jsonl").open("w", encoding="utf-8", newline="\n") as stream:
            for record in sorted(base_records, key=lambda item: item.event_time_ms):
                stream.write(json.dumps(asdict(record), ensure_ascii=False, separators=(",", ":")) + "\n")
    unmatched_path = day_dir / "unmatched.jsonl"
    with unmatched_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in pending:
            stream.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
    exposure_path = day_dir / "exposure_matches.jsonl"
    with exposure_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in exposure:
            stream.write(json.dumps(asdict(item), ensure_ascii=False, separators=(",", ":")) + "\n")
    exposure_remain_path = day_dir / "exposure_remain.jsonl"
    with exposure_remain_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in exposure_remain:
            stream.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
    return {
        "day": day_dir.name,
        "events": len(events),
        "matches": len(records),
        "dealProfit": deal_profit,
        "pending": sum(len(item.match_orders) for item in matchers.values()),
        "buyNotMatch": buy_unmatched_count,
        "sellNotMatch": sell_unmatched_count,
        "matchesDir": str(matches_dir),
        "unmatchedFile": str(unmatched_path),
        "exposureFile": str(exposure_path),
        "exposureRemainFile": str(exposure_remain_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay one day's Binance fill JSONL")
    parser.add_argument("--day-dir", type=Path, required=True, help="runtime/<account>/YYYYMMDD")
    args = parser.parse_args()
    result = replay_day(args.day_dir.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
