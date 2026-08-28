from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


@dataclass(slots=True)
class ReportData:
    account_id: str
    day: str
    matches: list[dict[str, Any]]
    unmatched: list[dict[str, Any]]
    snapshots: list[dict[str, Any]]
    fill_counts: dict[str, int]
    exposure_matches: list[dict[str, Any]]
    funding: list[dict[str, Any]]
    baseline_equity: float | None = None
    baseline_time: str | None = None
    baseline_positions: Any = None
    current_equity: float | None = None
    current_time: str | None = None
    current_positions: Any = None

    @property
    def profit_summary(self) -> dict[str, Any]:
        """Metrics that can be reconstructed from fills and match records.

        Actual P&L is measured from the 09:30 equity baseline.  Trading and
        funding P&L come from persisted records; mark-to-market P&L comes from
        persisted equity and position snapshots when available.
        """
        current_equity = self.current_equity
        if current_equity is None and self.snapshots:
            raw = self.last_snapshot.get("actualEquity", self.last_snapshot.get("accountEquity"))
            try:
                current_equity = float(raw) if raw is not None else None
            except (TypeError, ValueError):
                current_equity = None
        actual = (current_equity - self.baseline_equity) if current_equity is not None and self.baseline_equity is not None else None
        trade_profit = sum(float(row.get("profit", 0) or 0) for row in self.matches)
        funding_profit = 0.0
        for row in self.funding:
            value = row.get("data", row)
            try:
                funding_profit += float(value.get("income", 0) or 0)
            except (TypeError, ValueError):
                pass
        vibration = self._vibration_profit()
        # A zero value is meaningful only when both the 09:30 baseline and the
        # Keep a flag so the workbook can show the per-symbol calculation.
        volatility_available = bool(
            self._position_map(self.baseline_positions)
            or self._position_map(self.current_positions)
        )
        theoretical = trade_profit + funding_profit + vibration
        if actual is None:
            actual = theoretical
        matched_qty = sum(float(row.get("quantity", 0) or 0) for row in self.matches)
        unmatched_qty = sum(float(row.get("quantity", 0) or 0) for row in self.unmatched)
        total_records = len(self.matches) + len(self.unmatched)
        profit_count = sum(1 for row in self.matches if float(row.get("profit", 0) or 0) > 0)
        loss_count = sum(1 for row in self.matches if float(row.get("profit", 0) or 0) < 0)
        return {
            "actual_profit": actual,
            "theoretical_profit": theoretical,
            "profit_difference": actual - theoretical,
            "trade_profit": trade_profit,
            "fee_profit": funding_profit,
            "funding_profit": funding_profit,
            "volatility_profit": vibration,
            "volatility_available": volatility_available,
            "vibration_breakdown": self.vibration_breakdown(),
            "matched_quantity": matched_qty,
            "unmatched_quantity": unmatched_qty,
            "total_volume": matched_qty + unmatched_qty,
            "profit_count": profit_count,
            "loss_count": loss_count,
            "matched_count": len(self.matches),
            "unmatched_count": len(self.unmatched),
            "match_ratio": len(self.matches) / total_records if total_records else None,
            "fill_count": sum(self.fill_counts.values()),
        }

    def _vibration_profit(self) -> float:
        """Mark-to-market movement of positions since the 09:30 baseline."""
        return sum(item["profit"] for item in self.vibration_breakdown())

    @staticmethod
    def _position_map(raw: Any) -> dict[str, tuple[float, float]]:
        if isinstance(raw, dict):
            raw = raw.get("data", raw.get("rows", []))
        result: dict[str, tuple[float, float]] = {}
        if not isinstance(raw, list):
            return result
        for row in raw:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", ""))
            try:
                qty = float(row.get("positionAmt", row.get("pa", 0)) or 0)
                price = float(row.get("markPrice", row.get("mp", 0)) or 0)
            except (TypeError, ValueError):
                continue
            if symbol and abs(qty) > 1e-8 and price > 0:
                result[symbol] = (qty, price)
        return result

    def vibration_breakdown(self) -> list[dict[str, Any]]:
        """Return mark-to-market P&L per symbol for all symbols in either snapshot."""
        initial = self._position_map(self.baseline_positions)
        current = self._position_map(self.current_positions)
        rows: list[dict[str, Any]] = []
        for symbol in sorted(set(initial) | set(current)):
            initial_qty, initial_price = initial.get(symbol, (0.0, 0.0))
            current_qty, current_price = current.get(symbol, (0.0, 0.0))
            profit = 0.0
            if initial_qty and current_qty and initial_qty * current_qty > 0:
                effective = min(abs(initial_qty), abs(current_qty))
                profit = effective * (1 if initial_qty > 0 else -1) * (current_price - initial_price)
            rows.append({
                "symbol": symbol,
                "baseline_quantity": initial_qty,
                "current_quantity": current_qty,
                "baseline_mark_price": initial_price,
                "current_mark_price": current_price,
                "profit": profit,
            })
        return rows

    @property
    def last_snapshot(self) -> dict[str, Any]:
        return self.snapshots[-1].get("data", self.snapshots[-1]) if self.snapshots else {}

    def symbol_summary(self) -> list[dict[str, Any]]:
        summary: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "symbol": "", "fill_count": 0, "match_count": 0, "matched_quantity": 0.0,
            "profit": 0.0, "fees": 0.0, "unmatched_quantity": 0.0,
        })
        for symbol, count in self.fill_counts.items():
            summary[symbol].update(symbol=symbol, fill_count=count)
        for row in self.matches:
            symbol = str(row.get("current_symbol") or row.get("match_symbol") or "UNKNOWN")
            item = summary[symbol]
            item["symbol"] = symbol
            item["match_count"] += 1
            item["matched_quantity"] += float(row.get("quantity", 0) or 0)
            item["profit"] += float(row.get("profit", 0) or 0)
            item["fees"] += float(row.get("current_fee", 0) or 0) + float(row.get("match_fee", 0) or 0)
        for row in self.unmatched:
            symbol = str(row.get("symbol", "UNKNOWN"))
            summary[symbol]["unmatched_quantity"] += float(row.get("quantity", 0) or 0)
        return sorted(summary.values(), key=lambda x: x["symbol"])


def load_report_data(day_dir: Path, account_id: str) -> ReportData:
    fill_counts: dict[str, int] = {}
    for path in sorted(day_dir.glob("*.jsonl")):
        if path.name in {"account_info.jsonl", "matches.jsonl", "unmatched.jsonl", "exposure_matches.jsonl", "funding.jsonl"}:
            continue
        fill_counts[path.stem] = len(read_jsonl(path))
    snapshots = [row for row in read_jsonl(day_dir / "account_info.jsonl") if row.get("recordType") == "account_snapshot"]
    resets = [row for row in read_jsonl(day_dir / "account_info.jsonl") if row.get("recordType") == "equity_reset"]
    baseline = resets[-1] if resets else {}
    state: dict[str, Any] = {}
    state_path = day_dir / "equity.json"
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            state = {}
    try:
        baseline_equity = float(baseline["equity"]) if baseline else None
    except (TypeError, ValueError):
        baseline_equity = None
    return ReportData(
        account_id=account_id,
        day=day_dir.name,
        matches=read_jsonl(day_dir / "matches.jsonl"),
        unmatched=read_jsonl(day_dir / "unmatched.jsonl"),
        snapshots=snapshots,
        fill_counts=fill_counts,
        exposure_matches=read_jsonl(day_dir / "exposure_matches.jsonl"),
        funding=read_jsonl(day_dir / "funding.jsonl"),
        baseline_equity=(float(state["baselineEquity"]) if state.get("baselineEquity") is not None else baseline_equity),
        baseline_time=state.get("baselineTime") or baseline.get("resetTime"),
        baseline_positions=state.get("baselinePositions", baseline.get("positions", [])),
        current_equity=(float(state["latestEquity"]) if state.get("latestEquity") is not None else None),
        current_time=state.get("latestTime"),
        current_positions=state.get("latestPositions", []),
    )
