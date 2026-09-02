from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook

from replay.batch_replay import replay_day
from reports.excel_report import build_workbook
from reports.report_data import load_report_data


class CrossQuoteMatchingTest(unittest.TestCase):
    def test_usdt_and_usdc_symbols_match_by_id_without_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            day_dir = Path(temp_dir)
            usdt = {
                "data": {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1_000,
                    "o": {
                        "s": "AAVEUSDT", "c": "base-order", "S": "BUY",
                        "X": "FILLED", "i": 12345, "z": "1", "L": "100",
                        "n": "0.01", "N": "USDT", "T": 1_000,
                    },
                }
            }
            usdc = {
                "data": {
                    "e": "executionReport", "E": 1_001, "T": 1_001,
                    "s": "AAVEUSDC", "c": "hedge-12345", "S": "SELL",
                    "X": "FILLED", "i": 67890, "z": "1", "L": "101",
                    "n": "0.01", "N": "USDC",
                }
            }
            (day_dir / "trade_callbacks.jsonl").write_text(
                json.dumps(usdt) + "\n" + json.dumps(usdc) + "\n", encoding="utf-8"
            )

            result = replay_day(day_dir)

            self.assertEqual(result["matches"], 1)
            self.assertFalse((day_dir / "matches.jsonl").exists())
            match = json.loads((day_dir / "matches" / "AAVE.jsonl").read_text(encoding="utf-8"))
            self.assertEqual(match["current_symbol"], "AAVEUSDT")
            self.assertEqual(match["match_symbol"], "AAVEUSDC")
            self.assertAlmostEqual(match["profit"], 0.98)
            report = load_report_data(day_dir, "test")
            self.assertEqual(len(report.matches), 1)
            self.assertEqual(report.fill_counts, {"AAVEUSDT": 1, "AAVEUSDC": 1})
            summary = report.symbol_summary()
            self.assertEqual(len(summary), 1)
            self.assertEqual(summary[0]["symbol"], "AAVE")
            self.assertEqual(summary[0]["fill_count"], 2)
            workbook_path = build_workbook(report, day_dir / "report.xlsx")
            workbook = load_workbook(workbook_path, read_only=True)
            orders = workbook["成交订单明细"]
            self.assertEqual(orders["A1"].value, "交易对")
            self.assertEqual(orders["A2"].value, "AAVEUSDT_AAVEUSDC")
            self.assertEqual(orders["B1"].value, "订单ID")
            workbook.close()

    def test_unrelated_symbols_do_not_share_exposure_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            day_dir = Path(temp_dir)
            rows = [
                {"data": {"e": "executionReport", "E": 1_000, "T": 1_000,
                          "s": "AAVEUSDT", "c": "aave-buy", "S": "BUY", "X": "FILLED",
                          "i": 1, "z": "1", "L": "100", "n": "0", "N": "USDT"}},
                {"data": {"e": "executionReport", "E": 20_000, "T": 20_000,
                          "s": "SUIUSDC", "c": "sui-sell", "S": "SELL", "X": "FILLED",
                          "i": 2, "z": "1", "L": "1", "n": "0", "N": "USDC"}},
            ]
            (day_dir / "mixed.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )

            replay_day(day_dir)

            self.assertEqual((day_dir / "exposure_matches.jsonl").read_text(encoding="utf-8"), "")
            remains = (day_dir / "exposure_remain.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(remains), 2)


if __name__ == "__main__":
    unittest.main()
