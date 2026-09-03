from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from core.jsonl_io import iter_jsonl


class JsonlReaderTest(unittest.TestCase):
    def test_reads_two_objects_concatenated_on_one_physical_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trade_callbacks.jsonl"
            path.write_text('{"id":1}{"id":2}\n{"id":3}\n', encoding="utf-8")
            rows = [row for _, _, row in iter_jsonl(path)]
            self.assertEqual(rows, [{"id": 1}, {"id": 2}, {"id": 3}])


if __name__ == "__main__":
    unittest.main()
