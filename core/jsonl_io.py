"""Tolerant readers for append-only JSONL runtime files."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("binance_jsonl")


def iter_jsonl(path: Path) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield every JSON object, including concatenated objects on one line.

    A process stopped after writing a complete object but before its newline
    can leave ``}{`` when the next run appends. ``raw_decode`` recovers both
    objects instead of making the complete account report fail.
    """
    decoder = json.JSONDecoder()
    for line_no, physical_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        cursor = 0
        object_no = 0
        while cursor < len(physical_line):
            while cursor < len(physical_line) and physical_line[cursor].isspace():
                cursor += 1
            if cursor >= len(physical_line):
                break
            try:
                value, end = decoder.raw_decode(physical_line, cursor)
            except json.JSONDecodeError as exc:
                LOGGER.warning(
                    "invalid JSONL fragment skipped path=%s line=%d column=%d error=%s",
                    path,
                    line_no,
                    exc.colno,
                    exc.msg,
                )
                break
            cursor = end
            object_no += 1
            if isinstance(value, dict):
                yield line_no, object_no, value

