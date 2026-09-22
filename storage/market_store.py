"""Single-worker market archive: natural dates, hourly gzip, bounded retention."""
from __future__ import annotations

import gzip
import json
import logging
import os
import re
import shutil
import stat
from datetime import datetime, timedelta
from pathlib import Path

LOGGER = logging.getLogger("binance_market_store")


class MarketStore:
    def __init__(self, root: Path, retention_days: int = 3):
        self.root = root.resolve()
        self.retention_days = max(1, retention_days)

    def write(self, market: str, symbol: str, record: dict, metadata: bool = False):
        self.write_batch([(market, symbol, record, metadata)])

    def write_batch(self, records):
        grouped = {}
        for market, symbol, record, metadata in records:
            if market not in {"spot", "futures"} or not re.fullmatch(r"[A-Z0-9_]+", symbol):
                raise ValueError("invalid market archive key")
            # Bucket using capture time, never writer/parse time.
            micros = record.get("receivedTimeUs", record["receivedTimeMs"] * 1000)
            moment = datetime.fromtimestamp(micros // 1_000_000)
            directory = self.root / moment.strftime("%Y%m%d") / market / symbol
            path = directory / ("windows.jsonl" if metadata else moment.strftime("%H.jsonl"))
            grouped.setdefault(path, []).append(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        for path, lines in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            compressed = path.with_suffix(".jsonl.gz")
            opener = gzip.open if compressed.exists() else open
            with opener(compressed if compressed.exists() else path, "at", encoding="utf-8") as stream:
                stream.writelines(lines)

    def maintain(self, now: datetime | None = None):
        now = now or datetime.now()
        cutoff = (now.date() - timedelta(days=self.retention_days - 1))
        if not self.root.exists():
            return
        for directory in self.root.iterdir():
            if directory.is_symlink() or (getattr(directory.lstat(), "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)) or not directory.is_dir() or not re.fullmatch(r"\d{8}", directory.name):
                continue
            try:
                day = datetime.strptime(directory.name, "%Y%m%d").date()
            except ValueError:
                continue
            if day < cutoff:
                if directory.resolve().parent != self.root:
                    raise ValueError("retention path escaped raw directory")
                shutil.rmtree(directory)
                LOGGER.info("expired market archive deleted path=%s", directory)
                continue
            for path in directory.glob("*/*/[0-2][0-9].jsonl"):
                if path.is_symlink() or not path.resolve().is_relative_to(self.root):
                    continue
                hour = datetime.strptime(directory.name + path.stem, "%Y%m%d%H")
                # Let 30-second lookback windows finish crossing the boundary.
                if now < hour + timedelta(hours=1, seconds=60):
                    continue
                target = path.with_suffix(".jsonl.gz")
                temp = target.with_suffix(".gz.tmp")
                with path.open("rb") as src, gzip.open(temp, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                os.replace(temp, target)
                path.unlink()
