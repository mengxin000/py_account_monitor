"""Isolated OKX journals, bucketed at 09:30 China time even on UTC hosts."""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHINA = timezone(timedelta(hours=8))


def trading_day(ms):
    now = datetime.fromtimestamp(ms / 1000, CHINA)
    return (now - timedelta(days=(now.hour, now.minute) < (9, 30))).strftime("%Y%m%d")


def day_start_ms(ms):
    now = datetime.fromtimestamp(ms / 1000, CHINA)
    start = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now < start:
        start -= timedelta(days=1)
    return int(start.timestamp() * 1000)


class OKXJournal:
    def __init__(self, root):
        self.root = Path(root)
        self.lock = threading.Lock()

    def path(self, filename, ms):
        return self.root / trading_day(ms) / filename

    def append(self, filename, record, ms):
        path = self.path(filename, ms)
        with self.lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Do not acknowledge a failed append. Caller fails visibly and REST
            # recovery on restart re-reads trade history with durable dedup IDs.
            for attempt in range(5):
                try:
                    with path.open("a+b") as stream:
                        stream.seek(0, os.SEEK_END)
                        if stream.tell():
                            stream.seek(-1, os.SEEK_END)
                            if stream.read(1) != b"\n":
                                stream.write(b"\n")
                        stream.write((json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
                        stream.flush()
                    return
                except PermissionError:
                    if attempt == 4:
                        raise
                    time.sleep(.05 * (attempt + 1))

    def snapshot(self, filename, record, ms):
        path = self.path(filename, ms)
        with self.lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(f".{filename}.{uuid.uuid4().hex}.tmp")
            try:
                temp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
                for attempt in range(5):
                    try:
                        os.replace(temp, path)
                        return
                    except PermissionError:
                        if attempt == 4:
                            raise
                        time.sleep(.05 * (attempt + 1))
            finally:
                temp.unlink(missing_ok=True)
