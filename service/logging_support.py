"""Rotate lifecycle logs at the trading-day boundary, not process restart."""
import logging
from datetime import datetime, timedelta


class TradingDayFileHandler(logging.FileHandler):
    def __init__(self, directory):
        self.directory = directory
        self.day = self.current_day()
        super().__init__(directory / f"{self.day}run.txt", encoding="utf-8")

    @staticmethod
    def current_day():
        now = datetime.now()
        if (now.hour, now.minute) < (9,30):
            now -= timedelta(days=1)
        return now.strftime("%Y%m%d")

    def emit(self, record):
        day = self.current_day()
        if day != self.day:
            if self.stream:
                self.stream.close()
            self.day = day
            self.baseFilename = str((self.directory / f"{day}run.txt").resolve())
            self.stream = self._open()
        super().emit(record)
