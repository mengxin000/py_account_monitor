"""Incremental journal projection. Never writes analysis artifacts."""
import copy
import json
import time
from dataclasses import asdict
from pathlib import Path

try:
    from ..core.legacy_matching import LegacyMatcher
    from ..replay.batch_replay import _event_time, _exposure_key
except ImportError:
    from core.legacy_matching import LegacyMatcher
    from replay.batch_replay import _event_time, _exposure_key


class Journal:
    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0
        self.inode = None

    def read(self, limit=None):
        if not self.path.exists():
            return [], False
        stat = self.path.stat()
        identity = (stat.st_dev, stat.st_ino)
        reset = self.inode is not None and (identity != self.inode or stat.st_size < self.offset)
        offset = 0 if reset else self.offset
        rows = []
        # Fixed byte boundary: never chase a growing file indefinitely.
        limit = stat.st_size if limit is None else min(limit, stat.st_size)
        with self.path.open("rb") as stream:
            stream.seek(offset)
            while stream.tell() < limit:
                start = stream.tell()
                line = stream.readline(limit - start)
                if not line.endswith(b"\n"): break
                if line.strip():
                    try:
                        text = line.decode("utf-8-sig")
                        decoder, cursor = json.JSONDecoder(), 0
                        while cursor < len(text):
                            if text[cursor].isspace():
                                cursor += 1
                                continue
                            row, end = decoder.raw_decode(text,cursor)
                            if not isinstance(row,dict): raise ValueError("journal row is not an object")
                            rows.append((start+len(text[:cursor].encode("utf-8")),row))
                            cursor = end
                    except (ValueError, UnicodeError) as exc:
                        raise ValueError(f"invalid journal record at byte {start}") from exc
                offset = stream.tell()
        self.offset, self.inode = offset, identity
        return rows, reset


def event_identity(row):
    event = row.get("data", row)
    order = event.get("o") if isinstance(event.get("o"), dict) else event
    scope = row.get("accountScope") or event.get("fs") or event.get("e")
    # A terminal cancellation and its preceding fill are distinct callbacks.
    return (row.get("accountId"), scope, order.get("s"), str(order.get("i")),
            str(order.get("t")), order.get("x"), order.get("X"), order.get("z"), order.get("T"))


class AccountProjection:
    def __init__(self, root):
        self.root = Path(root)
        self.journal = Journal(self.root / "trade_callbacks.jsonl")
        self.events = []
        self.seen = set()
        self.matchers = {}
        self.last_event = -1
        self.last_check = 0
        self.check_status = "恢复中"
        self.version = 0
        self.snapshot = {}
        self.next_expiry = 0

    @staticmethod
    def ingest(matchers, row):
        event = dict(row.get("data", row))
        for key in ("exchange", "accountId", "accountScope"):
            if key in row: event[key] = row[key]
        order = LegacyMatcher.from_order_event(event)
        matcher = matchers.setdefault(_exposure_key(order.symbol), LegacyMatcher())
        matcher.ingest(order)
        matcher.find_not_match_order(now_ms=_event_time(event))

    def rebuild(self, records):
        matchers = {}
        for _, _, row in sorted(records, key=lambda item: (item[0], item[1])):
            self.ingest(matchers, row)
        return matchers

    @staticmethod
    def view(matchers, now):
        matches, unmatched, exposures, remain = [], [], [], []
        for base, matcher in matchers.items():
            # Expiration / Exposure in a clone preserves event-time ordering of
            # the canonical state when late callbacks arrive.
            clone = LegacyMatcher()
            for name in ("match_orders", "buy_not_match_orders", "sell_not_match_orders", "no_save_orders"):
                setattr(clone, name, copy.deepcopy(getattr(matcher, name)))
            clone.find_not_match_order(now_ms=now)
            matches.extend(asdict(x) for x in matcher.records)
            unmatched.extend({**asdict(x), "base":base} for x in clone.match_orders + clone.buy_not_match_orders + clone.sell_not_match_orders)
            exposures.extend({**asdict(x), "base":base} for x in clone.handle_not_match_order())
            remain.extend(asdict(x) for x in clone.buy_not_match_orders + clone.sell_not_match_orders)
        matches.sort(key=lambda row: row["event_time_ms"], reverse=True)
        return {"matches":matches, "unmatched":unmatched, "exposures":exposures, "remain":remain}

    def refresh(self, now=None):
        try:
            return self._refresh(now)
        except Exception:
            # A cursor must never acknowledge records whose application failed.
            # Keep the last published snapshot, but recover canonical state from
            # the journal on the next attempt instead of silently losing fills.
            self.journal = Journal(self.root / "trade_callbacks.jsonl")
            self.events, self.seen, self.matchers, self.last_event = [], set(), {}, -1
            self.last_check = -3600
            raise

    def _refresh(self, now=None):
        now = now or int(time.time()*1000)
        rows, reset = self.journal.read()
        if reset:
            self.events, self.seen, self.matchers, self.last_event = [], set(), {}, -1
        added = []
        for offset, row in rows:
            identity = event_identity(row)
            if identity in self.seen: continue
            self.seen.add(identity)
            item = (_event_time(row.get("data",row)), offset, row)
            self.events.append(item)
            added.append(item)
        late = any(t < self.last_event for t, _, _ in added)
        if late:
            self.matchers = self.rebuild(self.events)
        else:
            for _, _, row in sorted(added, key=lambda item:(item[0],item[1])):
                self.ingest(self.matchers,row)
        if added: self.last_event = max(self.last_event, max(t for t,_,_ in added))
        audit_due = time.monotonic() - self.last_check >= 3600 or reset or not self.snapshot
        if not added and not audit_due and now < self.next_expiry:
            return self.snapshot
        result = self.view(self.matchers, max(now,self.last_event))
        if audit_due:
            # Independently re-read the durable journal, not the in-memory list.
            audit = AccountProjection(self.root)
            checked, _ = audit.journal.read(limit=self.journal.offset)
            # Same byte cutoff as current projection.
            audit_records, audit_seen = [], set()
            for offset,row in checked:
                if offset >= self.journal.offset: break
                identity = event_identity(row)
                if identity in audit_seen: continue
                audit_seen.add(identity)
                audit_records.append((_event_time(row.get("data",row)),offset,row))
            rebuilt = self.rebuild(audit_records)
            corrected = self.view(rebuilt,max(now,self.last_event))
            self.check_status = "一致" if corrected == result else "差异已重建"
            self.matchers, self.events, self.seen = rebuilt, audit_records, audit_seen
            self.last_event = max((t for t,_,_ in audit_records),default=-1)
            result = corrected
            self.last_check = time.monotonic()
            self.checked_at = now
        self.next_expiry = min((o.time_ms+m.timeout_ms+1 for m in self.matchers.values()
                                for o in m.match_orders if o.time_ms+m.timeout_ms+1>now), default=float("inf"))
        self.version += 1
        ordinary = sum(x["profit"] for x in result["matches"])
        exposure = sum(x["profit_delta"] for x in result["exposures"])
        self.snapshot = {**result, "version":self.version, "asOf":now, "journalOffset":self.journal.offset,
                         "checkedAt":getattr(self,"checked_at",None), "checkStatus":self.check_status,
                         "tradeProfit":ordinary+exposure, "pairProfit":ordinary, "exposureProfit":exposure,
                         "pairCount":len(result["matches"]), "unmatchedCount":len(result["unmatched"]),
                         "recentTrades":[row for _,_,row in sorted(self.events,key=lambda x:(x[0],x[1]),reverse=True)[:100]]}
        return self.snapshot
