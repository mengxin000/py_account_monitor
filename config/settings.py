"""Email settings and the optional standalone report configuration loader."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class EmailMailbox:
    mailbox_id: str
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    sender: str
    use_starttls: bool = True
    daily_recipient_limit: int = 288


@dataclass(frozen=True, slots=True)
class EmailSettings:
    mailboxes: tuple[EmailMailbox, ...]
    recipients: tuple[str, ...]
    cc: tuple[str, ...] = ()
    subject_prefix: str = "Binance账户监控"


@dataclass(frozen=True, slots=True)
class ReportSettings:
    account_id: str
    day_dir: Path
    output_dir: Path
    email: EmailSettings | None = None


def _split_addresses(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(x).strip() for x in value if str(x).strip())
    return tuple(x.strip() for x in str(value or "").split(",") if x.strip())


def load_email_settings(path: Path) -> EmailSettings:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_mailboxes = data.get("mailboxes")
    if raw_mailboxes is None:
        # Backward compatibility for the original single-mailbox format.
        raw_mailboxes = [{
            "id": data.get("id") or data.get("sender") or data.get("user") or "primary",
            "address": data.get("address", "smtp://localhost:25"),
            "user": data.get("user", ""), "pass": data.get("pass", ""),
            "sender": data.get("sender") or data.get("user", ""),
            "use_starttls": data.get("use_starttls"),
            "daily_recipient_limit": data.get("daily_recipient_limit", 288),
        }]
    if not isinstance(raw_mailboxes, list) or not raw_mailboxes:
        raise ValueError("email mailboxes must be a non-empty list")
    mailboxes = []
    seen_ids = set()
    for index, item in enumerate(raw_mailboxes, 1):
        if not isinstance(item, dict):
            raise ValueError("each email mailbox must be an object")
        address = str(item.get("address", "smtp://localhost:25"))
        scheme = "smtps" if address.startswith("smtps://") else "smtp"
        address = address.removeprefix("smtps://").removeprefix("smtp://")
        host, _, port_text = address.partition(":")
        port = int(port_text or (465 if scheme == "smtps" else 25))
        mailbox_id = str(item.get("id") or item.get("sender") or item.get("user") or f"mailbox-{index}").strip()
        if not mailbox_id or mailbox_id in seen_ids:
            raise ValueError("email mailbox ids must be non-empty and unique")
        seen_ids.add(mailbox_id)
        limit = int(item.get("daily_recipient_limit", 288))
        if limit < 1:
            raise ValueError("daily_recipient_limit must be a positive integer")
        use_starttls = item.get("use_starttls")
        if use_starttls is None:
            use_starttls = port != 465
        mailboxes.append(EmailMailbox(
            mailbox_id=mailbox_id, smtp_host=host, smtp_port=port,
            username=str(item.get("user", "")), password=str(item.get("pass", "")),
            sender=str(item.get("sender") or item.get("user", "")),
            use_starttls=bool(use_starttls), daily_recipient_limit=limit,
        ))
    return EmailSettings(
        mailboxes=tuple(mailboxes),
        recipients=_split_addresses(data.get("recipients", data.get("sendUser"))),
        cc=_split_addresses(data.get("cc", data.get("ccUser"))),
        subject_prefix=str(data.get("subject_prefix") or data.get("sendTitle", "Binance账户监控")),
    )


def load_report_settings(path: Path) -> ReportSettings:
    data = json.loads(path.read_text(encoding="utf-8"))
    base = path.parent.parent
    day_dir = Path(data["day_dir"])
    output_dir = Path(data.get("output_dir", "reports"))
    if not day_dir.is_absolute():
        day_dir = base / day_dir
    if not output_dir.is_absolute():
        output_dir = base / output_dir
    email = None
    email_file = data.get("email_file")
    if email_file:
        email_path = path.parent / email_file
        if email_path.exists():
            email = load_email_settings(email_path)
    return ReportSettings(str(data.get("account_id", "account")), day_dir, output_dir, email)
