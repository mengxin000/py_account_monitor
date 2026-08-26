from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class EmailSettings:
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    sender: str
    recipients: tuple[str, ...]
    cc: tuple[str, ...] = ()
    use_starttls: bool = True
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
    address = str(data.get("address", "smtp://localhost:25"))
    # Supports the existing account_zdl/config/email.json shape.
    address = address.replace("smtps://", "").replace("smtp://", "")
    host, _, port_text = address.partition(":")
    port = int(port_text or (465 if str(data.get("address", "")).startswith("smtps") else 25))
    return EmailSettings(
        smtp_host=host,
        smtp_port=port,
        username=str(data.get("user", "")),
        password=str(data.get("pass", "")),
        sender=str(data.get("sender") or data.get("user", "")),
        recipients=_split_addresses(data.get("recipients", data.get("sendUser"))),
        cc=_split_addresses(data.get("cc", data.get("ccUser"))),
        use_starttls=bool(data.get("use_starttls", port != 465)),
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
