from __future__ import annotations

import json
import logging
import mimetypes
import os
import smtplib
import threading
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

try:
    from ..config.settings import EmailMailbox, EmailSettings
except ImportError:  # running from the project directory
    from config.settings import EmailMailbox, EmailSettings

LOGGER = logging.getLogger("binance_email_report")
_QUOTA_LOCK = threading.Lock()


def _rolling_usage(path: Path, now: datetime) -> dict[str, int]:
    usage: dict[str, int] = {}
    cutoff = now - timedelta(hours=24)
    if not path.exists():
        return usage
    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                # Old ledgers used server-local timestamps without an offset.
                # astimezone interprets those in the host timezone.
                reserved_at = datetime.fromisoformat(row["reserved_at"]).astimezone(timezone.utc)
                if reserved_at > cutoff:
                    mailbox_id = str(row["mailbox_id"])
                    count = int(row["recipient_count"])
                    if count < 0:
                        raise ValueError("negative recipient count")
                    usage[mailbox_id] = usage.get(mailbox_id, 0) + count
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(f"invalid email quota ledger {path} line={line_number}") from exc
    return usage


def _choose_and_reserve(settings: EmailSettings, recipient_count: int, quota_path: Path,
                        *, now: datetime | None = None) -> tuple[EmailMailbox, int]:
    with _QUOTA_LOCK:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        usage = _rolling_usage(quota_path, now)
        for mailbox in settings.mailboxes:
            current = usage.get(mailbox.mailbox_id, 0)
            if current + recipient_count <= mailbox.daily_recipient_limit:
                row = {
                    "quota_day": now.astimezone().date().isoformat(),
                    "mailbox_id": mailbox.mailbox_id,
                    "recipient_count": recipient_count,
                    "reserved_at": now.isoformat(),
                }
                quota_path.parent.mkdir(parents=True, exist_ok=True)
                with quota_path.open("a", encoding="utf-8", newline="\n") as output:
                    output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
                return mailbox, current + recipient_count
    raise RuntimeError(f"no email mailbox has enough rolling 24h quota; recipients={recipient_count}")


def _deliver(mailbox: EmailMailbox, message: EmailMessage, recipients: list[str]) -> None:
    send_started = False
    accepted = False

    def deliver(port: int, use_ssl: bool, use_starttls: bool) -> None:
        nonlocal send_started, accepted
        smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_class(mailbox.smtp_host, port, timeout=30) as smtp:
            if not use_ssl:
                smtp.ehlo()
                if use_starttls:
                    smtp.starttls()
                    smtp.ehlo()
            smtp.login(mailbox.username, mailbox.password)
            # A failure here may arrive after the server accepted DATA.
            send_started = True
            smtp.send_message(message, to_addrs=recipients)
            accepted = True

    candidates = [(mailbox.smtp_port, mailbox.smtp_port == 465, mailbox.use_starttls)]
    if mailbox.smtp_port == 587:
        candidates.append((465, True, False))
    elif mailbox.smtp_port == 465:
        candidates.append((587, False, True))
    errors: list[str] = []
    for port, use_ssl, use_starttls in candidates:
        for attempt in range(1, 3):
            try:
                deliver(port, use_ssl, use_starttls)
                return
            except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused):
                raise
            except smtplib.SMTPResponseException as exc:
                if accepted:
                    LOGGER.warning("email accepted but SMTP session close failed mailbox=%s code=%s",
                                   mailbox.mailbox_id, exc.smtp_code)
                    return
                if send_started or not isinstance(exc, smtplib.SMTPConnectError):
                    raise
                errors.append(f"{mailbox.smtp_host}:{port} attempt {attempt}: {exc}")
            except OSError as exc:
                if accepted:
                    LOGGER.warning("email accepted but SMTP session close failed mailbox=%s error=%s",
                                   mailbox.mailbox_id, type(exc).__name__)
                    return
                if send_started:
                    raise
                errors.append(f"{mailbox.smtp_host}:{port} attempt {attempt}: {exc}")
            if attempt < 2:
                time.sleep(2)
    raise RuntimeError("; ".join(errors) or "SMTP delivery failed")


def send_reports(settings: EmailSettings, subject: str, html_body: str, attachments: list[Path],
                 *, quota_state_path: Path | None = None) -> None:
    recipients = list(settings.recipients) + list(settings.cc)
    if not recipients:
        raise ValueError("email requires at least one To or Cc recipient")
    message = EmailMessage()
    message["Subject"] = subject
    message["To"] = ", ".join(settings.recipients)
    if settings.cc:
        message["Cc"] = ", ".join(settings.cc)
    message.set_content("请使用支持HTML的邮件客户端查看此报告。")
    message.add_alternative(html_body, subtype="html")
    for attachment in attachments:
        data = attachment.read_bytes()
        content_type, _ = mimetypes.guess_type(str(attachment))
        maintype, subtype = (content_type or "application/octet-stream").split("/", 1)
        message.add_attachment(data, maintype=maintype, subtype=subtype, filename=attachment.name)

    quota_path = quota_state_path or Path("runtime/email_pool_usage.jsonl")
    mailbox, used = _choose_and_reserve(settings, len(recipients), quota_path)
    message["From"] = mailbox.sender
    try:
        _deliver(mailbox, message, recipients)
    except Exception:
        LOGGER.exception("email send failed mailbox=%s quota=%s/%s recipients=%d",
                         mailbox.mailbox_id, used, mailbox.daily_recipient_limit, len(recipients))
        raise
    LOGGER.info("email sent mailbox=%s quota=%s/%s recipients=%d",
                mailbox.mailbox_id, used, mailbox.daily_recipient_limit, len(recipients))


def send_report(settings: EmailSettings, subject: str, html_body: str, attachment: Path) -> None:
    """Backward-compatible single-attachment wrapper."""
    send_reports(settings, subject, html_body, [attachment])
