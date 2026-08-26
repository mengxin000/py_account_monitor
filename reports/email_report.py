from __future__ import annotations

import mimetypes
import smtplib
import time
from email.message import EmailMessage
from pathlib import Path

try:
    from ..config.settings import EmailSettings
except ImportError:  # running from the project directory
    from config.settings import EmailSettings


def send_reports(settings: EmailSettings, subject: str, html_body: str, attachments: list[Path]) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.sender
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
    recipients = list(settings.recipients) + list(settings.cc)

    def deliver(port: int, use_ssl: bool, use_starttls: bool) -> None:
        if use_ssl:
            with smtplib.SMTP_SSL(settings.smtp_host, port, timeout=30) as smtp:
                smtp.login(settings.username, settings.password)
                smtp.send_message(message, to_addrs=recipients)
            return
        with smtplib.SMTP(settings.smtp_host, port, timeout=30) as smtp:
            smtp.ehlo()
            if use_starttls:
                smtp.starttls()
                smtp.ehlo()
            smtp.login(settings.username, settings.password)
            smtp.send_message(message, to_addrs=recipients)

    # Try the configured transport first, then the other standard Gmail
    # transport.  Connection failures are retried; authentication failures
    # are raised immediately so a bad app password is not hidden.
    candidates = [(settings.smtp_port, settings.smtp_port == 465, settings.use_starttls)]
    if settings.smtp_port == 587:
        candidates.append((465, True, False))
    elif settings.smtp_port == 465:
        candidates.append((587, False, True))
    errors: list[str] = []
    for port, use_ssl, use_starttls in candidates:
        for attempt in range(1, 3):
            try:
                deliver(port, use_ssl, use_starttls)
                return
            except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused):
                raise
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, ConnectionError, TimeoutError, OSError) as exc:
                errors.append(f"{settings.smtp_host}:{port} attempt {attempt}: {exc}")
                if attempt < 2:
                    time.sleep(2)
    raise RuntimeError("; ".join(errors) or "SMTP delivery failed")


def send_report(settings: EmailSettings, subject: str, html_body: str, attachment: Path) -> None:
    """Backward-compatible single-attachment wrapper."""
    send_reports(settings, subject, html_body, [attachment])
