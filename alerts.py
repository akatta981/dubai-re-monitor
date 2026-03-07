"""
alerts.py — Sends buy signal alerts via Gmail (SMTP) and WhatsApp (Twilio).

Credentials are loaded from environment variables (never hardcoded).
See .env.example for required variables.

Usage:
    from alerts import send_alerts
    send_alerts(signals)   # signals = list of dicts from anomaly_detector
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Emoji map for signal types ───────────────────────────────────────────────
SIGNAL_EMOJI = {
    "VOLUME_DROP": "📉",
    "PRICE_DIP": "💰",
    "SUPPLY_SURGE": "🏗️",
    "STRONG_BUY": "🚨",
}

SIGNAL_DESCRIPTION = {
    "VOLUME_DROP": "Transaction Volume Drop",
    "PRICE_DIP": "Price Per Sqm Dip",
    "SUPPLY_SURGE": "New Listings Surge",
    "STRONG_BUY": "⭐ STRONG BUY SIGNAL",
}


# ─── Formatters ───────────────────────────────────────────────────────────────

def _format_signal_text(signal: dict) -> str:
    """Plain text version of a single signal for WhatsApp/SMS."""
    emoji = SIGNAL_EMOJI.get(signal["signal_type"], "⚠️")
    desc = SIGNAL_DESCRIPTION.get(signal["signal_type"], signal["signal_type"])
    lines = [
        f"{emoji} *Dubai RE Alert — {desc}*",
        f"Area: {signal['area']}",
        f"Date: {signal['signal_date']}",
    ]
    if signal.get("notes"):
        lines.append(f"Detail: {signal['notes']}")
    if signal.get("deviation_pct") is not None:
        lines.append(f"Deviation: {signal['deviation_pct']:.1f}% from baseline")
    lines.append(f"\nDetected at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("— Dubai RE Monitor (personal research only)")
    return "\n".join(lines)


def _format_email_html(signals: list[dict]) -> str:
    """HTML email body for multiple signals."""
    rows = ""
    for s in signals:
        emoji = SIGNAL_EMOJI.get(s["signal_type"], "⚠️")
        desc = SIGNAL_DESCRIPTION.get(s["signal_type"], s["signal_type"])
        deviation = f"{s['deviation_pct']:.1f}%" if s.get("deviation_pct") is not None else "—"
        bg = "#ff4444" if s["signal_type"] == "STRONG_BUY" else "#2d3748"
        rows += f"""
        <tr style="background:{bg};">
            <td style="padding:10px;color:#fff;">{emoji} {desc}</td>
            <td style="padding:10px;color:#fff;">{s['area']}</td>
            <td style="padding:10px;color:#fff;">{s['signal_date']}</td>
            <td style="padding:10px;color:#fff;">{deviation}</td>
            <td style="padding:10px;color:#ddd;font-size:12px;">{s.get('notes', '')}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif;background:#1a202c;color:#e2e8f0;padding:20px;">
        <h2 style="color:#63b3ed;">🏙️ Dubai RE Monitor — Buy Signal Alert</h2>
        <p style="color:#a0aec0;">Detected {len(signals)} signal(s) at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
        <p style="color:#fc8181;font-size:13px;">⚠️ Personal research only. Not financial advice.</p>
        <table style="width:100%;border-collapse:collapse;margin-top:16px;">
            <thead>
                <tr style="background:#2c5282;">
                    <th style="padding:10px;color:#90cdf4;text-align:left;">Signal</th>
                    <th style="padding:10px;color:#90cdf4;text-align:left;">Area</th>
                    <th style="padding:10px;color:#90cdf4;text-align:left;">Date</th>
                    <th style="padding:10px;color:#90cdf4;text-align:left;">Deviation</th>
                    <th style="padding:10px;color:#90cdf4;text-align:left;">Detail</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        <p style="margin-top:24px;color:#718096;font-size:12px;">
            Target: Under AED 2M residential properties in monitored Dubai areas.<br>
            Thresholds: Volume drop &gt;20%, Price dip &gt;5%, Supply surge &gt;10%.
        </p>
    </body></html>"""


# ─── Gmail SMTP ───────────────────────────────────────────────────────────────

def send_email_alert(signals: list[dict]) -> bool:
    """
    Send buy signal alert email via Gmail SMTP.
    Requires SMTP_USER and SMTP_PASS in .env (use Gmail App Password).
    Returns True on success, False on failure.
    """
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    alert_to = os.getenv("ALERT_EMAIL_TO", smtp_user)

    if not smtp_user or not smtp_pass:
        logger.warning("Email not configured — set SMTP_USER and SMTP_PASS in .env")
        return False

    try:
        msg = MIMEMultipart("alternative")
        count = len(signals)
        strong = any(s["signal_type"] == "STRONG_BUY" for s in signals)
        subject = f"{'🚨 STRONG BUY' if strong else '📊 Buy Signal'} — Dubai RE Monitor ({count} alert{'s' if count > 1 else ''})"
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = alert_to

        # Plain text fallback
        plain = "\n\n".join(_format_signal_text(s) for s in signals)
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(_format_email_html(signals), "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, alert_to, msg.as_string())

        logger.info("Email alert sent to %s (%d signals)", alert_to, count)
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Gmail auth failed — make sure you're using an App Password, not your account password")
        return False
    except Exception as e:
        logger.error("Email send failed: %s", e, exc_info=True)
        return False


# ─── WhatsApp via Twilio ──────────────────────────────────────────────────────

def send_whatsapp_alert(signals: list[dict]) -> bool:
    """
    Send buy signal alert via WhatsApp using Twilio.
    Requires TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM,
    TWILIO_WHATSAPP_TO in .env.

    TWILIO_WHATSAPP_FROM = "whatsapp:+14155238886"  (Twilio sandbox number)
    TWILIO_WHATSAPP_TO   = "whatsapp:+61XXXXXXXXX"  (your number with country code)
    Returns True on success.
    """
    try:
        from twilio.rest import Client  # type: ignore
    except ImportError:
        logger.error("Twilio not installed — run: pip install twilio")
        return False

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_WHATSAPP_FROM")
    to_number = os.getenv("TWILIO_WHATSAPP_TO")

    if not all([account_sid, auth_token, from_number, to_number]):
        logger.warning("WhatsApp not configured — check TWILIO_* vars in .env")
        return False

    try:
        client = Client(account_sid, auth_token)
        success_count = 0

        for signal in signals:
            body = _format_signal_text(signal)
            # Truncate to Twilio's 1600 char limit
            if len(body) > 1600:
                body = body[:1597] + "..."
            message = client.messages.create(
                body=body,
                from_=from_number,
                to=to_number,
            )
            logger.info("WhatsApp sent: SID=%s", message.sid)
            success_count += 1

        return success_count == len(signals)

    except Exception as e:
        logger.error("WhatsApp send failed: %s", e, exc_info=True)
        return False


# ─── Combined Dispatcher ──────────────────────────────────────────────────────

def send_alerts(signals: list[dict], channels: Optional[list[str]] = None) -> dict[str, bool]:
    """
    Send alerts across configured channels.
    channels: list of "email" and/or "whatsapp" (default: both)
    Returns dict of {channel: success_bool}.
    """
    if not signals:
        logger.info("No signals to alert")
        return {}

    if channels is None:
        channels = ["email", "whatsapp"]

    results: dict[str, bool] = {}

    if "email" in channels:
        results["email"] = send_email_alert(signals)

    if "whatsapp" in channels:
        results["whatsapp"] = send_whatsapp_alert(signals)

    # Mark alerts as sent in DB
    from db import AnomalyLog, get_session
    with get_session() as session:
        for signal in signals:
            recent = (
                session.query(AnomalyLog)
                .filter_by(
                    area_canonical=signal["area"],
                    signal_type=signal["signal_type"],
                    signal_date=signal["signal_date"],
                )
                .order_by(AnomalyLog.detected_at.desc())
                .first()
            )
            if recent and not recent.alert_sent:
                recent.alert_sent = True
                sent_channels = [k for k, v in results.items() if v]
                recent.alert_channel = "+".join(sent_channels) if sent_channels else "failed"

    return results


def test_alerts() -> None:
    """Send a test alert to verify credentials are working."""
    test_signal = {
        "area": "Downtown Dubai",
        "signal_type": "STRONG_BUY",
        "signal_date": datetime.utcnow().date(),
        "signal_value": 1200.0,
        "baseline_value": 1400.0,
        "deviation_pct": -14.3,
        "notes": "TEST ALERT — This is a test from Dubai RE Monitor setup",
    }
    print("Sending test alerts...")
    results = send_alerts([test_signal])
    for channel, ok in results.items():
        status = "✅ Sent" if ok else "❌ Failed"
        print(f"  {channel}: {status}")
    if not results:
        print("  No channels configured — check your .env file")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    test_alerts()
