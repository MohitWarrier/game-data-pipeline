"""
Pipeline Failure Notifications
==============================
Sends an email when the pipeline run status is not "success".
Uses Gmail SMTP with an App Password (not your regular password).

Setup:
  1. Go to https://myaccount.google.com/apppasswords
  2. Generate an app password for "Mail"
  3. Add to .env:
       ALERT_EMAIL_FROM=your_gmail@gmail.com
       ALERT_EMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
       ALERT_EMAIL_TO=where_to_send@example.com

If these variables are not set, notifications are silently skipped.
The pipeline never crashes because of a notification failure.
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ingest.logger import get_logger

logger = get_logger("notify")


def _get_email_config():
    """Load email config from environment. Returns None if not configured."""
    email_from = os.getenv("ALERT_EMAIL_FROM")
    app_password = os.getenv("ALERT_EMAIL_APP_PASSWORD")
    email_to = os.getenv("ALERT_EMAIL_TO")

    if not all([email_from, app_password, email_to]):
        return None

    return {
        "from": email_from,
        "password": app_password,
        "to": email_to,
    }


def _build_email_body(report):
    """Build a readable failure report for the email body."""
    lines = []
    lines.append(f"Pipeline run at {report['started_at']} IST finished with status: {report['overall_status'].upper()}")
    lines.append(f"Duration: {report['duration_sec']}s")
    lines.append("")

    # step breakdown
    lines.append("Step Breakdown:")
    lines.append("-" * 40)
    for step in report.get("steps", []):
        status = step["status"].upper()
        duration = step.get("duration_sec", "?")
        rows = step.get("rows", "")
        row_info = f" ({rows} rows)" if rows != "" else ""
        error = step.get("error", "")

        if status == "SUCCESS":
            lines.append(f"  {step['step']:<10} OK     {duration}s{row_info}")
        else:
            lines.append(f"  {step['step']:<10} FAILED {duration}s")
            lines.append(f"             Error: {error}")
    lines.append("")

    # validation
    val = report.get("validation", {})
    passed = val.get("passed", 0)
    failed = val.get("failed_count", 0)
    lines.append(f"Validation: {passed} passed, {failed} failed")

    if val.get("failed_checks"):
        lines.append("")
        lines.append("Failed Checks:")
        lines.append("-" * 40)
        for fc in val["failed_checks"]:
            lines.append(f"  {fc['check']}: {fc['detail']}")

    return "\n".join(lines)


def send_alert(report):
    """Send an email alert if the pipeline run was not fully successful.

    - Does nothing if email is not configured (silent skip).
    - Does nothing if the run was successful.
    - Never raises — notification failures are logged, not propagated.
    """
    if report.get("overall_status") == "success":
        return

    config = _get_email_config()
    if config is None:
        logger.info("Alert skipped — email not configured (set ALERT_EMAIL_FROM, ALERT_EMAIL_APP_PASSWORD, ALERT_EMAIL_TO in .env)")
        return

    try:
        status = report["overall_status"].upper()
        subject = f"Game Pulse Pipeline {status} — {report['started_at']} IST"
        body = _build_email_body(report)

        msg = MIMEMultipart()
        msg["From"] = config["from"]
        msg["To"] = config["to"]
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(config["from"], config["password"])
            server.send_message(msg)

        logger.info(f"Alert email sent to {config['to']}")

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")