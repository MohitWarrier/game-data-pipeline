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
from dotenv import load_dotenv
from ingest.logger import get_logger

logger = get_logger("notify")


def _get_email_config():
    """Load email config from environment. Returns None if not configured."""
    load_dotenv()
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
    """Build a detailed failure report for the email body."""
    status = report["overall_status"].upper()
    lines = []

    # header
    lines.append("=" * 50)
    lines.append(f"  GAME PULSE PIPELINE — {status}")
    lines.append("=" * 50)
    lines.append("")
    lines.append(f"  Started:   {report['started_at']} IST")
    lines.append(f"  Finished:  {report.get('finished_at', '?')} IST")
    lines.append(f"  Duration:  {report['duration_sec']}s")
    lines.append("")

    # step-by-step results
    lines.append("-" * 50)
    lines.append("  STEP RESULTS")
    lines.append("-" * 50)

    for step in report.get("steps", []):
        name = step["step"].upper()
        s = step["status"]
        duration = step.get("duration_sec", "?")
        rows = step.get("rows", "")

        if s == "success":
            row_info = f" | {rows} rows" if rows != "" else ""
            dbt_info = ""
            if step.get("dbt_tests_passed") is False:
                dbt_info = " | dbt tests FAILED"
            elif step.get("dbt_tests_passed") is True:
                dbt_info = " | dbt tests passed"
            lines.append(f"  [OK]     {name:<10} {duration}s{row_info}{dbt_info}")
        else:
            lines.append(f"  [FAILED] {name:<10} {duration}s")
            error = step.get("error", "unknown")
            # wrap long errors
            if len(error) > 80:
                lines.append(f"           {error[:80]}")
                lines.append(f"           {error[80:]}")
            else:
                lines.append(f"           {error}")
    lines.append("")

    # validation results
    val = report.get("validation", {})
    passed = val.get("passed", 0)
    failed = val.get("failed_count", 0)
    total = passed + failed

    lines.append("-" * 50)
    lines.append("  VALIDATION")
    lines.append("-" * 50)
    if total > 0:
        lines.append(f"  {passed}/{total} checks passed")
    else:
        lines.append("  No validation ran")

    if val.get("failed_checks"):
        lines.append("")
        for fc in val["failed_checks"]:
            lines.append(f"  [FAIL] {fc['check']}")
            lines.append(f"         {fc['detail']}")
    lines.append("")

    # what to do
    lines.append("-" * 50)
    lines.append("  WHAT TO DO")
    lines.append("-" * 50)
    lines.append("  1. Check dashboard Pipeline tab for details")
    lines.append("  2. Run: make logs")
    lines.append("  3. Run: make validate")
    lines.append("  4. Check logs/runs/latest.json for full report")
    lines.append("")

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