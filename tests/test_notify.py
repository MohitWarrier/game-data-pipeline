import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.notify import send_alert

# Simulate a failed pipeline run
report = {
    "overall_status": "failed",
    "started_at": "15/03/26 10:00:00",
    "finished_at": "15/03/26 10:00:05",
    "duration_sec": 5,
    "steps": [
        {"step": "twitch", "status": "failed", "error": "test error", "duration_sec": 5}
    ],
    "validation": {"passed": 0, "failed_count": 0, "failed_checks": []},
}

send_alert(report)
print("Done — check for email or 'Alert skipped' message above")