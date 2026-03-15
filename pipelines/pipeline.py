"""
Game Pulse Pipeline
===================
Orchestrates: ingest (Twitch, IGDB, Steam) -> dbt transform -> validate.

Every run writes a JSON report to logs/runs/ so the dashboard shows exactly
what happened. All timestamps are IST.

Usage:
    python pipelines/pipeline.py            # run once
    python pipelines/pipeline.py --serve    # run now, then every 30 min
"""

import os
import sys
import json
import time
import subprocess
from datetime import timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# suppress Prefect's duplicate logging — we have our own logger
os.environ["PREFECT_LOGGING_LEVEL"] = "WARNING"

from prefect import flow, task

# Prefect hooks into Python's logging and duplicates every line.
# Silence all prefect loggers so only our custom logger writes output.
import logging
for _name in ["prefect", "prefect.flow_runs", "prefect.task_runs"]:
    logging.getLogger(_name).setLevel(logging.WARNING)

from ingest.logger import get_logger, now_ist
from ingest.config import load_config
from ingest.notify import send_alert
from ingest.fetch_twitch import run as run_twitch
from ingest.fetch_igdb import run as run_igdb
from ingest.fetch_steam import run as run_steam

logger = get_logger("pipeline")

RUNS_DIR = os.path.join(PROJECT_ROOT, "logs", "runs")
os.makedirs(RUNS_DIR, exist_ok=True)


def fmt(dt):
    """Format datetime as dd/mm/yy HH:MM:SS IST."""
    return dt.strftime("%d/%m/%y %H:%M:%S")


def save_run_report(report):
    """Write a JSON report for this pipeline run to logs/runs/."""
    ts = report["started_at"].replace(":", "-").replace(" ", "_").replace("/", "-")
    path = os.path.join(RUNS_DIR, f"run_{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"Run report saved: {path}")

    latest_path = os.path.join(RUNS_DIR, "latest.json")
    with open(latest_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return path


# ============================================================
#  TASKS
# ============================================================

@task(retries=2, retry_delay_seconds=30, timeout_seconds=120)
def ingest_twitch(config):
    return run_twitch(config)

@task(retries=2, retry_delay_seconds=30, timeout_seconds=120)
def ingest_igdb(config):
    return run_igdb(config)

@task(retries=2, retry_delay_seconds=30, timeout_seconds=120)
def ingest_steam():
    return run_steam()

@task(retries=1, retry_delay_seconds=10, timeout_seconds=300)
def transform_dbt():
    result = subprocess.run(
        ["dbt", "run"], cwd="game_pulse", capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"dbt run failed (exit {result.returncode})")
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    # count models from dbt output (e.g. "Completed successfully 5 ...")
    model_lines = [l for l in result.stdout.splitlines() if "OK" in l or "SUCCESS" in l]
    logger.info(f"dbt run OK ({len(model_lines)} models built)")

    test_result = subprocess.run(
        ["dbt", "test"], cwd="game_pulse", capture_output=True, text=True
    )
    test_lines = [l for l in test_result.stdout.splitlines() if "Pass" in l or "Fail" in l]
    if test_result.returncode != 0:
        logger.warning(f"dbt tests: some failures (exit {test_result.returncode})")
    else:
        logger.info(f"dbt tests OK ({len(test_lines)} tests)")

    return {
        "source": "dbt",
        "status": "success",
        "dbt_tests_passed": test_result.returncode == 0,
    }


def run_validation():
    """Run all validation checks after pipeline completes."""
    try:
        from tests.validate_pipeline import run as validate_run
        result = validate_run(verbose=False, max_freshness_hours=1)
        return {
            "status": "passed" if result.ok else "failed",
            "passed": len(result.passed),
            "failed_count": len(result.failed),
            "failed_checks": [
                {"check": name, "detail": detail}
                for name, detail in result.failed
            ],
        }
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return {
            "status": "error", "passed": 0, "failed_count": 0,
            "failed_checks": [], "error": str(e),
        }


# ============================================================
#  MAIN FLOW
# ============================================================

@flow(name="game-pulse")
def pipeline(trigger="manual"):
    run_start = now_ist()
    trigger_label = {"scheduled": "SCHEDULED", "manual": "MANUAL", "dashboard": "DASHBOARD"}.get(trigger, "MANUAL")
    logger.info("=" * 60)
    logger.info(f"{trigger_label} PIPELINE RUN STARTED")
    logger.info(f"  Time: {fmt(run_start)} IST")
    logger.info("=" * 60)

    config = load_config()
    steps = []

    for name, task_fn, args in [
        ("twitch", ingest_twitch, (config,)),
        ("igdb", ingest_igdb, (config,)),
        ("steam", ingest_steam, ()),
    ]:
        step_start = now_ist()
        step = {"step": name, "started_at": fmt(step_start)}
        try:
            result = task_fn(*args)
            step["status"] = "success"
            step["rows"] = result.get("rows", 0)
            step["duration_sec"] = round((now_ist() - step_start).total_seconds(), 1)
            logger.info(f"  {name:<10} OK  ({step['rows']} rows, {step['duration_sec']}s)")
        except Exception as e:
            step["status"] = "failed"
            step["error"] = str(e)
            step["duration_sec"] = round((now_ist() - step_start).total_seconds(), 1)
            logger.error(f"  {name:<10} FAILED  ({e})")
        steps.append(step)

    # dbt
    step_start = now_ist()
    dbt_step = {"step": "dbt", "started_at": fmt(step_start)}
    try:
        result = transform_dbt()
        dbt_step["status"] = "success"
        dbt_step["dbt_tests_passed"] = result.get("dbt_tests_passed", False)
        dbt_step["duration_sec"] = round((now_ist() - step_start).total_seconds(), 1)
        logger.info(f"  dbt        OK  ({dbt_step['duration_sec']}s, tests={'PASS' if dbt_step['dbt_tests_passed'] else 'FAIL'})")
    except Exception as e:
        dbt_step["status"] = "failed"
        dbt_step["error"] = str(e)
        dbt_step["duration_sec"] = round((now_ist() - step_start).total_seconds(), 1)
        logger.error(f"  dbt        FAILED  ({e})")
    steps.append(dbt_step)

    # validation
    logger.info("-" * 60)
    logger.info("  Running validation checks...")
    validation = run_validation()
    logger.info(f"  Validation: {validation['passed']} passed, {validation['failed_count']} failed — {validation['status'].upper()}")

    # build report
    run_end = now_ist()
    duration = round((run_end - run_start).total_seconds(), 1)
    all_ok = all(s["status"] == "success" for s in steps)
    overall = "success" if all_ok and validation["status"] == "passed" else "partial" if all_ok else "failed"

    report = {
        "started_at": fmt(run_start),
        "finished_at": fmt(run_end),
        "duration_sec": duration,
        "overall_status": overall,
        "trigger": trigger_label.lower(),
        "steps": steps,
        "validation": validation,
    }

    logger.info("=" * 60)
    logger.info("PIPELINE RUN COMPLETE")
    logger.info(f"  Status:   {overall.upper()}")
    logger.info(f"  Duration: {duration}s")
    logger.info(f"  Steps:    {sum(1 for s in steps if s['status'] == 'success')}/{len(steps)} succeeded")
    logger.info(f"  Checks:   {validation['passed']}/{validation['passed'] + validation['failed_count']} passed")
    if validation["failed_checks"]:
        for fc in validation["failed_checks"]:
            logger.warning(f"    FAIL: {fc['check']} — {fc['detail']}")
    logger.info("=" * 60)

    save_run_report(report)
    send_alert(report)
    return report


# ============================================================
#  ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    # parse --trigger flag (used by dashboard button)
    cli_trigger = "manual"
    if "--trigger" in sys.argv:
        idx = sys.argv.index("--trigger")
        if idx + 1 < len(sys.argv):
            cli_trigger = sys.argv[idx + 1]

    if "--serve" in sys.argv:
        interval = 30
        logger.info(f"Pipeline scheduler started (runs every {interval} min)")
        logger.info("Press Ctrl+C to stop")

        # run immediately
        try:
            pipeline(trigger="scheduled")
        except Exception as e:
            logger.error(f"Initial run failed: {e}")

        # then loop
        while True:
            next_time = (now_ist() + timedelta(minutes=interval)).strftime("%d/%m/%y %H:%M IST")
            logger.info(f"Next run at {next_time}. Ctrl+C to stop.")
            try:
                time.sleep(interval * 60)
                pipeline(trigger="scheduled")
            except KeyboardInterrupt:
                logger.info("Pipeline stopped by user")
                break
            except Exception as e:
                logger.error(f"Pipeline run failed: {e}")
                logger.info("Will retry at next interval")
    else:
        logger.info(f"Running pipeline once ({cli_trigger})")
        pipeline(trigger=cli_trigger)
