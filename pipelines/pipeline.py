import os
import sys
import subprocess
from prefect import flow, task

# add project root to path so ingest package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.logger import get_logger
from ingest.config import load_config
from ingest.fetch_twitch import run as run_twitch
from ingest.fetch_igdb import run as run_igdb
from ingest.fetch_steam import run as run_steam

logger = get_logger("pipeline")


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
def run_dbt():
    # dbt run
    result = subprocess.run(
        ["dbt", "run"], cwd="game_pulse", capture_output=True, text=True
    )
    logger.info(f"dbt run output:\n{result.stdout}")
    if result.returncode != 0:
        logger.error(f"dbt run errors:\n{result.stderr}")
        raise RuntimeError("dbt run failed")

    # dbt test
    test_result = subprocess.run(
        ["dbt", "test"], cwd="game_pulse", capture_output=True, text=True
    )
    logger.info(f"dbt test output:\n{test_result.stdout}")
    if test_result.returncode != 0:
        logger.warning(f"dbt test failures:\n{test_result.stderr}")

    return {"source": "dbt", "status": "success", "rows": "n/a"}


@flow(name="game-pulse")
def pipeline():
    config = load_config()
    results = []

    # ingest: twitch -> igdb -> steam (order matters)
    for task_fn, args in [
        (ingest_twitch, (config,)),
        (ingest_igdb, (config,)),
        (ingest_steam, ()),
    ]:
        try:
            result = task_fn(*args)
            results.append(result)
        except Exception as e:
            results.append({
                "source": task_fn.fn.__name__.replace("ingest_", ""),
                "status": "failed",
                "error": str(e),
            })
            logger.error(f"{task_fn.fn.__name__} failed: {e}")

    # dbt
    try:
        result = run_dbt()
        results.append(result)
    except Exception as e:
        results.append({"source": "dbt", "status": "failed", "error": str(e)})
        logger.error(f"dbt failed: {e}")

    # run summary
    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY")
    logger.info("-" * 60)
    for r in results:
        source = r.get("source", "unknown")
        status = r.get("status", "unknown")
        rows = r.get("rows", "n/a")
        error = r.get("error", "")
        if status == "success":
            logger.info(f"  {source:<10} SUCCESS  ({rows} rows)")
        else:
            logger.error(f"  {source:<10} FAILED   {error}")
    logger.info("=" * 60)


if __name__ == "__main__":
    if "--serve" in sys.argv:
        logger.info("Starting scheduled pipeline (every 30 minutes)")
        pipeline.serve(name="game-pulse-deployment", cron="*/30 * * * *")
    else:
        logger.info("Running pipeline once (use --serve for scheduled mode)")
        pipeline()
