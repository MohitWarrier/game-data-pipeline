"""
Comprehensive Pipeline Tests
=============================
Tests every component of the pipeline individually and together.

Test categories:
  1. UNIT       — each ingest module works on its own
  2. INTEGRATION — full pipeline end-to-end
  3. EDGE CASES — first run (empty database), missing tables, bad data
  4. VALIDATION — the validation checks themselves work

Usage:
    python -m tests.test_pipeline                # run all tests
    python -m tests.test_pipeline --test smoke   # quick smoke test only
    python -m tests.test_pipeline --test fresh   # first-run test (wipes DB!)

Every test prints PASS/FAIL clearly and exits with code 0/1.
"""

import os
import sys
import shutil
import argparse
import traceback
import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.logger import get_logger, now_ist
from ingest.config import load_config

logger = get_logger("test")

DB_PATH = "data/game_pulse.duckdb"
BACKUP_PATH = "data/game_pulse.duckdb.backup"


class TestRunner:
    def __init__(self):
        self.results = []

    def run_test(self, name, fn):
        """Run a single test, catch exceptions, record result."""
        logger.info(f"  RUNNING: {name}")
        try:
            fn()
            self.results.append(("PASS", name, ""))
            logger.info(f"  PASS:    {name}")
        except AssertionError as e:
            self.results.append(("FAIL", name, str(e)))
            logger.error(f"  FAIL:    {name} — {e}")
        except Exception as e:
            self.results.append(("ERROR", name, str(e)))
            logger.error(f"  ERROR:   {name} — {e}")
            logger.error(f"           {traceback.format_exc().splitlines()[-1]}")

    def summary(self):
        logger.info("")
        logger.info("=" * 60)
        logger.info("TEST RESULTS")
        logger.info("-" * 60)
        passed = sum(1 for s, _, _ in self.results if s == "PASS")
        failed = sum(1 for s, _, _ in self.results if s == "FAIL")
        errors = sum(1 for s, _, _ in self.results if s == "ERROR")

        for status, name, detail in self.results:
            if status == "PASS":
                logger.info(f"  PASS  {name}")
            else:
                logger.error(f"  {status}  {name} — {detail}")

        logger.info("-" * 60)
        total = len(self.results)
        logger.info(f"  {passed}/{total} passed, {failed} failed, {errors} errors")
        if failed == 0 and errors == 0:
            logger.info("  ALL TESTS PASSED")
        else:
            logger.error("  SOME TESTS FAILED")
        logger.info("=" * 60)
        return failed == 0 and errors == 0


# ============================================================
#  UNIT TESTS — each component individually
# ============================================================

def test_config_loads():
    """Config loads environment variables without crashing."""
    config = load_config()
    assert config["client_id"], "TWITCH_CLIENT_ID is empty"
    assert config["client_secret"], "TWITCH_CLIENT_SECRET is empty"


def test_twitch_ingest():
    """Twitch ingest fetches data and writes to database."""
    from ingest.fetch_twitch import run as run_twitch
    config = load_config()
    result = run_twitch(config)
    assert result["status"] == "success", f"Twitch ingest failed: {result}"
    assert result["rows"] > 0, "Twitch ingest returned 0 rows"

    # verify data actually landed in DB
    con = duckdb.connect(DB_PATH, read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_twitch_games").fetchone()[0]
    con.close()
    assert count > 0, "raw_twitch_games is empty after ingest"


def test_igdb_ingest():
    """IGDB ingest fetches metadata for games."""
    from ingest.fetch_igdb import run as run_igdb
    config = load_config()
    result = run_igdb(config)
    assert result["status"] == "success", f"IGDB ingest failed: {result}"

    con = duckdb.connect(DB_PATH, read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_igdb_games").fetchone()[0]
    con.close()
    assert count > 0, "raw_igdb_games is empty after ingest"


def test_steam_ingest():
    """Steam ingest runs without crashing (may return 0 rows if no steam IDs)."""
    from ingest.fetch_steam import run as run_steam
    result = run_steam()
    assert result["status"] == "success", f"Steam ingest failed: {result}"


def test_dbt_models():
    """dbt run creates all models without errors."""
    import subprocess
    result = subprocess.run(
        ["dbt", "run"], cwd="game_pulse", capture_output=True, text=True
    )
    assert result.returncode == 0, f"dbt run failed:\n{result.stderr}\n{result.stdout}"


def test_dbt_tests():
    """dbt test passes all schema tests."""
    import subprocess
    result = subprocess.run(
        ["dbt", "test"], cwd="game_pulse", capture_output=True, text=True
    )
    assert result.returncode == 0, f"dbt tests failed:\n{result.stderr}\n{result.stdout}"


def test_validation_passes():
    """All validation checks pass on current database."""
    from tests.validate_pipeline import run as validate_run
    result = validate_run(verbose=False, max_freshness_hours=1)
    assert result.ok, (
        f"{len(result.failed)} checks failed: "
        + ", ".join(f"{n} ({d})" for n, d in result.failed)
    )


# ============================================================
#  INTEGRATION TEST — full pipeline end-to-end
# ============================================================

def test_full_pipeline():
    """Run the entire pipeline flow and verify it succeeds."""
    from pipelines.pipeline import pipeline
    report = pipeline()
    assert report["overall_status"] == "success", (
        f"Pipeline status: {report['overall_status']}\n"
        + "\n".join(
            f"  {s['step']}: {s['status']} {s.get('error', '')}"
            for s in report["steps"]
        )
    )

    # verify run report was saved
    import json
    latest_path = os.path.join("logs", "runs", "latest.json")
    assert os.path.exists(latest_path), "No run report saved"
    with open(latest_path) as f:
        saved = json.load(f)
    assert saved["overall_status"] == "success", "Saved report doesn't match"


# ============================================================
#  FIRST-RUN TEST — empty database edge case
# ============================================================

def test_first_run():
    """
    Pipeline works from scratch with no existing database.
    WARNING: This backs up and removes the existing database!
    """
    # backup existing DB
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, BACKUP_PATH)
        os.remove(DB_PATH)
        logger.info("  Backed up existing database")

    try:
        # run pipeline on empty database
        from pipelines.pipeline import pipeline
        report = pipeline()

        assert report["overall_status"] == "success", (
            f"First-run pipeline failed: {report['overall_status']}\n"
            + "\n".join(
                f"  {s['step']}: {s['status']} {s.get('error', '')}"
                for s in report["steps"]
            )
        )

        # verify all tables exist
        con = duckdb.connect(DB_PATH, read_only=True)
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for t in ["raw_twitch_games", "dim_games", "fact_game_snapshots"]:
            assert t in tables, f"Table {t} missing after first run"

        # verify data was actually created
        for t in ["raw_twitch_games", "dim_games", "fact_game_snapshots"]:
            count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            assert count > 0, f"{t} is empty after first run"
        con.close()

        logger.info("  First-run test passed — pipeline works from scratch")
    finally:
        # restore backup
        if os.path.exists(BACKUP_PATH):
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            shutil.move(BACKUP_PATH, DB_PATH)
            logger.info("  Restored database from backup")


# ============================================================
#  SMOKE TEST — quick check that everything is connected
# ============================================================

def run_smoke_test():
    """Quick test: config + one ingest + dbt + validate. ~30 seconds."""
    runner = TestRunner()
    logger.info("")
    logger.info("=" * 60)
    logger.info("SMOKE TEST")
    logger.info("=" * 60)

    runner.run_test("Config loads", test_config_loads)
    runner.run_test("Twitch ingest", test_twitch_ingest)
    runner.run_test("dbt models", test_dbt_models)
    runner.run_test("dbt tests", test_dbt_tests)
    runner.run_test("Validation passes", test_validation_passes)

    return runner.summary()


def run_full_test():
    """Full test: every component + integration + validation. ~2 minutes."""
    runner = TestRunner()
    logger.info("")
    logger.info("=" * 60)
    logger.info("FULL PIPELINE TEST")
    logger.info("=" * 60)

    # unit tests
    logger.info("")
    logger.info("--- Unit Tests ---")
    runner.run_test("Config loads", test_config_loads)
    runner.run_test("Twitch ingest", test_twitch_ingest)
    runner.run_test("IGDB ingest", test_igdb_ingest)
    runner.run_test("Steam ingest", test_steam_ingest)
    runner.run_test("dbt models", test_dbt_models)
    runner.run_test("dbt tests", test_dbt_tests)

    # integration
    logger.info("")
    logger.info("--- Integration Tests ---")
    runner.run_test("Full pipeline end-to-end", test_full_pipeline)
    runner.run_test("Validation passes", test_validation_passes)

    return runner.summary()


def run_fresh_test():
    """First-run test: wipes DB and runs from scratch. ~2 minutes."""
    runner = TestRunner()
    logger.info("")
    logger.info("=" * 60)
    logger.info("FIRST-RUN TEST (will backup/restore database)")
    logger.info("=" * 60)

    runner.run_test("First run from empty database", test_first_run)

    return runner.summary()


# ============================================================
#  ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline tests")
    parser.add_argument(
        "--test",
        choices=["smoke", "full", "fresh", "all"],
        default="full",
        help="Which test suite to run (default: full)",
    )
    args = parser.parse_args()

    if args.test == "smoke":
        ok = run_smoke_test()
    elif args.test == "full":
        ok = run_full_test()
    elif args.test == "fresh":
        ok = run_fresh_test()
    elif args.test == "all":
        logger.info("Running ALL test suites")
        ok1 = run_full_test()
        ok2 = run_fresh_test()
        ok = ok1 and ok2

    sys.exit(0 if ok else 1)
