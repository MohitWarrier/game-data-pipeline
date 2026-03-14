"""
Pipeline Validation — automated checks that run after the pipeline to verify
everything worked correctly.

Three categories of checks:
1. Schema checks — do all expected tables/columns exist?
2. Data quality checks — are there nulls in key columns? duplicates where there shouldn't be?
3. Freshness checks — is the data recent enough?

Usage:
    python -m tests.validate_pipeline              # run all checks
    python -m tests.validate_pipeline --verbose     # show passing checks too

Exit code 0 = all checks passed, exit code 1 = at least one failed.
This can be plugged into CI or run after any pipeline change.
"""

import os
import sys
import argparse
import duckdb
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.logger import get_logger

logger = get_logger("validate")

DB_PATH = "data/game_pulse.duckdb"


class ValidationResult:
    def __init__(self):
        self.passed = []
        self.failed = []

    def check(self, name, condition, detail=""):
        if condition:
            self.passed.append(name)
        else:
            self.failed.append((name, detail))

    @property
    def ok(self):
        return len(self.failed) == 0


def check_tables_exist(con, result):
    """Verify all expected tables and views exist."""
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    views = set()
    try:
        view_rows = con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_type = 'VIEW'
        """).fetchall()
        views = {row[0] for row in view_rows}
    except Exception:
        pass

    all_objects = tables | views

    expected_tables = [
        "raw_twitch_games",
        "raw_igdb_games",
        "raw_steam_players",
        "dim_games",
        "fact_game_snapshots",
    ]
    expected_views = [
        "stg_twitch_games",
        "stg_igdb_games",
        "stg_steam_players",
    ]

    for t in expected_tables + expected_views:
        result.check(
            f"table_exists:{t}",
            t in all_objects,
            f"missing from database",
        )


def check_row_counts(con, result):
    """Verify tables have data (not empty)."""
    tables_to_check = [
        ("raw_twitch_games", 1),
        ("raw_igdb_games", 1),
        ("dim_games", 1),
        ("fact_game_snapshots", 1),
    ]
    for table, min_rows in tables_to_check:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            result.check(
                f"row_count:{table}",
                count >= min_rows,
                f"expected >= {min_rows} rows, got {count}",
            )
        except Exception as e:
            result.check(f"row_count:{table}", False, str(e))


def check_no_nulls_in_key_columns(con, result):
    """Verify key columns have no null values."""
    checks = [
        ("raw_twitch_games", "id"),
        ("raw_twitch_games", "name"),
        ("raw_twitch_games", "fetched_at"),
        ("raw_igdb_games", "igdb_id"),
        ("dim_games", "game_id"),
        ("dim_games", "game_name"),
        ("fact_game_snapshots", "game_id"),
        ("fact_game_snapshots", "fetched_at"),
        ("fact_game_snapshots", "viewer_count"),
        ("fact_game_snapshots", "rank_at_time"),
    ]
    for table, col in checks:
        try:
            null_count = con.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {col} IS NULL
            """).fetchone()[0]
            result.check(
                f"not_null:{table}.{col}",
                null_count == 0,
                f"{null_count} null values found",
            )
        except Exception as e:
            result.check(f"not_null:{table}.{col}", False, str(e))


def check_no_non_games(con, result):
    """Verify non-game categories are filtered out of transformed tables."""
    non_games = ["Just Chatting", "IRL"]
    for name in non_games:
        for table in ["dim_games", "fact_game_snapshots"]:
            try:
                count = con.execute(f"""
                    SELECT COUNT(*) FROM {table} WHERE game_name = '{name}'
                """).fetchone()[0]
                result.check(
                    f"no_non_games:{table}.{name}",
                    count == 0,
                    f"found {count} rows with '{name}'",
                )
            except Exception as e:
                result.check(f"no_non_games:{table}.{name}", False, str(e))


def check_unique_keys(con, result):
    """Verify uniqueness constraints on dimension tables."""
    try:
        total = con.execute("SELECT COUNT(*) FROM dim_games").fetchone()[0]
        distinct = con.execute("SELECT COUNT(DISTINCT game_id) FROM dim_games").fetchone()[0]
        result.check(
            "unique:dim_games.game_id",
            total == distinct,
            f"{total} total rows but {distinct} distinct game_ids",
        )
    except Exception as e:
        result.check("unique:dim_games.game_id", False, str(e))


def check_freshness(con, result, max_hours=1):
    """Verify data was updated recently."""
    try:
        latest = con.execute("""
            SELECT MAX(CAST(fetched_at AS TIMESTAMP)) FROM raw_twitch_games
        """).fetchone()[0]

        if latest is None:
            result.check("freshness:raw_twitch_games", False, "no data")
            return

        now = datetime.now(timezone.utc)
        # handle both timezone-aware and naive timestamps from DuckDB
        if latest.tzinfo is None:
            age = now - latest.replace(tzinfo=timezone.utc)
        else:
            age = now - latest

        result.check(
            "freshness:raw_twitch_games",
            age < timedelta(hours=max_hours),
            f"last update was {age} ago (max {max_hours}h)",
        )
    except Exception as e:
        result.check("freshness:raw_twitch_games", False, str(e))


def check_rank_continuity(con, result):
    """Verify ranks are continuous (1, 2, 3...) within each snapshot."""
    try:
        gaps = con.execute("""
            SELECT fetched_at, COUNT(*) AS n, MAX(rank_at_time) AS max_rank
            FROM fact_game_snapshots
            GROUP BY fetched_at
            HAVING MAX(rank_at_time) != COUNT(*)
        """).fetchall()
        result.check(
            "rank_continuity:fact_game_snapshots",
            len(gaps) == 0,
            f"{len(gaps)} snapshots have rank gaps",
        )
    except Exception as e:
        result.check("rank_continuity:fact_game_snapshots", False, str(e))


def run(verbose=False, max_freshness_hours=1):
    """Run all validation checks and return results."""
    result = ValidationResult()

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        check_tables_exist(con, result)
        check_row_counts(con, result)
        check_no_nulls_in_key_columns(con, result)
        check_no_non_games(con, result)
        check_unique_keys(con, result)
        check_freshness(con, result, max_hours=max_freshness_hours)
        check_rank_continuity(con, result)
    finally:
        con.close()

    # report
    logger.info("=" * 60)
    logger.info("PIPELINE VALIDATION REPORT")
    logger.info("-" * 60)

    if verbose:
        for name in result.passed:
            logger.info(f"  PASS  {name}")

    for name, detail in result.failed:
        logger.error(f"  FAIL  {name} — {detail}")

    logger.info("-" * 60)
    total = len(result.passed) + len(result.failed)
    logger.info(f"  {len(result.passed)}/{total} checks passed")

    if result.ok:
        logger.info("  STATUS: ALL CHECKS PASSED")
    else:
        logger.error(f"  STATUS: {len(result.failed)} CHECK(S) FAILED")

    logger.info("=" * 60)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate pipeline output")
    parser.add_argument("--verbose", action="store_true", help="Show passing checks too")
    parser.add_argument("--max-hours", type=int, default=1, help="Max hours since last update (default: 1)")
    args = parser.parse_args()

    result = run(verbose=args.verbose, max_freshness_hours=args.max_hours)
    sys.exit(0 if result.ok else 1)
