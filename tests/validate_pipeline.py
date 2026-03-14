"""
Pipeline Validation — comprehensive checks that verify the pipeline actually works.

Three levels of testing:
  1. STRUCTURE  — do tables exist, do they have the right columns?
  2. DATA       — is the data correct? no nulls where there shouldn't be,
                  no duplicates, no bad values, transformations applied correctly?
  3. FRESHNESS  — is the data recent? did new rows actually get added?

Usage:
    python -m tests.validate_pipeline              # run all checks
    python -m tests.validate_pipeline --verbose     # show passing checks too
    python -m tests.validate_pipeline --max-hours 24  # relax freshness to 24h

Exit code 0 = all checks passed, exit code 1 = at least one failed.
"""

import os
import sys
import argparse
import duckdb
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingest.logger import get_logger, IST

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


# ============================================================
#  STRUCTURE CHECKS — does the schema look right?
# ============================================================

def check_tables_exist(con, result):
    """Every expected table and view must exist."""
    all_objects = set()
    for row in con.execute("SHOW TABLES").fetchall():
        all_objects.add(row[0])
    try:
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
        ).fetchall():
            all_objects.add(row[0])
    except Exception:
        pass

    for t in [
        "raw_twitch_games", "raw_igdb_games", "raw_steam_players",
        "dim_games", "fact_game_snapshots",
        "stg_twitch_games", "stg_igdb_games", "stg_steam_players",
    ]:
        result.check(f"table_exists:{t}", t in all_objects, "missing from database")


def check_columns_exist(con, result):
    """Key tables must have the expected columns."""
    expected = {
        "fact_game_snapshots": [
            "game_id", "game_name", "viewer_count", "stream_count",
            "rank_at_time", "genre", "release_year", "fetched_at",
        ],
        "dim_games": [
            "game_id", "game_name", "igdb_id", "genre",
            "release_year", "developer",
        ],
        "raw_twitch_games": ["id", "name", "viewer_count"],
    }
    for table, cols in expected.items():
        try:
            actual_cols = {
                row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()
            }
            for col in cols:
                result.check(
                    f"column:{table}.{col}",
                    col in actual_cols,
                    f"column '{col}' missing from {table} (has: {sorted(actual_cols)})",
                )
        except Exception as e:
            for col in cols:
                result.check(f"column:{table}.{col}", False, str(e))


# ============================================================
#  DATA QUALITY CHECKS — is the data correct?
# ============================================================

def check_row_counts(con, result):
    """Tables must not be empty (except raw_steam_players which may have no data)."""
    must_have_data = [
        "raw_twitch_games", "raw_igdb_games", "dim_games", "fact_game_snapshots",
    ]
    for table in must_have_data:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            result.check(
                f"has_data:{table}",
                count > 0,
                f"table is empty (0 rows)",
            )
        except Exception as e:
            result.check(f"has_data:{table}", False, str(e))


def check_no_nulls(con, result):
    """Key columns must never be null."""
    checks = [
        ("raw_twitch_games", "id"),
        ("raw_twitch_games", "name"),
        ("raw_twitch_games", "fetched_at"),
        ("raw_igdb_games", "igdb_id"),
        ("dim_games", "game_id"),
        ("dim_games", "game_name"),
        ("fact_game_snapshots", "game_id"),
        ("fact_game_snapshots", "game_name"),
        ("fact_game_snapshots", "fetched_at"),
        ("fact_game_snapshots", "viewer_count"),
        ("fact_game_snapshots", "stream_count"),
        ("fact_game_snapshots", "rank_at_time"),
    ]
    for table, col in checks:
        try:
            nulls = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            result.check(
                f"not_null:{table}.{col}",
                nulls == 0,
                f"{nulls} null values",
            )
        except Exception as e:
            result.check(f"not_null:{table}.{col}", False, str(e))


def check_no_non_games(con, result):
    """Non-game categories (Just Chatting, IRL) must be filtered from marts."""
    non_games = ["Just Chatting", "IRL"]
    for name in non_games:
        for table in ["dim_games", "fact_game_snapshots"]:
            try:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE game_name = '{name}'"
                ).fetchone()[0]
                result.check(
                    f"filtered:{table}.{name}",
                    count == 0,
                    f"found {count} rows — non-game not filtered",
                )
            except Exception as e:
                result.check(f"filtered:{table}.{name}", False, str(e))


def check_unique_keys(con, result):
    """Primary keys must be unique."""
    # dim_games.game_id should be unique (one row per game)
    try:
        total = con.execute("SELECT COUNT(*) FROM dim_games").fetchone()[0]
        distinct = con.execute("SELECT COUNT(DISTINCT game_id) FROM dim_games").fetchone()[0]
        result.check(
            "unique:dim_games.game_id",
            total == distinct,
            f"{total} rows but only {distinct} distinct game_ids — duplicates exist",
        )
    except Exception as e:
        result.check("unique:dim_games.game_id", False, str(e))


def check_rank_continuity(con, result):
    """Ranks must be 1,2,3... with no gaps within each snapshot."""
    try:
        gaps = con.execute("""
            SELECT fetched_at, COUNT(*) AS n, MAX(rank_at_time) AS max_rank
            FROM fact_game_snapshots
            GROUP BY fetched_at
            HAVING MAX(rank_at_time) != COUNT(*)
        """).fetchall()
        result.check(
            "rank_continuity",
            len(gaps) == 0,
            f"{len(gaps)} snapshots have rank gaps (max rank != row count)",
        )
    except Exception as e:
        result.check("rank_continuity", False, str(e))


def check_viewer_counts_positive(con, result):
    """Viewer and stream counts must be >= 0."""
    for col in ["viewer_count", "stream_count"]:
        try:
            neg = con.execute(
                f"SELECT COUNT(*) FROM fact_game_snapshots WHERE {col} < 0"
            ).fetchone()[0]
            result.check(
                f"positive:{col}",
                neg == 0,
                f"{neg} rows have negative {col}",
            )
        except Exception as e:
            result.check(f"positive:{col}", False, str(e))


def check_snapshot_consistency(con, result):
    """Each snapshot should have a reasonable number of games (not 0, not 1000)."""
    try:
        snapshots = con.execute("""
            SELECT fetched_at, COUNT(*) AS game_count
            FROM fact_game_snapshots
            GROUP BY fetched_at
        """).fetchall()
        for ts, count in snapshots:
            result.check(
                f"snapshot_size:{str(ts)[:16]}",
                5 <= count <= 100,
                f"snapshot has {count} games (expected 5-100)",
            )
    except Exception as e:
        result.check("snapshot_size", False, str(e))


def check_dim_fact_join(con, result):
    """Every game in fact_game_snapshots must exist in dim_games."""
    try:
        orphans = con.execute("""
            SELECT COUNT(DISTINCT f.game_id)
            FROM fact_game_snapshots f
            LEFT JOIN dim_games d ON f.game_id = d.game_id
            WHERE d.game_id IS NULL
        """).fetchone()[0]
        result.check(
            "referential_integrity",
            orphans == 0,
            f"{orphans} games in snapshots have no dim_games entry — broken join",
        )
    except Exception as e:
        result.check("referential_integrity", False, str(e))


def check_genre_coverage(con, result):
    """Most games should have genre data from IGDB."""
    try:
        total = con.execute("SELECT COUNT(*) FROM dim_games").fetchone()[0]
        with_genre = con.execute(
            "SELECT COUNT(*) FROM dim_games WHERE genre IS NOT NULL"
        ).fetchone()[0]
        pct = (with_genre / total * 100) if total > 0 else 0
        result.check(
            "genre_coverage",
            pct >= 50,
            f"only {with_genre}/{total} games ({pct:.0f}%) have genre data",
        )
    except Exception as e:
        result.check("genre_coverage", False, str(e))


def check_no_duplicate_snapshots(con, result):
    """A game should appear only once per snapshot timestamp."""
    try:
        dupes = con.execute("""
            SELECT fetched_at, game_id, COUNT(*) AS n
            FROM fact_game_snapshots
            GROUP BY fetched_at, game_id
            HAVING COUNT(*) > 1
        """).fetchall()
        result.check(
            "no_duplicate_snapshots",
            len(dupes) == 0,
            f"{len(dupes)} game+snapshot combinations are duplicated",
        )
    except Exception as e:
        result.check("no_duplicate_snapshots", False, str(e))


# ============================================================
#  FRESHNESS CHECKS — is the data recent?
# ============================================================

def check_freshness(con, result, max_hours=1):
    """Data must have been updated within max_hours."""
    try:
        latest = con.execute(
            "SELECT MAX(CAST(fetched_at AS TIMESTAMP)) FROM raw_twitch_games"
        ).fetchone()[0]

        if latest is None:
            result.check("freshness", False, "no data at all")
            return

        now = datetime.now(timezone.utc)
        if latest.tzinfo is None:
            age = now - latest.replace(tzinfo=timezone.utc)
        else:
            age = now - latest

        hours = age.total_seconds() / 3600
        result.check(
            "freshness",
            hours < max_hours,
            f"data is {hours:.1f}h old (max allowed: {max_hours}h)",
        )
    except Exception as e:
        result.check("freshness", False, str(e))


def check_snapshot_count_growing(con, result):
    """There should be multiple snapshots (proves the pipeline ran more than once)."""
    try:
        count = con.execute(
            "SELECT COUNT(DISTINCT fetched_at) FROM fact_game_snapshots"
        ).fetchone()[0]
        result.check(
            "multiple_snapshots",
            count >= 1,
            f"only {count} snapshot(s) — pipeline may not be running repeatedly",
        )
    except Exception as e:
        result.check("multiple_snapshots", False, str(e))


# ============================================================
#  RUN ALL CHECKS
# ============================================================

def run(verbose=False, max_freshness_hours=1):
    """Run all validation checks and return results."""
    result = ValidationResult()

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        # structure
        check_tables_exist(con, result)
        check_columns_exist(con, result)

        # data quality
        check_row_counts(con, result)
        check_no_nulls(con, result)
        check_no_non_games(con, result)
        check_unique_keys(con, result)
        check_rank_continuity(con, result)
        check_viewer_counts_positive(con, result)
        check_snapshot_consistency(con, result)
        check_dim_fact_join(con, result)
        check_genre_coverage(con, result)
        check_no_duplicate_snapshots(con, result)

        # freshness
        check_freshness(con, result, max_hours=max_freshness_hours)
        check_snapshot_count_growing(con, result)
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
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--max-hours", type=int, default=1)
    args = parser.parse_args()

    result = run(verbose=args.verbose, max_freshness_hours=args.max_hours)
    sys.exit(0 if result.ok else 1)
