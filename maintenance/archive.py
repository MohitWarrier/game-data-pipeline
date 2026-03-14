"""
Hot/Cold Storage — Archive old data from DuckDB to Parquet files.

Hot storage: recent data in DuckDB (fast queries, used by dashboard and dbt).
Cold storage: older data exported to Parquet files in data/archive/ (cheap, still
queryable by DuckDB if needed, but not loaded by default).

This script:
1. Exports rows older than RETENTION_DAYS from each raw table to a dated Parquet file.
2. Deletes those rows from DuckDB.
3. Logs how many rows were archived and how many remain.

Usage:
    python -m maintenance.archive                  # default 30 day retention
    python -m maintenance.archive --days 7         # keep only last 7 days
    python -m maintenance.archive --dry-run        # show what would happen without doing it
"""

import os
import sys
import argparse
import duckdb
from datetime import datetime, timezone, timedelta

# add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest.logger import get_logger

logger = get_logger("archive")

DB_PATH = "data/game_pulse.duckdb"
ARCHIVE_DIR = "data/archive"

# tables that grow over time and need archival
# format: (table_name, timestamp_column)
ARCHIVABLE_TABLES = [
    ("raw_twitch_games", "fetched_at"),
    ("raw_steam_players", "fetched_at"),
]


def get_cutoff_date(retention_days):
    return (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()


def archive_table(con, table_name, timestamp_col, cutoff_date, dry_run=False):
    """Archive rows older than cutoff_date from a single table to Parquet."""
    # count rows to archive
    old_count = con.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE CAST({timestamp_col} AS TIMESTAMP) < CAST('{cutoff_date}' AS TIMESTAMP)
    """).fetchone()[0]

    if old_count == 0:
        logger.info(f"  {table_name}: nothing to archive")
        return 0

    total_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if dry_run:
        logger.info(f"  {table_name}: WOULD archive {old_count}/{total_count} rows")
        return 0

    # create archive directory
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # export old rows to parquet with date stamp
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parquet_path = os.path.join(ARCHIVE_DIR, f"{table_name}_{date_str}.parquet")

    con.execute(f"""
        COPY (
            SELECT * FROM {table_name}
            WHERE CAST({timestamp_col} AS TIMESTAMP) < CAST('{cutoff_date}' AS TIMESTAMP)
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)

    # delete archived rows from DuckDB
    con.execute(f"""
        DELETE FROM {table_name}
        WHERE CAST({timestamp_col} AS TIMESTAMP) < CAST('{cutoff_date}' AS TIMESTAMP)
    """)

    remaining = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"  {table_name}: archived {old_count} rows -> {parquet_path} ({remaining} rows remain)")
    return old_count


def run(retention_days=30, dry_run=False):
    """Archive old data from all archivable tables."""
    cutoff = get_cutoff_date(retention_days)
    mode = "DRY RUN" if dry_run else "LIVE"

    logger.info(f"Starting archival ({mode})")
    logger.info(f"  Retention: {retention_days} days")
    logger.info(f"  Cutoff: {cutoff}")
    logger.info(f"  Archive dir: {ARCHIVE_DIR}")

    con = duckdb.connect(DB_PATH)
    total_archived = 0
    try:
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        for table_name, timestamp_col in ARCHIVABLE_TABLES:
            if table_name not in tables:
                logger.info(f"  {table_name}: table does not exist, skipping")
                continue
            archived = archive_table(con, table_name, timestamp_col, cutoff, dry_run)
            total_archived += archived
    finally:
        con.close()

    logger.info(f"Archival complete: {total_archived} total rows archived")
    return total_archived


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Archive old pipeline data to Parquet")
    parser.add_argument("--days", type=int, default=30, help="Keep data from the last N days (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without doing it")
    args = parser.parse_args()

    run(retention_days=args.days, dry_run=args.dry_run)
