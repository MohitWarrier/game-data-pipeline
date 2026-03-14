import duckdb
import pandas as pd
import requests
from datetime import datetime, timezone

from ingest.logger import get_logger

logger = get_logger("fetch_steam")

DB_PATH = "data/game_pulse.duckdb"


def get_steam_app_ids(con):
    """Get distinct steam_app_ids from raw_igdb_games."""
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    if "raw_igdb_games" not in tables:
        logger.info("raw_igdb_games table does not exist yet, skipping Steam fetch")
        return pd.DataFrame(columns=["igdb_id", "steam_app_id"])

    rows = con.execute("""
        SELECT DISTINCT igdb_id, steam_app_id
        FROM raw_igdb_games
        WHERE steam_app_id IS NOT NULL AND steam_app_id != ''
    """).fetchdf()
    logger.info(f"Found {len(rows)} Steam app IDs to fetch")
    return rows


def fetch_player_count(steam_app_id):
    """Fetch current player count for a single Steam app."""
    try:
        r = requests.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
            params={"appid": steam_app_id},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("response", {}).get("result") == 1:
            return data["response"]["player_count"]
        return None
    except requests.RequestException as e:
        logger.warning(f"Steam API failed for app {steam_app_id}: {e}")
        return None


def fetch_all_player_counts(apps_df):
    """Fetch player counts for all Steam apps."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    failed = 0

    for _, row in apps_df.iterrows():
        steam_app_id = row["steam_app_id"]
        igdb_id = row["igdb_id"]
        count = fetch_player_count(steam_app_id)

        if count is not None:
            rows.append({
                "steam_app_id": steam_app_id,
                "igdb_id": igdb_id,
                "player_count": count,
                "fetched_at": now,
            })
        else:
            failed += 1

    total = len(apps_df)
    logger.info(f"Fetched {len(rows)}/{total} player counts ({failed} failed)")
    return pd.DataFrame(rows)


def save_to_duckdb(con, df):
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_steam_players (
                steam_app_id VARCHAR,
                igdb_id VARCHAR,
                player_count INTEGER,
                fetched_at VARCHAR
            )
        """)
        con.execute("INSERT INTO raw_steam_players SELECT * FROM df")
        logger.info(f"Saved {len(df)} rows to raw_steam_players")
    except Exception as e:
        logger.error(f"DuckDB write failed: {e}")
        raise


def ensure_table(con):
    """Create raw_steam_players if it doesn't exist so dbt never fails on first run."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_steam_players (
            steam_app_id VARCHAR,
            igdb_id VARCHAR,
            player_count INTEGER,
            fetched_at VARCHAR
        )
    """)


def run():
    """Entry point callable from pipeline.py or __main__."""
    con = duckdb.connect(DB_PATH)
    try:
        ensure_table(con)

        apps = get_steam_app_ids(con)

        if apps.empty:
            logger.info("No Steam app IDs to fetch")
            return {"source": "steam", "status": "success", "rows": 0}

        df = fetch_all_player_counts(apps)

        if df.empty:
            logger.warning("No player counts returned from Steam")
            return {"source": "steam", "status": "success", "rows": 0}

        save_to_duckdb(con, df)
        return {"source": "steam", "status": "success", "rows": len(df)}
    finally:
        con.close()


if __name__ == "__main__":
    try:
        result = run()
        logger.info(f"Done — {result['rows']} player counts saved")
    except Exception as e:
        logger.error(f"Steam ingest failed: {e}")
        raise
