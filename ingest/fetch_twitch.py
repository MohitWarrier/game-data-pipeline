import duckdb
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime, timezone

from ingest.config import load_config
from ingest.logger import get_logger

logger = get_logger("fetch_twitch")

DB_PATH = "data/game_pulse.duckdb"


def get_access_token(config):
    try:
        r = requests.post(
            "https://id.twitch.tv/oauth2/token",
            params={
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        r.raise_for_status()
        token = r.json()["access_token"]
        logger.info("Twitch auth successful")
        return token
    except requests.RequestException as e:
        logger.error(f"Twitch auth failed: {e}")
        raise


def fetch_top_games(token, config):
    headers = {
        "Client-ID": config["client_id"],
        "Authorization": f"Bearer {token}",
    }

    # fetch top 20 games
    r = requests.get(
        "https://api.twitch.tv/helix/games/top",
        headers=headers,
        params={"first": 20},
        timeout=10,
    )
    r.raise_for_status()
    games = r.json()["data"]
    logger.info(f"Games endpoint responded in {r.elapsed.total_seconds():.2f}s")

    # fetch stream viewer counts per game
    game_ids = [g["id"] for g in games]
    params = [("game_id", gid) for gid in game_ids] + [("first", 100)]
    r2 = requests.get(
        "https://api.twitch.tv/helix/streams",
        headers=headers,
        params=params,
        timeout=10,
    )
    r2.raise_for_status()
    streams = r2.json()["data"]
    logger.info(f"Streams endpoint responded in {r2.elapsed.total_seconds():.2f}s")

    # sum viewers per game
    viewer_counts = defaultdict(int)
    stream_counts = defaultdict(int)
    for s in streams:
        viewer_counts[s["game_id"]] += s["viewer_count"]
        stream_counts[s["game_id"]] += 1

    for g in games:
        g["viewer_count"] = viewer_counts.get(g["id"], 0)
        g["stream_count"] = stream_counts.get(g["id"], 0)

    df = pd.DataFrame(games)
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(f"Fetched {len(df)} games from Twitch")
    return df


def save_to_duckdb(df):
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_twitch_games (
                id VARCHAR,
                name VARCHAR,
                box_art_url VARCHAR,
                igdb_id VARCHAR,
                viewer_count INTEGER,
                stream_count INTEGER,
                fetched_at VARCHAR
            )
        """)
        con.execute("INSERT INTO raw_twitch_games SELECT * FROM df")
        logger.info(f"Saved {len(df)} rows to raw_twitch_games")
    except Exception as e:
        logger.error(f"DuckDB write failed: {e}")
        raise
    finally:
        con.close()


def run(config=None):
    """Entry point callable from pipeline.py or __main__."""
    if config is None:
        config = load_config()

    token = get_access_token(config)
    df = fetch_top_games(token, config)
    save_to_duckdb(df)
    return {"source": "twitch", "status": "success", "rows": len(df)}


if __name__ == "__main__":
    try:
        result = run()
        logger.info(f"Done — {result['rows']} games saved")
    except Exception as e:
        logger.error(f"Twitch ingest failed: {e}")
        raise
