import duckdb
import pandas as pd
import requests
from datetime import datetime, timezone

from ingest.logger import get_logger

logger = get_logger("fetch_steam")

DB_PATH = "data/game_pulse.duckdb"

# Cache the Steam app list in memory (fetched once per pipeline run, ~190k apps)
_steam_app_cache = None


def search_steam_app(game_name):
    """Search Steam store for a game by name. Returns app_id or None.

    Uses Steam's store search API which matches game names and returns
    the best results. We take the first result only if the name matches
    closely (case-insensitive).
    """
    try:
        r = requests.get(
            "https://store.steampowered.com/api/storesearch/",
            params={"term": game_name, "l": "english", "cc": "US"},
            timeout=10,
        )
        r.raise_for_status()
        items = r.json().get("items", [])

        if not items:
            return None

        # Take first result if name matches closely (case-insensitive)
        best = items[0]
        if best["name"].strip().lower() == game_name.strip().lower():
            return str(best["id"])

        # Also check second result in case first is a DLC/soundtrack
        if len(items) > 1:
            second = items[1]
            if second["name"].strip().lower() == game_name.strip().lower():
                return str(second["id"])

        return None

    except requests.RequestException:
        return None


def get_game_names(con):
    """Get distinct game names from dim_games (or raw_twitch_games if dim_games doesn't exist yet)."""
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]

    if "dim_games" in tables:
        rows = con.execute("""
            SELECT DISTINCT game_id, game_name
            FROM dim_games
            WHERE game_name IS NOT NULL
        """).fetchdf()
    elif "raw_twitch_games" in tables:
        rows = con.execute("""
            SELECT DISTINCT id AS game_id, name AS game_name
            FROM raw_twitch_games
            WHERE name IS NOT NULL
        """).fetchdf()
    else:
        logger.info("No game tables exist yet, skipping Steam fetch")
        return pd.DataFrame(columns=["game_id", "game_name"])

    logger.info(f"Found {len(rows)} games to match against Steam")
    return rows


def match_games_to_steam(games_df):
    """Match game names to Steam app IDs by searching the Steam store.

    Searches each game name individually. Returns list of dicts with
    game_id, game_name, steam_app_id. Games not on Steam are skipped.
    """
    global _steam_app_cache
    if _steam_app_cache is None:
        _steam_app_cache = {}

    matches = []
    searched = 0

    for _, row in games_df.iterrows():
        name = row["game_name"]
        name_lower = name.strip().lower()

        # Check cache first (avoid re-searching across runs)
        if name_lower in _steam_app_cache:
            app_id = _steam_app_cache[name_lower]
            if app_id:
                matches.append({
                    "game_id": row["game_id"],
                    "game_name": name,
                    "steam_app_id": app_id,
                })
            continue

        # Search Steam store
        app_id = search_steam_app(name)
        _steam_app_cache[name_lower] = app_id
        searched += 1

        if app_id:
            matches.append({
                "game_id": row["game_id"],
                "game_name": name,
                "steam_app_id": app_id,
            })

    logger.info(f"Matched {len(matches)}/{len(games_df)} games to Steam ({searched} new searches)")
    if matches:
        matched_names = [m["game_name"] for m in matches[:5]]
        logger.info(f"  Matched: {', '.join(matched_names)}")
    return matches


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


def fetch_all_player_counts(matches):
    """Fetch player counts for all matched Steam games."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    failed = 0

    for match in matches:
        count = fetch_player_count(match["steam_app_id"])

        if count is not None:
            rows.append({
                "steam_app_id": match["steam_app_id"],
                "game_id": match["game_id"],
                "game_name": match["game_name"],
                "player_count": count,
                "fetched_at": now,
            })
        else:
            failed += 1

    logger.info(f"Fetched {len(rows)}/{len(matches)} player counts ({failed} failed)")
    return pd.DataFrame(rows)


def save_to_duckdb(con, df):
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_steam_players (
                steam_app_id VARCHAR,
                game_id VARCHAR,
                game_name VARCHAR,
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
            game_id VARCHAR,
            game_name VARCHAR,
            player_count INTEGER,
            fetched_at VARCHAR
        )
    """)


def run():
    """Entry point callable from pipeline.py or __main__."""
    con = duckdb.connect(DB_PATH)
    try:
        ensure_table(con)

        # Get game names from our database
        games = get_game_names(con)
        if games.empty:
            logger.info("No games in database yet")
            return {"source": "steam", "status": "success", "rows": 0}

        # Search Steam store for each game name
        matches = match_games_to_steam(games)
        if not matches:
            logger.info("No games matched to Steam (most may not be on Steam)")
            return {"source": "steam", "status": "success", "rows": 0}

        # Fetch player counts for matched games
        df = fetch_all_player_counts(matches)

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