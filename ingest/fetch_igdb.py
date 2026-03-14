import duckdb
import pandas as pd
import requests
from datetime import datetime, timezone

from ingest.config import load_config
from ingest.logger import get_logger

logger = get_logger("fetch_igdb")

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
        logger.info("IGDB auth successful (via Twitch OAuth)")
        return r.json()["access_token"]
    except requests.RequestException as e:
        logger.error(f"IGDB auth failed: {e}")
        raise


def get_twitch_games(con):
    """Get distinct games from raw_twitch_games that haven't been fetched yet."""
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]

    if "raw_igdb_games" in tables:
        existing = con.execute(
            "SELECT DISTINCT igdb_id FROM raw_igdb_games"
        ).fetchall()
        existing_ids = {row[0] for row in existing}
    else:
        existing_ids = set()

    games = con.execute("""
        SELECT DISTINCT id, name, igdb_id
        FROM raw_twitch_games
        WHERE igdb_id IS NOT NULL AND igdb_id != ''
    """).fetchdf()

    games = games[~games["igdb_id"].isin(existing_ids)]
    logger.info(f"Found {len(games)} new games to fetch from IGDB (skipping {len(existing_ids)} already fetched)")
    return games


def fetch_igdb_metadata(token, config, igdb_ids):
    """Fetch game metadata from IGDB for a list of IGDB IDs."""
    headers = {
        "Client-ID": config["client_id"],
        "Authorization": f"Bearer {token}",
    }

    ids_str = ",".join(str(i) for i in igdb_ids)
    body = (
        f"fields name, genres.name, first_release_date, "
        f"involved_companies.company.name, involved_companies.developer, "
        f"external_games.category, external_games.uid; "
        f"where id = ({ids_str}); "
        f"limit 500;"
    )

    try:
        r = requests.post(
            "https://api.igdb.com/v4/games",
            headers=headers,
            data=body,
            timeout=15,
        )
        r.raise_for_status()
        logger.info(f"IGDB responded in {r.elapsed.total_seconds():.2f}s for {len(igdb_ids)} IDs")
        return r.json()
    except requests.RequestException as e:
        logger.error(f"IGDB API request failed: {e}")
        raise


def parse_igdb_response(games_data):
    """Parse IGDB API response into flat rows."""
    rows = []
    for game in games_data:
        igdb_id = str(game.get("id", ""))

        # extract first genre name
        genres = game.get("genres", [])
        genre = genres[0]["name"] if genres else None

        # extract release year from unix timestamp
        release_ts = game.get("first_release_date")
        release_year = (
            datetime.fromtimestamp(release_ts, tz=timezone.utc).year
            if release_ts else None
        )

        # extract developer name
        developer = None
        for company in game.get("involved_companies", []):
            if company.get("developer"):
                developer = company.get("company", {}).get("name")
                break

        # extract steam app id (category 1 = Steam)
        steam_app_id = None
        for ext in game.get("external_games", []):
            if ext.get("category") == 1:
                steam_app_id = ext.get("uid")
                break

        rows.append({
            "igdb_id": igdb_id,
            "name": game.get("name"),
            "genre": genre,
            "release_year": release_year,
            "developer": developer,
            "steam_app_id": steam_app_id,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })

    logger.info(f"Parsed {len(rows)} games from IGDB response")
    return pd.DataFrame(rows)


def save_to_duckdb(con, df):
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_igdb_games (
                igdb_id VARCHAR,
                name VARCHAR,
                genre VARCHAR,
                release_year INTEGER,
                developer VARCHAR,
                steam_app_id VARCHAR,
                fetched_at VARCHAR
            )
        """)
        con.execute("INSERT INTO raw_igdb_games SELECT * FROM df")
        logger.info(f"Saved {len(df)} rows to raw_igdb_games")
    except Exception as e:
        logger.error(f"DuckDB write failed: {e}")
        raise


def run(config=None):
    """Entry point callable from pipeline.py or __main__."""
    if config is None:
        config = load_config()

    con = duckdb.connect(DB_PATH)
    try:
        token = get_access_token(config)
        games = get_twitch_games(con)

        if games.empty:
            logger.info("No new games to fetch from IGDB")
            return {"source": "igdb", "status": "success", "rows": 0}

        igdb_ids = games["igdb_id"].astype(int).tolist()

        # IGDB allows up to 500 IDs per request, batch if needed
        all_rows = []
        for i in range(0, len(igdb_ids), 500):
            batch = igdb_ids[i:i + 500]
            logger.info(f"Fetching IGDB batch {i // 500 + 1} ({len(batch)} IDs)")
            data = fetch_igdb_metadata(token, config, batch)
            if isinstance(data, list):
                all_rows.extend(data)

        if not all_rows:
            logger.warning("No metadata returned from IGDB")
            return {"source": "igdb", "status": "success", "rows": 0}

        df = parse_igdb_response(all_rows)
        save_to_duckdb(con, df)
        return {"source": "igdb", "status": "success", "rows": len(df)}
    finally:
        con.close()


if __name__ == "__main__":
    try:
        result = run()
        logger.info(f"Done — {result['rows']} games saved from IGDB")
    except Exception as e:
        logger.error(f"IGDB ingest failed: {e}")
        raise
