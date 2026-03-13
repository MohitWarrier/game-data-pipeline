import os
import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

def get_access_token():
    r = requests.post("https://id.twitch.tv/oauth2/token", params={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    })
    return r.json()["access_token"]

def fetch_top_games(token):
    r = requests.get("https://api.twitch.tv/helix/games/top", 
        headers={"Client-ID": CLIENT_ID, "Authorization": f"Bearer {token}"},
        params={"first": 20}
    )
    games = r.json()["data"]

    # get viewer counts per game from streams endpoint
    game_ids = [g["id"] for g in games]
    params = [("game_id", gid) for gid in game_ids] + [("first", 100)]
    r2 = requests.get("https://api.twitch.tv/helix/streams",
        headers={"Client-ID": CLIENT_ID, "Authorization": f"Bearer {token}"},
        params=params
    )
    streams = r2.json()["data"]

    # sum viewers per game
    from collections import defaultdict
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
    return df

def save_to_duckdb(df):
    con = duckdb.connect("/workspaces/game-data-pipeline/data/game_pulse.duckdb")
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
    con.close()

if __name__ == "__main__":
    token = get_access_token()
    df = fetch_top_games(token)
    save_to_duckdb(df)
    print(f"Saved {len(df)} games at {datetime.now(timezone.utc).isoformat()}")
    print(df[["name", "fetched_at"]].to_string())