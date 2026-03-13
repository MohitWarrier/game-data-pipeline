import streamlit as st
import duckdb
import pandas as pd

con = duckdb.connect("data/game_pulse.duckdb")

st.title("Game Pulse")
st.subheader("What's hot and what's dying on Twitch")

df = con.execute("""
    SELECT 
        game_name,
        viewer_count,
        stream_count,
        rank_at_time,
        fetched_at
    FROM fact_game_snapshots
    ORDER BY fetched_at DESC, rank_at_time ASC
""").df()

st.subheader("Latest Snapshot")
latest = df[df['fetched_at'] == df['fetched_at'].max()]
st.dataframe(latest[['rank_at_time', 'game_name', 'viewer_count', 'stream_count']])

st.subheader("Viewer Count Over Time")
games = st.multiselect("Pick games", df['game_name'].unique(), default=["Just Chatting", "Counter-Strike", "VALORANT"])
filtered = df[df['game_name'].isin(games)]
st.line_chart(filtered.pivot(index='fetched_at', columns='game_name', values='viewer_count'))