import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Game Pulse", page_icon="🎮", layout="wide")

con = duckdb.connect("data/game_pulse.duckdb")

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

latest = df[df['fetched_at'] == df['fetched_at'].max()]
prev = df[df['fetched_at'] == df['fetched_at'].unique()[1]] if len(df['fetched_at'].unique()) > 1 else latest

st.title("🎮 Game Pulse")
st.caption("Live Twitch rankings — updated every 30 minutes")

st.divider()

# top metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Top Game", latest.iloc[0]['game_name'], f"{latest.iloc[0]['viewer_count']:,} viewers")
with col2:
    st.metric("Total Snapshots", len(df['fetched_at'].unique()))
with col3:
    st.metric("Last Updated", str(latest.iloc[0]['fetched_at'])[:16])

st.divider()

# current rankings
col_left, col_right = st.columns([1, 2])

with col_left:
    st.subheader("Current Top 20")
    display = latest[['rank_at_time', 'game_name', 'viewer_count']].copy()
    display.columns = ['Rank', 'Game', 'Viewers']
    display['Viewers'] = display['Viewers'].apply(lambda x: f"{x:,}")
    display = display.reset_index(drop=True)
    st.dataframe(display, hide_index=True, use_container_width=True)

with col_right:
    st.subheader("Viewer Count Over Time")
    all_games = df['game_name'].unique().tolist()
    default_games = ["Counter-Strike", "VALORANT", "Dota 2", "League of Legends"]
    default_games = [g for g in default_games if g in all_games]
    selected = st.multiselect("Select games", all_games, default=default_games)
    
    if selected:
        filtered = df[df['game_name'].isin(selected)]
        fig = px.line(
            filtered,
            x='fetched_at',
            y='viewer_count',
            color='game_name',
            labels={'fetched_at': 'Time', 'viewer_count': 'Viewers', 'game_name': 'Game'},
            template='plotly_dark'
        )
        fig.update_layout(
            legend_title_text='',
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# rank changes
if len(df['fetched_at'].unique()) > 1:
    st.subheader("Rank Changes")
    merged = latest[['game_name', 'rank_at_time']].merge(
        prev[['game_name', 'rank_at_time']], on='game_name', suffixes=('_now', '_prev')
    )
    merged['change'] = merged['rank_at_time_prev'] - merged['rank_at_time_now']
    merged['arrow'] = merged['change'].apply(lambda x: '🔺' if x > 0 else ('🔻' if x < 0 else '➡️'))
    merged['Change'] = merged.apply(lambda r: f"{r['arrow']} {abs(int(r['change']))}" if r['change'] != 0 else "➡️ same", axis=1)
    merged.columns = ['Game', 'Rank Now', 'Rank Before', 'change', 'arrow', 'Change']
    st.dataframe(merged[['Game', 'Rank Now', 'Rank Before', 'Change']].reset_index(drop=True), hide_index=True, use_container_width=True)