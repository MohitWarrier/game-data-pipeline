"""
Game Pulse Dashboard
====================
A game analytics dashboard. Streaming metrics (viewers, streams) are used
as proxies for game popularity — the focus is on games, not streamers.

Architecture:
  - Data loaders:  one @st.cache_data function per source (easy to extend)
  - Chart helpers: source-agnostic functions that take a df + column names
  - Tabs:          each tab is a self-contained section that calls helpers
  Adding a new data source = add a loader + call existing chart helpers.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import glob as globmod
from datetime import datetime

st.set_page_config(page_title="Game Pulse", page_icon="\U0001f3ae", layout="wide")

# inject minimal CSS for cleaner spacing
st.markdown("""
<style>
    /* tighter metric cards */
    [data-testid="stMetric"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
        padding: 12px 16px;
    }
    /* remove extra top padding in tabs */
    .stTabs [data-baseweb="tab-panel"] { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = "data/game_pulse.duckdb"

# --- visual constants ---
PALETTE = [
    "#36d399", "#66b2ff", "#ffb366", "#ff6b8a",
    "#a78bfa", "#38bdf8", "#fbbf24", "#f87171",
]
POSITIVE = "#36d399"
NEGATIVE = "#ff6b8a"
NEUTRAL = "#6b7280"

LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=10, r=10, t=40, b=10),
    font=dict(size=12),
)


def fmt_date(ts):
    """Format a timestamp as dd/mm/yy HH:MM IST."""
    if isinstance(ts, str):
        ts = pd.Timestamp(ts)
    return ts.strftime("%d/%m/%y %H:%M") + " IST"


def fmt_date_short(ts):
    """Format as dd/mm/yy."""
    if isinstance(ts, str):
        ts = pd.Timestamp(ts)
    return ts.strftime("%d/%m/%y")


# ============================================================
#  DATA LOADERS — one per source, easy to extend
# ============================================================

@st.cache_data(ttl=60)
def load_snapshots():
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT game_id, game_name, viewer_count, stream_count,
               rank_at_time, genre, release_year, fetched_at
        FROM fact_game_snapshots
        ORDER BY fetched_at DESC, rank_at_time ASC
    """).df()
    con.close()
    return df


@st.cache_data(ttl=60)
def load_games():
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT game_id, game_name, igdb_id, genre, release_year,
               developer, steam_app_id
        FROM dim_games
    """).df()
    con.close()
    return df


@st.cache_data(ttl=30)
def load_pipeline_health():
    """Load run reports from logs/runs/ and database health info."""
    info = {}

    # --- load run reports ---
    run_files = sorted(globmod.glob("logs/runs/run_*.json"), reverse=True)
    runs = []
    for path in run_files[:20]:  # last 20 runs
        try:
            with open(path, "r") as f:
                runs.append(json.load(f))
        except (json.JSONDecodeError, IOError):
            pass
    info["runs"] = runs

    # latest run (quick access)
    try:
        with open("logs/runs/latest.json", "r") as f:
            info["latest_run"] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        info["latest_run"] = None

    # --- database health ---
    con = duckdb.connect(DB_PATH, read_only=True)
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    info["tables"] = {}
    for t in tables:
        info["tables"][t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    if "fact_game_snapshots" in tables:
        row = con.execute("""
            SELECT MIN(fetched_at), MAX(fetched_at), COUNT(DISTINCT fetched_at)
            FROM fact_game_snapshots
        """).fetchone()
        info["first_snapshot"] = row[0]
        info["latest_snapshot"] = row[1]
        info["snapshot_count"] = row[2]
    else:
        info["first_snapshot"] = None
        info["latest_snapshot"] = None
        info["snapshot_count"] = 0
    con.close()

    # --- pipeline log tail ---
    info["log_lines"] = []
    try:
        with open("logs/pipeline.log", "r") as f:
            lines = f.readlines()
            info["log_lines"] = [l.rstrip() for l in reversed(lines[-50:])]
    except FileNotFoundError:
        pass

    return info


# ============================================================
#  CHART HELPERS — reusable, source-agnostic
# ============================================================

def make_bar_h(df, x, y, title="", height=350, show_text=True):
    fig = px.bar(
        df, x=x, y=y, orientation="h",
        color=y, color_discrete_sequence=PALETTE, title=title,
    )
    fig.update_layout(**LAYOUT, showlegend=False, height=height,
                      yaxis=dict(title=""), xaxis=dict(title=""))
    if show_text:
        fig.update_traces(texttemplate="%{x:,.0f}", textposition="inside")
    return fig


def make_line(df, x, y, color=None, title="", height=350, invert_y=False):
    fig = px.line(
        df, x=x, y=y, color=color,
        color_discrete_sequence=PALETTE, title=title, markers=True,
    )
    layout = dict(**LAYOUT, height=height, xaxis_title="", yaxis_title="",
                  legend_title_text="",
                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
    if invert_y:
        layout["yaxis"] = dict(autorange="reversed", dtick=1)
    fig.update_layout(**layout)
    return fig


def make_donut(df, values, names, title="", height=380):
    fig = px.pie(
        df, values=values, names=names, hole=0.45,
        color_discrete_sequence=PALETTE, title=title,
    )
    fig.update_layout(**LAYOUT, height=height,
                      legend=dict(orientation="h", yanchor="bottom", y=-0.15))
    fig.update_traces(textinfo="percent+label", textposition="inside")
    return fig


def make_area(df, x, y, color, title="", height=400):
    fig = px.area(
        df, x=x, y=y, color=color,
        color_discrete_sequence=PALETTE, title=title,
    )
    fig.update_layout(**LAYOUT, height=height, xaxis_title="", yaxis_title="",
                      legend_title_text="",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02))
    return fig


# ============================================================
#  ANALYTICS HELPERS
# ============================================================

def compute_movers(df, snapshots):
    """Compare latest vs previous snapshot — viewer/rank deltas."""
    if len(snapshots) < 2:
        return pd.DataFrame()

    curr = df[df["fetched_at"] == snapshots[-1]][
        ["game_name", "viewer_count", "rank_at_time", "genre"]
    ].copy()
    prev = df[df["fetched_at"] == snapshots[-2]][
        ["game_name", "viewer_count", "rank_at_time"]
    ].copy()

    curr.columns = ["game_name", "popularity_now", "rank_now", "genre"]
    prev.columns = ["game_name", "popularity_prev", "rank_prev"]

    m = curr.merge(prev, on="game_name", how="inner")
    m["pop_change"] = m["popularity_now"] - m["popularity_prev"]
    m["pop_pct"] = (
        (m["pop_change"] / m["popularity_prev"].replace(0, 1)) * 100
    ).round(1)
    m["rank_change"] = m["rank_prev"] - m["rank_now"]  # positive = climbed
    return m.sort_values("pop_change", ascending=False)


# ============================================================
#  LOAD DATA
# ============================================================

df = load_snapshots()
dim = load_games()

if df.empty:
    st.title("\U0001f3ae Game Pulse")
    st.warning("No data yet. Run `make start` to begin collecting.")
    st.stop()

snapshots = sorted(df["fetched_at"].unique())
latest = df[df["fetched_at"] == snapshots[-1]]
has_history = len(snapshots) > 1


# ============================================================
#  SIDEBAR
# ============================================================

with st.sidebar:
    st.title("\U0001f3ae Game Pulse")
    st.caption("Game Analytics Dashboard")
    st.divider()
    st.markdown(f"**{len(dim)}** games tracked")
    st.markdown(f"**{dim['genre'].dropna().nunique()}** genres")
    st.markdown(f"**{len(snapshots)}** snapshots")
    st.divider()
    st.markdown(f"First: `{fmt_date(snapshots[0])}`")
    st.markdown(f"Latest: `{fmt_date(snapshots[-1])}`")


# ============================================================
#  TABS
# ============================================================

tab_live, tab_movers, tab_trends, tab_genres, tab_deep, tab_health = st.tabs([
    "\U0001f3ae Overview",
    "\U0001f525 Movers",
    "\U0001f4c8 Trends",
    "\U0001f3af Genres",
    "\U0001f50d Deep Dive",
    "\u2699\ufe0f Pipeline",
])


# ==================== TAB 1: OVERVIEW ====================

with tab_live:
    top = latest.iloc[0]

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("\U0001f451 Most Popular", top["game_name"],
                  f"Rank #1 \u2022 {top['viewer_count']:,} watching")
    with c2:
        st.metric("\U0001f4ca Total Popularity", f"{latest['viewer_count'].sum():,}",
                  help="Sum of concurrent viewers across all tracked games")
    with c3:
        st.metric("\U0001f4e1 Active Streams", f"{latest['stream_count'].sum():,}")
    with c4:
        st.metric("\U0001f552 Last Snapshot", fmt_date(latest.iloc[0]["fetched_at"]))

    st.divider()

    # main visual: top 10 popularity bar chart
    st.subheader("Game Popularity Rankings")
    top10 = latest.nsmallest(10, "rank_at_time").copy()
    top10["label"] = top10.apply(
        lambda r: f"#{int(r['rank_at_time'])}  {r['game_name']}", axis=1
    )
    fig = make_bar_h(top10, x="viewer_count", y="label",
                     title="", height=400)
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, width="stretch")

    # rank movement bump chart (if history exists)
    if has_history:
        st.divider()
        st.subheader("Rank Movement")
        st.caption("How the top games shifted positions over time")
        top_names = latest.nsmallest(10, "rank_at_time")["game_name"].tolist()
        bump_data = df[df["game_name"].isin(top_names)].copy()
        bump_data["snapshot"] = bump_data["fetched_at"].apply(fmt_date_short)

        fig_bump = make_line(
            bump_data, x="fetched_at", y="rank_at_time",
            color="game_name", title="", height=400, invert_y=True,
        )
        fig_bump.update_traces(line=dict(width=3), marker=dict(size=8))
        st.plotly_chart(fig_bump, width="stretch")


# ==================== TAB 2: MOVERS ====================

with tab_movers:
    if not has_history:
        st.info(
            "Need at least 2 snapshots to detect momentum. "
            "The pipeline collects a new snapshot every 30 minutes."
        )
    else:
        movers = compute_movers(df, snapshots)
        if movers.empty:
            st.info("No comparable games between snapshots.")
        else:
            st.subheader("Biggest Movers")
            st.caption(
                f"Changes between {fmt_date(snapshots[-2])} and {fmt_date(snapshots[-1])}"
            )

            col_up, col_down = st.columns(2)

            gainers = movers.head(5)
            losers = movers.tail(5).sort_values("pop_change")

            with col_up:
                st.markdown("#### \U0001f4c8 Rising")
                for _, r in gainers.iterrows():
                    rank_info = ""
                    if r["rank_change"] > 0:
                        rank_info = f" \u2022 \u2191{int(r['rank_change'])} ranks"
                    elif r["rank_change"] < 0:
                        rank_info = f" \u2022 \u2193{int(abs(r['rank_change']))} ranks"
                    pct = f"+{r['pop_pct']}%" if r["pop_pct"] > 0 else f"{r['pop_pct']}%"
                    st.metric(
                        r["game_name"],
                        f"{r['popularity_now']:,} viewers",
                        delta=f"+{r['pop_change']:,} ({pct}){rank_info}",
                        delta_color="normal",
                    )

            with col_down:
                st.markdown("#### \U0001f4c9 Falling")
                for _, r in losers.iterrows():
                    rank_info = ""
                    if r["rank_change"] > 0:
                        rank_info = f" \u2022 \u2191{int(r['rank_change'])} ranks"
                    elif r["rank_change"] < 0:
                        rank_info = f" \u2022 \u2193{int(abs(r['rank_change']))} ranks"
                    pct = f"{r['pop_pct']}%"
                    st.metric(
                        r["game_name"],
                        f"{r['popularity_now']:,} viewers",
                        delta=f"{r['pop_change']:,} ({pct}){rank_info}",
                        delta_color="normal",
                    )

            st.divider()

            # change bar chart — every game, sorted
            st.subheader("Popularity Change — All Games")
            wf = movers[["game_name", "pop_change"]].sort_values("pop_change")
            wf["color"] = wf["pop_change"].apply(
                lambda x: POSITIVE if x > 0 else NEGATIVE
            )
            fig_wf = go.Figure(go.Bar(
                x=wf["pop_change"], y=wf["game_name"], orientation="h",
                marker_color=wf["color"],
                text=wf["pop_change"].apply(
                    lambda x: f"+{x:,}" if x > 0 else f"{x:,}"
                ),
                textposition="outside",
            ))
            fig_wf.update_layout(
                **LAYOUT, height=max(300, len(wf) * 32),
                xaxis_title="Viewer Change", yaxis_title="", showlegend=False,
            )
            st.plotly_chart(fig_wf, width="stretch")

            # volatility scatter
            st.divider()
            st.subheader("Volatility Map")
            st.caption(
                "Position = how much a game moved. Bubble size = current popularity. "
                "Games in the top-right are surging. Bottom-left are crashing."
            )
            fig_sc = px.scatter(
                movers, x="pop_pct", y="rank_change",
                size="popularity_now", color="genre",
                hover_name="game_name",
                color_discrete_sequence=PALETTE,
                size_max=40,
            )
            fig_sc.update_layout(
                **LAYOUT, height=400,
                xaxis_title="Popularity Change %",
                yaxis_title="Rank Change (up = climbed)",
                legend_title_text="",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig_sc.add_hline(y=0, line_dash="dot", line_color=NEUTRAL, opacity=0.4)
            fig_sc.add_vline(x=0, line_dash="dot", line_color=NEUTRAL, opacity=0.4)
            st.plotly_chart(fig_sc, width="stretch")


# ==================== TAB 3: TRENDS ====================

with tab_trends:
    all_games = sorted(df["game_name"].unique().tolist())
    default_top5 = latest.nsmallest(5, "rank_at_time")["game_name"].tolist()
    defaults = [g for g in default_top5 if g in all_games]

    selected = st.multiselect(
        "Select games to compare", all_games, default=defaults, key="trends_games",
    )

    if not selected:
        st.info("Pick at least one game to see trends.")
    else:
        filtered = df[df["game_name"].isin(selected)]

        fig_pop = make_line(
            filtered, x="fetched_at", y="viewer_count", color="game_name",
            title="Popularity Over Time",
        )
        st.plotly_chart(fig_pop, width="stretch")

        fig_rank = make_line(
            filtered, x="fetched_at", y="rank_at_time", color="game_name",
            title="Rank Over Time (Top = #1)", invert_y=True,
        )
        st.plotly_chart(fig_rank, width="stretch")

        fig_streams = make_line(
            filtered, x="fetched_at", y="stream_count", color="game_name",
            title="Stream Count Over Time",
        )
        st.plotly_chart(fig_streams, width="stretch")

        # viewers per stream — the "one streamer carries the game" detector
        if has_history:
            st.divider()
            st.subheader("Engagement Density")
            st.caption(
                "Viewers per stream. A high ratio means a few big streamers carry the game. "
                "A low ratio means popularity is spread across many channels. "
                "Watch for sudden spikes — that's the 'one famous streamer picked this up' signal."
            )
            vps = filtered.copy()
            vps["viewers_per_stream"] = (
                vps["viewer_count"] / vps["stream_count"].replace(0, 1)
            ).round(0)
            fig_vps = make_line(
                vps, x="fetched_at", y="viewers_per_stream", color="game_name",
                title="Avg Viewers per Stream",
            )
            st.plotly_chart(fig_vps, width="stretch")


# ==================== TAB 4: GENRES ====================

with tab_genres:
    genre_data = latest[latest["genre"].notna()]

    if genre_data.empty:
        st.info("No genre data. Run the pipeline to fetch IGDB metadata.")
    else:
        col_l, col_r = st.columns(2)

        with col_l:
            genre_pop = (
                genre_data.groupby("genre")["viewer_count"]
                .sum().reset_index()
                .sort_values("viewer_count", ascending=False)
            )
            genre_pop.columns = ["genre", "popularity"]
            fig_donut = make_donut(
                genre_pop, values="popularity", names="genre",
                title="Genre Market Share",
            )
            st.plotly_chart(fig_donut, width="stretch")

        with col_r:
            genre_stats = (
                genre_data.groupby("genre")
                .agg(popularity=("viewer_count", "sum"),
                     streams=("stream_count", "sum"),
                     games=("game_name", "nunique"))
                .reset_index()
            )
            genre_stats["avg_per_game"] = genre_stats["popularity"] // genre_stats["games"]
            genre_stats = genre_stats.sort_values("popularity", ascending=False)

            fig_genre = make_bar_h(
                genre_stats, x="popularity", y="genre",
                title="Total Popularity by Genre", height=380,
            )
            st.plotly_chart(fig_genre, width="stretch")

        st.divider()
        col_a, col_b = st.columns(2)

        with col_a:
            fig_count = make_bar_h(
                genre_stats, x="games", y="genre",
                title="Games per Genre", height=350,
            )
            st.plotly_chart(fig_count, width="stretch")

        with col_b:
            fig_avg = make_bar_h(
                genre_stats, x="avg_per_game", y="genre",
                title="Avg Popularity per Game (which genres punch above their weight?)",
                height=350,
            )
            st.plotly_chart(fig_avg, width="stretch")

        if has_history:
            st.divider()
            genre_time = (
                df[df["genre"].notna()]
                .groupby(["fetched_at", "genre"])["viewer_count"]
                .sum().reset_index()
            )
            genre_time.columns = ["fetched_at", "genre", "popularity"]
            fig_area = make_area(
                genre_time, x="fetched_at", y="popularity", color="genre",
                title="Genre Popularity Over Time",
            )
            st.plotly_chart(fig_area, width="stretch")


# ==================== TAB 5: DEEP DIVE ====================

with tab_deep:
    game_list = sorted(dim["game_name"].unique().tolist())

    if not game_list:
        st.info("No game data available.")
    else:
        selected_game = st.selectbox("Select a game", game_list, key="detail_game")

        info = dim[dim["game_name"] == selected_game].iloc[0]
        history = df[df["game_name"] == selected_game].sort_values("fetched_at")

        # metadata
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"**Genre:** {info['genre'] if pd.notna(info['genre']) else '\u2014'}")
        with c2:
            yr = info["release_year"]
            st.markdown(f"**Released:** {int(yr) if pd.notna(yr) else '\u2014'}")
        with c3:
            dev = info["developer"] if pd.notna(info["developer"]) else "\u2014"
            st.markdown(f"**Developer:** {dev}")

        st.divider()

        if history.empty:
            st.info("No snapshot data for this game yet.")
        else:
            lg = history.iloc[-1]

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                st.metric("Current Rank", f"#{int(lg['rank_at_time'])}")
            with c2:
                st.metric("Popularity", f"{lg['viewer_count']:,}")
            with c3:
                st.metric("Streams", f"{lg['stream_count']:,}")
            with c4:
                st.metric("Peak Popularity", f"{history['viewer_count'].max():,}")
            with c5:
                st.metric("Best Rank", f"#{int(history['rank_at_time'].min())}")

            if len(history) > 1:
                st.divider()

                # dual axis: popularity + streams
                fig_dual = go.Figure()
                fig_dual.add_trace(go.Scatter(
                    x=history["fetched_at"], y=history["viewer_count"],
                    name="Popularity", mode="lines+markers",
                    line=dict(color=PALETTE[0], width=3),
                ))
                fig_dual.add_trace(go.Scatter(
                    x=history["fetched_at"], y=history["stream_count"],
                    name="Streams", yaxis="y2", mode="lines+markers",
                    line=dict(color=PALETTE[1], width=2, dash="dot"),
                ))
                fig_dual.update_layout(
                    **LAYOUT, height=350,
                    title="Popularity & Stream Count",
                    yaxis=dict(title="Viewers", side="left"),
                    yaxis2=dict(title="Streams", side="right",
                                overlaying="y", showgrid=False),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_dual, width="stretch")

                # rank history
                fig_rh = go.Figure(go.Scatter(
                    x=history["fetched_at"], y=history["rank_at_time"],
                    mode="lines+markers+text",
                    text=history["rank_at_time"].apply(lambda x: f"#{int(x)}"),
                    textposition="top center",
                    line=dict(color=PALETTE[0], width=3),
                    marker=dict(size=10),
                ))
                fig_rh.update_layout(
                    **LAYOUT, height=300, title="Rank History",
                    yaxis=dict(autorange="reversed", dtick=1, title="Rank"),
                    xaxis_title="", showlegend=False,
                )
                st.plotly_chart(fig_rh, width="stretch")

                # momentum bars
                mom = history.copy()
                mom["change"] = mom["viewer_count"].diff()
                mom = mom[mom["change"].notna()]

                if not mom.empty:
                    st.divider()
                    st.subheader("Momentum")
                    st.caption(
                        "Snapshot-over-snapshot change in popularity. "
                        "Green bars = growing, red bars = shrinking."
                    )
                    colors = [POSITIVE if v > 0 else NEGATIVE for v in mom["change"]]
                    fig_mom = go.Figure(go.Bar(
                        x=mom["fetched_at"], y=mom["change"],
                        marker_color=colors,
                        text=mom["change"].apply(
                            lambda x: f"+{x:,.0f}" if x > 0 else f"{x:,.0f}"
                        ),
                        textposition="outside",
                    ))
                    fig_mom.update_layout(
                        **LAYOUT, height=300,
                        title="Popularity Change per Snapshot",
                        xaxis_title="", yaxis_title="Change", showlegend=False,
                    )
                    st.plotly_chart(fig_mom, width="stretch")


# ==================== TAB 6: PIPELINE HEALTH ====================

with tab_health:
    health = load_pipeline_health()
    latest_run = health.get("latest_run")

    # --- row 1: key status indicators ---
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        if health["latest_snapshot"]:
            age_min = (datetime.now() - health["latest_snapshot"]).total_seconds() / 60
            if age_min < 35:
                st.metric("\u2705 Data Freshness", f"{int(age_min)} min ago",
                          delta="Fresh", delta_color="normal")
            elif age_min < 120:
                st.metric("\u26a0\ufe0f Data Freshness", f"{int(age_min)} min ago",
                          delta="Getting stale", delta_color="off")
            else:
                hours = age_min / 60
                st.metric("\u274c Data Freshness", f"{hours:.1f} hours ago",
                          delta="Pipeline may not be running", delta_color="inverse")
        else:
            st.metric("\u274c Data Freshness", "No data")

    with c2:
        if latest_run:
            status = latest_run["overall_status"]
            icon = "\u2705" if status == "success" else "\u26a0\ufe0f" if status == "partial" else "\u274c"
            st.metric(f"{icon} Last Run Status", status.upper(),
                      delta=f"{latest_run['duration_sec']}s duration")
        else:
            st.metric("\u2753 Last Run Status", "No runs yet")

    with c3:
        st.metric("\U0001f4f8 Snapshots", str(health["snapshot_count"]))

    with c4:
        if latest_run:
            val = latest_run.get("validation", {})
            passed = val.get("passed", 0)
            failed = val.get("failed_count", 0)
            total = passed + failed
            icon = "\u2705" if failed == 0 else "\u274c"
            st.metric(f"{icon} Validation", f"{passed}/{total} checks",
                      delta="All passed" if failed == 0 else f"{failed} failed",
                      delta_color="normal" if failed == 0 else "inverse")
        else:
            st.metric("\u2753 Validation", "No data")

    # --- row 2: last run step-by-step breakdown ---
    if latest_run:
        st.divider()
        st.subheader("Last Run Breakdown")
        st.caption(f"Run at {latest_run['started_at']} \u2014 took {latest_run['duration_sec']}s total")

        steps = latest_run.get("steps", [])
        cols = st.columns(len(steps))
        for col, step in zip(cols, steps):
            with col:
                ok = step["status"] == "success"
                icon = "\u2705" if ok else "\u274c"
                label = step["step"].upper()
                if ok:
                    detail = f"{step.get('duration_sec', '?')}s"
                    if "rows" in step:
                        detail += f" \u2022 {step['rows']} rows"
                    if step.get("dbt_tests_passed") is False:
                        detail += " \u2022 tests failed"
                    st.metric(f"{icon} {label}", "OK", delta=detail, delta_color="normal")
                else:
                    err = step.get("error", "unknown error")
                    # truncate long errors
                    if len(err) > 60:
                        err = err[:57] + "..."
                    st.metric(f"{icon} {label}", "FAILED", delta=err, delta_color="inverse")

        # show failed validation checks if any
        val = latest_run.get("validation", {})
        if val.get("failed_checks"):
            st.divider()
            st.subheader("\u274c Failed Checks")
            for fc in val["failed_checks"]:
                st.error(f"**{fc['check']}** \u2014 {fc['detail']}")

    # --- row 3: run history ---
    runs = health.get("runs", [])
    if runs:
        st.divider()
        st.subheader("Run History")
        st.caption(f"Last {len(runs)} pipeline runs")

        # build a timeline of runs
        run_data = []
        for r in runs:
            run_data.append({
                "time": r["started_at"],
                "status": r["overall_status"],
                "duration": r.get("duration_sec", 0),
                "steps_ok": sum(1 for s in r.get("steps", []) if s["status"] == "success"),
                "steps_total": len(r.get("steps", [])),
                "val_passed": r.get("validation", {}).get("passed", 0),
                "val_failed": r.get("validation", {}).get("failed_count", 0),
            })

        run_df = pd.DataFrame(run_data)
        run_df["color"] = run_df["status"].map({
            "success": POSITIVE, "partial": "#fbbf24", "failed": NEGATIVE,
        }).fillna(NEUTRAL)

        fig_runs = go.Figure(go.Bar(
            x=run_df["time"],
            y=run_df["duration"],
            marker_color=run_df["color"],
            text=run_df["status"].str.upper(),
            textposition="inside",
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Duration: %{y}s<br>"
                "<extra></extra>"
            ),
        ))
        fig_runs.update_layout(
            **LAYOUT, height=250,
            xaxis_title="", yaxis_title="Duration (seconds)",
            showlegend=False,
            title="Run Duration & Status (green=success, yellow=partial, red=failed)",
        )
        st.plotly_chart(fig_runs, width="stretch")

    # --- row 4: database health ---
    st.divider()
    st.subheader("Database")

    col_db, col_period = st.columns([2, 1])

    with col_db:
        table_df = pd.DataFrame([
            {"Table": t, "Rows": c}
            for t, c in health["tables"].items()
        ])
        fig_tables = make_bar_h(
            table_df, x="Rows", y="Table", title="Table Row Counts",
            height=max(200, len(table_df) * 32),
        )
        st.plotly_chart(fig_tables, width="stretch")

    with col_period:
        if health["first_snapshot"] and health["latest_snapshot"]:
            st.markdown(f"**First snapshot:** {fmt_date(health['first_snapshot'])}")
            st.markdown(f"**Latest snapshot:** {fmt_date(health['latest_snapshot'])}")
            span = health["latest_snapshot"] - health["first_snapshot"]
            st.markdown(f"**Collection span:** {span.days}d {span.seconds // 3600}h")
        st.markdown(f"**Total tables:** {len(health['tables'])}")
        empty = sum(1 for c in health["tables"].values() if c == 0)
        if empty:
            st.warning(f"{empty} table(s) are empty")

    # --- row 5: raw log ---
    st.divider()
    with st.expander("Raw Pipeline Log (latest first)", expanded=False):
        if health["log_lines"]:
            st.code("\n".join(health["log_lines"]), language="text")
        else:
            st.info("No log file found. Run the pipeline to generate logs.")
