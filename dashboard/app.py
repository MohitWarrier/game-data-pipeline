import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Game Pulse", page_icon="\U0001f3ae", layout="wide")

DB_PATH = "data/game_pulse.duckdb"

# --- palette & theme ---

PALETTE = px.colors.qualitative.Pastel
ACCENT = "#00d4aa"       # teal accent for highlights
NEGATIVE = "#ff6b6b"     # red for drops
POSITIVE = "#51cf66"     # green for gains
NEUTRAL = "#868e96"      # grey for unchanged
BG_CARD = "#1e1e2e"      # dark card background

CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=30, b=0),
    font=dict(family="Inter, sans-serif", size=12),
)


# ============================================================
#  DATA LOADING — modular: one function per source
# ============================================================

@st.cache_data(ttl=60)
def load_snapshots():
    """Load all snapshot data. Add new fact tables here as sources grow."""
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
    """Load dimension table. Extend with new columns as sources are added."""
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT game_id, game_name, igdb_id, genre, release_year,
               developer, steam_app_id
        FROM dim_games
    """).df()
    con.close()
    return df


# ============================================================
#  CHART COMPONENTS — reusable, source-agnostic
# ============================================================

def chart_treemap(data, values_col, names_col, color_col=None, title=""):
    """Treemap showing proportional size of each item."""
    fig = px.treemap(
        data, path=[names_col], values=values_col,
        color=color_col or values_col,
        color_continuous_scale="Teal",
        title=title,
    )
    fig.update_layout(**CHART_LAYOUT, height=420, coloraxis_showscale=False)
    fig.update_traces(
        textinfo="label+value",
        texttemplate="<b>%{label}</b><br>%{value:,}",
        hovertemplate="<b>%{label}</b><br>Viewers: %{value:,}<extra></extra>",
    )
    return fig


def chart_bar_horizontal(data, x_col, y_col, color_col=None, title=""):
    """Clean horizontal bar chart."""
    fig = px.bar(
        data, x=x_col, y=y_col, orientation="h",
        color=color_col or y_col,
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_layout(
        **CHART_LAYOUT, showlegend=False, height=350,
        yaxis=dict(autorange="reversed", title=""),
        xaxis=dict(title=""),
    )
    fig.update_traces(texttemplate="%{x:,}", textposition="inside")
    return fig


def chart_line(data, x_col, y_col, color_col=None, title="",
               invert_y=False, height=350):
    """Multi-series line chart with consistent styling."""
    fig = px.line(
        data, x=x_col, y=y_col, color=color_col,
        color_discrete_sequence=PALETTE,
        title=title,
        markers=True,
    )
    fig.update_layout(
        **CHART_LAYOUT, height=height,
        legend_title_text="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis_title="", yaxis_title="",
    )
    if invert_y:
        fig.update_layout(yaxis=dict(autorange="reversed", dtick=1))
    return fig


def chart_area_stacked(data, x_col, y_col, color_col, title="", height=400):
    """Stacked area chart — great for composition over time."""
    fig = px.area(
        data, x=x_col, y=y_col, color=color_col,
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_layout(
        **CHART_LAYOUT, height=height,
        legend_title_text="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis_title="", yaxis_title="",
    )
    return fig


def chart_donut(data, values_col, names_col, title=""):
    """Donut/pie chart for proportional breakdowns."""
    fig = px.pie(
        data, values=values_col, names=names_col,
        hole=0.45,
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_layout(**CHART_LAYOUT, height=380,
                      legend=dict(orientation="h", yanchor="bottom", y=-0.15))
    fig.update_traces(textinfo="percent+label", textposition="inside")
    return fig


def chart_bump(data, x_col, y_col, color_col, title="", height=400):
    """Bump chart — rank trajectories over time. Rank 1 at top."""
    fig = px.line(
        data, x=x_col, y=y_col, color=color_col,
        color_discrete_sequence=PALETTE,
        title=title,
        markers=True,
    )
    fig.update_layout(
        **CHART_LAYOUT, height=height,
        yaxis=dict(autorange="reversed", dtick=1, title="Rank"),
        xaxis_title="",
        legend_title_text="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    return fig


def metric_card(label, value, delta=None, delta_color=None):
    """Render a styled metric using st.metric."""
    st.metric(label, value, delta=delta, delta_color=delta_color)


# ============================================================
#  MOMENTUM CALCULATIONS
# ============================================================

def compute_movers(df, snapshots):
    """
    Compare latest vs previous snapshot.
    Returns a dataframe with viewer_change, viewer_pct_change, rank_change.
    Positive viewer_change = gained viewers. Positive rank_change = moved UP.
    """
    if len(snapshots) < 2:
        return pd.DataFrame()

    latest = df[df["fetched_at"] == snapshots[-1]][
        ["game_name", "viewer_count", "rank_at_time", "genre"]
    ].copy()
    prev = df[df["fetched_at"] == snapshots[-2]][
        ["game_name", "viewer_count", "rank_at_time"]
    ].copy()

    latest.columns = ["game_name", "viewers_now", "rank_now", "genre"]
    prev.columns = ["game_name", "viewers_prev", "rank_prev"]

    merged = latest.merge(prev, on="game_name", how="inner")
    merged["viewer_change"] = merged["viewers_now"] - merged["viewers_prev"]
    merged["viewer_pct"] = (
        (merged["viewer_change"] / merged["viewers_prev"].replace(0, 1)) * 100
    ).round(1)
    merged["rank_change"] = merged["rank_prev"] - merged["rank_now"]  # positive = climbed

    return merged.sort_values("viewer_change", ascending=False)


def compute_momentum(df, snapshots, game_name):
    """
    For a single game, compute viewer trajectory across all snapshots.
    Returns a dataframe with snapshot-over-snapshot changes.
    """
    game_data = df[df["game_name"] == game_name].sort_values("fetched_at").copy()
    game_data["viewer_change"] = game_data["viewer_count"].diff()
    game_data["viewer_pct"] = (
        (game_data["viewer_change"] / game_data["viewer_count"].shift(1).replace(0, 1)) * 100
    ).round(1)
    game_data["rank_change"] = -game_data["rank_at_time"].diff()  # positive = climbed
    return game_data


# ============================================================
#  LOAD DATA
# ============================================================

df = load_snapshots()
dim = load_games()

if df.empty:
    st.title("\U0001f3ae Game Pulse")
    st.warning("No data yet. Run the pipeline first: `make start`")
    st.stop()

snapshots = sorted(df["fetched_at"].unique())
latest = df[df["fetched_at"] == snapshots[-1]]
has_history = len(snapshots) > 1


# ============================================================
#  SIDEBAR
# ============================================================

with st.sidebar:
    st.title("\U0001f3ae Game Pulse")
    st.caption("Live Twitch Gaming Analytics")
    st.divider()
    st.markdown(f"**{len(snapshots)}** snapshots collected")
    st.markdown(f"**{len(dim)}** games tracked")
    st.markdown(f"**{dim['genre'].dropna().nunique()}** genres")
    st.divider()
    st.markdown(f"First: `{str(snapshots[0])[:16]}`")
    st.markdown(f"Latest: `{str(snapshots[-1])[:16]}`")


# ============================================================
#  TABS
# ============================================================

tabs = st.tabs([
    "\U0001f3ae Live",
    "\U0001f525 Movers",
    "\U0001f4c8 Trends",
    "\U0001f3af Genres",
    "\U0001f50d Deep Dive",
])

tab_live, tab_movers, tab_trends, tab_genre, tab_detail = tabs


# ==================== TAB 1: LIVE ====================

with tab_live:
    # KPI row
    top_game = latest.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("\U0001f451 Top Game", top_game["game_name"],
                  f"{top_game['viewer_count']:,} viewers")
    with c2:
        st.metric("\U0001f441 Total Viewers", f"{latest['viewer_count'].sum():,}")
    with c3:
        st.metric("\U0001f4e1 Total Streams", f"{latest['stream_count'].sum():,}")
    with c4:
        st.metric("\U0001f552 Updated", str(latest.iloc[0]["fetched_at"])[:16])

    st.divider()

    # two-column layout: treemap + top 10 bar
    col_left, col_right = st.columns([3, 2])

    with col_left:
        fig_tree = chart_treemap(
            latest, values_col="viewer_count", names_col="game_name",
            title="Viewer Share — Every Box = A Game",
        )
        st.plotly_chart(fig_tree, width="stretch")

    with col_right:
        top10 = latest.nsmallest(10, "rank_at_time")
        fig_bar = chart_bar_horizontal(
            top10, x_col="viewer_count", y_col="game_name",
            title="Top 10 by Viewers",
        )
        st.plotly_chart(fig_bar, width="stretch")

    # bump chart if we have history
    if has_history:
        st.divider()
        top_names = latest.nsmallest(10, "rank_at_time")["game_name"].tolist()
        bump_data = df[df["game_name"].isin(top_names)]
        fig_bump = chart_bump(
            bump_data, x_col="fetched_at", y_col="rank_at_time",
            color_col="game_name",
            title="Rank Movement — Top 10 Games",
        )
        st.plotly_chart(fig_bump, width="stretch")


# ==================== TAB 2: MOVERS & SHAKERS ====================

with tab_movers:
    if not has_history:
        st.info("Need at least 2 snapshots to detect movement. Run the pipeline again to collect more data.")
    else:
        movers = compute_movers(df, snapshots)

        if movers.empty:
            st.info("No comparable games between snapshots.")
        else:
            st.subheader("Biggest Movers — Last Snapshot")
            st.caption("Which games surged or crashed between the last two data pulls?")

            # top gainers and losers side by side
            col_gain, col_lose = st.columns(2)

            gainers = movers.head(5)
            losers = movers.tail(5).sort_values("viewer_change")

            with col_gain:
                st.markdown("### \U0001f4c8 Rising")
                for _, row in gainers.iterrows():
                    pct = f"+{row['viewer_pct']}%" if row["viewer_pct"] > 0 else f"{row['viewer_pct']}%"
                    rank_txt = ""
                    if row["rank_change"] > 0:
                        rank_txt = f" \u2022 \u2b06 {int(row['rank_change'])} ranks"
                    elif row["rank_change"] < 0:
                        rank_txt = f" \u2022 \u2b07 {int(abs(row['rank_change']))} ranks"
                    delta_val = f"+{row['viewer_change']:,} viewers ({pct}){rank_txt}"
                    st.metric(
                        row["game_name"],
                        f"{row['viewers_now']:,} viewers",
                        delta=delta_val,
                        delta_color="normal",
                    )

            with col_lose:
                st.markdown("### \U0001f4c9 Falling")
                for _, row in losers.iterrows():
                    pct = f"{row['viewer_pct']}%"
                    rank_txt = ""
                    if row["rank_change"] > 0:
                        rank_txt = f" \u2022 \u2b06 {int(row['rank_change'])} ranks"
                    elif row["rank_change"] < 0:
                        rank_txt = f" \u2022 \u2b07 {int(abs(row['rank_change']))} ranks"
                    delta_val = f"{row['viewer_change']:,} viewers ({pct}){rank_txt}"
                    st.metric(
                        row["game_name"],
                        f"{row['viewers_now']:,} viewers",
                        delta=delta_val,
                        delta_color="normal",
                    )

            st.divider()

            # waterfall chart: viewer change by game
            st.subheader("Viewer Change by Game")
            st.caption("Green = gained viewers, Red = lost viewers")

            waterfall = movers[["game_name", "viewer_change"]].copy()
            waterfall["color"] = waterfall["viewer_change"].apply(
                lambda x: POSITIVE if x > 0 else NEGATIVE
            )
            waterfall = waterfall.sort_values("viewer_change", ascending=True)

            fig_waterfall = go.Figure(go.Bar(
                x=waterfall["viewer_change"],
                y=waterfall["game_name"],
                orientation="h",
                marker_color=waterfall["color"],
                text=waterfall["viewer_change"].apply(
                    lambda x: f"+{x:,}" if x > 0 else f"{x:,}"
                ),
                textposition="outside",
            ))
            fig_waterfall.update_layout(
                **CHART_LAYOUT, height=max(300, len(waterfall) * 35),
                xaxis_title="Viewer Change",
                yaxis_title="",
                showlegend=False,
            )
            st.plotly_chart(fig_waterfall, width="stretch")

            # viewer % change scatter — shows which games had the most dramatic swings
            st.divider()
            st.subheader("Volatility Map")
            st.caption("Bubble size = current viewers. Position = % viewer change vs rank change.")

            fig_scatter = px.scatter(
                movers,
                x="viewer_pct",
                y="rank_change",
                size="viewers_now",
                color="genre",
                hover_name="game_name",
                color_discrete_sequence=PALETTE,
                title="",
                size_max=40,
            )
            fig_scatter.update_layout(
                **CHART_LAYOUT, height=400,
                xaxis_title="Viewer Change %",
                yaxis_title="Rank Change (positive = climbed)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                legend_title_text="",
            )
            # add quadrant lines
            fig_scatter.add_hline(y=0, line_dash="dot", line_color=NEUTRAL, opacity=0.5)
            fig_scatter.add_vline(x=0, line_dash="dot", line_color=NEUTRAL, opacity=0.5)
            st.plotly_chart(fig_scatter, width="stretch")


# ==================== TAB 3: TRENDS ====================

with tab_trends:
    all_games = sorted(df["game_name"].unique().tolist())
    default_top5 = latest.nsmallest(5, "rank_at_time")["game_name"].tolist()
    default_games = [g for g in default_top5 if g in all_games]

    selected = st.multiselect(
        "Select games to compare",
        all_games,
        default=default_games,
        key="trends_games",
    )

    if not selected:
        st.info("Pick at least one game above to see trends.")
    else:
        filtered = df[df["game_name"].isin(selected)]

        # viewer count over time
        fig_v = chart_line(
            filtered, x_col="fetched_at", y_col="viewer_count",
            color_col="game_name", title="Viewer Count Over Time",
        )
        st.plotly_chart(fig_v, width="stretch")

        # rank over time
        fig_r = chart_line(
            filtered, x_col="fetched_at", y_col="rank_at_time",
            color_col="game_name", title="Rank Over Time (Top = #1)",
            invert_y=True,
        )
        st.plotly_chart(fig_r, width="stretch")

        # stream count over time
        fig_s = chart_line(
            filtered, x_col="fetched_at", y_col="stream_count",
            color_col="game_name", title="Stream Count Over Time",
        )
        st.plotly_chart(fig_s, width="stretch")

        # viewers per stream — shows engagement density
        if has_history:
            st.divider()
            st.subheader("Viewers per Stream")
            st.caption(
                "High ratio = few big streamers. Low ratio = many small streamers. "
                "This helps spot the 'one streamer carries the game' phenomenon."
            )
            vps = filtered.copy()
            vps["viewers_per_stream"] = (
                vps["viewer_count"] / vps["stream_count"].replace(0, 1)
            ).round(0)
            fig_vps = chart_line(
                vps, x_col="fetched_at", y_col="viewers_per_stream",
                color_col="game_name",
                title="Avg Viewers per Stream",
            )
            st.plotly_chart(fig_vps, width="stretch")


# ==================== TAB 4: GENRE BREAKDOWN ====================

with tab_genre:
    genre_data = latest[latest["genre"].notna()]

    if genre_data.empty:
        st.info("No genre data available. Run the pipeline to fetch IGDB metadata.")
    else:
        col_left, col_right = st.columns([1, 1])

        with col_left:
            genre_viewers = (
                genre_data.groupby("genre")["viewer_count"]
                .sum().reset_index()
                .sort_values("viewer_count", ascending=False)
            )
            fig_donut = chart_donut(
                genre_viewers, values_col="viewer_count", names_col="genre",
                title="Genre Share by Viewers",
            )
            st.plotly_chart(fig_donut, width="stretch")

        with col_right:
            genre_stats = (
                genre_data.groupby("genre")
                .agg(
                    viewers=("viewer_count", "sum"),
                    streams=("stream_count", "sum"),
                    games=("game_name", "nunique"),
                )
                .reset_index()
            )
            genre_stats["avg_per_game"] = (
                genre_stats["viewers"] // genre_stats["games"]
            )
            genre_stats = genre_stats.sort_values("viewers", ascending=False)

            # bar chart instead of table
            fig_genre_bar = px.bar(
                genre_stats, x="viewers", y="genre", orientation="h",
                color="genre", color_discrete_sequence=PALETTE,
                title="Total Viewers by Genre",
            )
            fig_genre_bar.update_layout(
                **CHART_LAYOUT, showlegend=False, height=380,
                yaxis=dict(title=""), xaxis=dict(title="Viewers"),
            )
            fig_genre_bar.update_traces(
                texttemplate="%{x:,}", textposition="inside"
            )
            st.plotly_chart(fig_genre_bar, width="stretch")

        # avg viewers per game by genre — shows which genres punch above their weight
        st.divider()
        col_a, col_b = st.columns([1, 1])

        with col_a:
            st.subheader("Games per Genre")
            fig_games_genre = px.bar(
                genre_stats, x="games", y="genre", orientation="h",
                color="genre", color_discrete_sequence=PALETTE,
                title="",
            )
            fig_games_genre.update_layout(
                **CHART_LAYOUT, showlegend=False, height=350,
                yaxis=dict(title=""), xaxis=dict(title="Games"),
            )
            fig_games_genre.update_traces(
                texttemplate="%{x}", textposition="inside"
            )
            st.plotly_chart(fig_games_genre, width="stretch")

        with col_b:
            st.subheader("Avg Viewers per Game")
            st.caption("Which genres punch above their weight?")
            fig_avg = px.bar(
                genre_stats, x="avg_per_game", y="genre", orientation="h",
                color="genre", color_discrete_sequence=PALETTE,
                title="",
            )
            fig_avg.update_layout(
                **CHART_LAYOUT, showlegend=False, height=350,
                yaxis=dict(title=""), xaxis=dict(title="Avg Viewers"),
            )
            fig_avg.update_traces(
                texttemplate="%{x:,}", textposition="inside"
            )
            st.plotly_chart(fig_avg, width="stretch")

        # genre trend over time
        if has_history:
            st.divider()
            genre_time = (
                df[df["genre"].notna()]
                .groupby(["fetched_at", "genre"])["viewer_count"]
                .sum().reset_index()
            )
            fig_area = chart_area_stacked(
                genre_time, x_col="fetched_at", y_col="viewer_count",
                color_col="genre",
                title="Genre Popularity Over Time",
            )
            st.plotly_chart(fig_area, width="stretch")


# ==================== TAB 5: DEEP DIVE ====================

with tab_detail:
    game_list = sorted(dim["game_name"].unique().tolist())

    if not game_list:
        st.info("No game data available.")
    else:
        selected_game = st.selectbox("Select a game", game_list, key="detail_game")

        game_info = dim[dim["game_name"] == selected_game].iloc[0]
        game_history = df[df["game_name"] == selected_game].sort_values("fetched_at")

        # metadata row
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            genre = game_info["genre"] if pd.notna(game_info["genre"]) else "\u2014"
            st.markdown(f"**Genre:** {genre}")
        with c2:
            release = game_info["release_year"]
            st.markdown(f"**Release Year:** {int(release) if pd.notna(release) else '\u2014'}")
        with c3:
            dev = game_info["developer"] if pd.notna(game_info["developer"]) else "\u2014"
            st.markdown(f"**Developer:** {dev}")

        st.divider()

        if game_history.empty:
            st.info("No snapshot data for this game yet.")
        else:
            latest_game = game_history.iloc[-1]

            # metrics row
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                st.metric("Current Rank", f"#{int(latest_game['rank_at_time'])}")
            with c2:
                st.metric("Current Viewers", f"{latest_game['viewer_count']:,}")
            with c3:
                st.metric("Current Streams", f"{latest_game['stream_count']:,}")
            with c4:
                st.metric("Peak Viewers", f"{game_history['viewer_count'].max():,}")
            with c5:
                st.metric("Best Rank", f"#{int(game_history['rank_at_time'].min())}")

            # charts
            if len(game_history) > 1:
                st.divider()

                # viewer + stream overlay
                fig_detail = go.Figure()
                fig_detail.add_trace(go.Scatter(
                    x=game_history["fetched_at"],
                    y=game_history["viewer_count"],
                    name="Viewers",
                    line=dict(color=ACCENT, width=3),
                    mode="lines+markers",
                ))
                fig_detail.add_trace(go.Scatter(
                    x=game_history["fetched_at"],
                    y=game_history["stream_count"],
                    name="Streams",
                    yaxis="y2",
                    line=dict(color=PALETTE[3], width=2, dash="dot"),
                    mode="lines+markers",
                ))
                fig_detail.update_layout(
                    **CHART_LAYOUT, height=350,
                    title="Viewers & Streams Over Time",
                    yaxis=dict(title="Viewers", side="left"),
                    yaxis2=dict(title="Streams", side="right",
                                overlaying="y", showgrid=False),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_detail, width="stretch")

                # rank history
                fig_rank = go.Figure(go.Scatter(
                    x=game_history["fetched_at"],
                    y=game_history["rank_at_time"],
                    mode="lines+markers+text",
                    text=game_history["rank_at_time"].apply(lambda x: f"#{int(x)}"),
                    textposition="top center",
                    line=dict(color=ACCENT, width=3),
                    marker=dict(size=10),
                ))
                fig_rank.update_layout(
                    **CHART_LAYOUT, height=300,
                    title="Rank History",
                    yaxis=dict(autorange="reversed", dtick=1, title="Rank"),
                    xaxis_title="",
                    showlegend=False,
                )
                st.plotly_chart(fig_rank, width="stretch")

                # momentum table — snapshot-over-snapshot changes
                momentum = compute_momentum(df, snapshots, selected_game)
                if len(momentum) > 1 and momentum["viewer_change"].notna().any():
                    st.divider()
                    st.subheader("Momentum")
                    st.caption(
                        "How this game's viewership changed between each snapshot. "
                        "Spot the surges and crashes."
                    )

                    mom_display = momentum[momentum["viewer_change"].notna()].copy()
                    fig_mom = go.Figure()
                    colors = [
                        POSITIVE if v > 0 else NEGATIVE
                        for v in mom_display["viewer_change"]
                    ]
                    fig_mom.add_trace(go.Bar(
                        x=mom_display["fetched_at"],
                        y=mom_display["viewer_change"],
                        marker_color=colors,
                        text=mom_display["viewer_change"].apply(
                            lambda x: f"+{x:,.0f}" if x > 0 else f"{x:,.0f}"
                        ),
                        textposition="outside",
                    ))
                    fig_mom.update_layout(
                        **CHART_LAYOUT, height=300,
                        title="Viewer Change per Snapshot",
                        xaxis_title="",
                        yaxis_title="Viewer Change",
                        showlegend=False,
                    )
                    st.plotly_chart(fig_mom, width="stretch")
