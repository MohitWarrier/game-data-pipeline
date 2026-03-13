SELECT
    id AS game_id,
    name AS game_name,
    igdb_id,
    CAST(viewer_count AS INTEGER) AS viewer_count,
    CAST(stream_count AS INTEGER) AS stream_count,
    CAST(fetched_at AS TIMESTAMP) AS fetched_at
FROM raw_twitch_games