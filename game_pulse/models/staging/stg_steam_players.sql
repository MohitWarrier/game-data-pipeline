SELECT
    steam_app_id,
    igdb_id,
    CAST(player_count AS INTEGER) AS player_count,
    CAST(fetched_at AS TIMESTAMP) AS fetched_at
FROM raw_steam_players