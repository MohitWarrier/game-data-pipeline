SELECT
    igdb_id,
    name AS game_name,
    genre,
    CAST(release_year AS INTEGER) AS release_year,
    developer,
    steam_app_id,
    CAST(fetched_at AS TIMESTAMP) AS fetched_at
FROM raw_igdb_games