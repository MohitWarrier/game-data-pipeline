SELECT
    s.game_id,
    s.game_name,
    s.viewer_count,
    s.stream_count,
    s.fetched_at,
    d.genre,
    d.release_year,
    ROW_NUMBER() OVER (PARTITION BY s.fetched_at ORDER BY s.viewer_count DESC) AS rank_at_time
FROM {{ ref('stg_twitch_games') }} s
INNER JOIN {{ ref('dim_games') }} d ON s.game_id = d.game_id