SELECT
    game_id,
    game_name,
    viewer_count,
    stream_count,
    fetched_at,
    ROW_NUMBER() OVER (PARTITION BY fetched_at ORDER BY viewer_count DESC) AS rank_at_time
FROM {{ ref('stg_twitch_games') }}