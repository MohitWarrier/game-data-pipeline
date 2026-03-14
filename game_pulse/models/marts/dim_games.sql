WITH twitch_games AS (
    SELECT DISTINCT
        game_id,
        game_name,
        igdb_id
    FROM {{ ref('stg_twitch_games') }}
    WHERE game_name NOT IN ('Just Chatting', 'IRL')
      AND igdb_id IS NOT NULL
      AND igdb_id != ''
),

igdb AS (
    SELECT
        igdb_id,
        genre,
        release_year,
        developer,
        steam_app_id
    FROM {{ ref('stg_igdb_games') }}
)

SELECT
    t.game_id,
    t.game_name,
    t.igdb_id,
    i.genre,
    i.release_year,
    i.developer,
    i.steam_app_id
FROM twitch_games t
LEFT JOIN igdb i ON t.igdb_id = i.igdb_id