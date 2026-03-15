{{ config(
    database='workspace',
    schema='fpl_transformed'
) }}

WITH current_gw AS (
    SELECT MIN(gameweek) as next_gw
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

upcoming_fixtures AS (
    SELECT
        f.fixture_id,
        f.gameweek,
        f.home_team_id,
        f.away_team_id,
        f.home_difficulty,
        f.away_difficulty
    FROM workspace.fpl_raw.fixtures f
    CROSS JOIN current_gw c
    WHERE f.gameweek >= c.next_gw
      AND f.gameweek <  c.next_gw + 5
      AND f.finished = false
),

player_fixtures AS (
    SELECT
        p.player_id,
        p.player_name,
        p.team_id,
        p.position_name,
        p.price_m,
        f.gameweek,
        CASE
            WHEN p.team_id = f.home_team_id THEN f.home_difficulty
            WHEN p.team_id = f.away_team_id THEN f.away_difficulty
        END AS fixture_difficulty,
        CASE
            WHEN p.team_id = f.home_team_id THEN 'H'
            ELSE 'A'
        END AS home_or_away
    FROM workspace.fpl_raw.players p
    LEFT JOIN upcoming_fixtures f
        ON p.team_id = f.home_team_id
        OR p.team_id = f.away_team_id
),

difficulty_summary AS (
    SELECT
        player_id,
        player_name,
        team_id,
        position_name,
        price_m,
        COUNT(gameweek)                       AS fixtures_in_next_5,
        ROUND(AVG(fixture_difficulty), 2)     AS avg_difficulty,
        ROUND(6 - AVG(fixture_difficulty), 2) AS fixture_score,
        MIN(fixture_difficulty)               AS easiest_upcoming,
        MAX(fixture_difficulty)               AS hardest_upcoming
    FROM player_fixtures
    WHERE fixture_difficulty IS NOT NULL
    GROUP BY player_id, player_name, team_id, position_name, price_m
)

SELECT * FROM difficulty_summary
ORDER BY fixture_score DESC