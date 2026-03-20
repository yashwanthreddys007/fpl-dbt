{{ config(
    database='workspace',
    schema='fpl_transformed'
) }}

WITH current_gw AS (
    SELECT MIN(gameweek) AS next_gw
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

-- Generate next 5 gameweeks
gw_range AS (
    SELECT next_gw + n AS gameweek
    FROM current_gw
    CROSS JOIN (
        SELECT 0 AS n UNION ALL SELECT 1 UNION ALL
        SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    ) nums
),

-- Normalize fixtures so each row = one team
fixtures_expanded AS (

    SELECT
        fixture_id,
        gameweek,
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        'H' AS venue,
        home_difficulty AS difficulty
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false

    UNION ALL

    SELECT
        fixture_id,
        gameweek,
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        'A' AS venue,
        away_difficulty AS difficulty
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

-- Only upcoming fixtures in the next 5 gameweeks
upcoming_fixtures AS (
    SELECT f.*
    FROM fixtures_expanded f
    INNER JOIN gw_range g
        ON f.gameweek = g.gameweek
),

-- All teams x next 5 gameweeks
-- ensures BGW rows exist
team_gameweeks AS (
    SELECT
        t.team_id,
        g.gameweek
    FROM workspace.fpl_raw.teams t
    CROSS JOIN gw_range g
),

player_fixtures AS (

    SELECT
        tg.team_id,
        tg.gameweek,

        COALESCE(
            MAX(CONCAT(opp.short_name, '(', f.venue, f.difficulty, ')')),
            'BGW'
        ) AS fixture_ticker,

        COALESCE(
            MIN(f.difficulty),
            5
        ) AS difficulty

    FROM team_gameweeks tg

    LEFT JOIN upcoming_fixtures f
        ON tg.team_id = f.team_id
        AND tg.gameweek = f.gameweek

    LEFT JOIN workspace.fpl_raw.teams opp
        ON opp.team_id = f.opponent_id

    GROUP BY
        tg.team_id,
        tg.gameweek
),

next_5 AS (
    SELECT
        team_id,
        -- Single column with all 5 fixtures concatenated
        CONCAT_WS(' | ',
            MAX(CASE WHEN gameweek = (SELECT next_gw FROM current_gw) 
                THEN fixture_ticker END),
            MAX(CASE WHEN gameweek = (SELECT next_gw + 1 FROM current_gw) 
                THEN fixture_ticker END),
            MAX(CASE WHEN gameweek = (SELECT next_gw + 2 FROM current_gw) 
                THEN fixture_ticker END),
            MAX(CASE WHEN gameweek = (SELECT next_gw + 3 FROM current_gw) 
                THEN fixture_ticker END),
            MAX(CASE WHEN gameweek = (SELECT next_gw + 4 FROM current_gw) 
                THEN fixture_ticker END)
        ) AS next_5_fixtures,
        ROUND(AVG(6 - difficulty), 2)                           AS fixture_score,
        COUNT(CASE WHEN fixture_ticker != 'BGW' THEN 1 END)     AS fixtures_in_next_5,
        ROUND(AVG(difficulty), 2)                               AS avg_difficulty,
        MIN(difficulty)                                         AS easiest_upcoming
    FROM player_fixtures
    GROUP BY team_id
)

SELECT
    p.player_id,
    p.player_name,
    p.team_id,
    p.position_name,
    p.price_m,
    n.fixtures_in_next_5,
    n.avg_difficulty,
    n.fixture_score,
    n.easiest_upcoming,
    n.next_5_fixtures
FROM workspace.fpl_raw.players p
LEFT JOIN next_5 n ON p.team_id = n.team_id