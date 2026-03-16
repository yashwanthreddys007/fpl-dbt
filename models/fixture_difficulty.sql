WITH current_gw AS (
    SELECT MIN(gameweek) as next_gw
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

gw_range AS (
    -- Generate exactly 5 gameweek numbers from current GW
    SELECT next_gw + n as gameweek
    FROM current_gw
    CROSS JOIN (
        SELECT 0 as n UNION ALL SELECT 1 UNION ALL 
        SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    ) nums
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
    INNER JOIN gw_range g ON f.gameweek = g.gameweek
    WHERE f.finished = false
),

-- All teams x all 5 gameweeks (creates blank slots)
team_gameweeks AS (
    SELECT DISTINCT
        t.team_id,
        g.gameweek
    FROM workspace.fpl_raw.teams t
    CROSS JOIN gw_range g
),

player_fixtures AS (
    SELECT
        tg.team_id,
        tg.gameweek,
        -- NULL when no fixture = blank gameweek
        CASE
            WHEN f.home_team_id IS NULL THEN 'BGW'
            WHEN tg.team_id = f.home_team_id 
                THEN CONCAT(opp.short_name, '(H', f.home_difficulty, ')')
            ELSE CONCAT(opp.short_name, '(A', f.away_difficulty, ')')
        END AS fixture_ticker,
        CASE
            WHEN f.home_team_id IS NULL THEN 5
            WHEN tg.team_id = f.home_team_id THEN f.home_difficulty
            ELSE f.away_difficulty
        END AS difficulty
    FROM team_gameweeks tg
    LEFT JOIN upcoming_fixtures f
        ON (tg.team_id = f.home_team_id OR tg.team_id = f.away_team_id)
        AND tg.gameweek = f.gameweek
    LEFT JOIN workspace.fpl_raw.teams opp
        ON opp.team_id = CASE 
            WHEN tg.team_id = f.home_team_id THEN f.away_team_id
            ELSE f.home_team_id
        END
),

next_5 AS (
    SELECT
        team_id,
        MAX(CASE WHEN gameweek = (SELECT next_gw FROM current_gw) 
            THEN fixture_ticker END) AS fixture_gw1,
        MAX(CASE WHEN gameweek = (SELECT next_gw + 1 FROM current_gw) 
            THEN fixture_ticker END) AS fixture_gw2,
        MAX(CASE WHEN gameweek = (SELECT next_gw + 2 FROM current_gw) 
            THEN fixture_ticker END) AS fixture_gw3,
        MAX(CASE WHEN gameweek = (SELECT next_gw + 3 FROM current_gw) 
            THEN fixture_ticker END) AS fixture_gw4,
        MAX(CASE WHEN gameweek = (SELECT next_gw + 4 FROM current_gw) 
            THEN fixture_ticker END) AS fixture_gw5,
        -- BGW counts as difficulty 5, penalizes blank gameweeks
        ROUND(AVG(6 - difficulty), 2) AS fixture_score,
        COUNT(CASE WHEN fixture_ticker != 'BGW' THEN 1 END) AS fixtures_in_next_5,
        ROUND(AVG(difficulty), 2) AS avg_difficulty,
        MIN(difficulty) AS easiest_upcoming
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
    n.fixture_gw1,
    n.fixture_gw2,
    n.fixture_gw3,
    n.fixture_gw4,
    n.fixture_gw5
FROM workspace.fpl_raw.players p
LEFT JOIN next_5 n ON p.team_id = n.team_id
