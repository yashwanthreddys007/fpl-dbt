{{ config(
    database='workspace',
    schema='fpl_transformed'
) }}

WITH form AS (
    SELECT * FROM {{ ref('player_form') }}
),

fixtures AS (
    SELECT
        player_id,
        fixture_score,
        fixtures_in_next_5,
        avg_difficulty,
        easiest_upcoming
    FROM {{ ref('fixture_difficulty') }}
),

current_gw AS (
    SELECT MIN(gameweek) as next_gw
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

upcoming AS (
    SELECT 
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        gameweek,
        home_difficulty AS difficulty,
        'H' AS venue
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false

    UNION ALL

    SELECT 
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        gameweek,
        away_difficulty AS difficulty,
        'A' AS venue
    FROM workspace.fpl_raw.fixtures
    WHERE finished = false
),

ranked_fixtures AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY gameweek ASC) AS fixture_num
    FROM upcoming
),

next_5_fixtures AS (
    SELECT
        r.team_id,
        MAX(CASE WHEN r.fixture_num = 1
            THEN CONCAT(t.short_name, '(', r.venue, r.difficulty, ')') END) AS fixture_gw1,
        MAX(CASE WHEN r.fixture_num = 2
            THEN CONCAT(t.short_name, '(', r.venue, r.difficulty, ')') END) AS fixture_gw2,
        MAX(CASE WHEN r.fixture_num = 3
            THEN CONCAT(t.short_name, '(', r.venue, r.difficulty, ')') END) AS fixture_gw3,
        MAX(CASE WHEN r.fixture_num = 4
            THEN CONCAT(t.short_name, '(', r.venue, r.difficulty, ')') END) AS fixture_gw4,
        MAX(CASE WHEN r.fixture_num = 5
            THEN CONCAT(t.short_name, '(', r.venue, r.difficulty, ')') END) AS fixture_gw5,
        -- Also store actual GW numbers for column labeling in the dashboard
        MAX(CASE WHEN r.fixture_num = 1 THEN r.gameweek END) AS gw1_num,
        MAX(CASE WHEN r.fixture_num = 2 THEN r.gameweek END) AS gw2_num,
        MAX(CASE WHEN r.fixture_num = 3 THEN r.gameweek END) AS gw3_num,
        MAX(CASE WHEN r.fixture_num = 4 THEN r.gameweek END) AS gw4_num,
        MAX(CASE WHEN r.fixture_num = 5 THEN r.gameweek END) AS gw5_num
    FROM ranked_fixtures r
    LEFT JOIN workspace.fpl_raw.teams t ON r.opponent_id = t.team_id
    WHERE r.fixture_num <= 5
    GROUP BY r.team_id
),

combined AS (
    SELECT
        f.player_id,
        f.player_name,
        f.team_id,
        t.team_name                             AS plays_for,
        t.short_name                            AS team_short,
        f.position_name,
        f.price_m,
        f.total_points,
        f.fpl_form,
        f.form_score,
        f.form_tier,
        f.points_per_game,
        f.points_per_90,
        f.minutes,
        f.goals_scored,
        f.assists,
        f.clean_sheets,
        f.bonus,
        f.ict_index,
        f.selected_by_percent,
        f.availability_status,
        f.availability_score,
        f.value_score,
        f.chance_of_playing_next_round,
        d.fixture_score,
        d.fixtures_in_next_5,
        d.avg_difficulty,
        d.easiest_upcoming,
        -- Next 5 fixture ticker
        n.fixture_gw1,
        n.fixture_gw2,
        n.fixture_gw3,
        n.fixture_gw4,
        n.fixture_gw5,
        n.gw1_num,
        n.gw2_num,
        n.gw3_num,
        n.gw4_num,
        n.gw5_num,
        -- FPL SCORE FORMULA
        ROUND(
            (f.form_score         * 0.50) +
            (d.fixture_score      * 0.30) +
            (f.availability_score * 2.0   * 0.20),
            2
        ) AS fpl_score,
        CASE
            WHEN ROUND(
                (f.form_score * 0.50) +
                (d.fixture_score * 0.30) +
                (f.availability_score * 2.0 * 0.20), 2
            ) / NULLIF(f.price_m, 0) > 0.5
            THEN true ELSE false
        END AS is_value_pick
    FROM form f
    LEFT JOIN fixtures d         ON f.player_id = d.player_id
    LEFT JOIN workspace.fpl_raw.teams t ON f.team_id = t.team_id
    LEFT JOIN next_5_fixtures n  ON f.team_id = n.team_id
    WHERE d.fixture_score IS NOT NULL
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY position_name
            ORDER BY fpl_score DESC
        ) AS position_rank
    FROM combined
)

SELECT * FROM ranked
ORDER BY fpl_score DESC