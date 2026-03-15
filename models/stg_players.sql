-- This tells dbt explicitly — write this model to `workspace.fpl_transformed`, not `hive_metastore`.
{{ config(
    database='workspace',
    schema='fpl_transformed'
) }}

-- stg_players: cleans the raw players table
-- This is the foundation every other model builds on

WITH source AS (

    SELECT * FROM workspace.fpl_raw.players

),

cleaned AS (

    SELECT
        player_id,
        player_name,
        first_name,
        second_name,
        team_id,
        position,
        position_name,
        price_m,
        total_points,
        form,
        points_per_game,
        minutes,
        goals_scored,
        assists,
        clean_sheets,
        bonus,
        influence,
        creativity,
        threat,
        ict_index,
        selected_by_percent,
        transfers_in_event,
        transfers_out_event,
        availability_status,
        chance_of_playing_next_round,
        chance_of_playing_this_round,

        -- Availability flag: 1 = available, 0 = doubt/injured/suspended
        CASE 
            WHEN availability_status = 'a' THEN 1.0
            WHEN availability_status = 'd' THEN 0.5
            ELSE 0.0
        END AS availability_score,

        -- Value score: points per million spent
        ROUND(total_points / NULLIF(price_m, 0), 2) AS value_score,

        ingested_at

    FROM source

    -- Only players who have played at least 1 minute
    WHERE minutes > 0

)

SELECT * FROM cleaned