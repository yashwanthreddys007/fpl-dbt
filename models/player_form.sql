{{ config(
    database='workspace',
    schema='fpl_transformed'
) }}

WITH base AS (
    SELECT * FROM {{ ref('stg_players') }}
),

form_scored AS (
    SELECT
        *,
        form AS fpl_form,
        ROUND(total_points / NULLIF(minutes, 0) * 90, 2) AS points_per_90,
        CASE
            WHEN form >= 8  THEN 'elite'
            WHEN form >= 6  THEN 'good'
            WHEN form >= 4  THEN 'average'
            WHEN form >= 2  THEN 'poor'
            ELSE 'out-of-form'
        END AS form_tier,
        ROUND(LEAST(form / 12.0 * 10, 10), 2) AS form_score
    FROM base
)

SELECT * FROM form_scored
ORDER BY form_score DESC