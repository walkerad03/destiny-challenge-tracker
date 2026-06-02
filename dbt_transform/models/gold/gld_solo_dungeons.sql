{{ config(materialized='view') }}

SELECT
    account_id,
    character_id,
    instance_id,
    activity_hash,
    period_start,
    player_count,
    duration_seconds,
    kills,
    deaths,
    assists,
    efficiency
FROM {{ ref('slv_activity_history') }}
WHERE activity_mode = 82
  AND is_completed = 1
  AND player_count = 1
