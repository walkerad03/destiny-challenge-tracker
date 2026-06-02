{{ config(
    materialized="incremental",
    unique_key=['account_id', 'character_id', 'instance_id']
) }}

WITH source_data AS (
    SELECT
        account_id,
        character_id,
        page_number,
        jsonb_array_elements(payload -> 'Response' -> 'activities') AS activity,
        ingested_at
    FROM {{ source('bronze', 'raw_activity_history') }}

    {% if is_incremental() %}

    WHERE ingested_at > (SELECT max(ingested_at) FROM {{ this }})
        {% endif %}
)

SELECT
    account_id,
    character_id,

    (activity -> 'activityDetails' ->> 'instanceId')::BIGINT AS instance_id,
    (activity -> 'activityDetails' ->> 'directorActivityHash')::BIGINT AS activity_hash,
    (activity -> 'activityDetails' ->> 'mode')::INT AS activity_mode,

    (activity ->> 'period')::TIMESTAMP AS period_start,
    (activity -> 'values' -> 'activityDurationSeconds' -> 'basic' ->> 'value')::NUMERIC::INT AS duration_seconds,

    (activity -> 'values' -> 'completed' -> 'basic' ->> 'value')::NUMERIC::INT AS is_completed,
    (activity -> 'values' -> 'kills' -> 'basic' ->> 'value')::NUMERIC::INT AS kills,
    (activity -> 'values' -> 'deaths' -> 'basic' ->> 'value')::NUMERIC::INT AS deaths,
    (activity -> 'values' -> 'assists' -> 'basic' ->> 'value')::NUMERIC::INT AS assists,
    (activity -> 'values' -> 'efficiency' -> 'basic' ->> 'value')::FLOAT AS efficiency,

    (activity -> 'values' -> 'team' -> 'basic' ->> 'value')::NUMERIC::INT AS team_id,
    (activity -> 'values' -> 'playerCount' -> 'basic' ->> 'value')::NUMERIC::INT AS player_count,

    ingested_at

FROM source_data
