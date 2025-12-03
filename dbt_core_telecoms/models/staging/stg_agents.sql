WITH source AS (
    SELECT * FROM {{ source('telecom_raw', 'agents') }}
),

renamed AS (
    SELECT
        id AS agent_id,
        name AS agent_name,
        experience,
        state,
        _ingestion_time
    FROM source
)

SELECT * FROM renamed
