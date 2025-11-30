WITH staging AS (
    SELECT * FROM {{ ref('stg_agents') }}
)

SELECT
    agent_id,
    agent_name,
    experience,
    state
FROM staging;
