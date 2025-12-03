WITH social AS (
    SELECT * FROM {{ ref('stg_social_media') }}
)

SELECT
    complaint_id,
    customer_id,
    agent_id,
    media_complaint_generation_date,
    media_channel,
    complaint_category,
    resolution_status,
    request_date,
    resolution_date,
    DATEDIFF('hour', request_date, resolution_date) AS time_to_resolve_hours
FROM social
