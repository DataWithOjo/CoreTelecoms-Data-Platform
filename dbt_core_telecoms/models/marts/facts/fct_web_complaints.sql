WITH web AS (
    SELECT * FROM {{ ref('stg_web_complaints') }}
)

SELECT
    request_id,
    customer_id,
    agent_id,
    web_form_generation_date,
    complaint_category,
    resolution_status,
    request_date,
    resolution_date,
    DATEDIFF('hour', request_date, resolution_date) AS time_to_resolve_hours
FROM web;