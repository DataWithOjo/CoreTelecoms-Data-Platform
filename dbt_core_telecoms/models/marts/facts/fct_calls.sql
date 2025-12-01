WITH calls AS (
    SELECT * FROM {{ ref('stg_call_logs') }}
)

SELECT
    call_id,
    customer_id,
    agent_id,
    call_logs_generation_date,
    complaint_category,
    resolution_status,
    call_start_time,
    call_end_time,
    DATEDIFF('second', call_start_time, call_end_time) AS call_duration_seconds
FROM calls
