with calls as (
    select * from {{ ref('stg_call_logs') }}
)

select
    call_id,
    customer_id,
    agent_id,
    call_logs_generation_date,
    complaint_category,
    resolution_status,
    call_start_time,
    call_end_time,
    datediff('second', call_start_time, call_end_time) as call_duration_seconds,
from calls