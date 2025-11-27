with web as (
    select * from {{ ref('stg_web_complaints') }}
)

select
    request_id,
    customer_id,
    agent_id,
    web_form_generation_date,
    complaint_category,
    resolution_status,
    request_date,
    resolution_date,
    datediff('hour', request_date, resolution_date) as time_to_resolve_hours,
from web