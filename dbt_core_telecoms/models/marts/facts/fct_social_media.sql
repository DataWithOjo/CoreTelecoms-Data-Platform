with social as (
    select * from {{ ref('stg_social_media') }}
)

select
    complaint_id,
    customer_id,
    agent_id,
    media_complaint_generation_date,
    media_channel,
    complaint_category,
    resolution_status,
    request_date,
    resolution_date,
    datediff('hour', request_date, resolution_date) as time_to_resolve_hours,
from social