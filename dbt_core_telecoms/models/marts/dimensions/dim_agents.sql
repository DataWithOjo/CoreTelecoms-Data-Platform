with staging as (
    select * from {{ ref('stg_agents') }}
)

select
    agent_id,
    agent_name,
    experience,
    state
from staging