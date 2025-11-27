with source as (
    select * from {{ source('telecom_raw', 'agents') }}
),

renamed as (
    select
        id as agent_id,
        name as agent_name,
        experience,
        state,
        _ingestion_time
    from source
)

select * from renamed