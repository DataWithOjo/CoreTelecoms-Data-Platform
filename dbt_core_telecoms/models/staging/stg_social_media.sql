with source as (
    select * from {{ source('telecom_raw', 'social_media') }}
),

renamed as (
    select
        complaint_id,
        customer_id,
        agent_id,
        complaint_catego_ry as complaint_category,
        media_channel,
        resolutionstatus as resolution_status,
        try_to_timestamp(request_date) as request_date,
        try_to_timestamp(resolution_date) as resolution_date,
        try_to_date(mediacomplaintgenerationdate) as media_complaint_generation_date,
        _ingestion_time
    from source
)

select * from renamed