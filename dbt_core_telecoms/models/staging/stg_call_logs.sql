with source as (
    select * from {{ source('telecom_raw', 'call_center_logs') }}
),

renamed as (
    select
        call_id,
        source_row_id,
        customer_id,
        agent_id,
        complaint_catego_ry as complaint_category,
        resolutionstatus as resolution_status,
        try_to_timestamp(call_start_time) as call_start_time,
        try_to_timestamp(call_end_time) as call_end_time,
        try_to_date(calllogsgenerationdate) as call_logs_generation_date,
        _ingestion_time
    from source
)

select * from renamed