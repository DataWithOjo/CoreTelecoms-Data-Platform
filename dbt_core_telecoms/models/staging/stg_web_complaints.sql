with source as (
    select * from {{ source('telecom_raw', 'web_complaints') }}
),

renamed as (
    select
        request_id,
        source_row_id,
        customer_id,
        agent_id,
        complaint_catego_ry as complaint_category,
        resolutionstatus as resolution_status,
        try_to_timestamp(request_date) as request_date,
        try_to_timestamp(resolution_date) as resolution_date,
        try_to_date(webformgenerationdate) as web_form_generation_date,
        _ingestion_time
    from source
)

select * from renamed