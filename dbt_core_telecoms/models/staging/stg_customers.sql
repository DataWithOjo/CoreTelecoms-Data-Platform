with source as (
    select * from {{ source('telecom_raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        name as customer_name,
        gender,
        try_to_date(date_of_birth) as date_of_birth,
        try_to_date(signup_date) as signup_date,
        email,
        address,
        _ingestion_time
    from source
)

select * from renamed