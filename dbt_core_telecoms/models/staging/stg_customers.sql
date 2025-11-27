WITH source AS (
    SELECT * FROM {{ source('core_telecoms', 'customers') }}
)
SELECT
    customer_id,
    TRIM(name) as customer_name,
    UPPER(gender) as gender,
    TRY_TO_DATE(date_of_birth) as birth_date,
    TRY_TO_DATE(signup_date) as signup_date,
    email,
    address,
    _ingestion_time as ingested_at
FROM source