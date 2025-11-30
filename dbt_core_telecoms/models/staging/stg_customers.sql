WITH source AS (
    SELECT * FROM {{ source('telecom_raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        name AS customer_name,
        gender,
        TRY_TO_DATE(date_of_birth) AS date_of_birth,
        TRY_TO_DATE(signup_date) AS signup_date,
        email,
        address,
        _ingestion_time
    FROM source
)

SELECT * FROM renamed;