WITH staging AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_name,
    gender,
    date_of_birth,
    signup_date,
    email,
    address,
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS customer_age
FROM staging;
