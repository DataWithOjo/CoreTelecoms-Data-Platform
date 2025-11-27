with staging as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    customer_name,
    gender,
    date_of_birth,
    signup_date,
    email,
    address,
    datediff('year', date_of_birth, current_date()) as customer_age
from staging