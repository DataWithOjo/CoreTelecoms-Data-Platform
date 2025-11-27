with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2024-01-01' as date)",
      end_date="cast('2030-01-01' as date)"
     )
  }}
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name,
    dayofweek(date_day) as day_of_week,
    quarter(date_day) as quarter
from date_spine