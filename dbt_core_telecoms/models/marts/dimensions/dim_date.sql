WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="CAST('2024-01-01' AS DATE)",
      end_date="CAST('2030-01-01' AS DATE)"
     )
  }}
)

SELECT
    date_day,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    MONTHNAME(date_day) AS month_name,
    DAYOFWEEK(date_day) AS day_of_week,
    QUARTER(date_day) AS quarter
FROM date_spine;