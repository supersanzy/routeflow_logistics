with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}

),

dim_date as (

    select
        cast(date_day as date) as full_date,

        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,

        week(date_day) as week_of_year,
        day(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,

        case
            when dayofweek(date_day) in (0, 6)
                then true
            else false
        end as is_weekend

    from date_spine

)

select
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
    *
from dim_date