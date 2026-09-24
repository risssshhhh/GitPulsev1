with dates as (
    select
        date_trunc('day', dd)::date as date_day
    from generate_series(
        '2024-01-01'::timestamp,
        '2027-12-31'::timestamp,
        '1 day'::interval
    ) dd
)
select
    date_day as date_id,
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(month from date_day) as integer) as month,
    cast(extract(day from date_day) as integer) as day,
    cast(extract(dow from date_day) as integer) as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    cast(extract(quarter from date_day) as integer) as quarter,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
from dates
