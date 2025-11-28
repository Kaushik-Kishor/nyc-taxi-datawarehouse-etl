{{ config(
    materialized='table',
    schema='warehouse'
) }}

with dates as (
    select generate_series(
        (select min(pickup_datetime) from {{ ref('stg_yellow_tripdata') }} )::timestamp,
        (select max(pickup_datetime) from {{ ref('stg_yellow_tripdata') }} )::timestamp,
        interval '1 hour'
    ) as datetime_value
)

select
    row_number() over (order by datetime_value) as datetime_id,
    datetime_value,
    extract(year from datetime_value)   as year,
    extract(month from datetime_value)  as month,
    extract(day from datetime_value)    as day,
    extract(hour from datetime_value)   as hour,
    to_char(datetime_value, 'Day')      as weekday,
    extract(isodow from datetime_value) as weekday_num
from dates
