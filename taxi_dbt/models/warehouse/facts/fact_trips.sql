{{ config(
    materialized='table',
    schema='warehouse'
) }}

with trips as (
    select *
    from {{ ref('stg_yellow_tripdata') }}
),

rate_codes as (
    select *
    from {{ ref('dim_rate_code') }}
),

payment_types as (
    select *
    from {{ ref('dim_payment_type') }}
),

vendors as (
    select *
    from {{ ref('dim_vendor') }}
)

select
    -- Foreign Keys (handle missing values with 0)
    coalesce(t.vendor_id, 0)        as vendor_id,
    coalesce(t.rate_code_id, 0)     as rate_code_id,
    coalesce(t.payment_type_id, 0)  as payment_type_id,
    coalesce(t.pu_location_id, 0)   as pu_location_id,
    coalesce(t.do_location_id, 0)   as do_location_id,

    -- Measures
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,

    -- Datetime columns
    t.pickup_datetime,
    t.dropoff_datetime

from trips t
left join vendors v on t.vendor_id = v.vendor_id
left join rate_codes r on t.rate_code_id = r.rate_code_id
left join payment_types p on t.payment_type_id = p.payment_type_id
