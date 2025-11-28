{{ config(
    materialized = 'view',
    schema = 'staging'
) }}

select
    -- Keys (some values may be "1.0", so cast to numeric first)
    (cast("VendorID"     as numeric))::integer    as vendor_id,

    -- Timestamps (keep raw here; datetime dimension will cast properly)
    "tpep_pickup_datetime"                        as pickup_datetime,
    "tpep_dropoff_datetime"                       as dropoff_datetime,

    -- Measures
    (cast("passenger_count" as numeric))::integer as passenger_count,
    "trip_distance"                               as trip_distance,

    -- Rate & payment keys (also sometimes "1.0")
    (cast("RatecodeID"   as numeric))::integer    as rate_code_id,
    (cast("payment_type" as numeric))::integer    as payment_type_id,

    -- Flags
    "store_and_fwd_flag"                          as store_and_fwd_flag,

    -- Location IDs (sometimes stored as "132.0")
    (cast("PULocationID" as numeric))::integer    as pu_location_id,
    (cast("DOLocationID" as numeric))::integer    as do_location_id,

    -- Monetary values
    "fare_amount"                                 as fare_amount,
    "extra"                                       as extra,
    "mta_tax"                                     as mta_tax,
    "tip_amount"                                  as tip_amount,
    "tolls_amount"                                as tolls_amount,
    "improvement_surcharge"                       as improvement_surcharge,
    "total_amount"                                as total_amount,
    "congestion_surcharge"                        as congestion_surcharge,
    "airport_fee"                                 as airport_fee

from {{ source('staging', 'yellow_tripdata_2023_01') }}
