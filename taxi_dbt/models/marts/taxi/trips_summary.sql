select
    vendor_id,
    count(*) as total_trips,
    avg(fare_amount) as avg_fare,
    avg(trip_distance) as avg_distance
from {{ ref('fact_trips') }}
group by vendor_id
