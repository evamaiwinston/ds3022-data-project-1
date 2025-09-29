
  
  create view "taxi"."main"."fct_trips_co2__dbt_tmp" as (
    with 
-- Look up info from tables
yellow as (
    select
        'yellow_taxi' as vehicle_type,
        trip_distance
    from "taxi"."main"."yellow_taxi_trips"
),
green as (
    select
        'green_taxi' as vehicle_type,
        trip_distance
    from "taxi"."main"."green_taxi_trips"
),
-- Union into single stream of trips
unioned as (
    select * from yellow
    union all
    select * from green
),
-- Match co2_grams_per_mile to vehicle_type
matched as (
    select
        u.vehicle_type, 
        u.trip_distance,
        v.co2_grams_per_mile
    from unioned u 
    left join "taxi"."main"."vehicle_emissions" v
        on lower(v.vehicle_type) = lower(u.vehicle_type)
)
-- Compute trip_co2_kgs 
select 
    vehicle_type,
    trip_distance,
    co2_grams_per_mile,
    -- co2 per trip in kg: miles * grams_per_mile / 1000
    (coalesce(trip_distance, 0.0) * coalesce(co2_grams_per_mile, 0.0)) / 1000.0
        as trip_co2_kgs
from matched
  );
