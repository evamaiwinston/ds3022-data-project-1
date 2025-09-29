
  
    
    

    create  table
      "taxi"."main"."stg_green__dbt_tmp"
  
    as (
      with src as (
  select * from "taxi"."main"."green_taxi_trips"
),
emissions as (
    select
        lower(vehicle_type) as vehicle_type,
        co2_grams_per_mile
    from "taxi"."main"."vehicle_emissions"
)
select
    'green_taxi' as vehicle_type,
    src.*,
    /* rename pickup/dropoff */
    lpep_pickup_datetime as pickup_time,
    lpep_dropoff_datetime as dropoff_time,
    /* get duration in hrs */
    (extract(epoch from (dropoff_time - pickup_time)) / 3600.0) as trip_hours,
    /* avg mph */
    trip_distance / ((extract(epoch from (dropoff_time - pickup_time)) / 3600.0)) as avg_mph,
    /* time extractions */
    extract(hour from pickup_time) as hour_of_day,
    extract(dow from pickup_time) as day_of_week,
    extract(week from pickup_time) as week_of_year,
    extract(month from pickup_time) as month_of_year,
    /* co2 calc */
    (coalesce(trip_distance,0.0) * coalesce(e.co2_grams_per_mile,0.0)) / 1000.0 as trip_co2_kgs
from src
left join emissions e
  on lower('green_taxi') = e.vehicle_type
    );
  
  