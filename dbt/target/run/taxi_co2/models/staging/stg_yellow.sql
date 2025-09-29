
  
    
    

    create  table
      "taxi"."main"."stg_yellow__dbt_tmp"
  
    as (
      with src as (
  select * from "taxi"."main"."yellow_taxi_trips"
)
select
    *,
    /* rename pickup/dropoff */
    tpep_pickup_datetime as pickup_time,
    tpep_dropoff_datetime as dropoff_time,
    /* get duration in hrs */
    (extract(epoch from (dropoff_time - pickup_time)) / 3600.0) as trip_hours,
    /* avg mph */
    trip_distance / ((extract(epoch from (dropoff_time - pickup_time)) / 3600.0)) as avg_mph,
    /* time extractions */
    extract(hour from pickup_time) as hour_of_day,
    extract(dow from pickup_time) as day_of_week,
    extract(week from pickup_time) as week_of_year,
    extract(month from pickup_time) as month_of_year
from src
    );
  
  