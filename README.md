# DS3022 - Data Project 1 (Fall 2025)

This is a project I did for my Data Engineering class. 

It uses the freely available NYC Trip Record data available from the NYC Taxi Commission Trip Record Data page:
**https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page**

This project calculates CO2 output for rides over the last ten years (2014-2024) and performs some basic statistical
analysis based on transformations added to the data.

Python scripts for loading, cleaning, and analyzing data are in the *scripts* folder. 
DBT model files are in the *dbt* > *models* > *staging* folder. 



<img src="https://s3.amazonaws.com/uvasds-systems/images/nyc-taxi-graphic.png" style="align:right;float:right;max-width:50%;">


## Load

I worked witht the data of both YELLOW and GREEN taxis. Each month is available as a Parquet file for each taxi type.

The `data/vehicle_emissions.csv` file provides a reference when calculating CO2 output based on distance (in miles) and `co2_grams_per_mile`.


The `load.py` script creates a local, persistent DuckDB database that creates and loads three tables:

1. A full table of YELLOW taxi trips for all of 2014-2024.
2. A full table of GREEN taxi trips for all of 2014-2024.
3. A lookup table of `vehicle_emissions` based on the included CSV file above.

It also outputs raw row counts for each of these tables, before cleaning. 


## Clean

The `clean.py` script cleans and checks the data for the following conditions:

1. Remove any duplicate trips.
2. Remove any trips with `0` passengers.
3. Remove any trips 0 miles in length.
4. Remove any trips longer than 100 miles in length.
5. Remove any trips lasting more than 1 day in length (86400 seconds).


## Transform

The following transformations to the data are performed using models in dbt.

1. Calculate total CO2 output per trip by multiplying the `trip_distance` by the `co2_grams_per_mile` value in the `vehicle_emissions` lookup table, then dividing by 1000 (to calculate Kg). Insert that value as a new column named `trip_co2_kgs`. This calculation should be based upon a real-time lookup from the `vehicle_emissions` table and not hard-coded as a numeric figure.
2. Calculate average miles per hour based on distance divided by the duration of the trip, and insert that value as a new column `avg_mph`.
3. Extract the HOUR of the day from the `pickup_time` and insert it as a new column `hour_of_day`.
4. Extract the DAY OF WEEK from the pickup time and insert it as a new column `day_of_week`.
5. Extract the WEEK NUMBER from the pickup time and insert it as a new column `week_of_year`.
6. Extract the MONTH from the pickup time and insert it as a new column `month_of_year`.


## Analyze

The `analysis.py` script provides an one answer for each cab type, YELLOW and GREEN:

1. What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
2. Across the entire year, what on average are the most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips? (1-24)
3. Across the entire year, what on average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips? (Sun-Sat)
4. Across the entire year, what on average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips? (1-52)
5. Across the entire year, what on average are the most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips? (Jan-Dec)
6. Use a plotting library of your choice (`matplotlib`, `seaborn`, etc.) to generate a time-series plot or histogram with MONTH
along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals.

The plot is committed within this repo.


