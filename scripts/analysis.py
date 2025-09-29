import duckdb
import logging
import pandas as pd 
import matplotlib.pyplot as plt 

# Configs
DB_PATH = "taxi.duckdb"

STG_YELLOW = "stg_yellow"
STG_GREEN  = "stg_green"

# Logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_one(con, table_name, pickup_col, cab_label, target_year):
    logger.info(f"Analyzing table: {table_name}")

    # Largest co2 trip
    max_trip_kg = con.execute(f"""
        SELECT MAX(trip_co2_kgs)
        FROM {table_name}
        WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)};
    """).fetchone()[0]
    logger.info(f"[largest_trip] {cab_label}: {max_trip_kg}")
    print(f"[Largest CO2 Trip] {cab_label}: {max_trip_kg:.3f} kg")


    # Hour of day (1-24)
    hour_heavy = con.execute(f"""
        SELECT hour FROM (
            SELECT (hour_of_day + 1) AS hour,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            GROUP BY 1
        )
        ORDER BY avg_kg DESC
        LIMIT 1;
    """).fetchone()[0]


    hour_light = con.execute(f"""
        SELECT hour FROM (
            SELECT (hour_of_day + 1) AS hour,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info("Succesfully found hours of day")
    print(f"[Hour (avg kg/trip)] {cab_label} heaviest={hour_heavy} | lightest={hour_light}")

    # Day of week (Sun-Sat)
    dow_heavy = con.execute(f"""
        SELECT dow FROM (
            SELECT
                CASE day_of_week
                    WHEN 0 THEN 'Sun'
                    WHEN 1 THEN 'Mon'
                    WHEN 2 THEN 'Tue'
                    WHEN 3 THEN 'Wed'
                    WHEN 4 THEN 'Thu'
                    WHEN 5 THEN 'Fri'
                    WHEN 6 THEN 'Sat'
                END AS dow,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            GROUP BY 1
        )
        ORDER BY avg_kg DESC
        LIMIT 1;
    """).fetchone()[0]

    dow_light = con.execute(f"""
        SELECT dow FROM (
            SELECT
                CASE day_of_week
                    WHEN 0 THEN 'Sun'
                    WHEN 1 THEN 'Mon'
                    WHEN 2 THEN 'Tue'
                    WHEN 3 THEN 'Wed'
                    WHEN 4 THEN 'Thu'
                    WHEN 5 THEN 'Fri'
                    WHEN 6 THEN 'Sat'
                END AS dow,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info("Succesfully found days of week")
    print(f"[Day of Week (avg kg/trip)] {cab_label} heaviest={dow_heavy} | lightest={dow_light}")


    # Week of year (1–52)
    week_heavy = con.execute(f"""
        SELECT week FROM (
            SELECT CAST(week_of_year AS INT) AS week,
                AVG(trip_co2_kgs)        AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            AND week_of_year BETWEEN 1 AND 52
            GROUP BY 1
        )
        ORDER BY avg_kg DESC
        LIMIT 1;
    """).fetchone()[0]

    week_light = con.execute(f"""
        SELECT week FROM (
            SELECT CAST(week_of_year AS INT) AS week,
                AVG(trip_co2_kgs)        AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            AND week_of_year BETWEEN 1 AND 52
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info("Successfully found weeks of year")
    print(f"[Week of Year (avg kg/trip)] {cab_label} heaviest={week_heavy} | lightest={week_light}")


    # Month of year (Jan–Dec)
    month_heavy = con.execute(f"""
        SELECT mon FROM (
            SELECT
                CASE CAST(month_of_year AS INT)
                    WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
                    WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
                    WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
                    WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
                END AS mon,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            AND month_of_year BETWEEN 1 AND 12
            GROUP BY 1
        )
        ORDER BY avg_kg DESC
        LIMIT 1;
    """).fetchone()[0]

    month_light = con.execute(f"""
        SELECT mon FROM (
            SELECT
                CASE CAST(month_of_year AS INT)
                    WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
                    WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
                    WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
                    WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
                END AS mon,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
            WHERE EXTRACT(YEAR FROM {pickup_col}) = {int(target_year)}
            AND month_of_year BETWEEN 1 AND 12
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info("Successfully found months of year")
    print(f"[Month (avg kg/trip)] {cab_label} heaviest={month_heavy} | lightest={month_light}")


def analyze_tables():

    con = None

    try:

        logger.info("------ New run ---------------")
        # Connect to local duckdb
        con = duckdb.connect(database= DB_PATH, read_only = False)

        TARGET_YEAR = 2024

        # Yellow analytics
        analyze_one(
            con,
            table_name = STG_YELLOW,
            pickup_col = "tpep_pickup_datetime",
            cab_label  = "YELLOW",
            target_year= TARGET_YEAR
        )

        # Green analytics 
        analyze_one(
            con,
            table_name = STG_GREEN,
            pickup_col = "lpep_pickup_datetime",
            cab_label  = "GREEN",
            target_year= TARGET_YEAR
        )

    except Exception as e: 
        logger.error(f"An error occurred: {e}")
    finally: 
        if con is not None:
            con.close()


if __name__ == "__main__":
    analyze_tables()