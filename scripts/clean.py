import duckdb
import logging


# Configs
DB_PATH = "taxi.duckdb"

YELLOW_TABLE = "yellow_taxi_trips"
GREEN_TABLE  = "green_taxi_trips"

YELLOW_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance"]
GREEN_COLS  = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "passenger_count", "trip_distance"]

# Logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/clean.log'
)
logger = logging.getLogger(__name__)


# Clean function
def clean_one(con, table_name, pickup_col, dropoff_col):
    logger.info(f"Cleaning table: {table_name}")

    # Count number of raw rows before cleaning
    count_raw = con.execute(f"""
                    SELECT COUNT(*) FROM {table_name};
                """).fetchone()[0]
    logger.info(f"[counts] Rows before: {count_raw}")


    # Drop table if it exists
    con.execute(f"""
        DROP TABLE IF EXISTS {table_name}_cleaned_temp;
    """)
    logger.info(f"Dropped {table_name}_cleaned_temp if exists")

    # Create cleaned_temp table
    # Deduplicate
    # Apply filters: remove trips with 0 passengers, 0 miles, >100 miles, >1 day(86400 seconds)
    con.execute(f"""
        CREATE TABLE {table_name}_cleaned_temp AS 
        SELECT DISTINCT *
        FROM {table_name}
        WHERE passenger_count > 0
            AND trip_distance > 0
            AND trip_distance <= 100
            AND date_diff('second', {pickup_col}, {dropoff_col}) <= 86400;
    """)
    logger.info(f"Deduplicated and cleaned {table_name}_cleaned_temp with filters")


    # Swap tables
    con.execute(f"""
        DROP TABLE {table_name};
    """)

    con.execute(f"""
        ALTER TABLE {table_name}_cleaned_temp 
        RENAME TO {table_name};
    """)
    logger.info(f"Swapped in cleaned table for {table_name}")

    # Count number of cleaned rows
    count_cleaned = con.execute(f"""
                    SELECT COUNT(*) FROM {table_name};
                """).fetchone()[0]
    logger.info(f"[counts] Rows after: {count_cleaned}")
    

    # Verify cleaning
    logger.info(f"Verifying cleaning on {table_name}...")

    # Check for duplicates
    dupes = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM {table_name})
        - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table_name}) AS deduped);
    """).fetchone()[0]
    logger.info(f"Duplicate rows: {dupes}")

    # Check for 0 passengers
    zero_pass = con.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE passenger_count = 0;
    """).fetchone()[0]
    logger.info(f"Trips with 0 passengers: {zero_pass}")

    # Check for 0 mile trips
    zero_miles = con.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE trip_distance = 0;
    """).fetchone()[0]
    logger.info(f"Trips with 0 miles: {zero_miles}")

    # Check for trips over 100 miles
    over_100_miles = con.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE trip_distance > 100;
    """).fetchone()[0]
    logger.info(f"Trips with over 100 miles: {over_100_miles}")

    # Check for trips longer than 1 day
    over_day = con.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE date_diff('second', {pickup_col}, {dropoff_col}) > 86400;
    """).fetchone()[0]
    logger.info(f"Trips over 1 day: {over_day}")
    logger.info(f"Done verifying {table_name}")


def clean_tables():

    con = None

    try:

        logger.info("------ New run ---------------")
        # Connect to local duckdb
        con = duckdb.connect(database= DB_PATH, read_only = False)

        # Yellow table
        clean_one(
            con,
            table_name = YELLOW_TABLE,
            pickup_col = YELLOW_COLS[0],
            dropoff_col= YELLOW_COLS[1]
        )

        # Green table
        clean_one(
            con,
            table_name = GREEN_TABLE,
            pickup_col = GREEN_COLS[0],
            dropoff_col= GREEN_COLS[1]
        )

        # Final cleaned counts
        yellow_cleaned = con.execute(f"""
                        SELECT COUNT(*) FROM {YELLOW_TABLE};
                    """).fetchone()[0]
        green_cleaned = con.execute(f"""
                        SELECT COUNT(*) FROM {GREEN_TABLE};
                    """).fetchone()[0]
        logger.info(f"Final cleaned row counts - Yellow: {yellow_cleaned} | Green: {green_cleaned}")

    except Exception as e: 
        logger.error(f"An error occurred: {e}")
    finally: 
        if con is not None:
            con.close()


if __name__ == "__main__":
    clean_tables()


