import duckdb
import requests 

import logging
import time
from pathlib import Path


# Configs
YEARS = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 
TAXIS = ["yellow", "green"]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet"

DATA_DIR = Path("data")
DB_PATH = Path("taxi.duckdb")
EMISSIONS_CSV = DATA_DIR / "vehicle_emissions.csv" 

DOWNLOAD_DELAY = 2.0 # seconds

YELLOW_TABLE = "yellow_taxi_trips"
GREEN_TABLE = "green_taxi_trips"
EMISSIONS_TABLE = "vehicle_emissions"

YELLOW_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance"]
GREEN_COLS  = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "passenger_count", "trip_distance"]


# Logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/load.log'
)
logger = logging.getLogger(__name__)
logger.info("---------New run-----------------")


# Load function
def load_parquet_files():

    con = None

    try:
        # Download needed parquet files 
        DATA_DIR.mkdir(parents = True, exist_ok = True)

        # Loop over yellow and green taxis
        for taxi in TAXIS:
            # Loop over each year 
            for year in YEARS:
                print(f'Downloading data for {year}')
                # Loop over each month
                for month in MONTHS:
                    url= BASE_URL.format(taxi = taxi, year =year, month=month) 
                    out= DATA_DIR / f'{taxi}_tripdata_{year}-{month}.parquet'

                    # Don't download if parquet file already exists 
                    if out.exists():
                        logger.info(f"[download] exists, skip {out.name}")
                        continue

                    # Get parquet from cloudfront
                    logger.info(f'[download] GET {url} FOR {out.name}')
                    try:
                        with requests.get(url, stream= True, timeout= (10, 60)) as r :
                            r.raise_for_status()
                            with open(out, "wb") as f:
                                for chunk in r.iter_content(chunk_size=1 << 20):
                                    if chunk:
                                        f.write(chunk)
                        # Space out to avoid error
                        time.sleep(DOWNLOAD_DELAY)
                    except Exception as e:
                        logger.error(f'[download] failed for {out.name}: {e}')   
                

        # Connect to local DuckDB instance
        con = duckdb.connect(database=str(DB_PATH), read_only=False)
        logger.info("Connected to DuckDB instance")

        # Drop tables if it already exists
        con.execute(f"""
            DROP TABLE IF EXISTS {YELLOW_TABLE};
        """)
        logger.info("Dropped yellow table if exists")

        con.execute(f"""
            DROP TABLE IF EXISTS {GREEN_TABLE};
        """)
        logger.info("Dropped green table if exists")

        con.execute(f"""
            DROP TABLE IF EXISTS {EMISSIONS_TABLE};
        """)
        logger.info("Dropped emissions table if exists")

        print("Creating tables...")

        # Create tables with necessary columns
        con.execute(f"""
            CREATE TABLE {YELLOW_TABLE} (
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE
            );
        """)
        logger.info("Created yellow trip table")

        con.execute(f"""
            CREATE TABLE {GREEN_TABLE} (
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE
            );
        """)
        logger.info("Created green trip table")

        con.execute(f"""
            CREATE TABLE {EMISSIONS_TABLE} AS 
            SELECT * FROM read_csv_auto('{EMISSIONS_CSV}', header = True);
        """)
        logger.info("Created emissions table")


        # Ingest parquet to database 
        for taxi in TAXIS:
            table = YELLOW_TABLE if taxi == "yellow" else GREEN_TABLE
            cols = ", ".join(YELLOW_COLS if taxi =="yellow" else GREEN_COLS)

            for year in YEARS:
                for month in MONTHS:
                    fp = DATA_DIR / f'{taxi}_tripdata_{year}-{month}.parquet'
                    if not fp.exists():
                        logger.error(f'[ingest] missing file, skip: {fp.name}')
                        continue

                    try:
                        con.execute(f"""
                            INSERT INTO {table}
                            SELECT {cols}
                            FROM read_parquet('{fp}');
                        """)
                        logger.info(f"[ingest] OK {taxi} {year}-{month}")
                    except Exception as e: 
                        logger.error(f"[ingest] FAIL {taxi} {year}-{month}: {e}")


        # Get raw counts
        yellow_count_raw = con.execute(f"""
                            SELECT COUNT(*) FROM {YELLOW_TABLE};
                        """).fetchone()[0]
        logger.info(f"[counts] {YELLOW_TABLE}: {yellow_count_raw} rows")

        green_count_raw = con.execute(f"""
                            SELECT COUNT(*) FROM {GREEN_TABLE};
                        """).fetchone()[0]
        logger.info(f"[counts] {GREEN_TABLE}: {green_count_raw} rows")

        emissions_count_raw = con.execute(f"""
                            SELECT COUNT(*) FROM {EMISSIONS_TABLE};
                        """).fetchone()[0]
        logger.info(f"[counts] {EMISSIONS_TABLE}: {emissions_count_raw} rows")

        # Output raw counts
        print("\nRAW ROW COUNTS BEFORE CLEANING: ")
        print(f"{YELLOW_TABLE}: {yellow_count_raw}")
        print(f"{GREEN_TABLE}:  {green_count_raw}")
        print(f"{EMISSIONS_TABLE}: {emissions_count_raw}")

        

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


    # Close duckdb connection 
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_parquet_files()