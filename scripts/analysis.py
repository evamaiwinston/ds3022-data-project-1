import duckdb
import logging
import pandas as pd 
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter, LogLocator, MultipleLocator

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

def analyze_one(con, table_name, pickup_col, cab_label):
    logger.info(f"Analyzing table: {table_name}")

    # Largest co2 trip
    max_trip_kg = con.execute(f"""
        SELECT MAX(trip_co2_kgs)
        FROM {table_name};
    """).fetchone()[0]
    logger.info(f"[Largest CO2 Trip] {cab_label}: {max_trip_kg:.3f} kg")
    print(f"[Largest CO2 Trip]           {cab_label}: {max_trip_kg:.3f} kg")


    # Hour of day (1-24)
    hour_heavy = con.execute(f"""
        SELECT hour FROM (
            SELECT (hour_of_day + 1) AS hour,
                AVG(trip_co2_kgs) AS avg_kg
            FROM {table_name}
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
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info(f"[Hour (avg kg/trip)] {cab_label} heaviest={hour_heavy} | lightest={hour_light}")
    print(f"[Hour (avg kg/trip)]         {cab_label} heaviest={hour_heavy} | lightest={hour_light}")

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
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info(f"[Day of Week (avg kg/trip)] {cab_label} heaviest={dow_heavy} | lightest={dow_light}")
    print(f"[Day of Week (avg kg/trip)]  {cab_label} heaviest={dow_heavy} | lightest={dow_light}")


    # Week of year (1–52)
    week_heavy = con.execute(f"""
        SELECT week FROM (
            SELECT CAST(week_of_year AS INT) AS week,
                AVG(trip_co2_kgs)        AS avg_kg
            FROM {table_name}
            WHERE week_of_year BETWEEN 1 AND 52
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
            WHERE week_of_year BETWEEN 1 AND 52
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info(f"[Week of Year (avg kg/trip)] {cab_label} heaviest={week_heavy} | lightest={week_light}")
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
            WHERE month_of_year BETWEEN 1 AND 12
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
            WHERE month_of_year BETWEEN 1 AND 12
            GROUP BY 1
        )
        ORDER BY avg_kg ASC
        LIMIT 1;
    """).fetchone()[0]

    logger.info(f"[Month (avg kg/trip)] {cab_label} heaviest={month_heavy} | lightest={month_light}")
    print(f"[Month (avg kg/trip)]        {cab_label} heaviest={month_heavy} | lightest={month_light}")


def analyze_tables():

    con = None

    try:

        logger.info("------ New run ---------------")
        # Connect to local duckdb
        con = duckdb.connect(database= DB_PATH, read_only = False)

        # Yellow analytics
        analyze_one(
            con,
            table_name = STG_YELLOW,
            pickup_col = "tpep_pickup_datetime",
            cab_label  = "YELLOW"
        )

        # Green analytics 
        analyze_one(
            con,
            table_name = STG_GREEN,
            pickup_col = "lpep_pickup_datetime",
            cab_label  = "GREEN"
        )

    except Exception as e: 
        logger.error(f"An error occurred: {e}")
    finally: 
        if con is not None:
            con.close()



def plot_over_time(con, year_start, year_end, 
                    out_path, yellow_pickup, green_pickup):

    logger.info(f"Plotting years {year_start} to {year_end}")

    # Build where clauses
    y_where = f"""
        WHERE EXTRACT(YEAR FROM {yellow_pickup}) 
        BETWEEN {int(year_start)} AND {int(year_end)}
    """

    g_where = f"""
        WHERE EXTRACT(YEAR FROM {green_pickup}) 
        BETWEEN {int(year_start)} AND {int(year_end)}
    """

    # Yellow yearlys into df 
    yearly_yellow_df = con.execute(f"""
        SELECT CAST(strftime({yellow_pickup}, '%Y') AS INT) AS yr,
        SUM(trip_co2_kgs) AS total_kg
        FROM stg_yellow {y_where}
        GROUP BY 1
        ORDER BY 1;
    """).df()
    logger.info(f"Created yellow monthly co2 df for plotting")

    # Green yearlys into df
    yearly_green_df = con.execute(f"""
        SELECT CAST(strftime({green_pickup}, '%Y') AS INT) AS yr,
        SUM(trip_co2_kgs) AS total_kg
        FROM stg_green {g_where}
        GROUP BY 1
        ORDER BY 1;
    """).df()
    logger.info(f"Created green monthly co2 df for plotting")

    # Convert yr to numeric
    yearly_yellow_df["yr"] = yearly_yellow_df["yr"].astype(int)
    yearly_green_df["yr"] = yearly_green_df["yr"].astype(int)


    # Line plot 
    fig, ax = plt.subplots(figsize=(11, 5))

    ax.plot(yearly_yellow_df["yr"], yearly_yellow_df["total_kg"],
            marker="o", label="Yellow Taxi", color="#FFD700")
    ax.plot(yearly_green_df["yr"], yearly_green_df["total_kg"],
            marker="o", label="Green Taxi",  color="green")

    # Use log scale on y-axis (otherwise green looks like it's always 0)
    ax.set_yscale("log")  

    # Clean up y-axis to show relevant ticks and abbreviate thousand to K and million to M
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=12))
    ax.yaxis.set_major_formatter(
        FuncFormatter(lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}k")
    )

    # Clean up x-axis ticks every year
    ax.xaxis.set_major_locator(MultipleLocator(1))
    ax.set_xlim(int(year_start), int(year_end))

    # Titles and legend
    ax.set_xlabel("Year")
    ax.set_ylabel("Total CO2 (kg, log scale)")
    ax.set_title(f"Yearly Taxi CO2 Totals ({year_start}-{year_end})")
    ax.legend()
    ax.grid(True, which="both", axis="y", alpha=0.3)
    
    # Fix the borders
    xmin = min(yearly_yellow_df["yr"].min(), yearly_green_df["yr"].min())
    xmax = max(yearly_yellow_df["yr"].max(), yearly_green_df["yr"].max())
    ax.set_xlim(int(xmin) - 0.5, int(xmax) + 0.5)  

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


    logger.info(f"Saved plot: {out_path}")
    print(f"[Plot] Saved: {out_path}")


if __name__ == "__main__":
    analyze_tables()

    # Connection for plot function
    con = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        plot_over_time(con, 2015, 2024, out_path="co2_by_year.png",
                       yellow_pickup="tpep_pickup_datetime",
                       green_pickup="lpep_pickup_datetime")
    finally:
        con.close()