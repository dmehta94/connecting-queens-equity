"""
Populate the `routes` table in BigQuery from MTA GTFS static feed files.

One-time script. Loads routes.txt from both the NYCT Queens feed and the MTA Bus
Company feed, filters out shuttle routes, and inserts the resulting 122-row
dataset into the `connecting_queens_equity.routes` table.

Usage:
    python scripts/populate_routes.py
"""

from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# Constants
ROOT = Path(__file__).parent.parent
Q_ROUTES = ROOT / "data" / "raw" / "gtfs_q" / "routes.txt"
BUSCO_ROUTES = ROOT / "data" / "raw" / "gtfs_busco" / "routes.txt"

client = bigquery.Client(project="connecting-queens-equity")
TABLE_ID = "connecting-queens-equity.connecting_queens_equity.routes"

SHUTTLES = ["Q92", "Q93", "Q96", "Q97", "Q107", "Q108", "Q109", "Q121"]

def load_routes() -> pd.DataFrame:
    """
    Load all of the routes from the source files by the NYCT and MTABC into
    DataFrames, filter out the ones that don't service Queens, concatenate, transform route_id to match the foreign key for the SIRI RealTime API, and output a single DataFrame for all routes servicing Queens, excluding shuttles.

    Returns:
        pd.DataFrame: a DataFrame containing route data for all 122 bus routes that service Queens
    """

    q_routes = pd.read_csv(Q_ROUTES, dtype=str)
    busco_routes = pd.read_csv(BUSCO_ROUTES, dtype=str)

    q_routes_queens = q_routes[q_routes["route_id"].str.match(r"^Q")]
    busco_routes_queens = busco_routes[busco_routes["route_id"].str.match(r"^Q")]

    queens_routes = pd.concat([q_routes_queens, busco_routes_queens])
    queens_routes = queens_routes[~queens_routes["route_id"].isin(SHUTTLES)]

    queens_routes["active"] = True

    queens_routes["route_id"] = queens_routes.apply(
        lambda row: f"MTA NYCT_{row['route_id']}" if row["agency_id"] == "MTA NYCT"
        else f"MTABC_{row['route_id']}",
        axis=1
    )

    queens_routes = queens_routes[["route_id", "route_long_name", "active"]]
    queens_routes = queens_routes.rename(columns={"route_long_name": "route_name"})
    return queens_routes

def insert_routes() -> None:
    """
    Load data from the queens_routes DataFrame onto the routes table on BigQuery.
    """
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
        load_routes(), TABLE_ID,
        job_config=job_config
    )
    job.result()

if __name__ == "__main__":
    insert_routes()