"""
Populate the `stops` table in BigQuery from MTA GTFS static feed files.

One-time script. Loads stops.txt from both the NYCT Queens feed and the MTA Bus
Company feed, trips.txt and stop_times.txt with usecols, joins to get unique
stop-route pairs, filters to the 122 Queens routes, sjoins with borough boundaries
to assign borough, and inserts into `stops` and `stop_routes` datasets into the
`connecting_queens_equity.stops` and `connecting_queens_equity.stop_routes` tables.

Usage:
    python scripts/populate_stops.py
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

# Constants
ROOT = Path(__file__).parent.parent
Q_STOPS = ROOT / "data" / "raw" / "gtfs_q" / "stops.txt"
BUSCO_STOPS = ROOT / "data" / "raw" / "gtfs_busco" / "stops.txt"

Q_TRIPS = ROOT / "data" / "raw" / "gtfs_q" / "trips.txt"
BUSCO_TRIPS = ROOT / "data" / "raw" / "gtfs_busco" / "trips.txt"

Q_STOP_TIMES = ROOT / "data" / "raw" / "gtfs_q" / "stop_times.txt"
BUSCO_STOP_TIMES = ROOT / "data" / "raw" / "gtfs_busco" / "stop_times.txt"

Q_ROUTES = ROOT / "data" / "raw" / "gtfs_q" / "routes.txt"
BUSCO_ROUTES = ROOT / "data" / "raw" / "gtfs_busco" / "routes.txt"

BOROUGH_BOUNDARIES = ROOT / "data" / "raw" / "nyc_borough_boundaries.geojson"
SHUTTLES = ["Q92", "Q93", "Q96", "Q97", "Q107", "Q108", "Q109", "Q121"]

client = bigquery.Client(project="connecting-queens-equity")
STOPS_ID = "connecting-queens-equity.connecting_queens_equity.stops"
STOP_ROUTES_ID = "connecting-queens-equity.connecting_queens_equity.stop_routes"

def _get_queens_routes() -> pd.DataFrame:
    """
    Retrieve the route_ids and agency_ids for routes that service Queens from the
    source files by the NYCT and MTABC into a DataFrame.
    
    Returns:
        pd.DataFrame: `agency_id` and `route_id` for all 122 bus routes servicing
        Queens, excluding shuttles.
    """

    q_routes = pd.read_csv(Q_ROUTES, dtype=str)
    busco_routes = pd.read_csv(BUSCO_ROUTES, dtype=str)

    q_routes_queens = q_routes[q_routes["route_id"].str.match(r"^Q")]
    busco_routes_queens = busco_routes[busco_routes["route_id"].str.match(r"^Q")]

    queens_routes = pd.concat([q_routes_queens, busco_routes_queens])
    queens_routes = queens_routes[~queens_routes["route_id"].isin(SHUTTLES)]
    queens_agency_route_ids = queens_routes[["agency_id", "route_id"]]
    
    return queens_agency_route_ids

def load_stop_routes() -> pd.DataFrame:
    """
    Load all (stop_id, route_id) pairs for bus routes that service Queens from an
    inner join of the stop_times and trips source files by the NYCT and MTABC into
    a DataFrame.

    Returns:
        pd.DataFrame: `stop_id` and transformed `route_id` for all bus routes that
        service Queens.
    """

    q_stop_times = pd.read_csv(
        Q_STOP_TIMES, dtype=str, usecols=["stop_id", "trip_id"]
    )
    busco_stop_times = pd.read_csv(
        BUSCO_STOP_TIMES, dtype=str, usecols=["stop_id", "trip_id"]
    )

    q_trips = pd.read_csv(
        Q_TRIPS, dtype=str, usecols=["route_id", "trip_id"]
    )
    busco_trips = pd.read_csv(
        BUSCO_TRIPS, dtype=str, usecols=["route_id", "trip_id"]
    )
    
    queens_stop_times = pd.concat([q_stop_times, busco_stop_times])

    queens_trips = pd.concat([q_trips, busco_trips])

    queens_agency_routes = _get_queens_routes()
    queens_trips = queens_trips[queens_trips["route_id"].isin(queens_agency_routes["route_id"])]

    queens_trips = pd.merge(
        queens_agency_routes, queens_trips, on="route_id", how="inner"
    )

    queens_trips["route_id"] = queens_trips.apply(
        lambda row: f"MTA NYCT_{row['route_id']}" if row["agency_id"] == "MTA NYCT"
        else f"MTABC_{row['route_id']}",
        axis=1
    )

    queens_stop_routes = pd.merge(
        queens_stop_times, queens_trips, on="trip_id", how="inner"
    )

    queens_stop_routes = queens_stop_routes[["stop_id", "route_id"]].drop_duplicates()
    
    return queens_stop_routes

def load_stops(stop_ids: pd.Series) -> gpd.GeoDataFrame:
    """
    Load all stops  from the source files by the NYCT and MTABC into DataFrames,
    filter to the those in `stop_ids`, establish geometry based on lat-lon coords,
    and spatially join to the Queens borough boundaries.

    Args:
        stop_ids (pd.Series): stop IDs used to filter the stops.txt source files.

    Returns:
        gpd.GeoDataFrame: `stop_id`, `stop_name`, `borough`, `latitude`, `longitude`, `geometry`
    """

    q_stops = pd.read_csv(
        Q_STOPS, dtype=str, usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"]
    )
    busco_stops = pd.read_csv(
        BUSCO_STOPS, dtype=str, usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"]
    )

    queens_stops = pd.concat([q_stops, busco_stops]).drop_duplicates(subset="stop_id")
    queens_stops = queens_stops[queens_stops["stop_id"].isin(stop_ids)]

    stops_gdf = gpd.GeoDataFrame(
        queens_stops,
        geometry = gpd.points_from_xy(queens_stops["stop_lon"], queens_stops["stop_lat"]),
        crs = "EPSG:4326"
    )

    boroughs = gpd.read_file(BOROUGH_BOUNDARIES)
    queens = boroughs[boroughs["boroname"] == "Queens"]

    queens_gdf = gpd.sjoin(
        stops_gdf, queens[["boroname", "geometry"]], how="left", predicate="within"
    )
    queens_gdf = queens_gdf.rename(columns={
        "stop_lat": "latitude",
        "stop_lon": "longitude", 
        "boroname": "borough"
    })

    return queens_gdf[["stop_id", "stop_name", "borough", "latitude", "longitude", "geometry"]]

def insert_all() -> None:
    """
    Load all data onto the stops and stop_routes tables on BigQuery.
    """

    stops_routes = load_stop_routes()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    stops_routes_job = client.load_table_from_dataframe(
        stops_routes, STOP_ROUTES_ID,
        job_config=job_config
    )
    stops_routes_job.result()

    stops = load_stops(stops_routes["stop_id"]).drop(columns=["geometry"])
    stops_job = client.load_table_from_dataframe(
        stops, STOPS_ID, job_config=job_config
    )
    stops_job.result()

if __name__ == "__main__":
    insert_all()