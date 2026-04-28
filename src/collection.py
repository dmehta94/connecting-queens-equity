"""
collection.py

Fetches real-time bus location data for bus routes serving Queens from the MTA
BusTime SIRI API and inserts them into the vehicle_positions table on BigQuery.

Usage:
    python collection.py

Requirements:
    MTA_API_KEY environment variable must be set before running.
    GCP Application Default Credentials must be active (gcloud auth application-default login).
"""

import os
import requests
import logging
import uuid
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery

ENDPOINT = "https://api.prod.obanyc.com/api/siri/vehicle-monitoring.json"
SLEEP_SECONDS = 0.5
PROJECT_ID = "connecting-queens-equity"
DATASET_ID = "connecting_queens_equity"
TABLE_ID = "vehicle_positions"
COLLECTION_ID = "collection_log"

VEHICLE_POSITIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
COLLECTION_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{COLLECTION_ID}"

logger = logging.getLogger(__name__)

load_dotenv()
MTA_API_KEY = os.getenv("MTA_API_KEY")

def fetch_vehicle_positions(
        route_id: str,
        api_key: str,
        session: requests.Session
) -> list[dict] | None:
    """
    Fetch real-time vehicle location data for a single bus route.

    Sends a GET request to the MTA BusTime SIRI vehicle-monitoring endpoint
    and returns the raw vehicle activity list from the SIRI Response. Returns
    an empty list if no vehicles are currently active on the route or if the
    request fails.

    Args:
        route_id: MTA route identifier (e.g., 'MTA NYCT_Q27').
        api_key: MTA BusTime API key.
        session: Active requests.Session for connection reuse across route calls.

    Returns:
        List of raw vehicle activity dicts from SIRI VehicleActivity array.
        Returns an empty list if no vehicles are active on the route or if the
        request fails.

    Example:
        >>> with requests.Session() as session:
        ...     vehicles = fetch_vehicle_positions(
        ...         "MTA NYCT_Q27",
        ...         api_key,
        ...         session
        ...     )
        ...     print(f"Found {len(vehicles)} active vehicles on Q27.")
    """

    params = {
        "key": api_key,
        "OperatorRef": "MTA",
        "LineRef": route_id
    }

    try:
        response = session.get(ENDPOINT, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        return (
            data.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("VehicleMonitoringDelivery", [{}])[0]
            .get("VehicleActivity", [])
        )

    except requests.exceptions.RequestException as e:
        logger.warning(f"Could not fetch data for {route_id}: {e}")
        return None

def parse_vehicle_positions(
          route_id: str,
          vehicles: list[dict]
) -> list[dict]:
    """
    Parse vehicle location data for a single bus route.
        
    Transforms a raw SIRI VehicleActivity list into a list of dicts shaped to
    the vehicle_positions BigQuery schema.
        
    Args:
        route_id: MTA route identifier (e.g., 'MTA NYCT_Q27).
        vehicles: List of raw vehicle activity dicts from SIRI VehicleActivity
        array.
            
    Returns:
        records: List of specified information for each vehicle active on the
        route in question: position_id, route_id, vehicle_id, latitude,
        longitude, direction_ref, next_stop_ref, bearing, collected_at.
        Returns an empty list if vehicles is empty.
    
    Example:
        >>> records = parse_vehicle_positions(
        ...     "MTA NYCT_Q27",
        ...     vehicles
        ... )
    """
    
    records = []
    for vehicle in vehicles:
        journey = vehicle["MonitoredVehicleJourney"]
        location = journey["VehicleLocation"]
        records.append({
            "position_id": str(uuid.uuid4()),
            "route_id": route_id,
            "vehicle_id": journey["VehicleRef"],
            "latitude": location.get("Latitude"),
            "longitude": location.get("Longitude"),
            "direction_ref": journey.get("DirectionRef"),
            "next_stop_ref": journey.get("MonitoredCall", {}).get("StopPointRef"),
            "bearing": journey.get("Bearing"),
            "collected_at": datetime.now(timezone.utc).isoformat()
        })
    return records

def insert_vehicle_positions(
          rows: list[dict],
          client: bigquery.Client
) -> int:
    """
    Insert parsed vehicle position records into the vehicle_positions BigQuery table.

    Uses the BigQuery streaming insert path (insert_rows_json) for low-latency
    appends. Returns immediately without writing if rows is empty. Logs the
    number of rows inserted on success, or logs a warning if BigQuery returns
    errors.

    Args:
        rows: List of dicts shaped to the vehicle_positions schema. Each dict
            must contain: position_id, route_id, vehicle_id, latitude,
            longitude, direction_ref, next_stop_ref, bearing, collected_at.
        client: Authenticated BigQuery client instance.

    Returns:
        int: number of errors from insertion
    """
    
    if not rows:
         return 0
    
    errors = client.insert_rows_json(VEHICLE_POSITIONS_TABLE, rows)
    if errors:
         logger.warning(f"BigQuery insert errors: {errors}")
         return len(errors)
    else:
         logger.info(f"Inserted {len(rows)} rows into {VEHICLE_POSITIONS_TABLE}")
         return 0

def log_collection_sweep(
    swept_at: datetime,
    duration_seconds: float,
    routes_attempted: int,
    rows_inserted: int,
    fetch_errors: int,
    parse_errors: int,
    insert_errors: int,
    client: bigquery.Client
) -> None:
    """
    Insert a single summary record of a completed collection sweep into the
    collection_log BigQuery table.
    
    Args:
        swept_at: UTC timestamp when the collection sweep ran.
        duration_seconds: Span of time in which the sweep ran measured in seconds.
        routes_attempted: Number of routes scanned by the sweep.
        rows_inserted: Number of rows added to BigQuery during the sweep.
        fetch_errors: Number of times collection.py failed to fetch a route.
        parse_errors: Number of times collection.py failed to parse the details of
            a route.
        insert_errors: Number of times collection.py failed to write parsed route
            details to BigQuery.
        client: Authenticated BigQuery client instance.
    
    Returns:
        None
    """
    log_record = [{
        "swept_at": swept_at.isoformat(),
        "duration_seconds": duration_seconds,
        "routes_attempted": routes_attempted,
        "rows_inserted": rows_inserted,
        "fetch_errors": fetch_errors,
        "parse_errors": parse_errors,
        "insert_errors": insert_errors
    }]

    errors = client.insert_rows_json(COLLECTION_LOG_TABLE, log_record)
    if errors:
         logger.warning(f"BigQuery insert errors: {errors}")
    else:
         logger.info(f"Inserted record at {swept_at} into {COLLECTION_LOG_TABLE}")

if __name__ == "__main__":
    if not MTA_API_KEY:
        raise EnvironmentError(
            "MTA_API_KEY environment variable not set. "
            "Export it before running: export MTA_API_KEY='your_key_here'"
        )
    client = bigquery.Client()
    result = client.query("SELECT route_id FROM `connecting-queens-equity.connecting_queens_equity.routes` WHERE active = TRUE")
    route_ids = [row["route_id"] for row in result]
     
    with requests.Session() as session:
        sweep_start = datetime.now(timezone.utc)
        routes_attempted = 0
        rows_inserted = 0
        fetch_errors = 0
        # Known gap: parse_errors to be fully implemented later.
        parse_errors = 0
        insert_errors = 0
        for route in route_ids:
            routes_attempted += 1
            vehicles = fetch_vehicle_positions(
                route,
                MTA_API_KEY,
                session
            )
            if vehicles is None:
                fetch_errors += 1
                continue
            rows = parse_vehicle_positions(
                route,
                vehicles
            )
            insert_errors += insert_vehicle_positions(rows, client)
            rows_inserted += len(rows)
            time.sleep(SLEEP_SECONDS)
        duration_seconds = (datetime.now(timezone.utc) - sweep_start).total_seconds()
        log_collection_sweep(sweep_start, duration_seconds, routes_attempted, rows_inserted, fetch_errors, parse_errors, insert_errors, client)