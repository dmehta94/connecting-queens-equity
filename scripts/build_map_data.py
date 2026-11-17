"""
Build the static data files for the workshop map in map/data/.

One-time script. Reads the 2020 NTA boundaries and the saved GTFS stops from
data/raw/ and the service snapshots from data/derived/, then writes two small
GeoJSON files for map/index.html to load:

- nta_queens.geojson: the 82 Queens NTAs, outlines simplified for phones.
- stops_queens.geojson: Queens bus stops (from proximity.load_queens_stops()),
  each tagged with its NTA and whether it has scheduled local service in the
  weekday midday and Sunday midday snapshots. Proximity uses local service
  only, so express-only stops count as not served here.

map/data/ is gitignored. Nothing is written to BigQuery.

Usage:
    python scripts/build_map_data.py
"""

import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
from proximity import load_queens_stops  # noqa: E402

NTA_BOUNDARIES = ROOT / "data" / "raw" / "nyc_nta_boundaries_2020.geojson"
SNAPSHOTS = ROOT / "data" / "derived" / "service_snapshots_2026.csv"
OUT_DIR = ROOT / "map" / "data"
WORK_CRS = "EPSG:6538"
WEB_CRS = "EPSG:4326"
SIMPLIFY_M = 5  # outline tolerance; well under the 366 m walk radius
COORD_DECIMALS = 5  # about 1 m at NYC's latitude

# NTA 2020 ntatype codes (NYC DCP): 0 is residential; the rest have few or no residents.
NTA_TYPES = {
    "0": "Residential",
    "5": "Rikers Island",
    "6": "Other special area",
    "7": "Cemetery",
    "8": "Airport",
    "9": "Park",
}


def load_queens_ntas() -> gpd.GeoDataFrame:
    """
    Read the 2020 NTA boundaries and keep Queens.

    Returns:
        gpd.GeoDataFrame: `nta2020`, `ntaname`, `ntatype`, `nta_type_label`,
        `geometry` (EPSG:6538, not simplified).
    """

    ntas = gpd.read_file(NTA_BOUNDARIES)
    queens = ntas[ntas["boroname"] == "Queens"].copy()
    queens["nta_type_label"] = queens["ntatype"].map(NTA_TYPES)
    unknown = queens.loc[queens["nta_type_label"].isna(), "ntatype"].unique()
    if len(unknown) > 0:
        raise ValueError(f"Unknown ntatype codes: {list(unknown)}")
    return queens[["nta2020", "ntaname", "ntatype", "nta_type_label", "geometry"]].to_crs(WORK_CRS)


def tag_stops(stops: gpd.GeoDataFrame, ntas: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add each stop's NTA and its local-service flags for both snapshots.

    Args:
        stops: Queens stops in EPSG:6538 (`stop_id`, `stop_name`, `geometry`).
        ntas: Queens NTAs in EPSG:6538 (unsimplified).

    Returns:
        gpd.GeoDataFrame: `stop_id`, `stop_name`, `nta2020`, `weekday`,
        `sunday`, `geometry`. A stop missing from the snapshots has
        weekday and sunday False.
    """

    tagged = gpd.sjoin(stops, ntas[["nta2020", "geometry"]], how="left", predicate="within")
    tagged = tagged.drop(columns="index_right")

    snapshots = pd.read_csv(
        SNAPSHOTS, dtype={"stop_id": str},
        usecols=["stop_id", "weekday_local_service", "sunday_local_service"],
    ).rename(columns={"weekday_local_service": "weekday", "sunday_local_service": "sunday"})
    tagged = tagged.merge(snapshots, on="stop_id", how="left")
    tagged[["weekday", "sunday"]] = tagged[["weekday", "sunday"]].fillna(False).astype(bool)
    return tagged


def write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> None:
    """
    Write a GeoDataFrame as compact WGS84 GeoJSON with rounded coordinates.

    Args:
        gdf: Any GeoDataFrame with a CRS.
        path: Output file path.
    """

    web = gdf.to_crs(WEB_CRS)
    web["geometry"] = web.geometry.set_precision(10 ** -COORD_DECIMALS)
    data = json.loads(web.to_json(drop_id=True))
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def main() -> None:
    """
    Build both map data files and print summary counts.
    """

    ntas = load_queens_ntas()
    stops = tag_stops(load_queens_stops(), ntas)

    outlines = ntas.copy()
    outlines["geometry"] = outlines.geometry.simplify(SIMPLIFY_M, preserve_topology=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    nta_path = OUT_DIR / "nta_queens.geojson"
    stops_path = OUT_DIR / "stops_queens.geojson"
    write_geojson(outlines, nta_path)
    write_geojson(stops, stops_path)

    print(f"NTAs: {len(ntas)}")
    print(ntas["nta_type_label"].value_counts().to_string())
    print(f"\nStops: {len(stops)}")
    print(f"  without an NTA: {int(stops['nta2020'].isna().sum())}")
    print(f"  weekday midday local service: {int(stops['weekday'].sum())}")
    print(f"  sunday midday local service:  {int(stops['sunday'].sum())}")
    print(f"\n{nta_path.name}: {nta_path.stat().st_size / 1024:.0f} KB")
    print(f"{stops_path.name}: {stops_path.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
