"""
Build the static data files for the workshop map in map/data/.

One-time script. Reads the 2020 NTA boundaries, 2020 census tracts, and the
saved GTFS stops from data/raw/ and the service snapshots and transit
dependency figures from data/derived/, then writes three small GeoJSON files
for map/index.html to load:

- nta_queens.geojson: the 82 Queens NTAs, outlines simplified for phones, with
  each NTA's transit dependency counts summed from its tracts.
- tracts_queens.geojson: the Queens census tracts, clipped to the shoreline
  (their NTA) and simplified, with rounded vehicle deficit and housing cost
  burden shares. Tracts with no households have null shares ("no data").
- stops_queens.geojson: Queens bus stops (from proximity.load_queens_stops()),
  each tagged with its NTA and whether it has scheduled local service in the
  weekday midday and Sunday midday snapshots. Proximity uses local service
  only, so express-only stops count as not served here.

map/data/ is committed so the map can be served as static files. Nothing is
written to BigQuery.

Each build stamps a data version into map/index.html (the `?v=` on app.js),
which app.js adds to its data fetches, so browsers fetch new files after a
rebuild instead of serving cached ones.

Usage:
    python scripts/build_map_data.py
"""

import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
from proximity import load_queens_stops  # noqa: E402

NTA_BOUNDARIES = ROOT / "data" / "raw" / "nyc_nta_boundaries_2020.geojson"
SNAPSHOTS = ROOT / "data" / "derived" / "service_snapshots_2026.csv"
TRACT_BOUNDARIES = ROOT / "data" / "raw" / "tiger_census_tracts_ny_2022.zip"
TRACT_FIGURES = ROOT / "data" / "derived" / "census_tracts_queens_2024.csv"
TRACT_NTA = ROOT / "data" / "derived" / "tract_nta_queens_2024.csv"
QUEENS_COUNTYFP = "081"
OUT_DIR = ROOT / "map" / "data"
MAP_INDEX = ROOT / "map" / "index.html"
APP_SCRIPT_TAG = re.compile(rb'src="app\.js(\?v=[^"]*)?"')
WORK_CRS = "EPSG:6538"
WEB_CRS = "EPSG:4326"
SIMPLIFY_M = 5  # outline tolerance; well under the 366 m walk radius
COORD_DECIMALS = 5  # about 1 m at NYC's latitude
SHARE_DECIMALS = 3  # 0.1 percentage point

# Counts summed to NTAs. NTA shares are recomputed from these sums, never averaged.
COUNT_COLUMNS = ["vehicle_deficit_households", "households", "burdened_households", "burden_households"]

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


def load_tract_figures() -> pd.DataFrame:
    """
    Read the per-tract transit dependency figures with each tract's NTA.

    Adds the pooled renter + owner counts behind `burden_share`: households
    with burden computed, and those paying 30% or more of income on housing.

    Returns:
        pd.DataFrame: `tract_id`, `nta2020`, `vehicle_deficit_share`,
        `burden_share`, and the COUNT_COLUMNS. One row per tract.
    """

    figures = pd.read_csv(TRACT_FIGURES, dtype={"tract_id": str})
    junction = pd.read_csv(TRACT_NTA, dtype={"tract_id": str}, usecols=["tract_id", "nta_code"])

    figures["burdened_households"] = figures["renter_burdened"].fillna(0) + figures["owner_burdened"].fillna(0)
    figures["burden_households"] = figures["renter_households"].fillna(0) + figures["owner_households"].fillna(0)
    # one_to_one fails if a tract maps to two NTAs, which would double-count it.
    merged = figures.merge(junction, on="tract_id", how="left", validate="one_to_one")
    missing = int(merged["nta_code"].isna().sum())
    if missing:
        raise ValueError(f"{missing} tracts have no NTA in {TRACT_NTA.name}")
    merged = merged.rename(columns={"nta_code": "nta2020"})
    return merged[["tract_id", "nta2020", "vehicle_deficit_share", "burden_share", *COUNT_COLUMNS]]


def summarize_ntas(figures: pd.DataFrame) -> pd.DataFrame:
    """
    Sum tract counts to NTAs.

    Args:
        figures: Output of load_tract_figures().

    Returns:
        pd.DataFrame: `nta2020` plus the COUNT_COLUMNS as integers. The map
        divides the sums; an NTA with no households shows "no data".
    """

    sums = figures.groupby("nta2020", as_index=False)[COUNT_COLUMNS].sum()
    sums[COUNT_COLUMNS] = sums[COUNT_COLUMNS].round().astype(int)
    return sums


def build_tracts(figures: pd.DataFrame, ntas: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Build tract shapes for the map: clipped to their NTA, simplified, rounded.

    TIGER/Line tracts extend into the water; DCP's NTAs follow the shoreline.
    Tracts nest in NTAs, so clipping each tract to its own NTA removes the
    water without moving any inland edge.

    Args:
        figures: Output of load_tract_figures().
        ntas: Queens NTAs in EPSG:6538 (unsimplified).

    Returns:
        gpd.GeoDataFrame: `tract_id`, `nta2020`, `vehicle_deficit_share`,
        `burden_share` (null where the tract has no households), `geometry`.
        Tracts left with no land after clipping are dropped.
    """

    tiger = gpd.read_file(TRACT_BOUNDARIES)
    tiger = tiger.loc[tiger["COUNTYFP"] == QUEENS_COUNTYFP, ["GEOID", "geometry"]].to_crs(WORK_CRS)
    tiger = tiger.rename(columns={"GEOID": "tract_id"})
    tracts = tiger.merge(figures, on="tract_id", how="inner", validate="one_to_one")
    if len(tracts) != len(figures):
        raise ValueError(f"{len(figures) - len(tracts)} tracts have no TIGER geometry")

    nta_shapes = ntas.set_index("nta2020").geometry
    clipped = [geom.intersection(nta_shapes[nta]) for geom, nta in zip(tracts.geometry, tracts["nta2020"])]
    tracts = tracts.set_geometry(gpd.GeoSeries(clipped, index=tracts.index, crs=WORK_CRS))
    tracts = tracts[tracts.geometry.area > 1].copy()  # drop all-water slivers (under 1 square meter)
    tracts["geometry"] = tracts.geometry.simplify(SIMPLIFY_M, preserve_topology=True)

    no_households = tracts["households"] == 0
    for col in ["vehicle_deficit_share", "burden_share"]:
        tracts[col] = tracts[col].round(SHARE_DECIMALS).where(~no_households)
    return tracts[["tract_id", "nta2020", "vehicle_deficit_share", "burden_share", "geometry"]]


def _round_coords(coords):
    """Round a nested GeoJSON coordinate list to COORD_DECIMALS."""

    if isinstance(coords[0], (int, float)):
        return [round(c, COORD_DECIMALS) for c in coords]
    return [_round_coords(c) for c in coords]


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
    # set_precision snaps to the grid, but the floats can still print as
    # 40.789550000000006; round again so the file stays small.
    for feature in data["features"]:
        geometry = feature["geometry"]
        geometry["coordinates"] = _round_coords(geometry["coordinates"])
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def stamp_data_version(paths: list[Path]) -> str:
    """
    Stamp a data version into map/index.html's app.js tag.

    The version is the build date plus a short hash of the data files, so a
    same-day rebuild with changed data still gets a new version. The file is
    edited as bytes, so its line endings are kept.

    Args:
        paths: The data files app.js fetches.

    Returns:
        str: The version, e.g. "2026-11-19-3fa9c1".
    """

    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.read_bytes())
    version = f"{date.today().isoformat()}-{digest.hexdigest()[:6]}"

    html = MAP_INDEX.read_bytes()
    stamped, count = APP_SCRIPT_TAG.subn(f'src="app.js?v={version}"'.encode(), html)
    if count != 1:
        raise ValueError(f'Expected one src="app.js" tag in {MAP_INDEX.name}, found {count}')
    MAP_INDEX.write_bytes(stamped)
    return version


def main() -> None:
    """
    Build the three map data files and print summary counts.
    """

    ntas = load_queens_ntas()
    stops = tag_stops(load_queens_stops(), ntas)
    figures = load_tract_figures()
    tracts = build_tracts(figures, ntas)

    outlines = ntas.copy()
    outlines["geometry"] = outlines.geometry.simplify(SIMPLIFY_M, preserve_topology=True)
    outlines = outlines.merge(summarize_ntas(figures), on="nta2020", how="left", validate="one_to_one")
    outlines[COUNT_COLUMNS] = outlines[COUNT_COLUMNS].fillna(0).astype(int)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    nta_path = OUT_DIR / "nta_queens.geojson"
    stops_path = OUT_DIR / "stops_queens.geojson"
    tracts_path = OUT_DIR / "tracts_queens.geojson"
    write_geojson(outlines, nta_path)
    write_geojson(stops, stops_path)
    write_geojson(tracts, tracts_path)
    version = stamp_data_version([nta_path, stops_path, tracts_path])

    print(f"NTAs: {len(ntas)}")
    print(ntas["nta_type_label"].value_counts().to_string())
    print(f"\nStops: {len(stops)}")
    print(f"  without an NTA: {int(stops['nta2020'].isna().sum())}")
    print(f"  weekday midday local service: {int(stops['weekday'].sum())}")
    print(f"  sunday midday local service:  {int(stops['sunday'].sum())}")
    print(f"\n{nta_path.name}: {nta_path.stat().st_size / 1024:.0f} KB")
    print(f"{stops_path.name}: {stops_path.stat().st_size / 1024:.0f} KB")
    print(f"{tracts_path.name}: {tracts_path.stat().st_size / 1024:.0f} KB")
    print(f"data version stamped in {MAP_INDEX.name}: {version}")
    print(f"\nTracts: {len(figures)} with figures, {len(tracts)} with land after clipping")
    print(f"  no households (no data): {int(tracts['vehicle_deficit_share'].isna().sum())}")
    print(f"  dropped as all water: {sorted(set(figures['tract_id']) - set(tracts['tract_id']))}")


if __name__ == "__main__":
    main()
