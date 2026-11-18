from pathlib import Path
import pandas as pd
import geopandas as gpd
import zipfile
import warnings

#Constants
ROOT = Path(__file__).parent.parent
FEEDS = {
    "q": ROOT / "data" / "raw" / "mta_gtfs_q_2026.zip",
    "busco": ROOT / "data" / "raw" / "mta_gtfs_busco_2026.zip",
}
BOROUGH_BOUNDARIES = ROOT / "data" / "raw" / "nyc_borough_boundaries_2024.geojson"
MAX_STOP_GAP_M = 37 # feeds' gap for one stop above which the midpoint could be >5% of the 366 m radius off.

SNAP_RETAILERS = ROOT / "data" / "raw" / "usda_snap_retailers_2026.geojson"

PHARMACY_PATTERN = r"pharmacy|cvs|walgreen|rite aid|duane reade|farmacia|drug|chemist|\brx\b"
DOLLAR_STORE_PATTERN = r"dollar|99 ?cent"
CONVENIENCE_PATTERN = r"\bdeli\b|mini ?mart|convenience"


def _read(zf: zipfile.ZipFile, name: str, **kwargs) -> pd.DataFrame:
    """
    Read one GTFS text file from an open zip as strings.

    Args:
        zf: Open GTFS zip file.
        name: File name inside the zip, such as "trips.txt".
        **kwargs: Passed through to pd.read_csv (for example usecols).

    Returns:
        pd.DataFrame: The file contents, all columns as strings.
    """

    with zf.open(name) as f:
        return pd.read_csv(f, dtype=str, encoding="utf-8-sig", **kwargs)

def load_queens_stops() -> gpd.GeoDataFrame:
    """
    Read all stops from the GTFS stops.txt files, with stop_id as text,
    stack the tables, drop exact copies of stops, make points and
    reproject to EPSG:6538, keep only those stops in Queens, and give
    conflicting stops their midpoint.

    Returns:
        gpd.GeoDataFrame: `stop_id`, `stop_name`, `geometry` (points, EPSG:6538)

    Warns:
        UserWarning: When any stop’s copies are more than MAX_STOP_GAP_M (37 m) apart. The warning names those stops.
    """

    stops_frames = []
    for feed in FEEDS.values():
        with zipfile.ZipFile(feed) as zf:
            stops_frames.append(_read(zf, "stops.txt", usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"]))

    stops_df = pd.concat(stops_frames, ignore_index=True).drop_duplicates(subset=["stop_id", "stop_lat", "stop_lon"])
    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df["stop_lon"], stops_df["stop_lat"]),
        crs="EPSG:4326",
    )
    stops_gdf = stops_gdf.to_crs("EPSG:6538")

    boroughs = gpd.read_file(BOROUGH_BOUNDARIES)
    queens = boroughs[boroughs["boroname"] == "Queens"]
    queens = queens.to_crs("EPSG:6538")

    queens_stops = stops_gdf[stops_gdf.within(queens.geometry.iloc[0])].copy()
    queens_stops["stop_x"] = queens_stops.geometry.x
    queens_stops["stop_y"] = queens_stops.geometry.y

    queens_mask = queens_stops.duplicated(subset=["stop_id"], keep=False)
    conflicts = queens_stops[queens_mask].copy()
    gap = conflicts.groupby("stop_id")[["stop_x", "stop_y"]].agg(lambda s: s.max() - s.min())
    gap["dist"] = (gap["stop_x"] ** 2 + gap["stop_y"] ** 2) ** 0.5

    midpoints = queens_stops.groupby("stop_id").agg(
        x = ("stop_x", "mean"), y = ("stop_y", "mean"), stop_name = ("stop_name", "first")
        ).reset_index()

    high_gap_stops = gap[gap["dist"] > MAX_STOP_GAP_M].index.tolist()
    if len(high_gap_stops) > 0:
        warnings.warn(f"{len(high_gap_stops)} stop_id(s) have multiple locations more than {MAX_STOP_GAP_M} m apart. Using midpoints.\n"
                      f"stop_id(s): {', '.join(high_gap_stops)}")

    stops_out = gpd.GeoDataFrame(
        midpoints[["stop_id", "stop_name"]],
        geometry=gpd.points_from_xy(midpoints["x"], midpoints["y"]),
        crs="EPSG:6538"
    )

    return stops_out

def load_candidate_snap_retailers(stops: gpd.GeoDataFrame, radius_m: float = 366) -> gpd.GeoDataFrame:
    """
    Create a buffer of the prescribed radius around each deduplicated bus stop, take the union of those buffers,
    filter all NY SNAP retailers within that union, and return the resulting GeoDataFrame of candidate SNAP retailers.

    Args:
        stops (gpd.GeoDataFrame): Deduplicated bus stops.
        radius_m (float): Radius of the buffer in meters (default: 366).

    Returns:
        gpd.GeoDataFrame: Candidate SNAP retailers within the union of the bus stop buffers, in EPSG:6538.
    """

    snap_gdf = gpd.read_file(SNAP_RETAILERS).to_crs("EPSG:6538")
    inclusion_zone = stops.buffer(radius_m).union_all()
    candidate_snap_gdf = snap_gdf[snap_gdf.within(inclusion_zone)].copy()

    return candidate_snap_gdf