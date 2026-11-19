import warnings
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import networkx as nx
import pandas as pd
import shapely

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

NETWORK_ZIP = ROOT / "data" / "raw" / "nycwalks_pedestrian_network_2026.zip"
NETWORK_LAYER = "NYC_pednetwork_estimates_counts_2018-2019.geojson"


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

def filter_to_stop_zone(places: gpd.GeoDataFrame, stops: gpd.GeoDataFrame, radius_m: float = 366) -> gpd.GeoDataFrame:
    """
    Converts both the places and stops to EPSG:6538, creates a buffer of the
    prescribed radius around each deduplicated bus stop, takes the union of
    those buffers, filters all places within that union, and returns the
    resulting GeoDataFrame of places within the union of the bus stop buffers.

    Args:
        places (gpd.GeoDataFrame): Places to filter.
        stops (gpd.GeoDataFrame): Deduplicated bus stops.
        radius_m (float): Radius of the buffer in meters (default: 366).

    Returns:
        gpd.GeoDataFrame: Places within the union of the bus stop buffers, in EPSG:6538.
    """

    places = places.to_crs("EPSG:6538")
    stops = stops.to_crs("EPSG:6538")
    inclusion_zone = stops.buffer(radius_m).union_all()
    filtered_places = places[places.within(inclusion_zone)].copy()

    return filtered_places

def load_candidate_snap_retailers(stops: gpd.GeoDataFrame, radius_m: float = 366) -> gpd.GeoDataFrame:
    """
    Read all NY SNAP retailers and keep those within radius_m (straight line) of
    any bus stop, using filter_to_stop_zone().

    Args:
        stops (gpd.GeoDataFrame): Deduplicated bus stops.
        radius_m (float): Radius of the buffer in meters (default: 366).

    Returns:
        gpd.GeoDataFrame: Candidate SNAP retailers within the union of the bus stop buffers, in EPSG:6538.
    """

    snap_gdf = gpd.read_file(SNAP_RETAILERS)
    candidate_snap_gdf = filter_to_stop_zone(snap_gdf, stops, radius_m=radius_m)

    return candidate_snap_gdf

def load_pedestrian_graph() -> tuple[nx.Graph, gpd.GeoDataFrame]:
    """
    Load the NYC Walks pedestrian network as a graph for walking distances.

    Read the GeoJSON from the NYC Walks zip at NETWORK_ZIP (GUID and geometry
    only), ensure that the CRS is EPSG:6538 by converting if necessary, build
    the segment table, give each node an integer ID, drop self-loops, keep the
    shortest of each parallel pair, and build a network graph.

    Returns:
        nx.Graph: A graph of the network with edges (length in m, GUID)
        gpd.GeoDataFrame: A GeoDataFrame of the graph's nodes, indexed by
            node ID, point geometry in EPSG:6538
    """
    network = gpd.read_file(f"zip://{NETWORK_ZIP}!{NETWORK_LAYER}", columns=["__GUID"]).to_crs("EPSG:6538")

    starts = shapely.get_point(network.geometry.values, 0)
    ends = shapely.get_point(network.geometry.values, -1)

    segments = pd.DataFrame({
        "start_x": shapely.get_x(starts),
        "start_y": shapely.get_y(starts),
        "end_x": shapely.get_x(ends),
        "end_y": shapely.get_y(ends),
        "length": network.length.to_numpy(),
        "guid": network["__GUID"].to_numpy()
    })

    all_xs = np.concatenate([segments["start_x"], segments["end_x"]])
    all_ys = np.concatenate([segments["start_y"], segments["end_y"]])

    codes, uniques = pd.factorize(pd.MultiIndex.from_arrays([all_xs, all_ys]))

    n = len(segments)
    segments["u"] = codes[:n]
    segments["v"] = codes[n:]
    segments["lo"] = np.minimum(segments["u"], segments["v"])
    segments["hi"] = np.maximum(segments["u"], segments["v"])

    segments = segments[segments["u"] != segments["v"]].sort_values("length").drop_duplicates(subset=["lo", "hi"])

    G = nx.from_pandas_edgelist(segments, source="u", target="v", edge_attr=["length", "guid"])

    nodes = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(uniques.get_level_values(0), uniques.get_level_values(1)), crs="EPSG:6538"
    )

    return G, nodes