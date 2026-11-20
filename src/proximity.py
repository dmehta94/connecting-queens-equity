import warnings
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import networkx as nx
import pandas as pd
import shapely

# Paths
ROOT = Path(__file__).parent.parent
FEEDS = {
    "q": ROOT / "data" / "raw" / "mta_gtfs_q_2026.zip",
    "busco": ROOT / "data" / "raw" / "mta_gtfs_busco_2026.zip",
}
BOROUGH_BOUNDARIES = ROOT / "data" / "raw" / "nyc_borough_boundaries_2024.geojson"
SNAP_RETAILERS = ROOT / "data" / "raw" / "usda_snap_retailers_2026.geojson"
FOOD_GROUP_TAGS = ROOT / "data" / "reference" / "usda_snap_food_group_tags_2026.csv"
NETWORK_ZIP = ROOT / "data" / "raw" / "nycwalks_pedestrian_network_2026.zip"
NETWORK_LAYER = "NYC_pednetwork_estimates_counts_2018-2019.geojson"

# Walking and stop parameters
MAX_STOP_GAP_M = 37 # feeds' gap for one stop above which the midpoint could be >5% of the 366 m radius off.
WALK_RADIUS_M = 366

# Food layer
PHARMACY_PATTERN = r"pharmacy|cvs|walgreen|rite aid|duane reade|farmacia|drug|chemist|\brx\b"
DOLLAR_STORE_PATTERN = r"dollar|99 ?cent"
CONVENIENCE_PATTERN = r"\bdeli\b|mini ?mar(?:t|ket)|convenience"
FULL_PLATE_TYPES = ("Supermarket", "Super Store", "Grocery Store")
CONTEXT_TYPES = ("Convenience Store", "Restaurant Meals Program")
TAGGED_TYPES = ("Specialty Store", "Other")
CONTEXT_NAME_PATTERN = "|".join([PHARMACY_PATTERN, DOLLAR_STORE_PATTERN, CONVENIENCE_PATTERN])

TAG_SHARES = {
    "full_plate": 1,
    "produce": 0.5,
    "bread+protein": 0.5,
    "bread": 0.25,
    "protein": 0.25,
    "no_plate_food": 0,
    "sweets": 0,
    "unknown": 0,
} # Share of a Harvard Healthy Eating Plate: vegetables and fruit 1/2, whole grains 1/4, healthy protein 1/4


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

def filter_to_stop_zone(places: gpd.GeoDataFrame, stops: gpd.GeoDataFrame, radius_m: float = WALK_RADIUS_M) -> gpd.GeoDataFrame:
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

def assign_food_shares(candidates: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Assign food shares to each candidate SNAP retailer that qualifies.

    Join the candidate SNAP retailers GeoDataFrame (EPSG:6538) with the
    reviewed tags in FOOD_GROUP_TAGS, add the layer ("scored", "context", or
    "none") and share (0 to 1, set only for scored retailers) to the
    GeoDataFrame.

    Args:
        candidates (gpd.GeoDataFrame): candidate SNAP retailers as loaded by 
        load_candidate_snap_retailers() in EPSG:6538
    
    Returns:
        gpd.GeoDataFrame: a copy of the original GeoDataFrame with two new columns (layer and share) appended to it and the Record_ID converted to a string in EPSG:6538
    """
    tags = pd.read_csv(FOOD_GROUP_TAGS, dtype={"Record_ID": str})
    candidates_with_shares = candidates.copy()
    candidates_with_shares["Record_ID"] = candidates_with_shares["Record_ID"].astype(str)
    candidates_with_shares = candidates_with_shares.merge(
        tags[["Record_ID", "proposed_tag"]], on="Record_ID", how="left"
    )

    store_type = candidates_with_shares["Store_Type"]
    tag = candidates_with_shares["proposed_tag"]
    name_matches = candidates_with_shares["Store_Name"].str.contains(CONTEXT_NAME_PATTERN, case=False, na=False)

    layer = np.select(
        [
            store_type.isin(FULL_PLATE_TYPES),
            store_type.isin(CONTEXT_TYPES),
            (store_type == "Other") & name_matches,
            store_type.isin(TAGGED_TYPES) & (tag == "context_convenience"),
            store_type.isin(TAGGED_TYPES) & tag.notna(),
            ], [
                "scored", "context", "context", "context", "scored"
            ],
            default="none"
    )

    candidates_with_shares["layer"] = layer
    candidates_with_shares["share"] = np.where(layer == "scored",
         np.where(store_type.isin(FULL_PLATE_TYPES), 1.0, tag.map(TAG_SHARES)),
         np.nan)
    candidates_with_shares = candidates_with_shares.drop(columns="proposed_tag")
    return candidates_with_shares

def giant_component_nodes(G: nx.Graph, nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Filter all nodes of physical network down to only those in the giant
    connected component.

    Snapping uses these nodes so every stop and essential service location
    lands on the four-borough network.
    
    Ensure the nodes GDF is in EPSG:6538, then identify the largest connected
    component of G as a set of node IDs, and keep the rows of nodes whose
    index is in that set.
    
    Args:
        G (nx.Graph): the network graph of which we want the giant component
        nodes (gpd.GeoDataFrame): a GeoDataFrame of the nodes of G with index
            node_ID.
        
    Returns:
        gpd.GeoDataFrame: a reduced GeoDataFrame of those nodes which lie in
            the largest connected component.
    """

    nodes = nodes.to_crs("EPSG:6538")
    giant = max(nx.connected_components(G), key=len)
    giant_nodes = nodes[nodes.index.isin(giant)]
    return giant_nodes

def snap_to_giant(points: gpd.GeoDataFrame, giant_nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add two new columns to the points GeoDataFrame: node (the ID of the nearest
    giant-component node) and snap_m (the distance to it, in meters).

    Convert points to the CRS of giant_nodes, join each point to its nearest
    node, drop tie duplicates, keeping one row per the original index, and
    rename the joined node column to node.

    Args:
        points (gpd.GeoDataFrame): locations of interest
        giant_nodes (gpd.GeoDataFrame): the nodes of a network that lie in its
            largest connected component

    Returns:
        gpd.GeoDataFrame: a copy of points with the new node and snap_m columns
    """
    snapped_points = points.to_crs(giant_nodes.crs)
    snapped_points = gpd.sjoin_nearest(snapped_points, giant_nodes[["geometry"]], how="left", distance_col="snap_m")
    snapped_points = snapped_points.rename(columns={"index_right": "node"})
    snapped_points = snapped_points[~snapped_points.index.duplicated(keep="first")]
    return snapped_points

def walk_distances(G: nx.Graph, snapped_stops: gpd.GeoDataFrame, radius_m: float = WALK_RADIUS_M) -> dict:
    """
    Determine which nodes of network G are walkable from each stop within the
    given radius in meters.

    Loop over all stops, determine the budget (radius_m - snap_m), store empty
    dictionaries for stops with a budget of 0 or less, run Dijkstra's algorithm
    on the rest with cutoff=budget, and store the result under the stop's
    stop_id. Return the dictionary of dictionaries for all stops.

    Args:
        G (nx.Graph): The pedestrian network with edge length in meters
        snapped_stops (gpd.GeoDataFrame): Stops from snap_to_giant() with
            stop_id, node, and snap_m columns.
        radius_m (float): The radius that determines the budget. Default is
            WALK_RADIUS_M.

    Returns:
        dict: {stop_id: {node: meters}}. Distances along the network from each
            stop's node and excluding the snap legs.
    """

    reachable = {}
    for stop_id, node, snap_m in zip(snapped_stops["stop_id"], snapped_stops["node"], snapped_stops["snap_m"]):
        budget = radius_m - snap_m
        if budget <= 0:
            reachable[stop_id] = {}
        else:
            reachable[stop_id] = nx.single_source_dijkstra_path_length(G, node, cutoff=budget, weight="length")
    return reachable