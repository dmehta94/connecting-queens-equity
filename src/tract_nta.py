"""
Tract-to-NTA junction for Queens.

2020 census tracts nest inside 2020 NTAs (NYC DCP equivalency file, NYC Open
Data `hm78-6dwm`), so each tract maps to exactly one NTA with overlap 1.0.
largest_overlap_share() is a geometric cross-check of that claim.
"""

import geopandas as gpd
import pandas as pd

WORKING_CRS = "EPSG:6538"


def tract_nta_from_equivalency(equivalency: pd.DataFrame, county_fips: str = "081") -> pd.DataFrame:
    """
    Build tract_nta rows for one county from the DCP equivalency file.

    Args:
        equivalency: DCP equivalency rows, read with every column as text.
        county_fips: Three-digit county FIPS code; Queens is "081".

    Returns:
        pd.DataFrame: `tract_id`, `nta_code`, `nta_name`, `overlap_pct` (always 1.0).

    Raises:
        ValueError: When a tract appears more than once, so it doesn't nest in one NTA.
    """

    county = equivalency[equivalency["CountyFIPS"] == county_fips]
    repeated = county.loc[county["GEOID"].duplicated(), "GEOID"]
    if not repeated.empty:
        raise ValueError(f"Tracts mapped to more than one NTA: {sorted(repeated.unique())}")

    return pd.DataFrame({
        "tract_id": county["GEOID"].to_numpy(),
        "nta_code": county["NTACode"].to_numpy(),
        "nta_name": county["NTAName"].to_numpy(),
        "overlap_pct": 1.0,
    })


def largest_overlap_share(tracts: gpd.GeoDataFrame, ntas: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    For each tract, find the NTA holding the largest share of its area.

    Used to check the equivalency file: a share well below 1.0 would mean the
    tract is split between NTAs.

    Args:
        tracts: Tract polygons with a `tract_id` column.
        ntas: NTA polygons with an `nta_code` column.

    Returns:
        pd.DataFrame: `tract_id`, `nta_code` (largest piece), `largest_share`
        (0 to 1), one row per tract that touches any NTA.
    """

    tracts = tracts[["tract_id", "geometry"]].to_crs(WORKING_CRS)
    ntas = ntas[["nta_code", "geometry"]].to_crs(WORKING_CRS)
    tract_area = tracts.set_index("tract_id").area

    pieces = gpd.overlay(tracts, ntas, how="intersection", keep_geom_type=True)
    pieces["share"] = pieces.area / pieces["tract_id"].map(tract_area)
    largest = pieces.sort_values("share", ascending=False).drop_duplicates("tract_id")

    return largest.rename(columns={"share": "largest_share"})[["tract_id", "nta_code", "largest_share"]].reset_index(drop=True)
