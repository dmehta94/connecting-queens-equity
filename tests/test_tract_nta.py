"""
Unit tests for src/tract_nta.py, on hand-built rows and squares.
"""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from tract_nta import largest_overlap_share, tract_nta_from_equivalency


def equivalency(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["GEOID", "CountyFIPS", "NTACode", "NTAName"])


def test_keeps_only_the_requested_county():
    result = tract_nta_from_equivalency(equivalency(
        ("36081000100", "081", "QN0101", "Astoria (North)"),
        ("36047000100", "047", "BK0101", "Greenpoint"),
    ))

    assert result["tract_id"].tolist() == ["36081000100"]
    assert result["nta_code"].tolist() == ["QN0101"]
    assert result["overlap_pct"].tolist() == [1.0]


def test_rejects_a_tract_listed_under_two_ntas():
    with pytest.raises(ValueError, match="36081000100"):
        tract_nta_from_equivalency(equivalency(
            ("36081000100", "081", "QN0101", "Astoria (North)"),
            ("36081000100", "081", "QN0102", "Astoria (Central)"),
        ))


def test_largest_overlap_share_finds_the_bigger_piece():
    crs = "EPSG:6538"
    tracts = gpd.GeoDataFrame({"tract_id": ["A"]}, geometry=[box(0, 0, 100, 100)], crs=crs)
    ntas = gpd.GeoDataFrame(
        {"nta_code": ["N1", "N2"]},
        geometry=[box(0, 0, 70, 100), box(70, 0, 200, 100)],
        crs=crs,
    )

    [row] = largest_overlap_share(tracts, ntas).to_dict("records")

    assert row["nta_code"] == "N1"
    assert row["largest_share"] == pytest.approx(0.70)
