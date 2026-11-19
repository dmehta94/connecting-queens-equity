"""
Unit tests for src/transit_dependency.py.

These tests run offline on hand-built rows shaped like Census API responses.
"""

import math

import pandas as pd
import pytest

from transit_dependency import ACS_VARIABLES, clean_acs, compute_transit_dependency


def make_row(**values) -> dict:
    """
    Build one Census API row for a Queens tract with every ACS variable set to "0".

    Args:
        **values: ACS variable IDs to override, as numbers.

    Returns:
        dict: A row with text values, like the API returns.
    """

    row = {"state": "36", "county": "081", "tract": "157101"}
    row.update({var: "0" for var in ACS_VARIABLES})
    row.update({var: str(v) for var, v in values.items()})
    return row


def run(*rows) -> pd.DataFrame:
    return compute_transit_dependency(clean_acs(pd.DataFrame(list(rows))))


def test_builds_11_digit_tract_geoid():
    [tract_id] = run(make_row(B08201_001E=10))["tract_id"]
    assert tract_id == "36081157101"


def test_vehicle_deficit_counts_only_fewer_vehicles_than_members():
    result = run(make_row(
        B08201_001E=100,
        B08201_008E=10,  # 1-person, no vehicle: short
        B08201_009E=20,  # 1-person, 1 vehicle: not short
        B08201_015E=5,   # 2-person, 1 vehicle: short
        B08201_016E=15,  # 2-person, 2 vehicles: not short
        B08201_022E=3,   # 3-person, 2 vehicles: short
        B08201_029E=2,   # 4+-person, 3 vehicles: short
        B08201_030E=45,  # 4+-person, 4+ vehicles: not short (can't tell)
    )).iloc[0]

    assert result["vehicle_deficit_households"] == 20
    assert result["vehicle_deficit_share"] == pytest.approx(0.20)


def test_burden_shares_drop_not_computed_and_use_30_percent_line():
    result = run(make_row(
        B25070_001E=50, B25070_011E=10,   # 40 renters computed
        B25070_006E=20,                   # 25-29.9%: not burdened
        B25070_007E=8, B25070_010E=12,    # 30-34.9% and 50%+: burdened
        B25091_001E=60, B25091_012E=5, B25091_023E=5,  # 50 owners computed
        B25091_008E=10,                   # with mortgage, 30-34.9%
        B25091_022E=5,                    # without mortgage, 50%+
    )).iloc[0]

    assert result["renter_households"] == 40
    assert result["renter_burden_share"] == pytest.approx(20 / 40)
    assert result["owner_households"] == 50
    assert result["owner_burden_share"] == pytest.approx(15 / 50)
    assert result["burden_share"] == pytest.approx(35 / 90)


def test_zero_households_give_missing_shares_not_errors():
    result = run(make_row()).iloc[0]

    assert math.isnan(result["vehicle_deficit_share"])
    assert math.isnan(result["burden_share"])
    assert math.isnan(result["commute_transit_share"])


def test_census_negative_sentinels_become_missing():
    result = run(make_row(B08201_001E=10, B25092_001E=-666666666)).iloc[0]

    assert math.isnan(result["owner_median_burden_pct"])


def test_commute_shares_use_all_workers():
    result = run(make_row(B08301_001E=200, B08301_010E=80, B08301_011E=30, B08301_019E=10)).iloc[0]

    assert result["commute_transit_share"] == pytest.approx(0.40)
    assert result["commute_bus_share"] == pytest.approx(0.15)
    assert result["commute_walk_share"] == pytest.approx(0.05)
