"""
Transit dependency by census tract, from ACS 5-year estimates.

Pure functions only: they take a DataFrame shaped like a Census API response
(one row per tract, ACS variable IDs as columns) and return derived columns.
The API pull and the BigQuery load live in scripts/transit_dependency.py.

Method (locked in CLAUDE.md):
- Vehicle deficit: households with fewer vehicles than household members (B08201).
- Housing cost burden: renters (B25070) and owners (B25091) paying 30% or more
  of income on housing. Medians from B25092.
- Commute mode (B08301) is kept as a validation check only.
- No composite score: components are stored separately.
"""

import numpy as np
import pandas as pd

ACS_YEAR = 2024  # ACS 2020-2024 5-year estimates; uses 2020 tract boundaries
QUEENS_STATE_FIPS = "36"
QUEENS_COUNTY_FIPS = "081"
BURDEN_THRESHOLD_PCT = 30

# B08201: household size by vehicles available. "Fewer vehicles than members."
# 4-or-more-person households with 4+ vehicles (_030) are counted as NOT short,
# because ACS can't tell a 5-person household with 4 vehicles apart.
VEHICLE_HOUSEHOLDS_TOTAL = "B08201_001E"
VEHICLE_DEFICIT_VARS = [
    "B08201_008E",  # 1-person, no vehicle
    "B08201_014E",  # 2-person, no vehicle
    "B08201_015E",  # 2-person, 1 vehicle
    "B08201_020E",  # 3-person, no vehicle
    "B08201_021E",  # 3-person, 1 vehicle
    "B08201_022E",  # 3-person, 2 vehicles
    "B08201_026E",  # 4+-person, no vehicle
    "B08201_027E",  # 4+-person, 1 vehicle
    "B08201_028E",  # 4+-person, 2 vehicles
    "B08201_029E",  # 4+-person, 3 vehicles
]

# B25070: gross rent as a percentage of household income (renters).
RENTER_TOTAL = "B25070_001E"
RENTER_NOT_COMPUTED = "B25070_011E"
RENTER_BURDENED_VARS = ["B25070_007E", "B25070_008E", "B25070_009E", "B25070_010E"]  # 30%+

# B25091: mortgage status by selected monthly owner costs as a percentage of income.
OWNER_TOTAL = "B25091_001E"
OWNER_NOT_COMPUTED_VARS = ["B25091_012E", "B25091_023E"]  # with / without mortgage
OWNER_BURDENED_VARS = [
    "B25091_008E", "B25091_009E", "B25091_010E", "B25091_011E",  # with mortgage, 30%+
    "B25091_019E", "B25091_020E", "B25091_021E", "B25091_022E",  # without mortgage, 30%+
]

# B25092: median selected monthly owner costs as a percentage of income.
OWNER_MEDIAN_VARS = {
    "B25092_001E": "owner_median_burden_pct",
    "B25092_002E": "owner_median_burden_pct_mortgage",
    "B25092_003E": "owner_median_burden_pct_no_mortgage",
}

# B08301: means of transportation to work (validation only).
COMMUTE_TOTAL = "B08301_001E"
COMMUTE_VARS = {
    "B08301_010E": "commute_transit_share",
    "B08301_011E": "commute_bus_share",
    "B08301_019E": "commute_walk_share",
}

HOUSEHOLDS_MOE = "B08201_001M"

ACS_VARIABLES = (
    [VEHICLE_HOUSEHOLDS_TOTAL, HOUSEHOLDS_MOE] + VEHICLE_DEFICIT_VARS
    + [RENTER_TOTAL, RENTER_NOT_COMPUTED] + RENTER_BURDENED_VARS
    + [OWNER_TOTAL] + OWNER_NOT_COMPUTED_VARS + OWNER_BURDENED_VARS
    + list(OWNER_MEDIAN_VARS) + [COMMUTE_TOTAL] + list(COMMUTE_VARS)
)


def _share(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """
    Divide two count columns, returning NaN where the denominator is 0 or missing.

    Args:
        numerator: Count of households or workers in the group.
        denominator: Count of households or workers in the universe.

    Returns:
        pd.Series: Share between 0 and 1, NaN where undefined.
    """

    return numerator / denominator.where(denominator > 0)


def clean_acs(acs: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Census API text values to numbers and build the 11-digit tract GEOID.

    The API returns every value as text and uses large negative sentinels
    (for example -666666666) for estimates that can't be computed; these become NaN.

    Args:
        acs: Census API rows with `state`, `county`, `tract` and ACS variable columns.

    Returns:
        pd.DataFrame: `tract_id` plus every ACS variable as float.
    """

    out = pd.DataFrame({"tract_id": acs["state"] + acs["county"] + acs["tract"]})
    for var in ACS_VARIABLES:
        values = pd.to_numeric(acs[var], errors="coerce")
        out[var] = values.where(values >= 0)
    return out


def compute_transit_dependency(acs: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-tract vehicle deficit, housing cost burden, and commute-mode
    validation columns from cleaned ACS data.

    Burden shares leave "not computed" households (no income or no cash rent)
    out of the denominator. `burden_share` pools renters and owners, so it is
    weighted by each tenure's share of computed households.

    Args:
        acs: Output of clean_acs().

    Returns:
        pd.DataFrame: One row per tract with counts, shares, and medians.
    """

    out = pd.DataFrame({"tract_id": acs["tract_id"]})

    out["households"] = acs[VEHICLE_HOUSEHOLDS_TOTAL]
    out["households_moe"] = acs[HOUSEHOLDS_MOE]
    out["vehicle_deficit_households"] = acs[VEHICLE_DEFICIT_VARS].sum(axis=1, min_count=1)
    out["vehicle_deficit_share"] = _share(out["vehicle_deficit_households"], out["households"])

    out["renter_households"] = acs[RENTER_TOTAL] - acs[RENTER_NOT_COMPUTED]
    out["renter_burdened"] = acs[RENTER_BURDENED_VARS].sum(axis=1, min_count=1)
    out["renter_burden_share"] = _share(out["renter_burdened"], out["renter_households"])

    out["owner_households"] = acs[OWNER_TOTAL] - acs[OWNER_NOT_COMPUTED_VARS].sum(axis=1)
    out["owner_burdened"] = acs[OWNER_BURDENED_VARS].sum(axis=1, min_count=1)
    out["owner_burden_share"] = _share(out["owner_burdened"], out["owner_households"])

    out["burden_share"] = _share(
        out["renter_burdened"].fillna(0) + out["owner_burdened"].fillna(0),
        out["renter_households"].fillna(0) + out["owner_households"].fillna(0),
    )

    for var, col in OWNER_MEDIAN_VARS.items():
        out[col] = acs[var]

    for var, col in COMMUTE_VARS.items():
        out[col] = _share(acs[var], acs[COMMUTE_TOTAL])

    return out.replace([np.inf, -np.inf], np.nan)
