"""
Pull ACS transit dependency data for Queens tracts and load `census_tracts`
and `tract_nta` in BigQuery.

One-time script. Steps:
1. Pull ACS 2020-2024 5-year variables for every Queens tract from the Census
   API and save them once to data/raw/census_acs5_queens_2024.csv. If the file
   already exists it is read, never re-pulled or modified.
2. Compute per-tract components with src/transit_dependency.py and attach
   TIGER/Line 2022 tract polygons (WGS84).
3. Build the tract-to-NTA junction from the DCP equivalency file with
   src/tract_nta.py.
4. Load both tables with WRITE_TRUNCATE, only when --target is given.
   Without --target, write both tables' attributes (no geometry) to
   data/derived/ as CSVs instead.

Usage:
    python scripts/transit_dependency.py                    # local only, no BigQuery writes
    python scripts/transit_dependency.py --target dryrun    # *_dryrun scratch tables
    python scripts/transit_dependency.py --target prod      # real tables
"""

import argparse
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from transit_dependency import (  # noqa: E402
    ACS_VARIABLES, ACS_YEAR, QUEENS_COUNTY_FIPS, QUEENS_STATE_FIPS,
    clean_acs, compute_transit_dependency,
)
from tract_nta import tract_nta_from_equivalency  # noqa: E402

ACS_RAW = ROOT / "data" / "raw" / f"census_acs5_queens_{ACS_YEAR}.csv"
TIGER_TRACTS = ROOT / "data" / "raw" / "tiger_census_tracts_ny_2022.zip"
EQUIVALENCY = ROOT / "data" / "raw" / "dcp_tract_nta_equivalency_2024.csv"
SCHEMAS = ROOT / "schemas"
DERIVED = ROOT / "data" / "derived"
CENSUS_TRACTS_CSV = DERIVED / f"census_tracts_queens_{ACS_YEAR}.csv"
TRACT_NTA_CSV = DERIVED / f"tract_nta_queens_{ACS_YEAR}.csv"

PROJECT = "connecting-queens-equity"
DATASET = "connecting_queens_equity"
ACS_URL = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"


def pull_acs() -> pd.DataFrame:
    """
    Read the saved ACS pull, or pull it from the Census API and save it once.

    Returns:
        pd.DataFrame: One row per Queens tract; every column as text.
    """

    if ACS_RAW.exists():
        print(f"Reading saved ACS pull: {ACS_RAW.name}")
        return pd.read_csv(ACS_RAW, dtype=str)

    load_dotenv(ROOT / ".env")
    params = {
        "get": ",".join(["NAME"] + ACS_VARIABLES),
        "for": "tract:*",
        "in": f"state:{QUEENS_STATE_FIPS} county:{QUEENS_COUNTY_FIPS}",
        "key": os.environ["CENSUS_API_KEY"],
    }
    response = requests.get(ACS_URL, params=params, timeout=60)
    if response.status_code != 200:
        # Don't echo response.url: it contains the API key.
        raise RuntimeError(f"Census API returned {response.status_code}: {response.text[:300]}")

    header, *rows = response.json()
    acs = pd.DataFrame(rows, columns=header)
    acs.to_csv(ACS_RAW, index=False)
    print(f"Pulled {len(acs)} tracts from the Census API; saved {ACS_RAW.name}")
    return acs


def build_census_tracts(acs: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Compute transit dependency components and attach tract polygons.

    Args:
        acs: Raw ACS rows from pull_acs().

    Returns:
        gpd.GeoDataFrame: Columns in schemas/census_tracts.json order, geometry in EPSG:4326.
    """

    components = compute_transit_dependency(clean_acs(acs))

    tracts = gpd.read_file(f"zip://{TIGER_TRACTS}")
    tracts = tracts[tracts["COUNTYFP"] == QUEENS_COUNTY_FIPS][["GEOID", "geometry"]]
    tracts = tracts.rename(columns={"GEOID": "tract_id"}).to_crs("EPSG:4326")

    missing = set(components["tract_id"]) ^ set(tracts["tract_id"])
    if missing:
        raise ValueError(f"ACS and TIGER tracts don't match: {sorted(missing)}")

    merged = tracts.merge(components, on="tract_id", how="inner")
    columns = [field["name"] for field in _schema("census_tracts")]
    return merged[columns]


def _schema(table: str) -> list[dict]:
    """Read a table's BigQuery schema JSON from schemas/."""

    return json.loads((SCHEMAS / f"{table}.json").read_text())


def load(df: pd.DataFrame, table: str, suffix: str) -> None:
    """
    Replace a BigQuery table's contents with df, using the schema in schemas/.

    Args:
        df: Rows to load; a GEOGRAPHY column is sent as WKT text.
        table: Table name, such as "census_tracts".
        suffix: "" for the real table, "_dryrun" for a scratch copy.
    """

    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{DATASET}.{table}{suffix}"
    schema = [bigquery.SchemaField.from_api_repr(field) for field in _schema(table)]

    out = pd.DataFrame(df)
    if "geometry" in out:
        out["geometry"] = gpd.GeoSeries(df["geometry"]).to_wkt()

    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(out, table_id, job_config=job_config).result()
    print(f"Loaded {client.get_table(table_id).num_rows} rows into {table_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=["dryrun", "prod"], help="Load to BigQuery; omit for a local-only run")
    args = parser.parse_args()

    census_tracts = build_census_tracts(pull_acs())
    tract_nta = tract_nta_from_equivalency(pd.read_csv(EQUIVALENCY, dtype=str), QUEENS_COUNTY_FIPS)

    print(f"census_tracts: {len(census_tracts)} rows; tract_nta: {len(tract_nta)} rows, "
          f"{tract_nta['nta_code'].nunique()} NTAs")
    print(census_tracts.drop(columns="geometry").describe().T[["count", "mean", "min", "max"]].round(3).to_string())

    if args.target:
        suffix = "_dryrun" if args.target == "dryrun" else ""
        load(census_tracts, "census_tracts", suffix)
        load(tract_nta, "tract_nta", suffix)
    else:
        # Attributes only: the map build joins TIGER geometry itself.
        DERIVED.mkdir(parents=True, exist_ok=True)
        census_tracts.drop(columns="geometry").to_csv(CENSUS_TRACTS_CSV, index=False)
        tract_nta.to_csv(TRACT_NTA_CSV, index=False)
        print("Local run only; nothing written to BigQuery.")
        print(f"Wrote {CENSUS_TRACTS_CSV}")
        print(f"Wrote {TRACT_NTA_CSV}")


if __name__ == "__main__":
    main()
