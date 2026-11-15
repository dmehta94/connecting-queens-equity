"""
Compute weekday-midday and Sunday service snapshots for Queens bus stops.

One-time script. Reads the saved MTA bus GTFS zips (NYCT Queens and MTA Bus
Company) directly from data/raw/, resolves which service_ids run on a chosen
weekday date and a chosen Sunday date (calendar.txt plus calendar_dates.txt
exceptions), and counts scheduled stop times per stop inside the midday
window. Covers the 122 Queens routes (routes starting with Q, excluding
the 8 shuttles). A stop has service in a snapshot if it has at least one
scheduled stop time in the window on that date.

A stop time counts unless pickup_type is 1 (no pickup). The last stop of a trip
counts too, so terminals where buses arrive and lay over are treated as served.

Departures are also split by route class. Express routes are the QM routes
(route_id starts with "QM"; these are exactly the routes with MTA's express
color 00933C). Every other route, including Select Bus Service (Q44+, Q52+,
Q53+, Q70+) and Limited and Rush routes, counts as local. Express service
stays in the project for service quality, but proximity uses local service
only. The "any service" columns count both classes.

Writes a local CSV only; nothing is written to BigQuery.

Usage:
    python scripts/service_snapshots.py
    python scripts/service_snapshots.py --weekday 2026-11-18 --sunday 2026-11-15
"""

import argparse
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd

# Constants
ROOT = Path(__file__).parent.parent
FEEDS = {
    "q": ROOT / "data" / "raw" / "mta_gtfs_q_2026.zip",
    "busco": ROOT / "data" / "raw" / "mta_gtfs_busco_2026.zip",
}
OUTPUT = ROOT / "data" / "derived" / "service_snapshots_2026.csv"
SHUTTLES = ["Q92", "Q93", "Q96", "Q97", "Q107", "Q108", "Q109", "Q121"]
DAY_COLUMNS = ["monday", "tuesday", "wednesday", "thursday", "friday",
               "saturday", "sunday"]
WINDOW_START_HOUR = 10
WINDOW_END_HOUR = 14
EXPRESS_PREFIX = "QM"


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


def is_express(route_ids: pd.Series) -> pd.Series:
    """
    Flag express routes by route_id.

    Express routes are the QM routes. SBS (for example Q44+), Limited, and Rush
    routes are local.

    Args:
        route_ids: Series of GTFS route_id strings.

    Returns:
        pd.Series: True where the route is express.
    """

    return route_ids.str.startswith(EXPRESS_PREFIX)


def active_service_ids(
        calendar: pd.DataFrame,
        calendar_dates: pd.DataFrame,
        service_date: date
) -> set:
    """
    Resolve the service_ids that run on a given date.

    Starts from calendar.txt rows whose weekday flag is 1 and whose date range
    contains the date, then applies calendar_dates.txt: exception_type 1 adds a
    service_id and exception_type 2 removes it.

    Args:
        calendar: calendar.txt as a DataFrame of strings.
        calendar_dates: calendar_dates.txt as a DataFrame of strings.
        service_date: The calendar date to resolve.

    Returns:
        set: service_ids in effect on that date.
    """

    ymd = service_date.strftime("%Y%m%d")
    weekday_column = DAY_COLUMNS[service_date.weekday()]
    in_range = (calendar["start_date"] <= ymd) & (calendar["end_date"] >= ymd)
    runs_today = calendar[weekday_column] == "1"
    active = set(calendar.loc[in_range & runs_today, "service_id"])

    exceptions = calendar_dates[calendar_dates["date"] == ymd]
    added = set(exceptions.loc[exceptions["exception_type"] == "1", "service_id"])
    removed = set(exceptions.loc[exceptions["exception_type"] == "2", "service_id"])

    return (active | added) - removed


def _seconds(clock: pd.Series) -> pd.Series:
    """
    Convert GTFS HH:MM:SS strings to seconds after midnight.

    GTFS hours can exceed 23 for trips running past midnight, so the strings are
    parsed by hand instead of as datetimes.

    Args:
        clock: Series of "HH:MM:SS" strings.

    Returns:
        pd.Series: Integer seconds after midnight.
    """

    parts = clock.str.strip().str.split(":", expand=True).astype(int)
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def feed_departures(zip_path: Path, dates: dict) -> pd.DataFrame:
    """
    Count scheduled midday stop times per stop for each snapshot in one feed.

    Args:
        zip_path: Path to a GTFS zip.
        dates: Snapshot name to date, such as {"weekday": date(...), "sunday": date(...)}.

    Returns:
        pd.DataFrame: One row per stop_id served by the 122 Queens routes in this
        feed, with departure-count columns per snapshot (0 when none):
        {name}_departures (all routes), {name}_local_departures, and
        {name}_express_departures.
    """

    with zipfile.ZipFile(zip_path) as zf:
        routes = _read(zf, "routes.txt", usecols=["route_id"])
        trips = _read(zf, "trips.txt", usecols=["route_id", "trip_id", "service_id"])
        calendar = _read(zf, "calendar.txt")
        calendar_dates = _read(zf, "calendar_dates.txt")
        stop_times = _read(
            zf, "stop_times.txt",
            usecols=["trip_id", "departure_time", "stop_id", "pickup_type"]
        )

    queens = routes[routes["route_id"].str.match(r"^Q") & ~routes["route_id"].isin(SHUTTLES)]
    trips = trips[trips["route_id"].isin(queens["route_id"])]
    stop_times = stop_times[stop_times["trip_id"].isin(trips["trip_id"])].copy()

    stop_times["boardable"] = stop_times["pickup_type"].fillna("0") != "1"
    stop_times["departure_seconds"] = _seconds(stop_times["departure_time"])
    stop_times = stop_times.merge(trips[["trip_id", "service_id", "route_id"]], on="trip_id")
    stop_times["express"] = is_express(stop_times["route_id"])

    window_start = WINDOW_START_HOUR * 3600
    window_end = WINDOW_END_HOUR * 3600
    in_window = (
        stop_times["boardable"]
        & (stop_times["departure_seconds"] >= window_start)
        & (stop_times["departure_seconds"] < window_end)
    )

    result = pd.DataFrame({"stop_id": stop_times["stop_id"].unique()})
    for name, service_date in dates.items():
        services = active_service_ids(calendar, calendar_dates, service_date)
        scheduled = in_window & stop_times["service_id"].isin(services)
        classes = {
            f"{name}_departures": scheduled,
            f"{name}_local_departures": scheduled & ~stop_times["express"],
            f"{name}_express_departures": scheduled & stop_times["express"],
        }
        for column, mask in classes.items():
            counts = stop_times[mask].groupby("stop_id").size().rename(column)
            result = result.merge(counts, on="stop_id", how="left")
            result[column] = result[column].fillna(0).astype(int)
        print(f"  {name} {service_date}: {len(services)} service_ids active")

    return result


def main() -> None:
    """
    Build the snapshot CSV from both bus feeds and print summary counts.
    """

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--weekday", default="2026-11-18", type=date.fromisoformat)
    parser.add_argument("--sunday", default="2026-11-15", type=date.fromisoformat)
    args = parser.parse_args()

    if args.weekday.weekday() > 4:
        raise ValueError(f"--weekday {args.weekday} is not Monday through Friday")
    if args.sunday.weekday() != 6:
        raise ValueError(f"--sunday {args.sunday} is not a Sunday")

    dates = {"weekday": args.weekday, "sunday": args.sunday}
    frames = []
    for name, path in FEEDS.items():
        print(f"{name}: {path.name}")
        frames.append(feed_departures(path, dates))

    combined = pd.concat(frames, ignore_index=True)
    # A stop id could appear in both feeds; sum its departures.
    snapshots = combined.groupby("stop_id", as_index=False).sum()
    for name in dates:
        for prefix in [name, f"{name}_local", f"{name}_express"]:
            snapshots[f"{prefix}_service"] = snapshots[f"{prefix}_departures"] > 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    snapshots.to_csv(OUTPUT, index=False)

    print(f"\nWrote {len(snapshots)} stops to {OUTPUT}")
    print(f"  weekday midday service: {int(snapshots['weekday_service'].sum())}")
    print(f"  sunday midday service:  {int(snapshots['sunday_service'].sum())}")
    print(f"  both: {int((snapshots['weekday_service'] & snapshots['sunday_service']).sum())}")
    print(f"  neither: {int((~snapshots['weekday_service'] & ~snapshots['sunday_service']).sum())}")
    for name in dates:
        local = snapshots[f"{name}_local_service"]
        express = snapshots[f"{name}_express_service"]
        print(f"\n  {name} local service:   {int(local.sum())}")
        print(f"  {name} express service: {int(express.sum())}")
        print(f"  {name} express only:    {int((express & ~local).sum())}")
        print(f"  {name} local and express: {int((express & local).sum())}")


if __name__ == "__main__":
    main()
