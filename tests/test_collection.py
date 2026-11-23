"""
Unit tests for src/collection.py.

These tests run offline: they feed parse_vehicle_positions() hand-built SIRI
VehicleActivity dicts and never call the MTA API or BigQuery.
"""

from datetime import datetime, timezone

from collection import parse_vehicle_positions


ROUTE_ID = "MTA NYCT_Q27"


def make_vehicle(**journey_overrides) -> dict:
    """
    Build one SIRI VehicleActivity entry with every field the parser reads.

    Args:
        **journey_overrides: Keys to replace in MonitoredVehicleJourney.

    Returns:
        dict: A VehicleActivity entry shaped like the Bus Time response.
    """
    journey = {
        "VehicleRef": "MTA NYCT_8123",
        "VehicleLocation": {"Latitude": 40.7282, "Longitude": -73.7137},
        "DirectionRef": "0",
        "MonitoredCall": {"StopPointRef": "MTA_501234"},
        "Bearing": 92.5,
    }
    journey.update(journey_overrides)
    return {"MonitoredVehicleJourney": journey}


def test_maps_siri_fields_to_schema_columns():
    [record] = parse_vehicle_positions(ROUTE_ID, [make_vehicle()])

    assert record["route_id"] == ROUTE_ID
    assert record["vehicle_id"] == "MTA NYCT_8123"
    assert record["latitude"] == 40.7282
    assert record["longitude"] == -73.7137
    assert record["direction_ref"] == "0"
    assert record["next_stop_ref"] == "MTA_501234"
    assert record["bearing"] == 92.5


def test_returns_exactly_the_schema_columns():
    [record] = parse_vehicle_positions(ROUTE_ID, [make_vehicle()])

    assert set(record) == {
        "position_id", "route_id", "vehicle_id", "latitude", "longitude",
        "direction_ref", "next_stop_ref", "bearing", "occupancy",
        "collected_at",
    }


def test_occupancy_is_read_from_monitored_vehicle_journey():
    vehicle = make_vehicle(Occupancy="seatsAvailable")

    [record] = parse_vehicle_positions(ROUTE_ID, [vehicle])

    assert record["occupancy"] == "seatsAvailable"


def test_missing_occupancy_becomes_none():
    # Bus Time omits Occupancy entirely for buses that don't report it.
    [record] = parse_vehicle_positions(ROUTE_ID, [make_vehicle()])

    assert record["occupancy"] is None


def test_missing_optional_fields_become_none():
    vehicle = make_vehicle(VehicleLocation={})
    journey = vehicle["MonitoredVehicleJourney"]
    for key in ("DirectionRef", "MonitoredCall", "Bearing"):
        del journey[key]

    [record] = parse_vehicle_positions(ROUTE_ID, [vehicle])

    assert record["latitude"] is None
    assert record["longitude"] is None
    assert record["direction_ref"] is None
    assert record["next_stop_ref"] is None
    assert record["bearing"] is None


def test_empty_vehicle_list_returns_empty_list():
    assert parse_vehicle_positions(ROUTE_ID, []) == []


def test_one_record_per_vehicle_with_unique_position_ids():
    vehicles = [make_vehicle(VehicleRef=f"MTA NYCT_{n}") for n in range(3)]

    records = parse_vehicle_positions(ROUTE_ID, vehicles)

    assert [r["vehicle_id"] for r in records] == [f"MTA NYCT_{n}" for n in range(3)]
    assert len({r["position_id"] for r in records}) == 3


def test_collected_at_is_current_utc_iso_timestamp():
    before = datetime.now(timezone.utc)
    [record] = parse_vehicle_positions(ROUTE_ID, [make_vehicle()])
    after = datetime.now(timezone.utc)

    collected_at = datetime.fromisoformat(record["collected_at"])
    assert collected_at.utcoffset().total_seconds() == 0
    assert before <= collected_at <= after
