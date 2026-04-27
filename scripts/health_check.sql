-- Query 1: Count the number of rows gathered in each collection window.
SELECT COUNT(*),
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(collected_at, HOUR),
    INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM collected_at) / 15) * 15 AS INT64) MINUTE) AS collection_bucket
FROM connecting-queens-equity.connecting_queens_equity.vehicle_positions
GROUP BY collection_bucket
ORDER BY collection_bucket

-- Query 2: Check for gaps in CloudRun running collection.py.
-- One gap identified: No collection on 2026-04-23 at 19:00, as Cloud Scheduler was stabilizing.
WITH collection_partition AS (
    SELECT COUNT(*) AS row_count,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(collected_at, HOUR), INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM collected_at) / 15) * 15 AS INT64) MINUTE) AS collection_bucket
    FROM connecting_queens_equity.vehicle_positions
    GROUP BY collection_bucket
),
previous_partition AS (
    SELECT row_count,
    collection_bucket,
    LAG(collection_bucket) OVER (ORDER BY collection_bucket) AS previous_bucket
    FROM collection_partition
) SELECT *
FROM previous_partition
WHERE TIMESTAMP_DIFF(collection_bucket, previous_bucket, MINUTE) > 15
ORDER BY collection_bucket

-- Query 3: Count null rates for all columns.
-- All columns return clean, except for `next_stop_ref`, which has a 2.86% null rate.
-- Three possible explanations: end of route, off-route deviation, pre-departure
SELECT
    COUNTIF(position_id IS NULL) / COUNT(*) * 100 AS position_id_null_rate,
    COUNTIF(route_id IS NULL) / COUNT(*) * 100 AS route_id_null_rate,
    COUNTIF(vehicle_id IS NULL) / COUNT(*) * 100 AS vehicle_id_null_rate,
    COUNTIF(latitude IS NULL) / COUNT(*) * 100 AS latitude_null_rate,
    COUNTIF(longitude IS NULL) / COUNT(*) * 100 AS longitude_null_rate,
    COUNTIF(direction_ref IS NULL) / COUNT(*) * 100 AS direction_ref_null_rate,
    COUNTIF(next_stop_ref IS NULL) / COUNT(*) * 100 AS next_stop_ref_null_rate,
    COUNTIF(bearing IS NULL) / COUNT(*) * 100 AS bearing_null_rate,
    COUNTIF(collected_at IS NULL) / COUNT(*) * 100 AS collected_at_null_rate
FROM connecting-queens-equity.connecting_queens_equity.vehicle_positions

-- Query 4: Verify route coverage.
SELECT COUNT(DISTINCT route_id) FROM connecting-queens-equity.connecting_queens_equity.vehicle_positions
-- If Query 4 returns something other than 122, then we'd have to perform a LEFT JOIN on routes and vehicle_positions to investigate further.

-- Query 5: Check for duplicates vehicles collected at the same time.
SELECT vehicle_id
FROM connecting-queens-equity.connecting_queens_equity.vehicle_positions
GROUP BY vehicle_id, collected_at
HAVING COUNT(*) > 1