/* @bruin
name: raw.citibike_trips
type: bq.sql
connection: nyc_citibike
depends:
  - ingestion.storage
@bruin */

DECLARE backfill_start DATE;
DECLARE backfill_end DATE;

-- Align with the Python asset:
-- Python processes the months derived from [BRUIN_START_DATE, BRUIN_END_DATE),
-- shifted back by one month.
SET backfill_start = DATE_TRUNC(DATE_SUB(DATE('{{ start_date }}'), INTERVAL 1 MONTH), MONTH);
SET backfill_end = DATE_TRUNC(DATE('{{ end_date }}'), MONTH);

-- Final raw table: long-term storage in BigQuery
CREATE TABLE IF NOT EXISTS `raw.citibike_trips` (
  ride_id STRING,
  rideable_type STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  start_station_name STRING,
  start_station_id STRING,
  end_station_name STRING,
  end_station_id STRING,
  start_lat FLOAT64,
  start_lng FLOAT64,
  end_lat FLOAT64,
  end_lng FLOAT64,
  member_casual STRING,
  source_month STRING,
  loaded_at TIMESTAMP
)
PARTITION BY DATE(started_at)
CLUSTER BY member_casual, rideable_type;

-- Helper load table: receives parquet exactly as loaded from GCS
CREATE TABLE IF NOT EXISTS `raw._citibike_trips_load` (
  ride_id STRING,
  rideable_type STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  start_station_name STRING,
  start_station_id STRING,
  end_station_name STRING,
  end_station_id STRING,
  start_lat FLOAT64,
  start_lng FLOAT64,
  end_lat FLOAT64,
  end_lng FLOAT64,
  member_casual STRING
);

-- Reload helper table from all parquet files currently in the bucket.
-- Flat bucket structure is preserved.
LOAD DATA OVERWRITE `raw._citibike_trips_load`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://nyc-citibike-bucket/*-citibike-tripdata.parquet']
);

-- Idempotent overwrite for the current run window in the final raw table
DELETE FROM `raw.citibike_trips`
WHERE DATE(started_at) >= backfill_start
  AND DATE(started_at) < backfill_end;

INSERT INTO `raw.citibike_trips` (
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  start_station_name,
  start_station_id,
  end_station_name,
  end_station_id,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  member_casual,
  source_month,
  loaded_at
)
SELECT
  CAST(ride_id AS STRING) AS ride_id,
  CAST(rideable_type AS STRING) AS rideable_type,
  started_at,
  ended_at,
  CAST(start_station_name AS STRING) AS start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  CAST(end_station_name AS STRING) AS end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  CAST(start_lat AS FLOAT64) AS start_lat,
  CAST(start_lng AS FLOAT64) AS start_lng,
  CAST(end_lat AS FLOAT64) AS end_lat,
  CAST(end_lng AS FLOAT64) AS end_lng,
  CAST(member_casual AS STRING) AS member_casual,
  FORMAT_DATE('%Y%m', DATE(started_at)) AS source_month,
  CURRENT_TIMESTAMP() AS loaded_at
FROM `raw._citibike_trips_load`
WHERE DATE(started_at) >= backfill_start
  AND DATE(started_at) < backfill_end;