/* @bruin
name: staging.citibike_trips_clean
type: bq.sql
connection: nyc_citibike
depends:
  - raw.citibike_trips

materialization:
  type: table
  strategy: create+replace
  partition_by: started_date
  cluster_by:
    - member_casual
    - rideable_type
    - start_station_id

columns:
  - name: ride_id
    type: STRING
    description: Unique trip identifier from the source dataset.
    nullable: false
    checks:
      - name: not_null

  - name: rideable_type
    type: STRING
    description: Type of bike used for the trip.
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value: ['classic_bike', 'electric_bike', 'docked_bike']

  - name: started_at
    type: TIMESTAMP
    description: Trip start timestamp.
    nullable: false
    checks:
      - name: not_null

  - name: ended_at
    type: TIMESTAMP
    description: Trip end timestamp.
    nullable: false
    checks:
      - name: not_null

  - name: started_date
    type: DATE
    description: Date extracted from started_at, used for partitioning.
    nullable: false
    checks:
      - name: not_null

  - name: started_month
    type: DATE
    description: First day of the month extracted from started_at.
    nullable: false
    checks:
      - name: not_null

  - name: started_hour
    type: INT64
    description: Hour of day extracted from started_at.
    nullable: false
    checks:
      - name: not_null

  - name: day_of_week
    type: INT64
    description: Day of week extracted from started_at using BigQuery numbering.
    nullable: false
    checks:
      - name: not_null

  - name: is_weekend
    type: BOOLEAN
    description: Whether the trip started on a weekend.
    nullable: false
    checks:
      - name: not_null

  - name: start_station_name
    type: STRING
    description: Cleaned origin station name.
    nullable: false
    checks:
      - name: not_null

  - name: start_station_id
    type: INT64
    description: Numeric identifier of the origin station.
    nullable: false
    checks:
      - name: not_null

  - name: end_station_name
    type: STRING
    description: Cleaned destination station name.
    nullable: false
    checks:
      - name: not_null

  - name: end_station_id
    type: INT64
    description: Numeric identifier of the destination station.
    nullable: false
    checks:
      - name: not_null

  - name: start_lat
    type: FLOAT64
    description: Latitude of trip origin.
    nullable: false
    checks:
      - name: not_null

  - name: start_lng
    type: FLOAT64
    description: Longitude of trip origin.
    nullable: false
    checks:
      - name: not_null

  - name: end_lat
    type: FLOAT64
    description: Latitude of trip destination.
    nullable: false
    checks:
      - name: not_null

  - name: end_lng
    type: FLOAT64
    description: Longitude of trip destination.
    nullable: false
    checks:
      - name: not_null

  - name: member_casual
    type: STRING
    description: Rider membership category.
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value: ['member', 'casual']

  - name: ride_duration_minutes
    type: INT64
    description: Ride duration in minutes.
    nullable: false
    checks:
      - name: not_null

  - name: ride_distance_km
    type: FLOAT64
    description: Geodesic distance between start and end points in kilometers.
    nullable: false
    checks:
      - name: not_null

  - name: avg_speed_kmh
    type: FLOAT64
    description: Average ride speed in kilometers per hour.
    nullable: true

  - name: source_month
    type: STRING
    description: Source batch month carried from the raw layer.
    nullable: false
    checks:
      - name: not_null

  - name: loaded_at
    type: TIMESTAMP
    description: Timestamp when the row was loaded into the raw layer.
    nullable: false
    checks:
      - name: not_null

@bruin */

WITH standardized AS (
  SELECT
    ride_id,
    NULLIF(TRIM(rideable_type), '') AS rideable_type,
    started_at,
    ended_at,
    NULLIF(TRIM(start_station_name), '') AS start_station_name,
    start_station_id,
    NULLIF(TRIM(end_station_name), '') AS end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    NULLIF(TRIM(member_casual), '') AS member_casual,
    source_month,
    loaded_at
  FROM `raw.citibike_trips`
),

filtered AS (
  SELECT
    *
  FROM standardized
  WHERE
    ride_id <> 'nan'
    AND ended_at >= started_at
    AND start_lat IS NOT NULL
    AND start_lng IS NOT NULL
    AND end_lat IS NOT NULL
    AND end_lng IS NOT NULL
    AND ABS(start_lat) <= 90
    AND ABS(end_lat) <= 90
    AND ABS(start_lng) <= 180
    AND ABS(end_lng) <= 180
)

SELECT
  ride_id,
  rideable_type,
  started_at,
  ended_at,

  DATE(started_at) AS started_date,
  DATE_TRUNC(DATE(started_at), MONTH) AS started_month,
  EXTRACT(HOUR FROM started_at) AS started_hour,
  EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
  EXTRACT(DAYOFWEEK FROM started_at) IN (1, 7) AS is_weekend,

  start_station_name,
  start_station_id,
  end_station_name,
  end_station_id,

  start_lat,
  start_lng,
  end_lat,
  end_lng,

  member_casual,

  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_duration_minutes,

  ST_DISTANCE(
    ST_GEOGPOINT(start_lng, start_lat),
    ST_GEOGPOINT(end_lng, end_lat)
  ) / 1000.0 AS ride_distance_km,

  CASE
    WHEN TIMESTAMP_DIFF(ended_at, started_at, MINUTE) > 0 THEN
      (
        ST_DISTANCE(
          ST_GEOGPOINT(start_lng, start_lat),
          ST_GEOGPOINT(end_lng, end_lat)
        ) / 1000.0
      ) / (TIMESTAMP_DIFF(ended_at, started_at, MINUTE) / 60.0)
    ELSE NULL
  END AS avg_speed_kmh,

  source_month,
  loaded_at
FROM filtered;