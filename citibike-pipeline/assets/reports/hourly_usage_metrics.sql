/* @bruin
name: report.hourly_usage_metrics
type: bq.sql
connection: nyc_citibike
depends:
  - staging.citibike_trips_clean

materialization:
  type: table
  strategy: create+replace
  partition_by: metric_date
  cluster_by:
    - started_hour
    - member_casual
    - rideable_type

columns:
  - name: metric_date
    type: DATE
    description: Daily aggregation date.
    nullable: false
    checks:
      - name: not_null

  - name: started_hour
    type: INT64
    description: Hour of day extracted from trip start time.
    nullable: false
    checks:
      - name: not_null

  - name: ride_count
    type: INT64
    description: Number of rides in the aggregation bucket.
    nullable: false
    checks:
      - name: not_null

  - name: unique_start_stations
    type: INT64
    description: Count of distinct start stations in the aggregation bucket.
    nullable: false
    checks:
      - name: not_null

  - name: unique_end_stations
    type: INT64
    description: Count of distinct end stations in the aggregation bucket.
    nullable: false
    checks:
      - name: not_null

@bruin */

SELECT
  started_date AS metric_date,
  started_hour,
  member_casual,
  rideable_type,
  is_weekend,
  COUNT(*) AS ride_count,
  COUNT(DISTINCT start_station_id) AS unique_start_stations,
  COUNT(DISTINCT end_station_id) AS unique_end_stations,
  AVG(ride_duration_minutes) AS avg_ride_duration_minutes,
  AVG(ride_distance_km) AS avg_ride_distance_km,
  AVG(avg_speed_kmh) AS avg_speed_kmh
FROM `staging.citibike_trips_clean`
GROUP BY
  metric_date,
  started_hour,
  member_casual,
  rideable_type,
  is_weekend;