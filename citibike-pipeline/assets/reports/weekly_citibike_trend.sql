/* @bruin
name: report.weekly_citibike_trend
type: bq.sql
connection: nyc_citibike
depends:
  - staging.citibike_trips_clean

materialization:
  type: table
  strategy: create+replace
  cluster_by:
    - day_of_week
    - member_casual
    - rideable_type

columns:
  - name: day_of_week
    type: INT64
    description: Day of week using BigQuery numbering where 1=Sunday and 7=Saturday.
    nullable: false
    checks:
      - name: not_null

  - name: weekday_name
    type: STRING
    description: English weekday label for visualization.
    nullable: false
    checks:
      - name: not_null

  - name: member_casual
    type: STRING
    description: Rider membership category.
    nullable: false
    checks:
      - name: not_null

  - name: rideable_type
    type: STRING
    description: Type of bike used for the trip.
    nullable: false
    checks:
      - name: not_null

  - name: ride_count
    type: INT64
    description: Total rides observed for that weekday bucket.
    nullable: false
    checks:
      - name: not_null

  - name: avg_ride_duration_minutes
    type: FLOAT64
    description: Average ride duration in minutes for that weekday bucket.
    nullable: false
    checks:
      - name: not_null

  - name: avg_ride_distance_km
    type: FLOAT64
    description: Average ride distance in kilometers for that weekday bucket.
    nullable: false
    checks:
      - name: not_null

  - name: avg_speed_kmh
    type: FLOAT64
    description: Average ride speed in kilometers per hour for that weekday bucket.
    nullable: true

  - name: unique_start_stations
    type: INT64
    description: Count of distinct start stations for that weekday bucket.
    nullable: false
    checks:
      - name: not_null

  - name: unique_end_stations
    type: INT64
    description: Count of distinct end stations for that weekday bucket.
    nullable: false
    checks:
      - name: not_null

@bruin */

SELECT
  day_of_week,
  CASE day_of_week
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS weekday_name,
  member_casual,
  rideable_type,
  COUNT(*) AS ride_count,
  AVG(ride_duration_minutes) AS avg_ride_duration_minutes,
  AVG(ride_distance_km) AS avg_ride_distance_km,
  AVG(avg_speed_kmh) AS avg_speed_kmh,
  COUNT(DISTINCT start_station_id) AS unique_start_stations,
  COUNT(DISTINCT end_station_id) AS unique_end_stations
FROM `staging.citibike_trips_clean`
GROUP BY
  day_of_week,
  weekday_name,
  member_casual,
  rideable_type;