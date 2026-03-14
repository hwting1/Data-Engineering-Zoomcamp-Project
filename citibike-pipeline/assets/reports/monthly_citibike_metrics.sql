/* @bruin
name: report.monthly_citibike_metrics
type: bq.sql
connection: nyc_citibike
depends:
  - staging.citibike_trips_clean

materialization:
  type: table
  strategy: create+replace
  partition_by: metric_month
  cluster_by:
    - member_casual
    - rideable_type

columns:
  - name: metric_month
    type: DATE
    description: First day of the month used as the monthly aggregation key.
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
    description: Total number of rides in the month.
    nullable: false
    checks:
      - name: not_null

  - name: avg_ride_duration_minutes
    type: FLOAT64
    description: Average ride duration in minutes for the month.
    nullable: false
    checks:
      - name: not_null

  - name: avg_ride_distance_km
    type: FLOAT64
    description: Average ride distance in kilometers for the month.
    nullable: false
    checks:
      - name: not_null

  - name: avg_speed_kmh
    type: FLOAT64
    description: Average ride speed in kilometers per hour for the month.
    nullable: true

@bruin */

SELECT
  started_month AS metric_month,
  member_casual,
  rideable_type,
  COUNT(*) AS ride_count,
  AVG(ride_duration_minutes) AS avg_ride_duration_minutes,
  AVG(ride_distance_km) AS avg_ride_distance_km,
  AVG(avg_speed_kmh) AS avg_speed_kmh
FROM `staging.citibike_trips_clean`
GROUP BY
  metric_month,
  member_casual,
  rideable_type;