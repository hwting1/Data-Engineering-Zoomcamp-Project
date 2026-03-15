"""Shared BigQuery data loading for the Citi Bike dashboard."""

import json
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
service_account_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

if service_account_json:
    info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    _client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
else:
    _client = bigquery.Client(project=PROJECT_ID)


def _query(sql: str) -> list[dict]:
    """Run a BigQuery SQL query and return rows as list of dicts."""
    try:
        return [dict(row) for row in _client.query(sql).result()]
    except Exception as exc:
        print(f"[dashboard] BigQuery query failed: {exc}")
        return []


def load_hourly_metrics() -> list[dict]:
    return _query(f"""
        SELECT
            metric_date, started_hour, member_casual, rideable_type,
            is_weekend, ride_count,
            avg_ride_duration_minutes, avg_ride_distance_km, avg_speed_kmh
        FROM `{PROJECT_ID}.report.hourly_usage_metrics`
        ORDER BY metric_date, started_hour
    """)


def load_weekly_trend() -> list[dict]:
    return _query(f"""
        SELECT
            day_of_week, weekday_name, member_casual, rideable_type,
            ride_count,
            avg_ride_duration_minutes, avg_ride_distance_km, avg_speed_kmh
        FROM `{PROJECT_ID}.report.weekly_citibike_trend`
        ORDER BY day_of_week
    """)


def load_monthly_metrics() -> list[dict]:
    return _query(f"""
        SELECT
            metric_month, member_casual, rideable_type,
            ride_count,
            avg_ride_duration_minutes, avg_ride_distance_km, avg_speed_kmh
        FROM `{PROJECT_ID}.report.monthly_citibike_metrics`
        ORDER BY metric_month
    """)


def load_top_stations(direction: str = "start",
                      limit: int = 10) -> list[dict]:
    """Top stations by avg monthly trip count. direction: 'start' or 'end'."""
    col_id = f"{direction}_station_id"
    col_name = f"{direction}_station_name"
    return _query(f"""
        SELECT
            station_name,
            ROUND(AVG(monthly_trips)) AS avg_trips
        FROM (
            SELECT
                {col_name} AS station_name,
                DATE_TRUNC(started_at, MONTH) AS month,
                COUNT(*) AS monthly_trips
            FROM `{PROJECT_ID}.staging.citibike_trips_clean`
            WHERE {col_id} IS NOT NULL
            GROUP BY station_name, month
        )
        GROUP BY station_name
        ORDER BY avg_trips DESC
        LIMIT {int(limit)}
    """)
