"""Shared BigQuery data loading for the Citi Bike dashboard."""

import os, json
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

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
