"""@bruin

name: ingestion.raw
type: python
image: python:3.12
connection: google_cloud_platform

depends:
  - ingestion.storage

@bruin"""

import os
from datetime import datetime, timedelta
from google.cloud import bigquery

PROJECT_ID = "de-zoomcamp-project-490007"
BUCKET_NAME = "nyc-citibike-bucket"
DATASET = "raw"
TABLE = "nyc-citibike-raw"


def _prev_month_first(dt):
    """Return the 1st day of the month before *dt*."""
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def _next_month_first(dt):
    """Return the 1st day of the month after *dt*."""
    return (dt.replace(day=28) + timedelta(days=4)).replace(day=1)


def get_months_to_load():
    """Return YYYYMM strings for the months within the BRUIN date window."""
    start = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

    current = _prev_month_first(start)
    last = _prev_month_first(end)

    months = []
    while current < last:
        months.append(current.strftime("%Y%m"))
        current = _next_month_first(current)

    return months


# ── Main execution ──────────────────────────────────────────────────────────

table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
months = get_months_to_load()
print(f"Months to load into BigQuery: {months}")

bq_client = bigquery.Client(project=PROJECT_ID)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

for ym in months:
    uri = f"gs://{BUCKET_NAME}/{ym}-citibike-tripdata.parquet"
    print(f"\nLoading {uri} -> {table_id}")

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # wait for completion

    print(f"Loaded {load_job.output_rows} rows from {ym}")

print("\nAll months loaded into BigQuery successfully.")
