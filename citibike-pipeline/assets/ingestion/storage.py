"""@bruin

name: ingestion.storage
type: python
image: python:3.12
connection: nyc_citibike

@bruin"""

import os
import json
import glob
import zipfile
import tempfile
from datetime import datetime, timedelta

import polars as pl
import requests
from google.oauth2 import service_account
from google.cloud import storage


BUCKET_NAME = "nyc-citibike-bucket"
BASE_URL = "https://s3.amazonaws.com/tripdata"
CSV_SCHEMA = {
    "ride_id": pl.Utf8,
    "rideable_type": pl.Utf8,
    "started_at": pl.Utf8,
    "ended_at": pl.Utf8,
    "start_station_name": pl.Utf8,
    "start_station_id": pl.Utf8,
    "end_station_name": pl.Utf8,
    "end_station_id": pl.Utf8,
    "start_lat": pl.Float64,
    "start_lng": pl.Float64,
    "end_lat": pl.Float64,
    "end_lng": pl.Float64,
    "member_casual": pl.Utf8,
}


def _prev_month_first(dt):
    """Return the 1st day of the month before *dt*."""
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def _next_month_first(dt):
    """Return the 1st day of the month after *dt*."""
    return (dt.replace(day=28) + timedelta(days=4)).replace(day=1)


def get_months_to_download():
    """Determine which YYYYMM values to download based on BRUIN date window."""
    start = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

    current = _prev_month_first(start)
    last = _prev_month_first(end)

    months = []
    while current < last:
        months.append(current.strftime("%Y%m"))
        current = _next_month_first(current)

    return months


def download_zip(year_month, dest_path):
    """Download the citibike tripdata zip for a given YYYYMM to *dest_path*."""
    urls = [
        f"{BASE_URL}/{year_month}-citibike-tripdata.csv.zip",
        f"{BASE_URL}/{year_month}-citibike-tripdata.zip",
    ]

    for url in urls:
        print(f"Trying {url} ...")
        with requests.get(url, stream=True, timeout=600) as resp:
            if resp.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                print(f"Downloaded {url}")
                return

    raise RuntimeError(
        f"Failed to download data for {year_month} from any known URL"
    )


def extract_zip(zip_path, extract_dir):
    """Extract *zip_path* into *extract_dir*, skipping unsafe paths."""
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            member_path = os.path.realpath(os.path.join(extract_dir, member))
            if not member_path.startswith(os.path.realpath(extract_dir)):
                print(f"Skipping unsafe zip entry: {member}")
                continue
            zf.extract(member, extract_dir)


def get_csv_files(extract_dir):
    """Return all CSV files under *extract_dir* recursively."""
    csv_files = sorted(
        glob.glob(os.path.join(extract_dir, "**", "*.csv"), recursive=True)
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {extract_dir}")

    print(f"Found {len(csv_files)} CSV file(s)")
    for path in csv_files:
        print(f" - {path}")

    return csv_files


def build_lazyframe(csv_files):
    """Build a lazy Polars pipeline for all CSV files."""
    lazy_frames = []

    for csv_file in csv_files:
        lf = (
            pl.scan_csv(
                csv_file,
                schema=CSV_SCHEMA,
                low_memory=True,
                ignore_errors=False,
            )
            .select([
                pl.col("ride_id"),
                pl.col("rideable_type"),
                pl.col("started_at").str.to_datetime(strict=False),
                pl.col("ended_at").str.to_datetime(strict=False),
                pl.col("start_station_name"),
                pl.col("start_station_id"),
                pl.col("end_station_name"),
                pl.col("end_station_id"),
                pl.col("start_lat"),
                pl.col("start_lng"),
                pl.col("end_lat"),
                pl.col("end_lng"),
                pl.col("member_casual"),
            ])
        )
        lazy_frames.append(lf)

    if len(lazy_frames) == 1:
        return lazy_frames[0]

    return pl.concat(lazy_frames, how="vertical")


def write_parquet_from_csvs(csv_files, parquet_path):
    """Stream all CSV files into a single parquet file."""
    lf = build_lazyframe(csv_files)

    # 使用 sink_parquet，避免 collect 成完整 DataFrame 後再寫檔
    lf.sink_parquet(
        parquet_path,
        compression="zstd",
        maintain_order=True,
    )

    size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"Parquet written: {size_mb:.1f} MB -> {parquet_path}")


def upload_to_gcs(gcs_client, bucket_name, local_path, blob_name):
    """Upload a local file to Google Cloud Storage."""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")


def get_gcs_client():
    conn = json.loads(os.environ["nyc_citibike"])
    sa_info = json.loads(conn["service_account_json"])
    project_id = conn.get("project_id") or sa_info.get("project_id")

    creds = service_account.Credentials.from_service_account_info(sa_info)
    return storage.Client(project=project_id, credentials=creds)


# ── Main execution ──────────────────────────────────────────────────────────

months = get_months_to_download()
print(f"Months to process: {months}")

gcs_client = get_gcs_client()

for ym in months:
    with tempfile.TemporaryDirectory() as tmp_dir:
        print(f"\n{'=' * 50}")
        print(f"Processing {ym}")
        print(f"{'=' * 50}")

        zip_path = os.path.join(tmp_dir, f"{ym}.zip")
        extract_dir = os.path.join(tmp_dir, "extracted")
        parquet_path = os.path.join(tmp_dir, f"{ym}-citibike-tripdata.parquet")

        os.makedirs(extract_dir, exist_ok=True)

        # 1. Download zip
        download_zip(ym, zip_path)

        # 2. Extract
        extract_zip(zip_path, extract_dir)

        # 3. Find CSV files
        csv_files = get_csv_files(extract_dir)

        # 4. Stream directly to parquet
        write_parquet_from_csvs(csv_files, parquet_path)

        # 5. Upload parquet to GCS
        blob_name = f"{ym}-citibike-tripdata.parquet"
        upload_to_gcs(gcs_client, BUCKET_NAME, parquet_path, blob_name)

        print(f"Completed {ym} — temp files cleaned up")

print("\nAll months processed successfully.")