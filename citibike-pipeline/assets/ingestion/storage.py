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
from google.cloud import storage
from google.oauth2 import service_account


# def get_memory_limit():
#     path = "/sys/fs/cgroup/memory.max"

#     if os.path.exists(path):
#         with open(path) as f:
#             limit = f.read().strip()
#             if limit != "max":
#                 limit = int(limit)
#                 print(f"Memory limit: {limit / (1024**3):.2f} GB")
#             else:
#                 print("Memory limit: unlimited")

# get_memory_limit()

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

OUTPUT_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]


from datetime import datetime


def get_months_to_download() -> list[str]:
    start = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")
    print("start:", start, "end:", end)
    start_year = start.year
    start_month = start.month - 1
    if start_month == 0:
        start_year -= 1
        start_month = 12

    end_year = end.year
    end_month = end.month

    months = []
    y, m = start_year, start_month

    while (y, m) < (end_year, end_month):
        months.append(f"{y}{m:02d}")
        m += 1
        if m == 13:
            y += 1
            m = 1

    return months


def download_zip(year_month: str, dest_path: str) -> None:
    url = f"{BASE_URL}/{year_month}-citibike-tripdata.zip"
    print(f"Downloading {url}")

    with requests.get(url, stream=True, timeout=600) as resp:
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to download {url} (status={resp.status_code})"
            )

        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"Downloaded {size_mb:.1f} MB -> {dest_path}")


def extract_zip(zip_path: str, extract_dir: str) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            member_path = os.path.realpath(os.path.join(extract_dir, member))
            if not member_path.startswith(os.path.realpath(extract_dir)):
                print(f"Skipping unsafe zip entry: {member}")
                continue
            zf.extract(member, extract_dir)


def get_csv_files(extract_dir: str) -> list[str]:
    csv_files = sorted(
        glob.glob(os.path.join(extract_dir, "**", "*.csv"), recursive=True)
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {extract_dir}")

    print(f"Found {len(csv_files)} CSV file(s)")
    for path in csv_files:
        print(f" - {path}")

    return csv_files


def build_csv_lazyframe(csv_file: str) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            csv_file,
            schema=CSV_SCHEMA,
            low_memory=True,
            glob=False,
        )
        .select([
            pl.col("ride_id").cast(pl.Utf8, strict=False),
            pl.col("rideable_type").cast(pl.Utf8, strict=False),

            pl.col("started_at")
                .str.to_datetime(strict=False)
                .cast(pl.Datetime("us")),

            pl.col("ended_at")
                .str.to_datetime(strict=False)
                .cast(pl.Datetime("us")),

            pl.col("start_station_name").cast(pl.Utf8, strict=False),
            pl.col("start_station_id").cast(pl.Utf8, strict=False),

            pl.col("end_station_name").cast(pl.Utf8, strict=False),
            pl.col("end_station_id").cast(pl.Utf8, strict=False),

            pl.col("start_lat").cast(pl.Float64, strict=False),
            pl.col("start_lng").cast(pl.Float64, strict=False),
            pl.col("end_lat").cast(pl.Float64, strict=False),
            pl.col("end_lng").cast(pl.Float64, strict=False),

            pl.col("member_casual").cast(pl.Utf8, strict=False),
        ])
    )
    

def csv_to_parquet(csv_file: str, parquet_path: str) -> None:
    print(f"Converting {csv_file} -> {parquet_path}")

    lf = build_csv_lazyframe(csv_file)

    lf.sink_parquet(
        parquet_path,
        compression="zstd",
        row_group_size=20000
    )

    size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"Parquet written: {size_mb:.1f} MB -> {parquet_path}")


def upload_to_gcs(
    gcs_client: storage.Client,
    bucket_name: str,
    local_path: str,
    blob_name: str,
) -> None:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")


def get_gcs_client() -> storage.Client:
    conn = json.loads(os.environ["nyc_citibike"])
    sa_info = json.loads(conn["service_account_json"])
    project_id = conn.get("project_id") or sa_info.get("project_id")

    creds = service_account.Credentials.from_service_account_info(sa_info)
    return storage.Client(project=project_id, credentials=creds)


def main() -> None:
    pl.Config.set_streaming_chunk_size(10000)
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
            os.makedirs(extract_dir, exist_ok=True)

            download_zip(ym, zip_path)
            extract_zip(zip_path, extract_dir)
            csv_files = get_csv_files(extract_dir)

            for idx, csv_file in enumerate(csv_files, start=1):
                parquet_filename = f"{ym}-{idx:02d}-citibike-tripdata.parquet"
                parquet_path = os.path.join(tmp_dir, parquet_filename)

                csv_to_parquet(csv_file, parquet_path)
                upload_to_gcs(
                    gcs_client=gcs_client,
                    bucket_name=BUCKET_NAME,
                    local_path=parquet_path,
                    blob_name=parquet_filename,
                )

            print(f"Completed {ym} — temp files cleaned up")

    print("\nAll months processed successfully.")


if __name__ == "__main__":
    main()