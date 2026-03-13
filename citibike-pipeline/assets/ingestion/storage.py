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
import pyarrow.parquet as pq
import requests
from google.cloud import storage
from google.oauth2 import service_account


BUCKET_NAME = "nyc-citibike-bucket"
BASE_URL = "https://s3.amazonaws.com/tripdata"

CSV_SCHEMA_OVERRIDES = {
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


def _prev_month_first(dt: datetime) -> datetime:
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def _next_month_first(dt: datetime) -> datetime:
    return (dt.replace(day=28) + timedelta(days=4)).replace(day=1)


def get_months_to_download() -> list[str]:
    start = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

    current = _prev_month_first(start)
    last = _prev_month_first(end)

    months = []
    while current < last:
        months.append(current.strftime("%Y%m"))
        current = _next_month_first(current)

    return months


def download_zip(year_month: str, dest_path: str) -> None:
    """Download the citibike tripdata zip for a given YYYYMM."""
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


def transform_batch(df: pl.DataFrame) -> pl.DataFrame:
    existing = set(df.columns)
    exprs = []

    for col in OUTPUT_COLUMNS:
        if col in existing:
            exprs.append(pl.col(col))
        else:
            exprs.append(pl.lit(None).alias(col))

    df = df.select(exprs)

    df = df.with_columns([
        pl.col("ride_id").cast(pl.Utf8, strict=False),
        pl.col("rideable_type").cast(pl.Utf8, strict=False),
        pl.col("started_at").str.to_datetime(strict=False),
        pl.col("ended_at").str.to_datetime(strict=False),
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

    return df.select(OUTPUT_COLUMNS)


def write_parquet_incrementally(
    csv_files: list[str],
    parquet_path: str,
    batch_size: int = 100_000,
) -> None:
    writer = None
    total_rows = 0

    try:
        for csv_file in csv_files:
            print(f"Reading batched CSV: {csv_file}")

            reader = pl.read_csv_batched(
                csv_file,
                schema_overrides=CSV_SCHEMA_OVERRIDES,
                infer_schema_length=0,
                batch_size=batch_size,
                low_memory=True,
                ignore_errors=False,
            )

            batch_idx = 0
            while True:
                batches = reader.next_batches(1)
                if not batches:
                    break

                for batch in batches:
                    batch_idx += 1
                    transformed = transform_batch(batch)
                    arrow_table = transformed.to_arrow()

                    if writer is None:
                        writer = pq.ParquetWriter(
                            parquet_path,
                            arrow_table.schema,
                            compression="zstd",
                        )

                    writer.write_table(arrow_table)
                    total_rows += transformed.height

                    print(
                        f"  wrote batch {batch_idx} from {os.path.basename(csv_file)} "
                        f"({transformed.height} rows, total={total_rows})"
                    )

                    del transformed
                    del arrow_table
                    del batch

        if writer is None:
            raise RuntimeError("No data was written to parquet.")

    finally:
        if writer is not None:
            writer.close()

    size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"Parquet written: {size_mb:.1f} MB -> {parquet_path}")
    print(f"Total rows written: {total_rows}")


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

            download_zip(ym, zip_path)
            extract_zip(zip_path, extract_dir)
            csv_files = get_csv_files(extract_dir)

            write_parquet_incrementally(
                csv_files=csv_files,
                parquet_path=parquet_path,
                batch_size=100_000,
            )

            blob_name = f"{ym}-citibike-tripdata.parquet"
            upload_to_gcs(gcs_client, BUCKET_NAME, parquet_path, blob_name)

            print(f"Completed {ym} — temp files cleaned up")

    print("\nAll months processed successfully.")


if __name__ == "__main__":
    main()