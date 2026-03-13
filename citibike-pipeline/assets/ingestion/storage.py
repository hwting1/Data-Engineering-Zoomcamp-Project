"""@bruin

name: ingestion.storage
type: python
image: python:3.12
connection: nyc_citibike

@bruin"""

import os
import zipfile
import tempfile
import glob
from datetime import datetime, timedelta

import polars as pl
import requests
from google.cloud import storage


BUCKET_NAME = "nyc-citibike-bucket"
BASE_URL = "https://s3.amazonaws.com/tripdata"


def _prev_month_first(dt):
    """Return the 1st day of the month before *dt*."""
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def _next_month_first(dt):
    """Return the 1st day of the month after *dt*."""
    return (dt.replace(day=28) + timedelta(days=4)).replace(day=1)


def get_months_to_download():
    """Determine which YYYYMM values to download based on BRUIN date window.

    The pipeline runs on the 15th of each month and downloads the previous
    month's data.  Given the window [BRUIN_START_DATE, BRUIN_END_DATE) this
    function shifts each execution month back by one to derive the target
    download months.
    """
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
                    for chunk in resp.iter_content(chunk_size=8192):
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


def read_and_combine_csvs(extract_dir):
    """Read every CSV under *extract_dir* (recursive) and return one DataFrame."""
    csv_files = sorted(
        glob.glob(os.path.join(extract_dir, "**/*.csv"), recursive=True)
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {extract_dir}")

    print(f"Found {len(csv_files)} CSV file(s)")
    dfs = [pl.read_csv(f, infer_schema_length=0) for f in csv_files]
    combined = pl.concat(dfs)
    print(f"Combined DataFrame: {combined.shape[0]} rows x {combined.shape[1]} columns")
    return combined


def enforce_schema(df):
    """Cast every column to a fixed type so the parquet schema is consistent."""
    return df.select([
        pl.col("ride_id").cast(pl.Utf8),
        pl.col("rideable_type").cast(pl.Utf8),
        pl.col("started_at").str.to_datetime(format=None, strict=False),
        pl.col("ended_at").str.to_datetime(format=None, strict=False),
        pl.col("start_station_name").cast(pl.Utf8),
        pl.col("start_station_id").cast(pl.Int64),
        pl.col("end_station_name").cast(pl.Utf8),
        pl.col("end_station_id").cast(pl.Int64),
        pl.col("start_lat").cast(pl.Float64, strict=False),
        pl.col("start_lng").cast(pl.Float64, strict=False),
        pl.col("end_lat").cast(pl.Float64, strict=False),
        pl.col("end_lng").cast(pl.Float64, strict=False),
        pl.col("member_casual").cast(pl.Utf8),
    ])


def upload_to_gcs(gcs_client, bucket_name, local_path, blob_name):
    """Upload a local file to Google Cloud Storage."""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")


# ── Main execution ──────────────────────────────────────────────────────────

months = get_months_to_download()
print(f"Months to process: {months}")


def get_gcs_client():
    conn = json.loads(os.environ["nyc_citibike"])
    sa_info = json.loads(conn["service_account_json"])
    project_id = conn.get("project_id") or sa_info.get("project_id")

    creds = service_account.Credentials.from_service_account_info(sa_info)
    return storage.Client(project=project_id, credentials=creds)


gcs_client = get_gcs_client()

for ym in months:
    # Each month gets its own temp directory — automatically cleaned up at the end
    with tempfile.TemporaryDirectory() as tmp_dir:
        print(f"\n{'=' * 50}")
        print(f"Processing {ym}")
        print(f"{'=' * 50}")

        # 1. Download zip
        zip_path = os.path.join(tmp_dir, f"{ym}.zip")
        download_zip(ym, zip_path)

        # 2. Extract
        extract_dir = os.path.join(tmp_dir, "extracted")
        os.makedirs(extract_dir)
        extract_zip(zip_path, extract_dir)

        # 3. Read & combine all CSVs
        df = read_and_combine_csvs(extract_dir)

        # 4. Enforce fixed column types
        df = enforce_schema(df)

        # 5. Write parquet
        parquet_path = os.path.join(tmp_dir, f"{ym}-citibike-tripdata.parquet")
        df.write_parquet(parquet_path)
        size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
        print(f"Parquet written: {size_mb:.1f} MB")

        # 6. Upload parquet to GCS
        blob_name = f"{ym}-citibike-tripdata.parquet"
        upload_to_gcs(gcs_client, BUCKET_NAME, parquet_path, blob_name)

        print(f"Completed {ym} — temp files cleaned up")
    # TemporaryDirectory.__exit__ deletes zip, extracted CSVs, and parquet

print("\nAll months processed successfully.")


