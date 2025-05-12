"""
feature_engineering_optimized.py

RAM-efficient Citi Bike data pipeline:
1. Streams 12 months of trip data from Citi Bike S3.
2. Dynamically finds the top 3 most active start stations.
3. Filters and engineers features only for those stations.
4. Uploads cleaned data to Hopsworks.
5. Cleans up all temp files and uses minimal memory.
"""

# === Imports ===
import os
import requests
import zipfile
import pandas as pd
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from io import BytesIO
from collections import Counter
from dotenv import load_dotenv
import hopsworks
from hsfs.feature_group import FeatureGroup

# === Configuration ===
TEMP_FOLDER = "tripdata_temp"
OUTPUT_FILE = "top3_stations_output.csv"
CHUNK_SIZE = 100_000

TARGET_COLS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id",
    "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual"
]

DTYPE_OVERRIDES = {
    "start_station_id": "str",
    "end_station_id": "str"
}

# === Helper Functions ===

def get_last_12_months_est():
    """Return list of last 12 months as YYYYMM in US Eastern Time."""
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_zip_to_memory(ym):
    """Download monthly ZIP from Citi Bike S3 into memory."""
    base_url = "https://s3.amazonaws.com/tripdata/"
    filenames = [f"{ym}-citibike-tripdata.zip", f"{ym}-citibike-tripdata.csv.zip"]
    for filename in filenames:
        url = base_url + filename
        try:
            print(f"🌐 Downloading: {url}")
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return BytesIO(response.content)
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
    return None

def extract_csvs_from_zip(zip_bytes):
    """Extract CSVs from a ZIP in memory to disk."""
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    with zipfile.ZipFile(zip_bytes) as zf:
        extracted = []
        for file in zf.namelist():
            if file.endswith(".csv"):
                path = os.path.join(TEMP_FOLDER, file)
                zf.extract(file, TEMP_FOLDER)
                extracted.append(path)
        return extracted

def build_top3_station_counter(months):
    """Stream all months and count start station frequency."""
    counter = Counter()
    for ym in months:
        zip_bytes = download_zip_to_memory(ym)
        if not zip_bytes:
            continue
        csv_paths = extract_csvs_from_zip(zip_bytes)
        for path in csv_paths:
            try:
                for chunk in pd.read_csv(path, usecols=["start_station_id"], dtype={"start_station_id": "str"}, chunksize=CHUNK_SIZE):
                    counter.update(chunk["start_station_id"].dropna())
            except Exception as e:
                print(f"⚠️ Skipping {path}: {e}")
            os.remove(path)
    return [station for station, _ in counter.most_common(3)]

def process_and_save_filtered_data(months, top3_ids):
    """Stream-filter-transform only top 3 station trips, return final DataFrame."""
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    output_chunks = []

    for ym in months:
        zip_bytes = download_zip_to_memory(ym)
        if not zip_bytes:
            continue
        csv_paths = extract_csvs_from_zip(zip_bytes)
        for path in csv_paths:
            try:
                for chunk in pd.read_csv(path, usecols=TARGET_COLS, dtype=DTYPE_OVERRIDES, chunksize=CHUNK_SIZE, low_memory=False):
                    chunk = chunk[chunk['start_station_id'].isin(top3_ids)]
                    chunk['started_at'] = pd.to_datetime(chunk['started_at'], errors='coerce')
                    chunk['ended_at'] = pd.to_datetime(chunk['ended_at'], errors='coerce')
                    chunk = chunk.dropna(subset=['started_at', 'ended_at'])

                    chunk['ride_duration_mins'] = (chunk['ended_at'] - chunk['started_at']).dt.total_seconds() / 60
                    chunk = chunk[chunk['ride_duration_mins'] > 0]

                    chunk['start_hour'] = chunk['started_at'].dt.floor('H')
                    chunk['hour_of_day'] = chunk['started_at'].dt.hour
                    chunk['day_of_week'] = chunk['started_at'].dt.dayofweek
                    chunk['month'] = chunk['started_at'].dt.month

                    output_chunks.append(chunk)
            except Exception as e:
                print(f"⚠️ Failed to process {path}: {e}")
            os.remove(path)
    return pd.concat(output_chunks, ignore_index=True)

def upload_to_hopsworks(df):
    """Push cleaned data to Hopsworks feature store."""
    load_dotenv()
    project = hopsworks.login(
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
        project=os.getenv("HOPSWORKS_PROJECT")
    )
    fs = project.get_feature_store()
    fg = FeatureGroup(
        name="citi_bike_trips",
        version=1,
        primary_key=["ride_id"],
        description="Citi Bike trips for top 3 stations - RAM optimized"
    )
    fg.insert(df, write_options={"wait_for_job": True})
    print("✅ Uploaded to Hopsworks")

# === Main Logic ===
if __name__ == "__main__":
    months = get_last_12_months_est()

    print("🔍 Identifying top 3 start stations...")
    top3_ids = build_top3_station_counter(months)
    print(f"🏆 Top 3 start_station_id values: {top3_ids}")

    print("🚴 Filtering + transforming only top 3 station trips...")
    df_final = process_and_save_filtered_data(months, top3_ids)

    print(f"💾 Writing cleaned data to CSV: {OUTPUT_FILE}")
    df_final.to_csv(OUTPUT_FILE, index=False)

    print("📤 Uploading cleaned data to Hopsworks...")
    upload_to_hopsworks(df_final)

    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)
        print(f"🧹 Removed temporary folder: {TEMP_FOLDER}")
