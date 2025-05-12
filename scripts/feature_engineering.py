"""
feature_engineering.py

Pipeline for:
1. Downloading Citi Bike trip data for the past 12 months.
2. Extracting and filtering top 3 most frequent start stations.
3. Cleaning and parsing records.
4. Uploading to Hopsworks feature store.
5. Cleaning up temporary files.
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

# === Config ===
TEMP_FOLDER = "tripdata_temp"
OUTPUT_FILE = "top3_stations_output.csv"
CHUNK_SIZE = 500_000

# Target columns from the CSVs
TARGET_COLS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id",
    "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual"
]

# === Helpers ===

def get_last_12_months_est():
    """Get last 12 months in 'YYYYMM' format (Eastern Time)."""
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_zip_to_memory(ym):
    """Download Citi Bike ZIP file for a given YYYYMM into memory."""
    base_url = "https://s3.amazonaws.com/tripdata/"
    filenames = [
        f"{ym}-citibike-tripdata.zip",
        f"{ym}-citibike-tripdata.csv.zip"
    ]
    for filename in filenames:
        url = base_url + filename
        try:
            print(f"🌐 Trying: {url}")
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print(f"✅ Downloaded: {filename}")
                return BytesIO(response.content)
        except Exception as e:
            print(f"❌ Failed: {url} with error {e}")
    return None

def extract_csvs_from_zip(zip_bytes, temp_folder):
    """Extract CSVs from a ZIP file in memory to temp folder."""
    os.makedirs(temp_folder, exist_ok=True)
    with zipfile.ZipFile(zip_bytes) as zf:
        extracted = []
        for file in zf.namelist():
            if file.endswith(".csv"):
                print(f"📁 Extracting CSV: {file}")
                zf.extract(file, path=temp_folder)
                extracted.append(os.path.join(temp_folder, file))
        return extracted

def filter_columns_and_concat(csv_files, target_cols):
    """Read, filter columns, and combine all CSV files."""
    dfs = []
    dtype_overrides = {
        "start_station_id": "str",
        "end_station_id": "str"
    }
    for file in csv_files:
        try:
            df = pd.read_csv(file, usecols=target_cols, dtype=dtype_overrides, low_memory=False)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipped {file}: {e}")
    return pd.concat(dfs, ignore_index=True)


def get_top3_start_stations(df):
    """Identify top 3 most frequent start_station_id."""
    top3 = df['start_station_id'].value_counts().nlargest(3).index.tolist()
    return top3

def filter_to_top3_stations(df, top3_ids):
    """Filter only records from top 3 start stations."""
    return df[df['start_station_id'].isin(top3_ids)]

def clean_and_transform(df):
    """Clean datetime fields and create time-based features."""
    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
    df = df.dropna(subset=['started_at', 'ended_at'])

    df['ride_duration_mins'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df = df[df['ride_duration_mins'] > 0]

    df['start_hour'] = df['started_at'].dt.floor('H')
    df['hour_of_day'] = df['started_at'].dt.hour
    df['day_of_week'] = df['started_at'].dt.dayofweek
    df['month'] = df['started_at'].dt.month

    return df

def upload_to_hopsworks(df):
    """Upload cleaned dataframe to Hopsworks feature store."""
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
        description="Cleaned Citi Bike trip data for top 3 stations",
    )
    fg.insert(df, write_options={"wait_for_job": True})
    print("✅ Uploaded to Hopsworks!")

# === Main Execution ===

if __name__ == "__main__":
    months = get_last_12_months_est()
    all_csv_paths = []

    for ym in months:
        zip_bytes = download_zip_to_memory(ym)
        if zip_bytes:
            extracted = extract_csvs_from_zip(zip_bytes, TEMP_FOLDER)
            all_csv_paths.extend(extracted)

    df_all = filter_columns_and_concat(all_csv_paths, TARGET_COLS)
    top3_ids = get_top3_start_stations(df_all)
    df_top3 = filter_to_top3_stations(df_all, top3_ids)
    df_cleaned = clean_and_transform(df_top3)

    df_cleaned.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved cleaned data to {OUTPUT_FILE}")

    upload_to_hopsworks(df_cleaned)

    # Final cleanup
    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)
        print(f"🧹 Cleaned up temp folder: {TEMP_FOLDER}")
