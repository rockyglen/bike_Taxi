"""
feature_engineering_parallel.py

Fast + RAM-safe Citi Bike pipeline:
- Parallel download & unzip
- Per-month processing (top 3 stations)
- Stream to CSV to save RAM
- Upload to Hopsworks with correct schema
"""

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
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import hopsworks

# === CONFIG ===
TEMP_FOLDER = "tripdata_temp"
OUTPUT_FILE = "top3_stations_output.csv"
MAX_THREADS = 4

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

# === UTILITIES ===

def get_last_12_months_est():
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_and_extract(ym):
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    base_url = "https://s3.amazonaws.com/tripdata/"
    filenames = [f"{ym}-citibike-tripdata.zip", f"{ym}-citibike-tripdata.csv.zip"]
    for fname in filenames:
        try:
            url = base_url + fname
            print(f"🌐 Downloading: {url}")
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with zipfile.ZipFile(BytesIO(r.content)) as zf:
                    extracted = []
                    for file in zf.namelist():
                        if file.endswith(".csv") and "__MACOSX" not in file:
                            path = os.path.join(TEMP_FOLDER, f"{ym}_{os.path.basename(file)}")
                            with open(path, "wb") as f:
                                f.write(zf.read(file))
                            extracted.append(path)
                    return extracted
        except Exception as e:
            print(f"❌ Failed for {fname}: {e}")
    return []

# === STATION FREQUENCY ===

def build_top3_station_counter(months):
    counter = Counter()
    for ym in months:
        files = download_and_extract(ym)
        for file in files:
            try:
                df = pd.read_csv(file, usecols=["start_station_id"], dtype={"start_station_id": "str"})
                counter.update(df["start_station_id"].dropna())
            except Exception as e:
                print(f"⚠️ Skipping {file}: {e}")
            os.remove(file)
    return [s for s, _ in counter.most_common(3)]

# === MONTHLY PROCESSING ===

def process_month(ym, top3_ids):
    files = download_and_extract(ym)
    for file in files:
        try:
            df = pd.read_csv(file, usecols=TARGET_COLS, dtype=DTYPE_OVERRIDES, low_memory=False)
            df = df[df["start_station_id"].isin(top3_ids)]

            df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
            df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")
            df = df.dropna(subset=["started_at", "ended_at"])

            df["ride_duration_mins"] = (df["ended_at"] - df["started_at"]).dt.total_seconds() / 60
            df = df[df["ride_duration_mins"] > 0]

            df["start_hour"] = df["started_at"].dt.floor("H")
            df["hour_of_day"] = df["started_at"].dt.hour.astype("int32")
            df["day_of_week"] = df["started_at"].dt.dayofweek.astype(str)
            df["month"] = df["started_at"].dt.month.astype("int32")

            # Ensure IDs are strings
            df["start_station_id"] = df["start_station_id"].astype(str)
            df["end_station_id"] = df["end_station_id"].astype(str)

            df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not os.path.exists(OUTPUT_FILE))
        except Exception as e:
            print(f"⚠️ Failed to process {file}: {e}")
        os.remove(file)

# === UPLOAD TO HOPSWORKS ===

def upload_to_hopsworks():
    load_dotenv()
    project = hopsworks.login(
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
        project=os.getenv("HOPSWORKS_PROJECT")
    )
    fs = project.get_feature_store()

    df = pd.read_csv(OUTPUT_FILE, dtype={
        "ride_id": "str",
        "rideable_type": "str",
        "start_station_id": "str",
        "end_station_id": "str",
        "start_station_name": "str",
        "end_station_name": "str",
        "member_casual": "str",
        "day_of_week": "str"
    }, low_memory=False)

    df["start_hour"] = pd.to_datetime(df["start_hour"], format="mixed", errors="coerce")
    df["started_at"] = pd.to_datetime(df["started_at"], format="mixed", errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], format="mixed", errors="coerce")

    df["hour_of_day"] = df["hour_of_day"].astype("int32")
    df["month"] = df["month"].astype("int32")

    fg = fs.get_or_create_feature_group(
        name="citi_bike_trips",
        version=1,
        primary_key=["ride_id"],
        event_time="start_hour",
        description="Top 3 Citi Bike stations - fast and type-safe"
    )

    fg.insert(df, write_options={"wait_for_job": True})
    print("✅ Upload to Hopsworks successful.")

# === MAIN EXECUTION ===

if __name__ == "__main__":
    months = get_last_12_months_est()
    print("🔍 Identifying top 3 stations...")
    top3 = build_top3_station_counter(months)
    print(f"🏆 Top 3 start_station_ids: {top3}")

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    print("🚴 Starting parallel processing...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        pool.map(lambda m: process_month(m, top3), months)

    print("📤 Uploading to Hopsworks...")
    upload_to_hopsworks()

    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)
        print("🧹 Temp folder cleaned up.")
