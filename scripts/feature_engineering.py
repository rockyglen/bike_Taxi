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

# ======================= CONFIG =======================
TEMP_FOLDER = "tripdata_temp"
OUTPUT_FILE = "top3_stations_output.csv"
CHUNK_SIZE = 500_000

TARGET_COLS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id",
    "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual"
]

DTYPES = {
    "ride_id": str,
    "rideable_type": str,
    "started_at": str,
    "ended_at": str,
    "start_station_name": str,
    "start_station_id": str,
    "end_station_name": str,
    "end_station_id": str,
    "start_lat": float,
    "start_lng": float,
    "end_lat": float,
    "end_lng": float,
    "member_casual": str
}

# ======================= DOWNLOAD & EXTRACTION =======================

def get_last_12_months_est():
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_zip_to_memory(ym):
    base_url = "https://s3.amazonaws.com/tripdata/"
    filenames = [
        f"{ym}-citibike-tripdata.zip",
        f"{ym}-citibike-tripdata.csv.zip"
    ]
    for fname in filenames:
        url = base_url + fname
        try:
            print(f"🌐 Trying: {url}")
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                print(f"✅ Downloaded: {fname}")
                return BytesIO(r.content)
            else:
                print(f"❌ Not found: {url}")
        except Exception as e:
            print(f"⚠️ Error downloading {url}: {e}")
    return None

def extract_all_csvs(zip_bytes_io, extract_to):
    try:
        with zipfile.ZipFile(zip_bytes_io) as zf:
            for member in zf.namelist():
                if member.endswith('.zip'):
                    nested_zip_data = zf.read(member)
                    with zipfile.ZipFile(BytesIO(nested_zip_data)) as nested_zf:
                        for nested_member in nested_zf.namelist():
                            if nested_member.endswith('.csv'):
                                print(f"📦 Extracting nested CSV: {nested_member}")
                                nested_zf.extract(nested_member, extract_to)
                elif member.endswith('.csv'):
                    print(f"📁 Extracting CSV: {member}")
                    zf.extract(member, extract_to)
    except Exception as e:
        print(f"⚠️ Error extracting zip: {e}")

def flatten_csvs_folder(root_folder):
    flat_files = []
    for root, _, files in os.walk(root_folder):
        for fname in files:
            if fname.endswith(".csv"):
                full_path = os.path.join(root, fname)
                flat_files.append(full_path)
    return flat_files

# ======================= TOP 3 STATION FILTERING =======================

def get_top3_station_names(filepaths):
    freq = Counter()
    for path in filepaths:
        try:
            for chunk in pd.read_csv(path, usecols=["start_station_name"], dtype={"start_station_name": str}, chunksize=CHUNK_SIZE):
                chunk = chunk.dropna(subset=["start_station_name"])
                freq.update(chunk["start_station_name"])
        except Exception as e:
            print(f"⚠️ Skipping {path}: {e}")
    top3 = [name for name, _ in freq.most_common(3)]
    return top3

def write_top3_data(filepaths, top3, output=OUTPUT_FILE):
    first_write = True
    for path in filepaths:
        try:
            for chunk in pd.read_csv(path, usecols=TARGET_COLS, dtype=DTYPES, chunksize=CHUNK_SIZE, low_memory=False):
                chunk = chunk.dropna(subset=["start_station_name"])
                filtered = chunk[chunk["start_station_name"].isin(top3)]
                if not filtered.empty:
                    filtered.to_csv(output, index=False, mode='a' if not first_write else 'w', header=first_write)
                    first_write = False
        except Exception as e:
            print(f"⚠️ Skipping {path}: {e}")

# ======================= CLEANING & FEATURES =======================

def clean_and_engineer_features(file_path):
    print("🧼 Cleaning and engineering features...")
    df = pd.read_csv(file_path)

    # 1. Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # 2. Parse datetime columns
    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')

    # 3. Drop rows with invalid/missing datetime
    df = df.dropna(subset=['started_at', 'ended_at'])

    # 4. Drop rows with critical missing values
    critical_cols = ['ride_id', 'rideable_type', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual']
    df = df.dropna(subset=critical_cols)

    # 5. Fill optional missing values
    df['start_station_name'] = df['start_station_name'].fillna('Unknown')
    df['end_station_name'] = df['end_station_name'].fillna('Unknown')
    df['start_station_id'] = df['start_station_id'].fillna('-1')
    df['end_station_id'] = df['end_station_id'].fillna('-1')

    # 6. Convert types explicitly
    string_cols = [
        'ride_id', 'rideable_type', 'start_station_name', 'start_station_id',
        'end_station_name', 'end_station_id', 'member_casual'
    ]
    for col in string_cols:
        df[col] = df[col].astype(str)

    df['rideable_type'] = df['rideable_type'].astype('category')
    df['member_casual'] = df['member_casual'].astype('category')

    # 7. Ride duration
    df['ride_duration_mins'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df = df[df['ride_duration_mins'] > 0]

    # 8. Time features
    df['day_of_week'] = df['started_at'].dt.day_name()
    df['hour_of_day'] = df['started_at'].dt.hour
    df['month'] = df['started_at'].dt.month

    # 9. Final dtype normalization
    df = df.convert_dtypes()

    print(f"✅ Cleaned dataset: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df

# ======================= HOPSWORKS =======================

def connect_to_hopsworks():
    print("🔐 Connecting to Hopsworks...")
    load_dotenv()
    project = hopsworks.login(
        project=os.getenv("HOPSWORKS_PROJECT"),
        api_key_value=os.getenv("HOPSWORKS_API_KEY")
    )
    return project.get_feature_store()

def push_to_feature_store(df, fs):
    print("🚀 Pushing to Hopsworks Feature Store...")
    fg = fs.get_or_create_feature_group(
        name="citi_bike_trips",
        version=1,
        description="Citi Bike data from top 3 stations in last 12 months",
        primary_key=["ride_id"],
        event_time="started_at"
    )
    fg.insert(df, write_options={"wait_for_job": True})
    print("✅ Feature group created/updated successfully.")

# ======================= MAIN =======================

def main():
    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)
    os.makedirs(TEMP_FOLDER, exist_ok=True)

    months = get_last_12_months_est()
    print("🚀 Starting download + extraction...")

    for ym in months:
        zip_mem = download_zip_to_memory(ym)
        if zip_mem:
            extract_all_csvs(zip_mem, TEMP_FOLDER)

    all_csvs = flatten_csvs_folder(TEMP_FOLDER)
    if not all_csvs:
        print("❌ No CSV files found.")
        return

    print("\n🔍 Counting top 3 stations...")
    top3 = get_top3_station_names(all_csvs)
    print(f"🏆 Top 3 Stations: {top3}")

    print("\n📤 Writing filtered data...")
    write_top3_data(all_csvs, top3)
    print(f"✅ Output written to `{OUTPUT_FILE}`")

    print("\n🧹 Cleaning up temp files...")
    shutil.rmtree(TEMP_FOLDER)
    print("✅ Temp folder deleted.")

    df = clean_and_engineer_features(OUTPUT_FILE)
    fs = connect_to_hopsworks()
    push_to_feature_store(df, fs)

if __name__ == "__main__":
    main()
