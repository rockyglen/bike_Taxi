import os
import io
import zipfile
import requests
import pandas as pd
import numpy as np
from collections import Counter
from dotenv import load_dotenv
import hopsworks
from hsfs.feature_group import FeatureGroup

CHUNK_SIZE = 1_000_000
URL = 'https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip'
OUTPUT_FILE = 'top3_stations_output.csv'

def download_and_extract_top3():
    print("⬇️ Downloading outer ZIP...")
    response = requests.get(URL)
    response.raise_for_status()
    outer_zip = zipfile.ZipFile(io.BytesIO(response.content))

    inner_zip_names = [f for f in outer_zip.namelist() if f.endswith('.zip')]
    print(f"📦 Found {len(inner_zip_names)} monthly zip files.")

    # First pass — count stations
    station_counter = Counter()
    for inner_name in inner_zip_names:
        with outer_zip.open(inner_name) as inner_file:
            with zipfile.ZipFile(io.BytesIO(inner_file.read())) as inner_zip:
                csv_names = [f for f in inner_zip.namelist() if f.endswith('.csv')]
                if not csv_names:
                    continue
                with inner_zip.open(csv_names[0]) as csv_file:
                    for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE, low_memory=False,
                                             dtype={'start_station_id': str, 'end_station_id': str}):
                        if 'start_station_name' in chunk.columns:
                            station_counter.update(chunk['start_station_name'].dropna())

    top3_stations = [s for s, _ in station_counter.most_common(3)]
    print(f"🏆 Top 3 Stations: {top3_stations}")

    # Second pass — filter rows
    is_first_chunk = True
    for inner_name in inner_zip_names:
        with outer_zip.open(inner_name) as inner_file:
            with zipfile.ZipFile(io.BytesIO(inner_file.read())) as inner_zip:
                csv_names = [f for f in inner_zip.namelist() if f.endswith('.csv')]
                if not csv_names:
                    continue
                with inner_zip.open(csv_names[0]) as csv_file:
                    for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE, low_memory=False,
                                             dtype={'start_station_id': str, 'end_station_id': str}):
                        if 'start_station_name' not in chunk.columns:
                            continue
                        filtered = chunk[chunk['start_station_name'].isin(top3_stations)]
                        if not filtered.empty:
                            filtered.to_csv(OUTPUT_FILE, mode='w' if is_first_chunk else 'a',
                                            index=False, header=is_first_chunk)
                            is_first_chunk = False
    print(f"✅ Filtered data saved to {OUTPUT_FILE}")
    return OUTPUT_FILE

def clean_and_engineer_features(file_path):
    print("🧼 Cleaning and engineering features...")
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower()

    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
    df = df.dropna(subset=['started_at', 'ended_at'])

    critical_cols = ['ride_id', 'rideable_type', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual']
    df = df.dropna(subset=critical_cols)

    df['start_station_name'] = df['start_station_name'].fillna('Unknown')
    df['end_station_name'] = df['end_station_name'].fillna('Unknown')
    df['start_station_id'] = df['start_station_id'].fillna('-1')
    df['end_station_id'] = df['end_station_id'].fillna('-1')

    df['ride_id'] = df['ride_id'].astype(str)
    df['rideable_type'] = df['rideable_type'].astype('category')
    df['member_casual'] = df['member_casual'].astype('category')

    df['ride_duration_mins'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df = df[df['ride_duration_mins'] > 0]

    df['day_of_week'] = df['started_at'].dt.day_name()
    df['hour_of_day'] = df['started_at'].dt.hour
    df['month'] = df['started_at'].dt.month

    print(f"✅ Final cleaned shape: {df.shape}")
    return df

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
        description="Citi Bike data from top 3 stations in 2023",
        primary_key=["ride_id"],
        event_time="started_at"
    )
    fg.insert(df, write_options={"wait_for_job": True})
    print("✅ Feature group created/updated successfully.")

def main():
    csv_path = download_and_extract_top3()
    df = clean_and_engineer_features(csv_path)
    fs = connect_to_hopsworks()
    push_to_feature_store(df, fs)

if __name__ == "__main__":
    main()
