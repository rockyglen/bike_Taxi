"""
feature_engineering.py

Optimized Demand Forecasting Pipeline (GitHub Actions & AWS S3 Version):
1. Disk-Efficient Streaming: Processes each month individually (Download -> Process -> Delete).
2. Single Pass: Collects station statistics and hourly aggregates in memory to avoid redundant downloads.
3. Resource Optimized: Stays within GitHub Runner limits (~14GB disk, ~7GB RAM).
4. Cloud Sync: Upload final features to AWS S3 in Parquet format.
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
from dotenv import load_dotenv
import boto3

# === CONFIG ===
DATA_FOLDER = "data"
load_dotenv()

# === UTILITIES ===

def get_last_12_months():
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_and_extract(ym):
    os.makedirs(DATA_FOLDER, exist_ok=True)
    zip_path = os.path.join(DATA_FOLDER, f"{ym}-tripdata.zip")
    csv_exists = any(f.startswith(ym) and f.endswith('.csv') for f in os.listdir(DATA_FOLDER))
    
    if not os.path.exists(zip_path) and not csv_exists:
        print(f"🌐 Downloading {ym} from Citi Bike S3...")
        urls = [
            f"https://s3.amazonaws.com/tripdata/{ym}-citibike-tripdata.csv.zip",
            f"https://s3.amazonaws.com/tripdata/{ym}-citibike-tripdata.zip"
        ]
        
        success = False
        for url in urls:
            try:
                # Streaming download to avoid memory spikes
                with requests.get(url, stream=True, timeout=90) as r:
                    if r.status_code == 200:
                        with open(zip_path, 'wb') as f:
                            shutil.copyfileobj(r.raw, f)
                        success = True
                        break
            except Exception as e:
                print(f"🔗 Try next URL... (Error: {e})")
                continue
        if not success:
            print(f"⚠️ Could not find data for {ym}")
            return None

    if not csv_exists:
        print(f"📦 Extracting {ym}...")
        try:
            with zipfile.ZipFile(zip_path) as zf:
                csv_in_zip = [n for n in zf.namelist() if n.endswith('.csv') and '__MACOSX' not in n][0]
                target_path = os.path.join(DATA_FOLDER, f"{ym}_citibike_tripdata.csv")
                with zf.open(csv_in_zip) as source, open(target_path, "wb") as target:
                    target.write(source.read())
            # Immediately delete zip to save space on GitHub Runner
            os.remove(zip_path)
            return target_path
        except Exception as e:
            print(f"❌ Extraction failed for {ym}: {e}")
            return None
    else:
        existing_csv = [f for f in os.listdir(DATA_FOLDER) if f.startswith(ym) and f.endswith('.csv')][0]
        return os.path.join(DATA_FOLDER, existing_csv)

def upload_to_s3(local_file, bucket, s3_path):
    """Uploads a file to an S3 bucket using boto3."""
    print(f"📤 Uploading to S3 bucket: {bucket}...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        s3.upload_file(local_file, bucket, s3_path)
        print(f"✅ Successfully uploaded to s3://{bucket}/{s3_path}")
    except Exception as e:
        print(f"❌ S3 Upload failed: {e}")

def run_pipeline():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    months = get_last_12_months()
    
    station_counter = Counter()
    all_hourly_list = []
    
    print(f"� Starting Optimized Pipeline for {len(months)} months...")
    
    for ym in months:
        csv_path = download_and_extract(ym)
        if not csv_path:
            continue
            
        print(f"📊 Processing {ym} with pyarrow engine...")
        # Load only necessary columns with the fastest engine available
        try:
            df = pd.read_csv(
                csv_path, 
                usecols=['started_at', 'start_station_id', 'member_casual', 'rideable_type'],
                engine='pyarrow',
                dtype_backend='pyarrow'
            )
        except:
            # Fallback if pyarrow engine fails for any reason
            df = pd.read_csv(
                csv_path, 
                usecols=['started_at', 'start_station_id', 'member_casual', 'rideable_type'],
                low_memory=False
            )
        
        # 1. Update Global Station Counts
        station_counter.update(df['start_station_id'].dropna().astype(str))
        
        # 2. Interim Hourly Aggregation (Saves memory over 12 months)
        df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('h')
        
        # Group by all dimensions we need later
        hourly_agg = df.groupby(['start_hour', 'start_station_id', 'member_casual', 'rideable_type']).size().reset_index(name='count')
        all_hourly_list.append(hourly_agg)
        
        # 3. CRITICAL: Delete CSV immediately after processing
        print(f"🧹 Deleting raw data for {ym}...")
        os.remove(csv_path)

    if not all_hourly_list:
        print("❌ No data processed. Aborting.")
        return

    # Determine Top 3 Busiest Stations globally
    top3_ids = [s for s, count in station_counter.most_common(3)]
    print(f"🏆 Top 3 Stations identified: {top3_ids}")

    # Combine all hourly aggregates
    print("🏗️ Finalizing Global Aggregates...")
    full_hourly = pd.concat(all_hourly_list)
    
    # Filter for Top 3 Stations
    full_hourly = full_hourly[full_hourly['start_station_id'].isin(top3_ids)]
    
    # Pivot to get Member/Casual and Bike Type columns
    # We aggregate counts per start_hour for the Top 3 stations combined
    # (Since we are building a "Top 3 Fleet Demand" model)
    final_pivot = full_hourly.groupby(['start_hour', 'member_casual']).agg({'count': 'sum'}).unstack(fill_value=0)
    final_pivot.columns = final_pivot.columns.get_level_values(1)
    
    bike_pivot = full_hourly.groupby(['start_hour', 'rideable_type']).agg({'count': 'sum'}).unstack(fill_value=0)
    
    features_df = pd.concat([final_pivot, bike_pivot], axis=1).fillna(0).reset_index()
    
    # Ensure consistent column naming
    for col in ['member', 'casual', 'electric_bike', 'classic_bike']:
        if col not in features_df.columns:
            features_df[col] = 0
            
    features_df['total_trips'] = features_df.get('member', 0) + features_df.get('casual', 0)

    # 4. Feature Engineering (Lags & Time Context)
    print("📈 Generating 28-hour Lags...")
    features_df = features_df.sort_values('start_hour')
    for lag in range(1, 29):
        features_df[f'lag_{lag}'] = features_df['total_trips'].shift(lag)
    
    features_df['hour'] = features_df['start_hour'].dt.hour
    features_df['day_of_week'] = features_df['start_hour'].dt.dayofweek
    features_df['is_weekend'] = features_df['day_of_week'].isin([5, 6]).astype(int)
    
    features_df = features_df.dropna().reset_index(drop=True)
    
    # Export locally
    local_parquet = os.path.join(DATA_FOLDER, "final_features.parquet")
    features_df.to_parquet(local_parquet, index=False)
    print(f"💾 Features exported: {local_parquet}")

    # 5. Cloud Upload
    bucket = os.getenv('AWS_S3_BUCKET')
    if bucket:
        # 5a. Stable Pointer
        upload_to_s3(local_parquet, bucket, "citi_bike/forecast_features.parquet")
        
        # 5b. Timestamped Snapshot (Archive)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        snapshot_path = f"citi_bike/archive/features_{timestamp}.parquet"
        upload_to_s3(local_parquet, bucket, snapshot_path)
    else:
        print("⚠️ AWS_S3_BUCKET not found. Skipping cloud sync.")

if __name__ == "__main__":
    run_pipeline()
    print("\n✨ GitHub Action Optimized Pipeline Complete!")
