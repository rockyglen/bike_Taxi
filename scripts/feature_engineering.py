"""
feature_engineering.py

Optimized Demand Forecasting Pipeline (GitHub Actions + Hopsworks Version):
1. Disk-Efficient Streaming: Processes each month individually (Download -> Process -> Delete).
2. Single Pass: Collects station statistics and hourly aggregates in memory to avoid redundant downloads.
3. Resource Optimized: Stays within GitHub Runner limits (~14GB disk, ~7GB RAM).
4. Feature Store: Uploads final features to Hopsworks Feature Store.
5. S3: Uploads monthly_stats.json to S3 for the Next.js frontend.
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
import hopsworks

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
    """Uploads a file to S3 (used only for frontend-serving files)."""
    print(f"📤 Uploading to S3: {s3_path}...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        s3.upload_file(local_file, bucket, s3_path)
        print(f"✅ Uploaded to s3://{bucket}/{s3_path}")
    except Exception as e:
        print(f"❌ S3 Upload failed: {e}")

def upload_features_to_hopsworks(df):
    """Inserts the engineered features into the Hopsworks Feature Store."""
    print("🔗 Connecting to Hopsworks...")
    project = hopsworks.login(
        api_key_value=os.getenv('HOPSWORKS_API_KEY'),
        project=os.getenv('HOPSWORKS_PROJECT'),
    )
    fs = project.get_feature_store()

    # Ensure standard numpy types (pyarrow-backed types cause schema issues)
    df_insert = df.copy()
    for col in df_insert.columns:
        if pd.api.types.is_extension_array_dtype(df_insert[col]):
            df_insert[col] = pd.to_numeric(df_insert[col], errors='coerce').astype(float)
    # start_hour must be a plain datetime for Hopsworks event_time
    df_insert['start_hour'] = pd.to_datetime(df_insert['start_hour']).dt.tz_localize(None)

    fg = fs.get_or_create_feature_group(
        name="forecast_features",
        version=1,
        primary_key=["start_hour"],
        event_time="start_hour",
        description="Hourly Citi Bike demand features (top-3 stations, 28 lags)",
    )
    print("📤 Inserting features into Hopsworks Feature Store...")
    fg.insert(df_insert, write_options={"wait_for_job": True})
    print("✅ Features inserted into Hopsworks Feature Store.")


def generate_monthly_stats(ym, bucket):
    """
    Downloads the most recent month's CSV with full columns and computes
    all aggregates needed by the Monthly Insights dashboard, then uploads
    as monthly_stats.json to S3 so the Next.js frontend can read it.
    """
    import json
    from collections import defaultdict

    print(f"\n📊 Generating Monthly Dashboard Stats for {ym}...")
    csv_path = download_and_extract(ym)
    if not csv_path:
        print(f"⚠️ Could not download {ym} for monthly stats. Skipping.")
        return

    # Full columns needed for dashboard (more than the model needs)
    full_cols = [
        'started_at', 'ended_at',
        'member_casual', 'rideable_type',
        'start_station_name', 'end_station_name',
        'start_lat', 'start_lng',
    ]
    try:
        df = pd.read_csv(csv_path, usecols=full_cols, low_memory=False)
    except Exception as e:
        # Some older files may not have all columns — read what's available
        print(f"⚠️ Column mismatch, loading available columns: {e}")
        df = pd.read_csv(csv_path, low_memory=False)

    # Parse timestamps & compute duration
    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
    df['duration_min'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60

    # Outlier removal: same as Streamlit app (1–240 min)
    df = df[(df['duration_min'] > 1) & (df['duration_min'] < 240)].copy()
    df = df.dropna(subset=['started_at'])

    df['hour'] = df['started_at'].dt.hour

    total_trips = len(df)
    avg_duration = round(float(df['duration_min'].mean()), 1)
    member_ratio = round(float((df['member_casual'] == 'member').mean() * 100), 1)
    peak_hour = int(df['hour'].mode()[0])

    # Hourly density by member/casual
    hourly = df.groupby(['hour', 'member_casual']).size().reset_index(name='count')
    hourly_density = [
        {'hour': int(r.hour), 'type': r.member_casual, 'count': int(r['count'])}
        for _, r in hourly.iterrows()
    ]

    # Rideable type counts
    rideable = df['rideable_type'].value_counts().reset_index()
    rideable.columns = ['type', 'count']
    rideable_data = [{'type': r['type'], 'count': int(r['count'])} for _, r in rideable.iterrows()]

    # Duration distribution (5-min bins, capped at 60)
    bin_size = 5
    df['bin'] = (df['duration_min'] // bin_size * bin_size).clip(upper=60).astype(int)
    dur = df.groupby(['bin', 'member_casual']).size().reset_index(name='count')
    duration_data = [
        {
            'bin': int(r['bin']),
            'binLabel': f"{int(r['bin'])}-{int(r['bin']) + bin_size}",
            'type': r['member_casual'],
            'count': int(r['count'])
        }
        for _, r in dur.iterrows()
    ]

    # Top 10 routes
    if 'start_station_name' in df.columns and 'end_station_name' in df.columns:
        df['route'] = df['start_station_name'].fillna('') + ' → ' + df['end_station_name'].fillna('')
        route_counts = df[df['route'] != ' → ']['route'].value_counts().head(10)
        top_routes = [{'route': r, 'count': int(c)} for r, c in route_counts.items()]
    else:
        top_routes = []

    # Station geo data (top 500 by volume)
    if 'start_station_name' in df.columns and 'start_lat' in df.columns:
        geo_df = df.groupby('start_station_name').agg(
            lat=('start_lat', 'first'),
            lng=('start_lng', 'first'),
            count=('started_at', 'count')
        ).reset_index()
        geo_df = geo_df.dropna(subset=['lat', 'lng'])
        geo_df = geo_df.sort_values('count', ascending=False).head(500)
        geo_data = [
            {'station': r['start_station_name'], 'lat': float(r['lat']),
             'lng': float(r['lng']), 'count': int(r['count'])}
            for _, r in geo_df.iterrows()
        ]
    else:
        geo_data = []

    # Top 10 stations
    if 'start_station_name' in df.columns:
        station_counts = df['start_station_name'].value_counts().head(10)
        top_stations = [{'station': s, 'count': int(c)} for s, c in station_counts.items()]
    else:
        top_stations = []

    stats = {
        'summary': {
            'totalTrips': total_trips,
            'avgDuration': avg_duration,
            'memberRatio': member_ratio,
            'peakHour': peak_hour,
            'fileName': f'{ym}-citibike-tripdata.csv',
        },
        'hourlyDensity': hourly_density,
        'rideableData': rideable_data,
        'durationData': duration_data,
        'topRoutes': top_routes,
        'geoData': geo_data,
        'topStations': top_stations,
    }

    # Save locally and upload to S3 (frontend reads this)
    local_json = os.path.join(DATA_FOLDER, 'monthly_stats.json')
    with open(local_json, 'w') as f:
        json.dump(stats, f)

    print(f"💾 Monthly stats written: {local_json}")
    os.remove(csv_path)  # Clean up immediately

    if bucket:
        upload_to_s3(local_json, bucket, 'citi_bike/monthly_stats.json')
        print(f"✅ Monthly stats uploaded to S3.")
    else:
        print("⚠️ AWS_S3_BUCKET not set. Skipping S3 upload for monthly stats.")

def run_pipeline():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    months = get_last_12_months()

    station_counter = Counter()
    all_hourly_list = []

    print(f"🚀 Starting Optimized Pipeline for {len(months)} months...")

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
    final_pivot = full_hourly.groupby(['start_hour', 'member_casual']).agg({'count': 'sum'}).unstack(fill_value=0)
    final_pivot.columns = final_pivot.columns.get_level_values(1)

    bike_pivot = full_hourly.groupby(['start_hour', 'rideable_type']).agg({'count': 'sum'}).unstack(fill_value=0)
    bike_pivot.columns = bike_pivot.columns.get_level_values(1)

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
    features_df['month'] = features_df['start_hour'].dt.month

    features_df = features_df.dropna().reset_index(drop=True)

    # Export locally (kept for debugging)
    local_parquet = os.path.join(DATA_FOLDER, "final_features.parquet")
    features_df.to_parquet(local_parquet, index=False)
    print(f"💾 Features exported locally: {local_parquet}")

    # 5. Upload features to Hopsworks Feature Store
    upload_features_to_hopsworks(features_df)

    # 6. Monthly Dashboard Stats (for the Next.js frontend — still served from S3)
    bucket = os.getenv('AWS_S3_BUCKET')
    most_recent_month = months[0]
    generate_monthly_stats(most_recent_month, bucket)

if __name__ == "__main__":
    run_pipeline()
    print("\n✨ Pipeline Complete!")
