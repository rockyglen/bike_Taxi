import pandas as pd
import hopsworks
import os
from datetime import datetime
from dotenv import load_dotenv

# ----------------------
# Load env (if running locally)
# ----------------------
load_dotenv()

# ----------------------
# Load raw data
# ----------------------
df = pd.read_csv("top3_stations_output.csv")

# ----------------------
# Preprocessing
# ----------------------
df.columns = df.columns.str.strip().str.lower()
df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
df = df.dropna(subset=['started_at', 'ended_at'])

# Calculate ride duration in minutes
df['ride_duration_mins'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
df = df[df['ride_duration_mins'] > 0]

# Add time-based features
df['start_hour'] = df['started_at'].dt.floor('H')
df['hour_of_day'] = df['started_at'].dt.hour
df['day_of_week'] = df['started_at'].dt.day_name()
df['month'] = df['started_at'].dt.month

# ----------------------
# Replace NaNs with None for nullable string columns
# ----------------------
nullable_str_cols = ['start_station_name', 'end_station_name', 'start_station_id', 'end_station_id']
for col in nullable_str_cols:
    if col in df.columns:
        df[col] = df[col].where(pd.notnull(df[col]), None)

# ----------------------
# Confirm shape before upload
# ----------------------
print(f"📦 Final DataFrame: {df.shape[0]} rows × {df.shape[1]} columns")

# ----------------------
# Hopsworks login
# ----------------------
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()

# ----------------------
# Create or get feature group
# ----------------------
fg = fs.get_or_create_feature_group(
    name="citi_bike_hourly_features",
    version=1,
    primary_key=["ride_id"],
    description="Cleaned Citi Bike trip data with hourly aggregation"
)

# ----------------------
# Insert data with ingestion confirmation
# ----------------------
fg.insert(df, overwrite=True, await_ingestion=True)
print(f"✅ Successfully inserted {len(df):,} rows into Hopsworks feature group.")
