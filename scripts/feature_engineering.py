import pandas as pd
import hopsworks
import os
from datetime import datetime

# Load raw data
df = pd.read_csv("data/top3_stations_output.csv")

# Preprocess
df.columns = df.columns.str.strip().str.lower()
df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
df = df.dropna(subset=['started_at', 'ended_at'])

# Drop invalid durations
df['ride_duration_mins'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
df = df[df['ride_duration_mins'] > 0]

# Add features
df['start_hour'] = df['started_at'].dt.floor('H')
df['hour_of_day'] = df['started_at'].dt.hour
df['day_of_week'] = df['started_at'].dt.day_name()
df['month'] = df['started_at'].dt.month

# Login to Hopsworks
project = hopsworks.login(api_key_value=os.getenv("HOPSWORKS_API_KEY"), project=os.getenv("HOPSWORKS_PROJECT"))
fs = project.get_feature_store()

# Store features
fg = fs.get_or_create_feature_group(
    name="citi_bike_hourly_features",
    version=1,
    primary_key=["ride_id"],
    description="Cleaned Citi Bike trip data with hourly aggregation",
)

fg.insert(df, overwrite=True)
print(f"✅ Stored {len(df)} rows in Hopsworks feature group.")
