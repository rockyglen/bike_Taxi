"""
inference.py

Production Inference Pipeline:
1. Data Source: AWS S3 (Latest features).
2. Model Source: AWS S3 (Production Champion model).
3. Logic: Predicts the next 24 hours of demand.
4. Output: Saves predictions to S3 for the Dashboard.
"""

import os
import pandas as pd
import joblib
import boto3
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

# === CONFIG & LOAD ENV ===
load_dotenv()
DATA_FOLDER = "data"
os.makedirs(DATA_FOLDER, exist_ok=True)

# === UTILITIES ===

def download_from_s3(bucket, s3_path, local_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        s3.download_file(bucket, s3_path, local_path)
        return local_path
    except Exception as e:
        print(f"⚠️ S3 Download failed: {e}")
        return None

def upload_to_s3(local_file, bucket, s3_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3.upload_file(local_file, bucket, s3_path)

def run_inference():
    bucket = os.getenv('AWS_S3_BUCKET')
    
    # 1. Download Production Model & Features
    s3_model_path = "models/production_model.joblib"
    s3_feature_path = "citi_bike/forecast_features.parquet"
    
    local_model = download_from_s3(bucket, s3_model_path, os.path.join(DATA_FOLDER, "production_model.joblib"))
    local_features = download_from_s3(bucket, s3_feature_path, os.path.join(DATA_FOLDER, "latest_features.parquet"))
    
    if not local_model or not local_features:
        print("❌ Model or features not found. Aborting inference.")
        return

    # 2. Load Model & Prepare Input
    model = joblib.load(local_model)
    df = pd.read_parquet(local_features)
    
    # Get the the latest historical hour to start predicting from
    df = df.sort_values('start_hour')
    last_known_hour = df['start_hour'].max()
    
    # Features required by the model (matches train_model.py)
    lags = [f'lag_{i}' for i in range(1, 29)]
    categorical = ['hour', 'day_of_week', 'is_weekend', 'month']
    feature_cols = lags + categorical
    
    # 3. Recursive Forecasting (Bridge to Now + Next 24 Hours)
    # Perform all time math in UTC to avoid timezone comparison issues
    now_utc = datetime.now(pytz.UTC)
    
    # Ensure last_known_hour is UTC
    # Note: Citi Bike timestamps in data are usually naive; we treat them as UTC for bridging consistency
    if last_known_hour.tzinfo is None:
        last_known_hour = pytz.UTC.localize(last_known_hour)
    else:
        last_known_hour = last_known_hour.astimezone(pytz.UTC)
    
    hours_to_now = int((now_utc - last_known_hour).total_seconds() / 3600)
    
    print(f"🌉 Bridging {hours_to_now}h gap from {last_known_hour} to reach current time...")
    
    current_state = df.iloc[-1].copy()
    
    # Step A: Walk the gap (don't save results)
    for i in range(1, hours_to_now + 1):
        X_input = pd.DataFrame([current_state[feature_cols]])
        pred_value = max(0, float(model.predict(X_input)[0]))
        
        # Advance state
        this_time = last_known_hour + timedelta(hours=i)
        
        # Shift lags: lag_28 falls off, lag_2 gets old lag_1, lag_1 gets new prediction
        for l in range(28, 1, -1):
            current_state[f'lag_{l}'] = current_state[f'lag_{l-1}']
        current_state['lag_1'] = pred_value
        
        current_state['hour'] = this_time.hour
        current_state['day_of_week'] = this_time.weekday()
        current_state['is_weekend'] = 1 if this_time.weekday() >= 5 else 0
        current_state['month'] = this_time.month

    # Step B: Generate the actual 24h forecast
    print("🔮 Generating live 24h forecast...")
    predictions = []
    bridge_end_time = last_known_hour + timedelta(hours=hours_to_now)
    
    for i in range(1, 25):
        X_input = pd.DataFrame([current_state[feature_cols]])
        pred_value = max(0, float(model.predict(X_input)[0]))
        
        future_time = bridge_end_time + timedelta(hours=i)
        predictions.append({
            'target_hour': future_time,
            'predicted_trips': pred_value
        })
        
        # Advance state
        # Shift lags: lag_28 falls off, lag_2 gets old lag_1, lag_1 gets new prediction
        for l in range(28, 1, -1):
            current_state[f'lag_{l}'] = current_state[f'lag_{l-1}']
        current_state['lag_1'] = pred_value
        
        current_state['hour'] = future_time.hour
        current_state['day_of_week'] = future_time.weekday()
        current_state['is_weekend'] = 1 if future_time.weekday() >= 5 else 0
        current_state['month'] = future_time.month

    # 4. Save and Upload Predictions
    if not predictions:
        print("⚠️ No predictions generated for the requested window.")
        return

    pred_df = pd.DataFrame(predictions)
    now_utc = datetime.now(pytz.UTC)
    pred_df['prediction_generated_at'] = now_utc
    
    local_preds = os.path.join(DATA_FOLDER, "latest_predictions.parquet")
    pred_df.to_parquet(local_preds, index=False)
    
    # 4a. Stable Dashboard Pointer
    upload_to_s3(local_preds, bucket, "citi_bike/latest_predictions.parquet")
    
    # 4b. Timestamped Snapshot (Archive)
    timestamp = now_utc.strftime("%Y%m%d_%H%M")
    snapshot_path = f"citi_bike/archive/predictions_{timestamp}.parquet"
    upload_to_s3(local_preds, bucket, snapshot_path)
    
    print(f"✅ Inference complete. Predictions uploaded to S3.")

if __name__ == "__main__":
    run_inference()
