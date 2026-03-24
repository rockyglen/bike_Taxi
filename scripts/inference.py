"""
inference.py

Production Inference Pipeline:
1. Features: Hopsworks Feature Store.
2. Model: Hopsworks Model Registry (champion by lowest MAE).
3. Logic: Recursive Bridge — walks the ~20-day Citi Bike data lag to the present,
          then generates a 24h demand forecast.
4. Output: Saves predictions to S3 for the Next.js dashboard.
"""

import os
import pandas as pd
import joblib
import boto3
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import hopsworks

# === CONFIG & LOAD ENV ===
load_dotenv()
DATA_FOLDER = "data"
os.makedirs(DATA_FOLDER, exist_ok=True)

# === UTILITIES ===

def upload_to_s3(local_file, bucket, s3_path):
    """Uploads a file to S3 (frontend-serving files only)."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3.upload_file(local_file, bucket, s3_path)

def run_inference():
    bucket = os.getenv('AWS_S3_BUCKET')

    # 1. Connect to Hopsworks
    print(" Connecting to Hopsworks...")
    project = hopsworks.login(
        api_key_value=os.getenv('HOPSWORKS_API_KEY'),
        project=os.getenv('HOPSWORKS_PROJECT'),
    )
    fs = project.get_feature_store()
    mr = project.get_model_registry()

    # 2. Download champion model from Model Registry
    print(" Fetching champion model from Hopsworks Model Registry...")
    try:
        model_meta = mr.get_best_model("demand_forecaster", metric="MAE", direction="min")
    except Exception as e:
        print(f" Could not fetch model from registry: {e}")
        return
    model_dir = model_meta.download()
    model = joblib.load(os.path.join(model_dir, "production_model.joblib"))
    print(" Model loaded.")

    # 3. Read latest features from Feature Store
    print(" Reading features from Hopsworks Feature Store...")
    fg = fs.get_feature_group("forecast_features", version=1)
    df = fg.read()

    # Ensure standard numpy types
    for col in df.columns:
        if pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    # Features required by the model (matches train_model.py)
    lags = [f'lag_{i}' for i in range(1, 29)]
    categorical = ['hour', 'day_of_week', 'is_weekend', 'month']
    feature_cols = lags + categorical

    # Get the latest historical hour to start predicting from
    df = df.sort_values('start_hour')
    last_known_hour = df['start_hour'].max()

    # 4. Recursive Forecasting (Bridge to Now + Next 24 Hours)
    # Perform all time math in UTC to avoid timezone comparison issues
    now_utc = datetime.now(pytz.UTC)

    # Ensure last_known_hour is UTC
    # Note: Citi Bike timestamps in data are usually naive; we treat them as UTC for bridging consistency
    if last_known_hour.tzinfo is None:
        last_known_hour = pytz.UTC.localize(last_known_hour)
    else:
        last_known_hour = last_known_hour.astimezone(pytz.UTC)

    hours_to_now = int((now_utc - last_known_hour).total_seconds() / 3600)

    print(f" Bridging {hours_to_now}h gap from {last_known_hour} to reach current time...")

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
    print(" Generating live 24h forecast...")
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

        # Shift lags
        for l in range(28, 1, -1):
            current_state[f'lag_{l}'] = current_state[f'lag_{l-1}']
        current_state['lag_1'] = pred_value

        current_state['hour'] = future_time.hour
        current_state['day_of_week'] = future_time.weekday()
        current_state['is_weekend'] = 1 if future_time.weekday() >= 5 else 0
        current_state['month'] = future_time.month

    # 5. Save and Upload Predictions to S3 (frontend reads this)
    if not predictions:
        print(" No predictions generated for the requested window.")
        return

    pred_df = pd.DataFrame(predictions)
    now_utc = datetime.now(pytz.UTC)
    pred_df['prediction_generated_at'] = now_utc

    local_preds = os.path.join(DATA_FOLDER, "latest_predictions.parquet")
    pred_df.to_parquet(local_preds, index=False)

    if bucket:
        # Stable dashboard pointer
        upload_to_s3(local_preds, bucket, "citi_bike/latest_predictions.parquet")
        # Timestamped snapshot
        timestamp = now_utc.strftime("%Y%m%d_%H%M")
        upload_to_s3(local_preds, bucket, f"citi_bike/archive/predictions_{timestamp}.parquet")
        print(f" Inference complete. Predictions uploaded to S3.")
    else:
        print(f" AWS_S3_BUCKET not set. Predictions saved locally only.")

if __name__ == "__main__":
    run_inference()
