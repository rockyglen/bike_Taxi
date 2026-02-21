"""
train_model.py

Advanced MLOps Pipeline:
1. Data Source: AWS S3 (Parquet).
2. Data Drift: Evidently AI (Reference vs. Current).
3. Challenger vs. Champion: Automated model promotion based on MAE.
4. Logging: MLflow (Tracking) + S3 (Production Weights).
"""

import os
from datetime import datetime
import pandas as pd
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import boto3
import shutil
import uuid
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error
from dotenv import load_dotenv

# Evidently AI for Drift
from evidently import Report
from evidently.presets import DataDriftPreset
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
    except:
        return None

def upload_to_s3(local_file, bucket, s3_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3.upload_file(local_file, bucket, s3_path)

def get_rmse(y_true, y_pred):
    try:
        from sklearn.metrics import root_mean_squared_error
        return root_mean_squared_error(y_true, y_pred)
    except ImportError:
        from sklearn.metrics import mean_squared_error
        return mean_squared_error(y_true, y_pred, squared=False)

def run_drift_report(reference, current):
    """Generates a data drift report using Evidently AI."""
    print("📡 Monitoring Data Drift...")
    drift_report = Report(metrics=[
        DataDriftPreset(),
    ])
    
    # In modern Evidently, run() returns a snapshot containing the results
    snapshot = drift_report.run(reference_data=reference, current_data=current)
    
    report_path = os.path.join(DATA_FOLDER, "drift_report.html")
    snapshot.save_html(report_path)
    return report_path, snapshot.dict()

def train_and_log():
    bucket = os.getenv('AWS_S3_BUCKET')
    s3_feature_path = "citi_bike/forecast_features.parquet"
    local_feature_path = os.path.join(DATA_FOLDER, "features_for_training.parquet")
    
    if not download_from_s3(bucket, s3_feature_path, local_feature_path):
        print("❌ Features not found on S3.")
        return

    df = pd.read_parquet(local_feature_path)
    
    # Ensure all columns are standard numpy-compatible types
    # pyarrow-backed types (int64[pyarrow]) cause issues with legacy numpy functions & LightGBM
    for col in df.columns:
        if pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
            
    df = df.sort_values('start_hour').reset_index(drop=True)

    # 1. Split Data for Training and Drift Analysis
    # Reference: Older 80%, Current: Newest 20%
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]

    lags = [f'lag_{i}' for i in range(1, 29)]
    categorical = ['hour', 'day_of_week', 'is_weekend']
    demographics = ['member', 'casual', 'electric_bike', 'classic_bike']
    features = lags + categorical + demographics
    target = 'total_trips'

    # 2. DRIFT DETECTION
    report_html, report_dict = run_drift_report(
        train_df[features + [target]], 
        test_df[features + [target]]
    )
    
    # Extract dataset drift status from the modern Evidently report structure
    try:
        # The first metric in DataDriftPreset is typically the DatasetDriftMetric
        drift_detected = report_dict['metrics'][0]['value']['dataset_drift']
    except (KeyError, IndexError):
        drift_detected = False
    
    print(f"📊 Data Drift Detected: {drift_detected}")

    # 3. TRAINING (Challenger)
    print("🚀 Training Challenger Model...")
    challenger = lgb.LGBMRegressor(n_estimators=1000, learning_rate=0.05, random_state=42)
    challenger.fit(train_df[features], train_df[target], eval_set=[(test_df[features], test_df[target])])
    
    challenger_preds = challenger.predict(test_df[features])
    challenger_mae = mean_absolute_error(test_df[target], challenger_preds)
    print(f"🏆 Challenger MAE: {challenger_mae:.4f}")

    # 4. DOWNLOAD CHAMPION (Current Production)
    s3_prod_path = "models/production_model.joblib"
    local_prod_path = os.path.join(DATA_FOLDER, "champion_model.joblib")
    champion_mae = float('inf')
    
    if download_from_s3(bucket, s3_prod_path, local_prod_path):
        print("🛡️ Champion found. Evaluating performance...")
        champion = joblib.load(local_prod_path)
        champion_preds = champion.predict(test_df[features])
        champion_mae = mean_absolute_error(test_df[target], champion_preds)
        print(f"👑 Champion MAE: {champion_mae:.4f}")
    else:
        print("🆕 No Champion found in S3. Challenger will be promoted automatically.")

    # 5. MLFLOW LOGGING
    mlflow_uri = os.getenv('MLFLOW_TRACKING_URI')
    if mlflow_uri: mlflow.set_tracking_uri(mlflow_uri)

    with mlflow.start_run(run_name=f"LGBM_Run_{uuid.uuid4().hex[:4]}"):
        mlflow.log_params({"drift_status": drift_detected, "challenger_mae": challenger_mae, "champion_mae": champion_mae})
        mlflow.log_metric("MAE", challenger_mae)
        mlflow.log_metric("Drift_Detected", int(drift_detected))
        mlflow.log_artifact(report_html, "drift_reports")
        
        # 6. PROMOTION LOGIC
        if challenger_mae < champion_mae:
            print("🎊 CHALLENGER IS BETTER! Promoting to Production...")
            joblib.dump(challenger, "production_model.joblib")
            
            # 6a. Stable Champion Pointer
            upload_to_s3("production_model.joblib", bucket, s3_prod_path)
            
            # 6b. Timestamped Snapshot (Archive)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            snapshot_path = f"models/archive/model_{timestamp}.joblib"
            upload_to_s3("production_model.joblib", bucket, snapshot_path)
            
            mlflow.set_tag("status", "Promoted")
            mlflow.lightgbm.log_model(challenger, "model")
            os.remove("production_model.joblib")
        else:
            print("✋ Champion remains superior. Decline promotion.")
            mlflow.set_tag("status", "Rejected")

    print("✨ Pipeline Complete.")

if __name__ == "__main__":
    train_and_log()
