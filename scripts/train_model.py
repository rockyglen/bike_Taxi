"""
train_model.py

Advanced MLOps Pipeline:
1. Data Source: Hopsworks Feature Store.
2. Challenger vs. Champion: Automated model promotion based on MAE.
3. Logging: MLflow (experiment tracking) + Hopsworks Model Registry (weights).
4. Metrics: Saved to S3 as JSON for the Next.js frontend.
"""

import os
import shutil
from datetime import datetime
import json
import math
import uuid
import pandas as pd
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import boto3
import joblib
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
from dotenv import load_dotenv
import hopsworks

# === CONFIG & LOAD ENV ===
load_dotenv()
DATA_FOLDER = "data"
os.makedirs(DATA_FOLDER, exist_ok=True)

# === UTILITIES ===

def upload_to_s3(local_file, bucket, s3_path):
    """Uploads a file to S3 (used only for frontend-serving files)."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3.upload_file(local_file, bucket, s3_path)

def wmape(y_true, y_pred):
    """Weighted MAPE — weights each error by actual volume, so low-traffic hours don't dominate."""
    return float(np.sum(np.abs(y_true - y_pred)) / np.sum(y_true) * 100)

def get_rmse(y_true, y_pred):
    try:
        from sklearn.metrics import root_mean_squared_error
        return root_mean_squared_error(y_true, y_pred)
    except ImportError:
        return mean_squared_error(y_true, y_pred, squared=False)

def connect_to_hopsworks():
    project = hopsworks.login(
        api_key_value=os.getenv('HOPSWORKS_API_KEY'),
        project=os.getenv('HOPSWORKS_PROJECT'),
    )
    return project

def train_and_log():
    bucket = os.getenv('AWS_S3_BUCKET')

    # 1. LOAD FEATURES FROM HOPSWORKS FEATURE STORE
    print(" Connecting to Hopsworks...")
    project = connect_to_hopsworks()
    fs = project.get_feature_store()
    mr = project.get_model_registry()

    print(" Reading features from Hopsworks Feature Store...")
    fg = fs.get_feature_group("forecast_features", version=1)
    df = fg.read()

    # Ensure all columns are standard numpy-compatible types
    for col in df.columns:
        if pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    df = df.sort_values('start_hour').reset_index(drop=True)

    # 2. Split Data (Reference: older 80%, Current: newest 20%)
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]

    lags = [f'lag_{i}' for i in range(1, 29)]
    categorical = ['hour', 'day_of_week', 'is_weekend', 'month']
    features = lags + categorical
    target = 'total_trips'

    # 3. TRAINING (Challenger)
    print(" Training Challenger Model...")
    challenger = lgb.LGBMRegressor(n_estimators=1000, learning_rate=0.05, random_state=42)
    cat_features = ['hour', 'day_of_week', 'is_weekend', 'month']
    challenger.fit(
        train_df[features],
        train_df[target],
        eval_set=[(test_df[features], test_df[target])],
        categorical_feature=cat_features,
        callbacks=[lgb.early_stopping(stopping_rounds=50)]
    )

    challenger_preds = challenger.predict(test_df[features])
    challenger_mae = mean_absolute_error(test_df[target], challenger_preds)
    print(f" Challenger MAE: {challenger_mae:.4f}")

    # 4. LOAD CHAMPION FROM HOPSWORKS MODEL REGISTRY
    champion_mae = float('inf')
    try:
        print(" Fetching champion from Hopsworks Model Registry...")
        champion_meta = mr.get_best_model("demand_forecaster", metric="MAE", direction="min")
        model_dir = champion_meta.download()
        champion = joblib.load(os.path.join(model_dir, "production_model.joblib"))
        champion_preds = champion.predict(test_df[features])
        champion_mae = mean_absolute_error(test_df[target], champion_preds)
        print(f" Champion MAE: {champion_mae:.4f}")
    except Exception as e:
        print(f" No champion found in registry ({e}). Challenger will be promoted automatically.")

    # 5. MLFLOW LOGGING
    promotion_status = "Rejected"
    mlflow_uri = os.getenv('MLFLOW_TRACKING_URI')
    if mlflow_uri:
        mlflow.set_tracking_uri(mlflow_uri)

    with mlflow.start_run(run_name=f"LGBM_Run_{uuid.uuid4().hex[:4]}"):
        mlflow.log_params({
            "challenger_mae": challenger_mae,
            "champion_mae": champion_mae if champion_mae != float('inf') else None,
        })
        mlflow.log_metric("MAE", challenger_mae)

        # 6. PROMOTION LOGIC
        if challenger_mae < champion_mae:
            print(" CHALLENGER IS BETTER! Promoting to Hopsworks Model Registry...")
            promotion_status = "Promoted"

            # Save model to a temp directory and push to Hopsworks
            model_export_dir = os.path.join(DATA_FOLDER, "model_export")
            os.makedirs(model_export_dir, exist_ok=True)
            joblib.dump(challenger, os.path.join(model_export_dir, "production_model.joblib"))

            hw_model = mr.python.create_model(
                name="demand_forecaster",
                metrics={"MAE": challenger_mae},
                description="LightGBM demand forecaster — Citi Bike top-3 stations",
            )
            hw_model.save(model_export_dir)
            shutil.rmtree(model_export_dir)
            print(" Challenger promoted to Hopsworks Model Registry.")

            mlflow.set_tag("status", "Promoted")
            try:
                mlflow.lightgbm.log_model(challenger, "model")
            except Exception as log_model_err:
                print(f" mlflow.log_model skipped (tracking server error): {log_model_err}")
        else:
            print(" Champion remains superior. Declining promotion.")
            mlflow.set_tag("status", "Rejected")

    # 7. SAVE EVALUATION METRICS TO S3 (frontend reads this)
    print(" Computing model evaluation metrics...")
    challenger_rmse = math.sqrt(mean_squared_error(test_df[target], challenger_preds))
    try:
        challenger_mape = wmape(test_df[target].values, challenger_preds)
    except Exception:
        challenger_mape = None

    fi = dict(zip(features, map(float, challenger.feature_importances_)))
    top_features = dict(sorted(fi.items(), key=lambda x: x[1], reverse=True)[:10])
    total_fi = sum(top_features.values()) or 1
    top_features_pct = {k: round(v / total_fi * 100, 1) for k, v in top_features.items()}

    metrics_payload = {
        "mae": round(float(challenger_mae), 4),
        "rmse": round(float(challenger_rmse), 4),
        "mape": round(float(challenger_mape), 2) if challenger_mape is not None else None,  # WMAPE (volume-weighted)
        "champion_mae": round(float(champion_mae), 4) if champion_mae != float('inf') else None,
        "n_train": len(train_df),
        "n_test": len(test_df),
        "n_features": len(features),
        "n_estimators": challenger.best_iteration_ if hasattr(challenger, 'best_iteration_') else challenger.n_estimators,
        "top_features": top_features_pct,
        "promotion_status": promotion_status,
        "run_date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    metrics_local = os.path.join(DATA_FOLDER, "model_metrics.json")
    with open(metrics_local, "w") as f:
        json.dump(metrics_payload, f, indent=2)

    if bucket:
        upload_to_s3(metrics_local, bucket, "models/model_metrics.json")
        print(f" Metrics uploaded to S3: MAE={metrics_payload['mae']} | Status={promotion_status}")
    else:
        print(f" AWS_S3_BUCKET not set. Metrics saved locally only.")

    print(" Pipeline Complete.")

if __name__ == "__main__":
    train_and_log()
