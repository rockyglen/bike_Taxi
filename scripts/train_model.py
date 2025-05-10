import pandas as pd
import numpy as np
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
from dotenv import load_dotenv
import os
import hopsworks
import shutil
import json

# ---------------------
# Load secrets
# ---------------------
load_dotenv()

# ---------------------
# Connect to Hopsworks
# ---------------------
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()
mr = project.get_model_registry()
fg = fs.get_feature_group(name="citi_bike_hourly_features", version=1)
df = fg.read()

# ---------------------
# Preprocess hourly data
# ---------------------
df['start_hour'] = pd.to_datetime(df['start_hour'])
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df['hour_of_day'] = hourly_df['start_hour'].dt.hour
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# ---------------------
# Baseline Model — Mean per Hour
# ---------------------
split_idx = int(len(hourly_df) * 0.8)
train = hourly_df.iloc[:split_idx]
test = hourly_df.iloc[split_idx:]

mean_per_hour = train.groupby('hour_of_day')['trip_count'].mean()
test['predicted'] = test['hour_of_day'].map(mean_per_hour)
baseline_mae = mean_absolute_error(test['trip_count'], test['predicted'])
print(f"📉 Baseline MAE: {baseline_mae:.2f}")

# ---------------------
# Save Baseline Model (as lookup + logic)
# ---------------------
baseline_dir = "baseline_model"
os.makedirs(baseline_dir, exist_ok=True)
mean_per_hour.to_csv(f"{baseline_dir}/mean_per_hour.csv")

with open(f"{baseline_dir}/metadata.json", "w") as f:
    json.dump({
        "model_type": "mean_per_hour",
        "features_used": "hour_of_day",
        "mae": baseline_mae
    }, f)

baseline_model = mr.python.create_model(
    name="citi_bike_baseline_model",
    metrics={"mae": baseline_mae},
    description="Baseline model using mean per hour-of-day"
)
baseline_model.save(baseline_dir)
shutil.rmtree(baseline_dir)
print("✅ Baseline model registered to Hopsworks.")

# ---------------------
# Full Lag Model (LightGBM 28 lags)
# ---------------------
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag)
hourly_df = hourly_df.dropna().reset_index(drop=True)

split_idx = int(len(hourly_df) * 0.8)
train = hourly_df.iloc[:split_idx]
test = hourly_df.iloc[split_idx:]

X_train = train[[f'lag_{i}' for i in range(1, 29)]]
y_train = train['trip_count']
X_test = test[[f'lag_{i}' for i in range(1, 29)]]
y_test = test['trip_count']

model = lgb.LGBMRegressor()
model.fit(X_train, y_train)
preds = model.predict(X_test)
lag_mae = mean_absolute_error(y_test, preds)
print(f"🚀 LightGBM (28 lags) MAE: {lag_mae:.2f}")

# ---------------------
# Register Full Lag Model
# ---------------------
lag_model_dir = "lag_model"
os.makedirs(lag_model_dir, exist_ok=True)
model.booster_.save_model(f"{lag_model_dir}/model.txt")

lag_model = mr.python.create_model(
    name="citi_bike_lgbm_full",
    metrics={"mae": lag_mae},
    description="LightGBM model using 28 hourly lag features"
)
lag_model.save(lag_model_dir)
shutil.rmtree(lag_model_dir)
print("✅ Full lag model registered to Hopsworks.")

# ---------------------
# Feature-Reduced Model (Top 10 lag features)
# ---------------------
importances = model.feature_importances_
top10 = [f'lag_{i}' for i, _ in sorted(enumerate(importances, 1), key=lambda x: x[1], reverse=True)[:10]]

X_train_red = train[top10]
X_test_red = test[top10]

model_red = lgb.LGBMRegressor()
model_red.fit(X_train_red, y_train)
preds_red = model_red.predict(X_test_red)
reduced_mae = mean_absolute_error(y_test, preds_red)
print(f"⚡ Feature-Reduced MAE: {reduced_mae:.2f}")

# ---------------------
# Register Feature-Reduced Model
# ---------------------
reduced_model_dir = "reduced_model"
os.makedirs(reduced_model_dir, exist_ok=True)
model_red.booster_.save_model(f"{reduced_model_dir}/model.txt")

reduced_model = mr.python.create_model(
    name="citi_bike_lgbm_reduced",
    metrics={"mae": reduced_mae},
    description="LightGBM model using top 10 lag features by importance"
)
reduced_model.save(reduced_model_dir)
shutil.rmtree(reduced_model_dir)
print("✅ Feature-reduced model registered to Hopsworks.")
