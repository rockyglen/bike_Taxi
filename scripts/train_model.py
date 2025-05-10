import os
import pandas as pd
import numpy as np
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import hopsworks
import shutil
from sklearn.metrics import mean_absolute_error
from dotenv import load_dotenv

# --------------------
# ENV + Login
# --------------------
load_dotenv()

project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()
mr = project.get_model_registry()

# --------------------
# Load and aggregate data
# --------------------
fg = fs.get_feature_group(name="citi_bike_trips", version=1)
df = fg.read()

df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('H')
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df['hour_of_day'] = hourly_df['start_hour'].dt.hour
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# --------------------
# Baseline model
# --------------------
split_idx = int(len(hourly_df) * 0.8)
train = hourly_df.iloc[:split_idx]
test = hourly_df.iloc[split_idx:]

mean_per_hour = train.groupby('hour_of_day')['trip_count'].mean()
test['predicted'] = test['hour_of_day'].map(mean_per_hour)
baseline_mae = mean_absolute_error(test['trip_count'], test['predicted'])

# --------------------
# Log to DagsHub MLflow
# --------------------
mlflow.set_tracking_uri(
    f"https://{os.getenv('DAGSHUB_USERNAME')}:{os.getenv('DAGSHUB_TOKEN')}@dagshub.com/{os.getenv('DAGSHUB_USERNAME')}/{os.getenv('DAGSHUB_REPO_NAME')}.mlflow"
)

with mlflow.start_run(run_name="Baseline_Mean_Per_Hour"):
    mlflow.log_param("model_type", "mean_per_hour")
    mlflow.log_param("features_used", "hour_of_day")
    mlflow.log_metric("MAE", baseline_mae)
    test[['start_hour', 'trip_count', 'predicted']].to_csv("baseline_preds.csv", index=False)
    mlflow.log_artifact("baseline_preds.csv")

# --------------------
# Full lag LightGBM
# --------------------
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

with mlflow.start_run(run_name="LightGBM_28_Lags"):
    mlflow.log_param("model_type", "LightGBM")
    mlflow.log_param("lags_used", 28)
    mlflow.log_metric("MAE", lag_mae)
    mlflow.lightgbm.log_model(model, artifact_path="model")
    test[['start_hour', 'trip_count']].assign(predicted=preds).to_csv("lgbm_preds.csv", index=False)
    mlflow.log_artifact("lgbm_preds.csv")

# --------------------
# Feature-reduced model (top 10 features)
# --------------------
importances = model.feature_importances_
top10 = [f'lag_{i}' for i, _ in sorted(enumerate(importances, 1), key=lambda x: x[1], reverse=True)[:10]]

X_train_red = train[top10]
X_test_red = test[top10]

model_red = lgb.LGBMRegressor()
model_red.fit(X_train_red, y_train)
preds_red = model_red.predict(X_test_red)
reduced_mae = mean_absolute_error(y_test, preds_red)

with mlflow.start_run(run_name="LightGBM_Feature_Reduced"):
    mlflow.log_param("model_type", "LightGBM")
    mlflow.log_param("features_used", str(top10))
    mlflow.log_metric("MAE", reduced_mae)
    mlflow.lightgbm.log_model(model_red, artifact_path="model")
    test[['start_hour', 'trip_count']].assign(predicted=preds_red).to_csv("reduced_preds.csv", index=False)
    mlflow.log_artifact("reduced_preds.csv")

# --------------------
# Register models to Hopsworks
# --------------------
print("📦 Registering models to Hopsworks...")

# 1. Full lag model
dir1 = "model_lag"
os.makedirs(dir1, exist_ok=True)
model.booster_.save_model(f"{dir1}/model.txt")

mr.python.create_model(
    name="citi_bike_lgbm_full",
    metrics={"mae": lag_mae},
    description="LGBM with 28 lag features"
).save(dir1)
shutil.rmtree(dir1)

# 2. Feature-reduced model
dir2 = "model_reduced"
os.makedirs(dir2, exist_ok=True)
model_red.booster_.save_model(f"{dir2}/model.txt")

mr.python.create_model(
    name="citi_bike_lgbm_reduced",
    metrics={"mae": reduced_mae},
    description="LGBM with top 10 lag features"
).save(dir2)
shutil.rmtree(dir2)

# 3. Baseline model (as CSV logic)
dir3 = "model_baseline"
os.makedirs(dir3, exist_ok=True)
mean_per_hour.to_csv(f"{dir3}/mean_per_hour.csv")

mr.python.create_model(
    name="citi_bike_baseline_model",
    metrics={"mae": baseline_mae},
    description="Mean trips per hour-of-day"
).save(dir3)
shutil.rmtree(dir3)

print("✅ All models logged and registered to Hopsworks.")
