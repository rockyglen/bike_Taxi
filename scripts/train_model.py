import pandas as pd
import numpy as np
import hopsworks
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error
import os
from dotenv import load_dotenv

load_dotenv()

# Hopsworks + DagsHub setup
project = hopsworks.login(api_key_value=os.getenv("HOPSWORKS_API_KEY"), project=os.getenv("HOPSWORKS_PROJECT"))
fs = project.get_feature_store()
fg = fs.get_feature_group(name="citi_bike_hourly_features", version=1)
df = fg.read()

# Aggregate
df['start_hour'] = pd.to_datetime(df['start_hour'])
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# Create 28 lag features
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag)
hourly_df = hourly_df.dropna().reset_index(drop=True)

# Train/test split
split = int(len(hourly_df) * 0.8)
train, test = hourly_df.iloc[:split], hourly_df.iloc[split:]

X_train = train[[f'lag_{i}' for i in range(1, 29)]]
y_train = train['trip_count']
X_test = test[[f'lag_{i}' for i in range(1, 29)]]
y_test = test['trip_count']

# LightGBM
model = lgb.LGBMRegressor()
model.fit(X_train, y_train)
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)

# MLflow to DagsHub
mlflow.set_tracking_uri(f"https://{os.getenv('DAGSHUB_USERNAME')}:{os.getenv('DAGSHUB_TOKEN')}@dagshub.com/{os.getenv('DAGSHUB_USERNAME')}/{os.getenv('DAGSHUB_REPO_NAME')}.mlflow")

with mlflow.start_run(run_name="LightGBM_28_Lags"):
    mlflow.log_param("model_type", "LightGBM")
    mlflow.log_param("lags_used", 28)
    mlflow.log_metric("MAE", mae)
    mlflow.lightgbm.log_model(model, artifact_path="model")
    print(f"🚀 Model trained and logged. MAE: {mae:.2f}")
