import pandas as pd
import hopsworks
import mlflow
import lightgbm as lgb
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Hopsworks login
project = hopsworks.login(api_key_value=os.getenv("HOPSWORKS_API_KEY"), project=os.getenv("HOPSWORKS_PROJECT"))
fs = project.get_feature_store()
fg = fs.get_feature_group(name="citi_bike_hourly_features", version=1)

# Load and aggregate
df = fg.read()
df['start_hour'] = pd.to_datetime(df['start_hour'])
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# Lag features
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag)
hourly_df = hourly_df.dropna().reset_index(drop=True)

# Latest row
latest = hourly_df.tail(1)
X_latest = latest[[f'lag_{i}' for i in range(1, 29)]]

# Load best model
mlflow.set_tracking_uri(f"https://{os.getenv('DAGSHUB_USERNAME')}:{os.getenv('DAGSHUB_TOKEN')}@dagshub.com/{os.getenv('DAGSHUB_USERNAME')}/{os.getenv('DAGSHUB_REPO_NAME')}.mlflow")
model = mlflow.lightgbm.load_model("models:/LightGBM_28_Lags/Production")
prediction = model.predict(X_latest)[0]

# Store prediction to Hopsworks
pred_df = pd.DataFrame({
    "timestamp": [latest["start_hour"].values[0] + np.timedelta64(1, 'h')],
    "predicted_trip_count": [prediction]
})

pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=1,
    primary_key=["timestamp"],
    description="Predicted trip counts from best model"
)

pred_fg.insert(pred_df, overwrite=False)
print(f"🧠 Predicted next hour trips: {prediction:.2f}")
