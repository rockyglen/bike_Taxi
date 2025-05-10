import os
import pandas as pd
import hopsworks
import lightgbm as lgb
from dotenv import load_dotenv
from datetime import timedelta

# -----------------------------
# 1. Setup
# -----------------------------
load_dotenv()

project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)

fs = project.get_feature_store()
mr = project.get_model_registry()

# -----------------------------
# 2. Read recent data (hourly trip counts)
# -----------------------------
fg = fs.get_feature_group(name="citi_bike_trips", version=1)
df = fg.read()
df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('H')

hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# -----------------------------
# 3. Create 28 lag features from most recent data
# -----------------------------
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag)

latest = hourly_df.dropna().iloc[-1:]  # last row with full lag window
X_latest = latest[[f'lag_{i}' for i in range(1, 29)]]
next_hour = latest['start_hour'].values[0] + timedelta(hours=1)

# -----------------------------
# 4. Load latest model
# -----------------------------
model_obj = mr.get_model("citi_bike_lgbm_full", version=2)  # or use .get_latest_version()
model_dir = model_obj.download()
model = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

# -----------------------------
# 5. Predict
# -----------------------------
prediction = model.predict(X_latest)[0]
print(f"📈 Predicted trips for {next_hour}: {prediction:.2f}")

# -----------------------------
# 6. Log prediction to feature group
# -----------------------------
pred_df = pd.DataFrame({
    'prediction_time': [pd.Timestamp.utcnow()],
    'target_hour': [next_hour],
    'predicted_trip_count': [prediction]
})

pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=1,
    description="Predicted Citi Bike trips for next hour",
    primary_key=["prediction_time"]
)

pred_fg.insert(pred_df)
print("✅ Prediction logged to Hopsworks.")
