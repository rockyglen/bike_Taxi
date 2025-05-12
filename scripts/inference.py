import os
import pandas as pd
import lightgbm as lgb
import hopsworks
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

# -----------------------------
# 🔐 Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# 🔗 Connect to Hopsworks
# -----------------------------
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()
mr = project.get_model_registry()

# -----------------------------
# 📦 Load raw trip data
# -----------------------------
fg = fs.get_feature_group("citi_bike_trips", version=1)
df = fg.read()

# -----------------------------
# 🧹 Preprocess to get hourly trip count
# -----------------------------
df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('H')
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# -----------------------------
# 🕒 Time reference
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_est = datetime.now(eastern).replace(minute=0, second=0, microsecond=0)
prediction_time = now_est  # fixed per run

# Filter only up to now - 1 hour
hourly_df = hourly_df[hourly_df['start_hour'] <= now_est - timedelta(hours=1)]
print(f"✅ Data available for prediction: {hourly_df.shape[0]} rows")

# -----------------------------
# 📉 Get last 28 lag values
# -----------------------------
lag_series = hourly_df['trip_count'].tail(28)
if lag_series.shape[0] < 28:
    raise ValueError("❌ Not enough lag data to run inference.")

lag_values = lag_series.tolist()
current_hour = now_est

# -----------------------------
# 📥 Load global model
# -----------------------------
models = mr.get_models("citi_bike_lgbm_full")
if not models:
    raise RuntimeError("❌ No model found with name 'citi_bike_lgbm_full'.")

latest_model = sorted(models, key=lambda m: m.version)[-1]
model_dir = latest_model.download()
model = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

# -----------------------------
# 🔮 Predict next 168 hours
# -----------------------------
all_predictions = []
for _ in range(168):
    X_input = pd.DataFrame([lag_values[-28:]], columns=[f'lag_{i}' for i in range(1, 29)])
    prediction = model.predict(X_input)[0]
    current_hour += timedelta(hours=1)

    all_predictions.append({
        'prediction_time': prediction_time,
        'target_hour': current_hour,
        'predicted_trip_count': prediction
    })
    lag_values.append(prediction)

pred_df = pd.DataFrame(all_predictions)

if pred_df.empty:
    raise RuntimeError("❌ Prediction DataFrame is empty.")

# -----------------------------
# 🧨 Delete and recreate feature group (Batch mode, offline-only, no Kafka)
# -----------------------------
fg_name = "citi_bike_predictions_global"
fg_version = 1

# Delete existing FG if it exists
try:
    old_fg = fs.get_feature_group(fg_name, version=fg_version)
    old_fg.delete()
    print(f"⚠️ Deleted existing feature group: {fg_name}_v{fg_version}")
except:
    print(f"✅ No existing feature group found for deletion")

# ✅ Recreate FG in strict batch mode with event_time to bypass Kafka
pred_fg = fs.create_feature_group(
    name=fg_name,
    version=fg_version,
    primary_key=["prediction_time"],
    description="Global forecast of Citi Bike trip counts for next 168 hours",
    online_enabled=False,
    event_time="prediction_time"  # ✅ THIS FIX IS CRITICAL
)

# -----------------------------
# 🗃️ Insert into offline store
# -----------------------------
pred_fg.insert(
    pred_df,
    overwrite=True,
    write_options={"storage": "offline", "external": False, "wait_for_job": True}
)

print("✅ Global predictions successfully logged to offline feature store.")
