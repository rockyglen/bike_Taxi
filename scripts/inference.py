import os
import pandas as pd
import lightgbm as lgb
import hopsworks
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

# -----------------------------
# 1. Load credentials & login
# -----------------------------
load_dotenv()
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()
mr = project.get_model_registry()

# -----------------------------
# 2. Load and aggregate trip data
# -----------------------------
fg = fs.get_feature_group("citi_bike_trips", version=1)
df = fg.read()
df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('H')

hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# -----------------------------
# 3. Align to current EST time
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_est = datetime.now(eastern).replace(minute=0, second=0, microsecond=0)
now_utc = now_est.astimezone(pytz.UTC)

# Get last 28 hours before prediction start
cutoff_time = now_utc - timedelta(hours=1)
lag_rows = hourly_df[hourly_df['start_hour'] <= cutoff_time].tail(28)

if lag_rows.shape[0] < 28:
    raise ValueError("❌ Not enough lag data to generate features.")

lag_values = lag_rows['trip_count'].astype("float32").tolist()
current_hour = now_est  # prediction starts 1 hour from now

# -----------------------------
# 4. Load latest LGBM model
# -----------------------------
models = mr.get_models("citi_bike_lgbm_full")
latest_model = sorted(models, key=lambda m: m.version)[-1]
model_dir = latest_model.download()
model = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

# -----------------------------
# 5. Predict next 24 hours
# -----------------------------
predictions = []

for _ in range(24):
    X_input = pd.DataFrame([lag_values[-28:]], columns=[f'lag_{i}' for i in range(1, 29)])
    prediction = model.predict(X_input)[0]
    current_hour += timedelta(hours=1)

    predictions.append({
        'prediction_time': pd.Timestamp.utcnow(),
        'target_hour': current_hour,
        'predicted_trip_count': float(prediction)
    })
    lag_values.append(prediction)

# -----------------------------
# 6. Build predictions DataFrame
# -----------------------------
pred_df = pd.DataFrame(predictions)
pred_df['prediction_time'] = pd.to_datetime(pred_df['prediction_time'])
pred_df['target_hour'] = pd.to_datetime(pred_df['target_hour'])
pred_df['predicted_trip_count'] = pred_df['predicted_trip_count'].astype("float32")

# -----------------------------
# 7. Log to Hopsworks (overwrite mode)
# -----------------------------
pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=3,  # updated version without station-level detail
    primary_key=["prediction_time", "target_hour"],
    description="24-hour Citi Bike trip predictions (no station split)",
    event_time="prediction_time"
)

# ❌ Clear any existing data to overwrite safely
pred_fg.delete_all()

# ✅ Insert fresh predictions
pred_fg.insert(pred_df, write_options={"wait_for_job": True})
print("✅ 24-hour predictions logged to Hopsworks (overwritten).")
