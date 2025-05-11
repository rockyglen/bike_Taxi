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

# Get the last 28 hours of actual data (before "now")
cutoff_time = now_utc - timedelta(hours=1)
lag_rows = hourly_df[hourly_df['start_hour'] <= cutoff_time].tail(28)

if lag_rows.shape[0] < 28:
    raise ValueError("Not enough lag data to predict from current time.")

lag_values = lag_rows['trip_count'].tolist()
current_hour = now_est  # start predicting from the next hour in EST

# -----------------------------
# 4. Load latest model
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
        'predicted_trip_count': prediction
    })
    lag_values.append(prediction)

print(f"📈 Predicted trip count for next 24 hours starting from {predictions[0]['target_hour']}:")
for p in predictions:
    print(f"{p['target_hour']}: {p['predicted_trip_count']:.2f}")

# -----------------------------
# 6. Duplicate predictions for top 3 stations
# -----------------------------
top_stations = df['start_station_name'].value_counts().nlargest(3).index.tolist()

extended_predictions = []
for station in top_stations:
    for record in predictions:
        extended_predictions.append({
            'prediction_time': record['prediction_time'],
            'target_hour': record['target_hour'],
            'predicted_trip_count': record['predicted_trip_count'],
            'start_station_name': station
        })

pred_df = pd.DataFrame(extended_predictions)

# -----------------------------
# 7. Log predictions to feature store (using new version)
# -----------------------------
pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=2,  # New version to include "start_station_name"
    primary_key=["prediction_time", "start_station_name"],
    description="Predicted Citi Bike trips for next 24 hours by top 3 stations"
)

pred_fg.insert(pred_df)
print("✅ 24-hour predictions (duplicated for top 3 stations) logged to Hopsworks.")
