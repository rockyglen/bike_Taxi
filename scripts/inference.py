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
# 2. Load trip data
# -----------------------------
fg = fs.get_feature_group("citi_bike_trips", version=1)
df = fg.read()

df['start_hour'] = pd.to_datetime(df['start_hour'])
df = df.sort_values(['start_station_name', 'start_hour'])

# Get top 3 stations
top3_stations = df['start_station_name'].value_counts().nlargest(3).index.tolist()

# -----------------------------
# 3. Prediction block
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_est = datetime.now(eastern).replace(minute=0, second=0, microsecond=0)

all_predictions = []

for station in top3_stations:
    print(f"🔮 Predicting for station: {station}")
    
    station_df = df[df['start_station_name'] == station].copy()
    station_df = station_df[station_df['start_hour'] <= now_est - timedelta(hours=1)]
    station_df = station_df.sort_values('start_hour')
    
    lag_series = station_df['trip_count'].tail(28)
    
    if lag_series.shape[0] < 28:
        print(f"⚠️ Skipping {station}: not enough lag data.")
        continue

    lag_values = lag_series.tolist()
    current_hour = now_est

    # -----------------------------
    # Load latest station-specific model
    # -----------------------------
    model_name = f"citi_bike_lgbm_{station.replace(' ', '_').lower()}"
    models = mr.get_models(model_name)
    if not models:
        print(f"⚠️ No model found for {station}. Skipping.")
        continue

    latest_model = sorted(models, key=lambda m: m.version)[-1]
    model_dir = latest_model.download()
    model = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

    # -----------------------------
    # Predict next 168 hours
    # -----------------------------
    for _ in range(168):
        X_input = pd.DataFrame([lag_values[-28:]], columns=[f'lag_{i}' for i in range(1, 29)])
        prediction = model.predict(X_input)[0]
        current_hour += timedelta(hours=1)

        all_predictions.append({
            'prediction_time': datetime.now(eastern),
            'target_hour': current_hour,
            'predicted_trip_count': prediction,
            'start_station_name': station
        })
        lag_values.append(prediction)

# -----------------------------
# 4. Insert into Hopsworks
# -----------------------------
pred_df = pd.DataFrame(all_predictions)

if pred_df.empty:
    raise RuntimeError("❌ No predictions were made. Check lag data or model availability.")

pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=2,
    primary_key=["prediction_time", "start_station_name"],
    description="Predicted Citi Bike trips for next 168 hours by top 3 stations"
)

pred_fg.insert(pred_df, overwrite=True)
print("✅ Per-station predictions logged to Hopsworks.")
