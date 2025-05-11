import os
import pandas as pd
import hopsworks
import lightgbm as lgb
from dotenv import load_dotenv
from datetime import timedelta

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
# 2. Load hourly data from trips
# -----------------------------
fg = fs.get_feature_group("citi_bike_trips", version=1)
df = fg.read()
df['start_hour'] = pd.to_datetime(df['started_at']).dt.floor('H')

hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# -----------------------------
# 3. Create 28 lag features
# -----------------------------
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag)

latest = hourly_df.dropna().iloc[-1:].copy()
lag_values = latest[[f'lag_{i}' for i in range(1, 29)]].values.flatten().tolist()
current_hour = pd.to_datetime(latest['start_hour'].values[0])

# -----------------------------
# 4. Load latest model version
# -----------------------------
models = mr.get_models("citi_bike_lgbm_full")
latest_model = sorted(models, key=lambda m: m.version)[-1]
model_dir = latest_model.download()
model = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

# -----------------------------
# 5. Make 24-hour predictions
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
# 6. Log predictions to feature store
# -----------------------------
pred_df = pd.DataFrame(predictions)

pred_fg = fs.get_or_create_feature_group(
    name="citi_bike_predictions",
    version=1,
    primary_key=["prediction_time"],
    description="Predicted Citi Bike trips for next hour"
)

pred_fg.insert(pred_df)
print("✅ 24-hour predictions logged to Hopsworks.")
