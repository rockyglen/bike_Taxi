import os
import pandas as pd
import lightgbm as lgb
import hopsworks
from datetime import datetime, timedelta
from dotenv import load_dotenv
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
# ⏱️ Time setup (EST)
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now = datetime.now(eastern).replace(minute=0, second=0, microsecond=0)
start_prediction_hour = now + timedelta(hours=1)

# -----------------------------
# 🧠 Load latest model
# -----------------------------
model = mr.get_model("citi_bike_lgbm_full", version=None)
model_dir = model.download()
booster = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))

# -----------------------------
# 📥 Load recent trip data
# -----------------------------
fg = fs.get_feature_group("citi_bike_trips", version=1)
df = fg.read()

df["start_hour"] = pd.to_datetime(df["started_at"], errors="coerce").dt.floor("H")
hourly_df = df.groupby("start_hour").size().reset_index(name="trip_count")
hourly_df = hourly_df.sort_values("start_hour").reset_index(drop=True)

# -----------------------------
# 🔁 Build 28-lag rolling window
# -----------------------------
latest_window = hourly_df.tail(28).copy()
if len(latest_window) < 28:
    raise ValueError("❌ Not enough history to compute lag features.")

lags = list(latest_window["trip_count"].astype("float32").values)
predictions = []
target_hours = []

for step in range(24):  # predict next 24 hours
    X = pd.DataFrame([lags[-28:]], columns=[f"lag_{i}" for i in range(1, 29)])
    pred = booster.predict(X)[0]
    prediction_time = start_prediction_hour + timedelta(hours=step)
    predictions.append(float(pred))
    target_hours.append(prediction_time)
    lags.append(pred)

# -----------------------------
# 📄 Build output DataFrame
# -----------------------------
pred_df = pd.DataFrame({
    "prediction_time": [now] * 24,
    "target_hour": target_hours,
    "predicted_trip_count": predictions
})

# Enforce safe types
pred_df["prediction_time"] = pd.to_datetime(pred_df["prediction_time"])
pred_df["target_hour"] = pd.to_datetime(pred_df["target_hour"])
pred_df["predicted_trip_count"] = pred_df["predicted_trip_count"].astype("float32")

# -----------------------------
# 🗃️ Create or reuse FG
# -----------------------------
pred_fg_name = "citi_bike_predictions"
pred_fg_version = 2

try:
    pred_fg = fs.get_feature_group(name=pred_fg_name, version=pred_fg_version)
    print("📦 Using existing prediction feature group.")
except:
    print("🛠️ Creating new prediction feature group...")
    from hsfs.feature_group import FeatureGroup

    pred_fg = FeatureGroup(
        name=pred_fg_name,
        version=pred_fg_version,
        description="24-hour trip count predictions using LGBM",
        primary_key=["prediction_time", "target_hour"],
        event_time="prediction_time",
        online_enabled=False
    )
    fs.create_feature_group(pred_fg)
    pred_fg.save(pred_df)
    print("✅ Feature group created and schema saved.")

# -----------------------------
# 📤 Insert predictions
# -----------------------------
try:
    pred_fg.insert(pred_df, write_options={"wait_for_job": True})
    print("✅ Predictions inserted into Hopsworks.")
except Exception as e:
    print(f"❌ Insertion failed: {e}")
