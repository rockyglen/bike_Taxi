import os
import pandas as pd
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import hopsworks
import shutil
import uuid
from sklearn.metrics import mean_absolute_error
from dotenv import load_dotenv

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
# 📦 Load feature group data
# -----------------------------
fg = fs.get_feature_group(name="citi_bike_trips", version=1)
df = fg.read()

# -----------------------------
# 🧹 Preprocess data
# -----------------------------
df['start_hour'] = pd.to_datetime(df['started_at'], errors="coerce").dt.floor('H')
df = df.dropna(subset=['start_hour'])
hourly_df = df.groupby('start_hour').size().reset_index(name='trip_count')
hourly_df['hour_of_day'] = hourly_df['start_hour'].dt.hour.astype("int32")
hourly_df = hourly_df.sort_values('start_hour').reset_index(drop=True)

# -----------------------------
# 🧠 Create lag features
# -----------------------------
for lag in range(1, 29):
    hourly_df[f'lag_{lag}'] = hourly_df['trip_count'].shift(lag).astype("float32")

hourly_df = hourly_df.dropna().reset_index(drop=True)

# -----------------------------
# ✂️ Train/Test Split
# -----------------------------
split_idx = int(len(hourly_df) * 0.8)
train = hourly_df.iloc[:split_idx]
test = hourly_df.iloc[split_idx:]

X_train = train[[f'lag_{i}' for i in range(1, 29)]]
y_train = train['trip_count']
X_test = test[[f'lag_{i}' for i in range(1, 29)]]
y_test = test['trip_count']

# -----------------------------
# 🚀 Train model
# -----------------------------
model = lgb.LGBMRegressor(random_state=42)
model.fit(X_train, y_train)
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
print(f"🚀 LightGBM (28 lags) MAE: {mae:.2f}")

# -----------------------------
# 📝 Log to MLflow
# -----------------------------
mlflow.set_tracking_uri(
    f"https://{os.getenv('DAGSHUB_USERNAME')}:{os.getenv('DAGSHUB_TOKEN')}@dagshub.com/{os.getenv('DAGSHUB_USERNAME')}/{os.getenv('DAGSHUB_REPO_NAME')}.mlflow"
)

version_tag = str(uuid.uuid4())[:8]
with mlflow.start_run(run_name="LightGBM_28_Lags"):
    mlflow.set_tag("version_tag", version_tag)
    mlflow.log_param("model_type", "LightGBM")
    mlflow.log_param("lags_used", 28)
    mlflow.log_param("train_rows", len(train))
    mlflow.log_param("test_rows", len(test))
    mlflow.log_metric("MAE", mae)
    mlflow.lightgbm.log_model(model, artifact_path="model")

# -----------------------------
# 🗃️ Register model in Hopsworks
# -----------------------------
model_dir = "full_lag_model_dir"
os.makedirs(model_dir, exist_ok=True)
model.booster_.save_model(f"{model_dir}/model.txt")

mr.python.create_model(
    name="citi_bike_lgbm_full",
    metrics={"mae": mae},
    description="LGBM with 28 lag features (clean)"
).save(model_dir)

shutil.rmtree(model_dir)
print("✅ Model training, logging, and registration complete.")
