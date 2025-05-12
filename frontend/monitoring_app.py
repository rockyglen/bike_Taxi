import os
import pandas as pd
import hopsworks
import streamlit as st
import altair as alt
from datetime import datetime
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error

# -----------------------------
# 🌐 Streamlit Setup
# -----------------------------
st.set_page_config(page_title="Citi Bike Model Monitoring", layout="wide")
st.title("📊 Citi Bike Model Monitoring Dashboard")
st.markdown("Monitoring prediction accuracy, drift, and trends in **Eastern Time (US/Eastern)**.")

# -----------------------------
# 🔐 Hopsworks Login
# -----------------------------
@st.cache_resource
def connect_to_hopsworks():
    load_dotenv()
    project = hopsworks.login(
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
        project=os.getenv("HOPSWORKS_PROJECT")
    )
    return project.get_feature_store()

fs = connect_to_hopsworks()

# -----------------------------
# 📥 Load Predictions
# -----------------------------
@st.cache_data(ttl=1800)
def load_predictions():
    fg = fs.get_feature_group("citi_bike_predictions", version=3)
    df = fg.read()
    df["target_hour"] = pd.to_datetime(df["target_hour"]).dt.floor("H")
    df["prediction_time"] = pd.to_datetime(df["prediction_time"])
    df["predicted_trip_count"] = df["predicted_trip_count"].astype("float32")

    if df["target_hour"].dt.tz is None:
        df["target_hour"] = df["target_hour"].dt.tz_localize("UTC")
    else:
        df["target_hour"] = df["target_hour"].dt.tz_convert("UTC")
    df["target_hour"] = df["target_hour"].dt.tz_convert("US/Eastern")

    if df["prediction_time"].dt.tz is None:
        df["prediction_time"] = df["prediction_time"].dt.tz_localize("UTC")
    else:
        df["prediction_time"] = df["prediction_time"].dt.tz_convert("UTC")
    df["prediction_time"] = df["prediction_time"].dt.tz_convert("US/Eastern")

    return df.sort_values("target_hour")

# -----------------------------
# 📥 Load Actuals
# -----------------------------
@st.cache_data(ttl=1800)
def load_actuals():
    fg = fs.get_feature_group("citi_bike_trips", version=1)
    df = fg.read()
    df["start_hour"] = pd.to_datetime(df["started_at"]).dt.floor("H")

    if df["start_hour"].dt.tz is None:
        df["start_hour"] = df["start_hour"].dt.tz_localize("UTC")
    else:
        df["start_hour"] = df["start_hour"].dt.tz_convert("UTC")
    df["start_hour"] = df["start_hour"].dt.tz_convert("US/Eastern")

    actual_df = df.groupby("start_hour").size().reset_index(name="actual_trip_count")
    return actual_df.sort_values("start_hour")

# -----------------------------
# 📊 Load & Preview Data
# -----------------------------
pred_df = load_predictions()
actual_df = load_actuals()

# -----------------------------
# 🧪 Timestamp Diagnostics
# -----------------------------
st.subheader("🔍 Timestamp Alignment Diagnostics")

st.markdown("**Prediction Range:**")
st.write(f"Start: `{pred_df['target_hour'].min()}`")
st.write(f"End:   `{pred_df['target_hour'].max()}`")

st.markdown("**Actual Range:**")
st.write(f"Start: `{actual_df['start_hour'].min()}`")
st.write(f"End:   `{actual_df['start_hour'].max()}`")

st.markdown("**Last 5 Prediction Rows:**")
st.dataframe(pred_df[["target_hour"]].tail())

st.markdown("**Last 5 Actual Rows:**")
st.dataframe(actual_df[["start_hour"]].tail())

# -----------------------------
# 🤝 Merge Predictions + Actuals
# -----------------------------
merged = pd.merge(
    pred_df,
    actual_df,
    left_on="target_hour",
    right_on="start_hour",
    how="inner"
)

# Fallback to fuzzy merge
if merged.empty:
    st.warning("⚠️ Exact merge failed. Trying fuzzy merge with 1-minute tolerance...")
    merged = pd.merge_asof(
        pred_df.sort_values("target_hour"),
        actual_df.sort_values("start_hour"),
        left_on="target_hour",
        right_on="start_hour",
        tolerance=pd.Timedelta("1min"),
        direction="nearest"
    ).dropna()

# If still no overlap, stop
if merged.empty:
    st.error("❌ No overlapping prediction and actual data found. Check timestamps and feature groups.")
    st.stop()

# -----------------------------
# 📊 Data Prep
# -----------------------------
merged = merged[["target_hour", "predicted_trip_count", "actual_trip_count"]]
merged["error"] = merged["predicted_trip_count"] - merged["actual_trip_count"]
merged["abs_error"] = merged["error"].abs()
merged["hour"] = merged["target_hour"].dt.strftime("%Y-%m-%d %H:%M")

# -----------------------------
# 📈 Metrics
# -----------------------------
mae = mean_absolute_error(merged["actual_trip_count"], merged["predicted_trip_count"])
max_error_hour = merged.iloc[merged["abs_error"].idxmax()]["hour"]
max_abs_error = merged["abs_error"].max()

col1, col2, col3 = st.columns(3)
col1.metric("Mean Absolute Error", f"{mae:.2f}")
col2.metric("Max Error Hour", max_error_hour)
col3.metric("Max Absolute Error", f"{int(max_abs_error)}")

# -----------------------------
# 📉 Line Chart
# -----------------------------
st.subheader("📉 Prediction vs. Actuals (US/Eastern)")

chart = alt.Chart(merged).transform_fold(
    ["predicted_trip_count", "actual_trip_count"],
    as_=["Type", "Trip Count"]
).mark_line(point=True).encode(
    x=alt.X("target_hour:T", title="Hour (EST)"),
    y=alt.Y("Trip Count:Q"),
    color=alt.Color("Type:N", title="Source"),
    tooltip=["hour", "Trip Count", "Type"]
).properties(width=900, height=400)

st.altair_chart(chart, use_container_width=True)

# -----------------------------
# 📊 Error Histogram
# -----------------------------
st.subheader("🧪 Absolute Error Distribution")

hist = alt.Chart(merged).mark_bar().encode(
    x=alt.X("abs_error:Q", bin=alt.Bin(maxbins=30), title="Absolute Error"),
    y=alt.Y("count():Q", title="Frequency"),
    tooltip=["count()"]
).properties(width=700, height=300)

st.altair_chart(hist)

# -----------------------------
# 🔍 Raw Table
# -----------------------------
with st.expander("🔍 View Merged Data Table"):
    st.dataframe(merged, use_container_width=True)

# -----------------------------
# 🧭 Footer
# -----------------------------
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit + Hopsworks + Altair. All timestamps shown in **US/Eastern**.")
