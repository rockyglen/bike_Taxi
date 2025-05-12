import os
import pandas as pd
import hopsworks
import streamlit as st
import altair as alt
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error
import pytz

# -----------------------------
# Streamlit App Setup
# -----------------------------
st.set_page_config(page_title="Citi Bike Model Monitoring", layout="wide")
st.title("📊 Citi Bike Model Monitoring Dashboard")
st.markdown("Monitoring prediction accuracy, drift, and trends for the past 7 days.")

# -----------------------------
# Hopsworks Connection
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
# Load Predictions
# -----------------------------
@st.cache_data(ttl=1800)
def load_predictions():
    fg = fs.get_feature_group("citi_bike_predictions", version=3)
    df = fg.read()
    df["target_hour"] = pd.to_datetime(df["target_hour"])
    df["prediction_time"] = pd.to_datetime(df["prediction_time"])
    df["predicted_trip_count"] = df["predicted_trip_count"].astype("float32")
    return df.sort_values("target_hour")

# -----------------------------
# Load Actuals
# -----------------------------
@st.cache_data(ttl=1800)
def load_actuals():
    fg = fs.get_feature_group("citi_bike_trips", version=1)
    df = fg.read()
    df["start_hour"] = pd.to_datetime(df["started_at"]).dt.floor("H")
    actual_df = df.groupby("start_hour").size().reset_index(name="actual_trip_count")
    return actual_df.sort_values("start_hour")

pred_df = load_predictions()
actual_df = load_actuals()

# -----------------------------
# Merge: Predictions + Actuals
# -----------------------------
merged = pd.merge(
    pred_df,
    actual_df,
    left_on="target_hour",
    right_on="start_hour",
    how="inner"
).drop(columns=["start_hour"])

merged = merged[["target_hour", "predicted_trip_count", "actual_trip_count"]]
merged["error"] = merged["predicted_trip_count"] - merged["actual_trip_count"]
merged["abs_error"] = merged["error"].abs()
merged["hour"] = merged["target_hour"].dt.strftime("%Y-%m-%d %H:%M")

# -----------------------------
# Metrics
# -----------------------------
mae = mean_absolute_error(merged["actual_trip_count"], merged["predicted_trip_count"])
max_error_hour = merged.iloc[merged["abs_error"].idxmax()]["hour"]
max_abs_error = merged["abs_error"].max()

col1, col2, col3 = st.columns(3)

col1.metric("Mean Absolute Error", f"{mae:.2f}")
col2.metric("Max Error Hour", max_error_hour)
col3.metric("Max Absolute Error", f"{int(max_abs_error)}")

# -----------------------------
# Error Plot
# -----------------------------
st.subheader("📉 Prediction vs. Actuals (Past 7 Days)")
error_chart = alt.Chart(merged).transform_fold(
    ["predicted_trip_count", "actual_trip_count"],
    as_=["Type", "Trip Count"]
).mark_line(point=True).encode(
    x=alt.X("target_hour:T", title="Hour"),
    y=alt.Y("Trip Count:Q"),
    color="Type:N",
    tooltip=["hour", "Trip Count", "Type"]
).properties(width=900, height=400)

st.altair_chart(error_chart, use_container_width=True)

# -----------------------------
# Error Distribution
# -----------------------------
st.subheader("🧪 Error Distribution")

hist = alt.Chart(merged).mark_bar().encode(
    x=alt.X("abs_error:Q", bin=alt.Bin(maxbins=30), title="Absolute Error"),
    y=alt.Y("count():Q", title="Frequency"),
    tooltip=["count()"]
).properties(width=700, height=300)

st.altair_chart(hist)

# -----------------------------
# Raw Data
# -----------------------------
with st.expander("🔍 View Merged Data Table"):
    st.dataframe(merged, use_container_width=True)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit + Hopsworks + Altair.")
