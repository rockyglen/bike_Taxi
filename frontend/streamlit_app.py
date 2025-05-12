import os
import pandas as pd
import hopsworks
import pytz
import altair as alt
import streamlit as st
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --------------------------------------
# ✅ Must be the first Streamlit command
# --------------------------------------
st.set_page_config(page_title="Citi Bike Forecast", layout="wide")

# -----------------------------
# 🔐 Load secrets + Hopsworks login
# -----------------------------
@st.cache_resource
def init_hopsworks_connection():
    load_dotenv()
    return hopsworks.login(
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
        project=os.getenv("HOPSWORKS_PROJECT")
    )

project = init_hopsworks_connection()
fs = project.get_feature_store()

# -----------------------------
# 📦 Load prediction feature group (filter to next 24h from now in EST)
# -----------------------------
@st.cache_data(ttl=1800)
def load_latest_predictions():
    fg = fs.get_feature_group("citi_bike_predictions", version=3)
    df = fg.read()

    df["target_hour"] = pd.to_datetime(df["target_hour"]).dt.floor("H")
    df["prediction_time"] = pd.to_datetime(df["prediction_time"])
    df["predicted_trip_count"] = df["predicted_trip_count"].astype("float32")

    # Convert to US/Eastern timezone
    if df["target_hour"].dt.tz is None:
        df["target_hour"] = df["target_hour"].dt.tz_localize("UTC")
    df["target_hour"] = df["target_hour"].dt.tz_convert("US/Eastern")

    if df["prediction_time"].dt.tz is None:
        df["prediction_time"] = df["prediction_time"].dt.tz_localize("UTC")
    df["prediction_time"] = df["prediction_time"].dt.tz_convert("US/Eastern")

    # 🔥 Filter for next 24 hours from current US/Eastern time
    now_est = datetime.now(pytz.timezone("US/Eastern")).replace(minute=0, second=0, microsecond=0)
    future_24h = now_est + timedelta(hours=24)
    df = df[(df["target_hour"] >= now_est) & (df["target_hour"] < future_24h)]

    return df.sort_values("target_hour")

pred_df = load_latest_predictions()

# -----------------------------
# 🎨 Streamlit UI
# -----------------------------
st.title("🚲 Citi Bike Trip Prediction Dashboard")
st.markdown(f"""
This dashboard shows predicted Citi Bike trip counts for the next 24 hours  
(all timestamps shown in **US/Eastern** timezone).
""")

# -----------------------------
# 📈 Main Forecast Plot
# -----------------------------
st.subheader("📊 Forecast: Hourly Trip Counts (Next 24 Hours)")

chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x=alt.X("target_hour:T", title="Forecasted Hour (EST)"),
    y=alt.Y("predicted_trip_count:Q", title="Predicted Trip Count"),
    tooltip=[
        alt.Tooltip("target_hour:T", title="Hour (EST)"),
        alt.Tooltip("predicted_trip_count:Q", title="Trips", format=".2f")
    ]
).properties(
    width=900,
    height=400
).interactive()

st.altair_chart(chart, use_container_width=True)

# -----------------------------
# 🧠 Forecast Summary Stats
# -----------------------------
st.subheader("📈 Forecast Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_trips = pred_df["predicted_trip_count"].sum()
    st.metric("Total Trips (24h)", f"{int(total_trips):,}")

with col2:
    peak_hour = pred_df.loc[pred_df["predicted_trip_count"].idxmax()]["target_hour"]
    st.metric("Peak Hour (EST)", peak_hour.strftime("%Y-%m-%d %H:%M"))

with col3:
    min_count = pred_df["predicted_trip_count"].min()
    max_count = pred_df["predicted_trip_count"].max()
    st.metric("Range", f"{int(min_count)} - {int(max_count)}")

with col4:
    ts = pred_df["prediction_time"].max()
    st.metric("Prediction Timestamp (EST)", ts.strftime("%Y-%m-%d %H:%M:%S"))

# -----------------------------
# 🧾 Data Table
# -----------------------------
with st.expander("🔍 View Prediction Data Table"):
    st.dataframe(
        pred_df[["target_hour", "predicted_trip_count"]],
        use_container_width=True,
        hide_index=True
    )

# -----------------------------
# 🧭 Footer
# -----------------------------
st.markdown("---")
st.markdown(
    """
    Built with ❤️ using [Hopsworks](https://www.hopsworks.ai/) and [Streamlit](https://streamlit.io/).  
    All time references are displayed in **US/Eastern** timezone (NYC local time).
    """
)
