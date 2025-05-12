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
# 📦 Load prediction feature group
# -----------------------------
@st.cache_data(ttl=1800)
def load_latest_predictions():
    fg = fs.get_feature_group("citi_bike_predictions", version=3)
    df = fg.read()
    df["target_hour"] = pd.to_datetime(df["target_hour"])
    df["prediction_time"] = pd.to_datetime(df["prediction_time"])
    df["predicted_trip_count"] = df["predicted_trip_count"].astype("float32")
    return df.sort_values("target_hour")

pred_df = load_latest_predictions()

# -----------------------------
# 🧭 Time context
# -----------------------------
eastern = pytz.timezone("US/Eastern")
now_est = datetime.now(eastern)

# -----------------------------
# 🎨 Streamlit UI
# -----------------------------
st.title("🚲 Citi Bike Trip Prediction Dashboard")
st.markdown(f"""
This dashboard shows predicted Citi Bike trip counts for the next 24 hours, 
based on the latest model trained with 28-hour lag features. Data is updated hourly from a Hopsworks pipeline.
""")

# -----------------------------
# 📈 Main Forecast Plot
# -----------------------------
st.subheader("📊 Forecast: Hourly Trip Counts (Next 24 Hours)")

chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x=alt.X("target_hour:T", title="Forecasted Hour"),
    y=alt.Y("predicted_trip_count:Q", title="Predicted Trip Count"),
    tooltip=[
        alt.Tooltip("target_hour:T", title="Hour"),
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
    st.metric("Peak Hour", peak_hour.strftime("%Y-%m-%d %H:%M"))

with col3:
    min_count = pred_df["predicted_trip_count"].min()
    max_count = pred_df["predicted_trip_count"].max()
    st.metric("Range", f"{int(min_count)} - {int(max_count)}")

with col4:
    st.metric("Prediction Timestamp", pred_df["prediction_time"].max().strftime("%Y-%m-%d %H:%M:%S UTC"))

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
    Data pipeline powered by LightGBM forecasting model trained on hourly Citi Bike trips in NYC.
    """
)
