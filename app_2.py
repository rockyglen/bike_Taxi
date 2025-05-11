import os
import pandas as pd
import streamlit as st
import hopsworks
import altair as alt
import numpy as np
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv
from scipy.stats import ks_2samp

# -----------------------------
# Streamlit setup
# -----------------------------
st.set_page_config(page_title="Citi Bike Trip Monitoring", layout="wide")
st.title("📈 Citi Bike Model Monitoring Dashboard")

# -----------------------------
# Load environment and connect
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
@st.cache_data(ttl=3600)
def load_predictions():
    fg = fs.get_feature_group("citi_bike_predictions", version=2)
    df = fg.read()
    df['prediction_time'] = pd.to_datetime(df['prediction_time'])
    df['target_hour'] = pd.to_datetime(df['target_hour'])
    return df

pred_df = load_predictions()

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("🔍 Filter Options")
station = st.sidebar.selectbox("Select Station", sorted(pred_df['start_station_name'].unique()))
time_range = st.sidebar.slider("Forecast Horizon (Hours)", 1, 168, (1, 72))

# -----------------------------
# Filter Data with timezone-aware datetime
# -----------------------------
station_df = pred_df[pred_df['start_station_name'] == station].copy()
station_df = station_df.sort_values("target_hour")

eastern = pytz.timezone("US/Eastern")
now = datetime.now(eastern)
end_time = now + timedelta(hours=time_range[1])

station_df = station_df[
    (station_df['target_hour'] >= now) &
    (station_df['target_hour'] <= end_time)
]

# -----------------------------
# Plot Forecast
# -----------------------------
st.subheader(f"📅 Forecast for: {station}")
forecast_chart = alt.Chart(station_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=['target_hour', 'predicted_trip_count']
).properties(
    width=800,
    height=400,
    title="Predicted Trip Count"
)
st.altair_chart(forecast_chart, use_container_width=True)

# -----------------------------
# Model Drift / Distribution Check
# -----------------------------
with st.expander("📊 Feature Drift (Lag Distributions)"):
    # Simulated train vs recent (use real stored training stats in prod)
    fake_train_dist = np.random.normal(100, 10, 1000)
    recent_pred_dist = station_df['predicted_trip_count'].values
    if len(recent_pred_dist) > 10:
        stat, pval = ks_2samp(fake_train_dist, recent_pred_dist)
        st.metric("KS Test p-value", f"{pval:.4f}", delta_color="inverse")
        st.write("Low p-value (< 0.05) indicates drift")
        st.line_chart(pd.DataFrame({
            'Train Distribution': pd.Series(fake_train_dist[:len(recent_pred_dist)]),
            'Recent Predictions': recent_pred_dist
        }))
    else:
        st.info("Not enough recent data to compare distribution.")

# -----------------------------
# Download Raw Data
# -----------------------------
st.download_button(
    label="🗂️ Download Predictions CSV",
    data=station_df.to_csv(index=False),
    file_name=f"predictions_{station}.csv",
    mime="text/csv"
)

# -----------------------------
# Data Table
# -----------------------------
st.markdown("### 🔢 Raw Prediction Data")
st.dataframe(station_df, use_container_width=True)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Developed with Streamlit | Hopsworks | Altair | LightGBM")
