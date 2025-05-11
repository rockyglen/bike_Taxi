import streamlit as st
import pandas as pd
import hopsworks
import pytz
from datetime import datetime, timedelta
import altair as alt
import os
from dotenv import load_dotenv

# -----------------------------
# Streamlit setup
# -----------------------------
st.set_page_config(page_title="Citi Bike Trip Prediction", layout="wide")
st.title("🚲 Citi Bike Hourly Trip Prediction Dashboard")

# -----------------------------
# Hopsworks Login
# -----------------------------
load_dotenv()
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT")
)
fs = project.get_feature_store()

# -----------------------------
# Load Predictions
# -----------------------------
pred_fg = fs.get_feature_group("citi_bike_predictions", version=2)
pred_df = pred_fg.read()
pred_df['prediction_time'] = pd.to_datetime(pred_df['prediction_time'])
pred_df['target_hour'] = pd.to_datetime(pred_df['target_hour'])

# -----------------------------
# Show Top Station by Volume
# -----------------------------
st.markdown("### 🏆 Top Station by Predicted Trip Volume (Next 24h)")
top_station_df = (
    pred_df.groupby('start_station_name')['predicted_trip_count']
    .sum()
    .sort_values(ascending=False)
    .reset_index()
)
if not top_station_df.empty:
    top_station = top_station_df.iloc[0]
    st.success(
        f"**{top_station['start_station_name']}** is expected to have the most rides "
        f"with **{int(top_station['predicted_trip_count'])} predicted trips** in the next 24 hours."
    )

# -----------------------------
# Station Selection + Filtering
# -----------------------------
stations = sorted(pred_df['start_station_name'].unique())
selected_station = st.selectbox(
    "📍 Select Start Station (leave empty for all stations)",
    options=["All Stations"] + stations,
    index=0
)

if selected_station != "All Stations":
    filtered_df = pred_df[pred_df['start_station_name'] == selected_station]
else:
    filtered_df = (
        pred_df.groupby("target_hour", as_index=False)['predicted_trip_count'].sum()
    )
    filtered_df['start_station_name'] = "All Stations"

# -----------------------------
# Generate Next 5 Hour Options (EST)
# -----------------------------
eastern = pytz.timezone("America/New_York")
now_est = datetime.now(pytz.utc).astimezone(eastern).replace(minute=0, second=0, microsecond=0)
next_5_est = [(now_est + timedelta(hours=i)) for i in range(1, 6)]
option_map = {t.strftime("%Y-%m-%d %H:%M %Z"): t for t in next_5_est}
selected_label = st.selectbox("🕒 Select a Target Hour (EST)", list(option_map.keys()))
selected_time = option_map[selected_label]

# -----------------------------
# Display Prediction Metric
# -----------------------------
matched = filtered_df[filtered_df['target_hour'] == selected_time]
if not matched.empty:
    val = int(matched['predicted_trip_count'].values[0])
    st.metric("📈 Predicted Trip Count", value=val, delta=str(selected_time))
else:
    st.warning("No prediction found for this hour.")

# -----------------------------
# Prediction Timeline
# -----------------------------
st.markdown(f"### 📊 Prediction Timeline for **{selected_station}**")
chart = alt.Chart(filtered_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=['target_hour:T', 'predicted_trip_count']
).properties(height=400)
st.altair_chart(chart, use_container_width=True)

# -----------------------------
# Prediction Table (Sorted by Count)
# -----------------------------
st.markdown("### 🧾 Prediction Table (Sorted by Highest Predicted Count)")
st.dataframe(
    filtered_df.sort_values("predicted_trip_count", ascending=False)[
        ['target_hour', 'predicted_trip_count']
    ],
    use_container_width=True
)


# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Predictions powered by Hopsworks + LightGBM | Streamlit dashboard by Glen")
