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
pred_fg = fs.get_feature_group("citi_bike_predictions", version=1)
pred_df = pred_fg.read()
pred_df['prediction_time'] = pd.to_datetime(pred_df['prediction_time'])
pred_df['target_hour'] = pd.to_datetime(pred_df['target_hour'])

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
# Find prediction for selected time
# -----------------------------
matched = pred_df[pred_df['target_hour'] == selected_time]
if not matched.empty:
    val = int(matched['predicted_trip_count'].values[0])
    st.metric("📈 Predicted Trip Count", value=val, delta=str(selected_time))
else:
    st.warning("No prediction found for this hour yet.")

# -----------------------------
# Show prediction timeline
# -----------------------------
st.markdown("### 📊 Prediction Timeline")
chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=['target_hour:T', 'predicted_trip_count']
).properties(height=400)
st.altair_chart(chart, use_container_width=True)

# -----------------------------
# Show data table
# -----------------------------
st.markdown("### 🧾 Prediction Table (Latest)")
st.dataframe(
    pred_df.sort_values("target_hour", ascending=False)[['target_hour', 'predicted_trip_count']],
    use_container_width=True
)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Predictions powered by Hopsworks + LightGBM | Streamlit dashboard by Glen")
