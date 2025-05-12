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
st.set_page_config(page_title="Citi Bike Global Trip Forecast", layout="wide")
st.title("🚲 Citi Bike Global Trip Forecast (Next 7 Days)")

# -----------------------------
# Hopsworks Login
# -----------------------------
load_dotenv()
try:
    project = hopsworks.login(
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
        project=os.getenv("HOPSWORKS_PROJECT")
    )
    fs = project.get_feature_store()
except Exception as e:
    st.error(f"❌ Failed to connect to Hopsworks: {e}")
    st.stop()

# -----------------------------
# Load Global Predictions
# -----------------------------
try:
    pred_fg = fs.get_feature_group("citi_bike_predictions_global", version=1)
    pred_df = pred_fg.read()

    if pred_df.empty:
        st.warning("⚠️ Feature group loaded but contains no data.")
        st.stop()

    pred_df['prediction_time'] = pd.to_datetime(pred_df['prediction_time'])
    pred_df['target_hour'] = pd.to_datetime(pred_df['target_hour'])
except Exception as e:
    st.error(f"❌ Could not load prediction data: {e}")
    st.stop()

# -----------------------------
# Filter Predictions (Next 7 Days)
# -----------------------------
eastern = pytz.timezone("America/New_York")
now_est = datetime.now(pytz.utc).astimezone(eastern).replace(minute=0, second=0, microsecond=0)
end_est = now_est + timedelta(hours=168)

pred_df['target_hour'] = pred_df['target_hour'].dt.tz_convert(eastern)
pred_df = pred_df[(pred_df['target_hour'] >= now_est) & (pred_df['target_hour'] < end_est)]

# -----------------------------
# Hour Selector (Next 5 Hours)
# -----------------------------
st.markdown("### 🕒 Select an Hour (EST) to View Prediction")
next_5_est = [(now_est + timedelta(hours=i)) for i in range(1, 6)]
option_map = {t.strftime("%Y-%m-%d %H:%M %Z"): t for t in next_5_est}
selected_label = st.selectbox("Select Target Hour", list(option_map.keys()))
selected_time = option_map[selected_label]

# -----------------------------
# Show Selected Prediction
# -----------------------------
matched = pred_df[pred_df['target_hour'] == selected_time]
if not matched.empty:
    val = int(matched['predicted_trip_count'].values[0])
    delta_display = selected_time.strftime("%I %p on %b %d")
    st.metric("📈 Predicted Trips", value=val, delta=delta_display)
else:
    st.warning("No prediction found for this hour.")

# -----------------------------
# Timeline Chart
# -----------------------------
st.markdown("### 📊 Prediction Timeline (Next 7 Days)")
chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=[
        alt.Tooltip('target_hour:T', format='%Y-%m-%d %H'),
        'predicted_trip_count'
    ]
).properties(height=400)
st.altair_chart(chart, use_container_width=True)

# -----------------------------
# Table View
# -----------------------------
st.markdown("### 🧾 Full Prediction Table (Rounded)")
rounded_df = pred_df.copy()
rounded_df['predicted_trip_count'] = rounded_df['predicted_trip_count'].round(0).astype(int)
rounded_df['target_hour'] = rounded_df['target_hour'].dt.strftime('%Y-%m-%d %H')

st.dataframe(
    rounded_df.sort_values("target_hour", ascending=True)[
        ['target_hour', 'predicted_trip_count']
    ],
    use_container_width=True
)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Global Forecast | LightGBM + Hopsworks | Dashboard by Glen")
