import streamlit as st
import pandas as pd
import hopsworks
from datetime import datetime
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
# Load Prediction Feature Group
# -----------------------------
pred_fg = fs.get_feature_group("citi_bike_predictions", version=1)
pred_df = pred_fg.read()
pred_df['prediction_time'] = pd.to_datetime(pred_df['prediction_time'])
pred_df['target_hour'] = pd.to_datetime(pred_df['target_hour'])

# -----------------------------
# Let user select a target hour
# -----------------------------
available_hours = sorted(pred_df['target_hour'].unique())
default_idx = len(available_hours) - 1  # latest prediction

selected_hour = st.selectbox(
    "🕒 Select Target Hour to View Prediction",
    options=available_hours,
    index=default_idx
)

# Filter for selected prediction
selected_pred = pred_df[pred_df['target_hour'] == selected_hour]

if not selected_pred.empty:
    pred_val = int(selected_pred['predicted_trip_count'].values[0])
    st.metric(
        label="📈 Predicted Trip Count",
        value=pred_val,
        delta=str(selected_hour)
    )
else:
    st.warning("No prediction found for selected hour.")

# -----------------------------
# Show full prediction timeline
# -----------------------------
st.markdown("### 📊 Prediction Timeline")
chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=['target_hour:T', 'predicted_trip_count']
).properties(height=400)

st.altair_chart(chart, use_container_width=True)

# -----------------------------
# Show full prediction table
# -----------------------------
st.markdown("### 🧾 Prediction Table (Recent)")
st.dataframe(
    pred_df.sort_values("target_hour", ascending=False)[['target_hour', 'predicted_trip_count']],
    use_container_width=True
)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Predictions powered by Hopsworks + LightGBM | Streamlit deployed")
