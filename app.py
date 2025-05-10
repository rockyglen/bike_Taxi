import streamlit as st
import pandas as pd
import hopsworks
from datetime import datetime
import altair as alt

st.set_page_config(page_title="Citi Bike Trip Predictor", layout="wide")
st.title("🚲 Citi Bike Hourly Trip Prediction Dashboard")

# -----------------------------
# Hopsworks Login
# -----------------------------
import os
from dotenv import load_dotenv
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
# Display Latest Prediction
# -----------------------------
latest = pred_df.sort_values('target_hour').iloc[-1]
st.metric(
    label="📈 Predicted Trip Count for Next Hour",
    value=f"{int(latest['predicted_trip_count'])}",
    delta=str(latest["target_hour"])
)

# -----------------------------
# Show Prediction Timeline
# -----------------------------
st.markdown("### 🔁 Prediction Timeline")
chart = alt.Chart(pred_df).mark_line(point=True).encode(
    x='target_hour:T',
    y='predicted_trip_count:Q',
    tooltip=['target_hour:T', 'predicted_trip_count']
).properties(height=400)

st.altair_chart(chart, use_container_width=True)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("Predictions powered by Hopsworks + LightGBM | Streamlit deployed")
