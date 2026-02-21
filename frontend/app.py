import streamlit as st
import pandas as pd
import boto3
import os
from datetime import datetime
import altair as alt
import pytz
from dotenv import load_dotenv

# === PAGE CONFIG ===
st.set_page_config(
    page_title="NYC Citi Bike | Demand Intelligence",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === LOAD ENV & STYLING ===
load_dotenv()

# Premium NYC Dark Mode Styling
css_code = """
<style>
    .main {
        background: #0e1117;
        font-family: 'Inter', sans-serif;
    }
    h1, h2, h3 {
        font-family: 'Outfit', sans-serif;
        font-weight: 700 !important;
        letter-spacing: -0.5px;
    }
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 20px !important;
        border-radius: 16px !important;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: transform 0.3s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        border-color: #ff4b4b;
    }
    section[data-testid="stSidebar"] {
        background-color: #161b22 !important;
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }
    .rush-badge {
        background: linear-gradient(90deg, #ff4b4b 0%, #ff8a8a 100%);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 800;
        text-transform: uppercase;
        margin-bottom: 10px;
        display: inline-block;
    }
    .status-tag {
        font-size: 0.7rem;
        color: #888;
        border: 1px solid #444;
        padding: 2px 8px;
        border-radius: 4px;
        margin-left: 10px;
    }
</style>
"""
st.markdown('<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=Outfit:wght@300;500;700&display=swap" rel="stylesheet">', unsafe_allow_html=True)
st.markdown(css_code, unsafe_allow_html=True)

# === UTILITIES ===
def download_and_load_data():
    import pytz # Explicitly import inside to bypass any streamlit scope issues
    bucket = os.getenv('AWS_S3_BUCKET')
    s3_path = "citi_bike/latest_predictions.parquet"
    local_path = "latest_predictions.parquet"
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    try:
        s3.download_file(bucket, s3_path, local_path)
        df = pd.read_parquet(local_path)
        
        # Ensure target_hour is datetime and UTC-aware
        df['target_hour'] = pd.to_datetime(df['target_hour'])
        if df['target_hour'].dt.tz is None:
            df['target_hour'] = df['target_hour'].dt.tz_localize(pytz.UTC)
        else:
            df['target_hour'] = df['target_hour'].dt.tz_convert(pytz.UTC)
            
        # Convert everything to US/Eastern for display
        nyc_tz = pytz.timezone("US/Eastern")
        df['target_hour'] = df['target_hour'].dt.tz_convert(nyc_tz)
        
        # Filter: Only show predictions from 'now' (current hour) onwards
        # Note: comparison works even with different TZs if both are aware
        now_nyc = datetime.now(nyc_tz).replace(minute=0, second=0, microsecond=0)
        filtered_df = df[df['target_hour'] >= now_nyc].copy()
        
        if filtered_df.empty:
            st.sidebar.warning("⚠️ No future predictions found. Showing latest available cache.")
            return df.tail(24) 
            
        return filtered_df
    except Exception as e:
        st.error(f"❌ Data Load Error: {e}")
        return None

# === DASHBOARD CONTENT ===
df = download_and_load_data()

if df is not None:
    # --- HEADER ---
    st.markdown("<h1>NYC Demand Intelligence <span style='color:#ff4b4b'>Core</span></h1>", unsafe_allow_html=True)
    st.markdown("<p style='opacity:0.7; font-size:1.1rem;'>Premium Predictive Analytics for the Citi Bike Fleet</p>", unsafe_allow_html=True)
    
    # --- TOP METRICS ---
    st.write("###") # Spacer
    col1, col2, col3, col4 = st.columns(4)
    
    peak_demand = df['predicted_trips'].max()
    peak_hour = df.loc[df['predicted_trips'].idxmax(), 'target_hour']
    
    # Format Last Sync (convert to Eastern if needed)
    sync_time = pd.to_datetime(df.iloc[0]['prediction_generated_at'])
    if sync_time.tzinfo is None:
        sync_time = pytz.UTC.localize(sync_time)
    sync_time_nyc = sync_time.astimezone(pytz.timezone("US/Eastern"))
    last_sync = sync_time_nyc.strftime('%I:%M %p')
    
    col1.metric("Live Forecast", f"{df.iloc[0]['predicted_trips']:.1f}", delta="Trips/Hr")
    col2.metric("24h Peak", f"{peak_demand:.1f}", delta="Projected")
    col3.metric("Peak Window", peak_hour.strftime('%I %p'))
    col4.metric("Last Data Sync (ET)", last_sync)

    # --- MAIN VISUALIZATION ---
    st.write("###")
    container = st.container(border=True)
    with container:
        st.markdown("<div class='rush-badge'>24H Projection Horizon</div>", unsafe_allow_html=True)
        
        # Advanced Brushing Chart
        brush = alt.selection_interval(encodings=['x'])
        
        base = alt.Chart(df).encode(
            x=alt.X('target_hour:T', title='Timeline (Next 24 Hours)', axis=alt.Axis(grid=False)),
            tooltip=[
                alt.Tooltip('target_hour:T', title='Time'),
                alt.Tooltip('predicted_trips:Q', title='Pred. Trips', format='.1f')
            ]
        ).properties(width='container', height=400)

        # Upper Chart: Gradient Area with Interactive Brush
        upper = base.mark_area(
            line={'color':'#ff4b4b', 'strokeWidth': 3},
            color=alt.Gradient(
                gradient='linear',
                stops=[alt.GradientStop(color='#ff4b4b', offset=0),
                       alt.GradientStop(color='rgba(255, 75, 75, 0.05)', offset=1)],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            x=alt.X('target_hour:T', scale=alt.Scale(domain=brush)),
            y=alt.Y('predicted_trips:Q', title='Trip Demand', axis=alt.Axis(gridOpacity=0.1))
        )

        # Lower Mini-Chart: Navigation Context
        lower = base.mark_area(
            line={'color':'#ff4b4b', 'strokeWidth': 1},
            color='rgba(255, 75, 75, 0.2)'
        ).properties(height=60).add_params(brush)

        st.altair_chart(upper & lower, use_container_width=True)

    # --- SIDEBAR INSIGHTS ---
    with st.sidebar:
        # Using a reliable image source for the NYC skyline or Citi Bike motif
        st.markdown("### 🗽 NYC Fleet Intelligence")
        st.markdown("### ⚙️ System Intelligence")
        st.info("Model: **LGBM-V4-Recursive**")
        
        st.divider()
        st.markdown("### 🚇 Rush Hour Alert")
        # Refined logic: Only alert if demand is 25% above the 24h average
        avg_demand = df['predicted_trips'].mean()
        rush_hours = df[df['predicted_trips'] > avg_demand * 1.25]
        
        if not rush_hours.empty:
            start_t = rush_hours['target_hour'].min().strftime('%I%p')
            end_t = rush_hours['target_hour'].max().strftime('%I%p')
            st.warning(f"High Demand detected between **{start_t}** and **{end_t}**")
        else:
            st.success("No critical peak demand expected.")
            
        st.divider()
        st.markdown("### 📊 Architecture")
        st.code(f"AWS Region: us-east-1\nStorage: S3 Parquet\nMLflow: Tracked\nArchived: Versioned", language="ini")

    # --- DATA EXPLORER ---
    with st.expander("🛠️ Developer Inspection Corner"):
        st.dataframe(df.style.highlight_max(axis=0, subset=['predicted_trips'], color='#ff4b4b'), use_container_width=True)

else:
    st.error("🚀 High-performance Data Stream not found. Please trigger the Inference Pipeline.")
    st.code("uv run scripts/inference.py")

# Footer
st.markdown("<p style='text-align:center; opacity:0.4; margin-top:50px;'>NYC Fleet Intelligence | Built with Streamlit, uv & S3</p>", unsafe_allow_html=True)
