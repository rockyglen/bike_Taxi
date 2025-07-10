# Citi Bike Trip Prediction System

This project forecasts hourly Citi Bike usage at the busiest stations in New York City using a full machine learning pipeline. It integrates automated data processing, model training, inference, monitoring, and deployment, all orchestrated with modern MLOps practices.

## 🚴 Overview

- **Objective**: Predict hourly demand at top NYC Citi Bike stations for a 168-hour (7-day) forecast horizon.
- **Tech Stack**: Python, LightGBM, Streamlit, FastAPI, Hopsworks, GitHub Actions
- **Key Features**:
  - Automated feature engineering and ML training pipeline
  - Real-time forecasting via Streamlit web interface
  - Model performance monitoring
  - CI/CD integration with GitHub Actions
  - Feature versioning and retrieval with Hopsworks



## Pipeline Components

### Data Engineering

- Implemented in `01_Data_Engineering.ipynb` and `feature_engineering.py`
- Hourly aggregation of trip data
- Lag features, day-of-week, hour-of-day, and holiday encoding
- Feature storage in Hopsworks Feature Store for training/inference consistency

### Model Training

- `train_model.py` trains a LightGBM model with historical features
- Time-aware cross-validation
- Tracks experiments and artifacts using MLflow
- Outputs model artifacts to `model_lags/`

### Inference

- `inference.py` generates future predictions using the latest features and model
- Can run in batch or real-time mode
- Outputs 168-hour forecast window for each selected station

### Streamlit App

- `frontend/streamlit_app.py` provides a user interface for:
  - Selecting stations
  - Visualizing demand forecasts
  - Downloading predictions

### Monitoring

- `frontend/monitoring_app.py` provides:
  - Forecast accuracy metrics (e.g. RMSE, MAE)
  - Visualization of prediction errors and drifts
  - Hook for retraining trigger logic

### CI/CD Automation

- GitHub Actions automate:
  - Model retraining pipelines
  - Unit tests for `scripts/`
  - Deployment of the Streamlit app

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/rockyglen/bike_Taxi.git
cd bike_Taxi
```
### 2. Set Up Environment
Using Conda:
```bash
conda env create -f model_lags/conda.yaml
conda activate bike_taxi_env
```
Or using pip:
```bash
pip install -r requirements.txt
```
### 3. Launch the Streamlit App

```bash
streamlit run frontend/streamlit_app.py
```

