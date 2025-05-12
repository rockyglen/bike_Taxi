import os
import requests
import zipfile
import polars as pl
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from io import BytesIO
from collections import Counter
import time

TEMP_FOLDER = "tripdata_temp"
OUTPUT_FILE = "top3_stations_output.csv"
PARQUET_OUTPUT = "tabular_citibike.parquet"
CHUNK_SIZE = 500_000
WINDOW_SIZE = 24 * 28
STEP_SIZE = 24

TARGET_COLS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id",
    "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual"
]

def get_last_12_months_est():
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    return [(now_est - relativedelta(months=i + 1)).strftime('%Y%m') for i in range(12)]

def download_zip_to_memory(ym):
    base_url = "https://s3.amazonaws.com/tripdata/"
    filenames = [f"{ym}-citibike-tripdata.zip", f"{ym}-citibike-tripdata.csv.zip"]
    for fname in filenames:
        url = base_url + fname
        try:
            print(f"🌐 Trying: {url}")
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                print(f"✅ Downloaded: {fname}")
                return BytesIO(r.content)
        except Exception as e:
            print(f"⚠️ Error downloading {url}: {e}")
    return None

def extract_all_csvs(zip_bytes_io, extract_to):
    try:
        with zipfile.ZipFile(zip_bytes_io) as zf:
            extracted_files = 0
            for member in zf.namelist():
                if member.endswith('.csv'):
                    print(f"📁 Extracting CSV: {member}")
                    zf.extract(member, extract_to)
                    extracted_files += 1
            print(f"✅ Extracted {extracted_files} CSV files.")
    except Exception as e:
        print(f"⚠️ Error extracting zip: {e}")

def flatten_csvs_folder(root_folder):
    return [os.path.join(root, f)
            for root, _, files in os.walk(root_folder)
            for f in files if f.endswith(".csv")]

def get_top3_station_names(filepaths):
    freq = Counter()
    for path in filepaths:
        try:
            df = pl.read_csv(path, columns=["start_station_name"]).drop_nulls()
            freq.update(df["start_station_name"].to_list())
        except Exception as e:
            print(f"⚠️ Skipping {path}: {e}")
    return [name for name, _ in freq.most_common(3)]

def write_top3_data(filepaths, top3, output=OUTPUT_FILE):
    top3_set = set(top3)
    total_written = 0
    files_written = 0
    first_write = True

    for path in filepaths:
        written_from_file = 0
        try:
            df = pl.read_csv(path, columns=TARGET_COLS)
            df = df.filter(pl.col("start_station_name").is_in(top3_set))
            if df.height > 0:
                mode = 'w' if first_write else 'a'
                df.write_csv(output, include_header=first_write, separator=",", append=not first_write)
                total_written += df.height
                written_from_file = df.height
                files_written += 1
                first_write = False
                print(f"✅ Wrote {written_from_file:,} rows from {os.path.basename(path)}")
        except Exception as e:
            print(f"⚠️ Skipping {path}: {e}")

    print(f"📈 Total rows written: {total_written:,}")
    print(f"🗂️ Files that contributed data: {files_written}/{len(filepaths)}")

def clean_and_aggregate(file_path):
    print("🧼 Cleaning and aggregating...")
    df = pl.read_csv(file_path, try_parse_dates=True)

    df = df.drop_nulls(["started_at", "ride_id", "rideable_type", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"])
    df = df.with_columns([
        pl.col("started_at").str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S", strict=False).alias("started_at_dt")
    ]).drop_nulls(["started_at_dt"])

    df = df.with_columns([
        pl.col("started_at_dt").dt.convert_time_zone("US/Eastern").alias("est"),
        pl.col("start_station_name").fill_null("Unknown").cast(pl.Utf8),
    ])

    df = df.with_columns([
        pl.col("est").dt.truncate("1h").alias("start_hour")
    ])

    grouped = df.groupby(["start_station_name", "start_hour"]).agg([
        pl.count().alias("trip_count")
    ])

    print(f"📊 Aggregated {grouped.shape[0]:,} hourly rows")
    return grouped

def transform_to_supervised(df, window_size, step_size):
    print("🧠 Transforming to supervised format...")
    records = []

    for station in df["start_station_name"].unique():
        station_df = df.filter(pl.col("start_station_name") == station).sort("start_hour")
        series = station_df.select("trip_count").to_series().to_list()

        for i in range(0, len(series) - window_size, step_size):
            X = series[i:i+window_size]
            y_idx = i + window_size
            if y_idx < len(series):
                record = {f"lag_{j+1}": X[j] for j in range(window_size)}
                record["target"] = series[y_idx]
                record["start_station_name"] = station
                records.append(record)

    out = pl.DataFrame(records)
    print(f"✅ Transformed shape: {out.shape}")
    return out

def main():
    start_all = time.time()

    if os.path.exists(TEMP_FOLDER):
        shutil.rmtree(TEMP_FOLDER)
    os.makedirs(TEMP_FOLDER, exist_ok=True)

    print("🚀 Downloading + extracting data...")
    for ym in get_last_12_months_est():
        zip_mem = download_zip_to_memory(ym)
        if zip_mem:
            extract_all_csvs(zip_mem, TEMP_FOLDER)

    all_csvs = flatten_csvs_folder(TEMP_FOLDER)
    print(f"📂 Total CSVs: {len(all_csvs)}")
    if not all_csvs:
        print("❌ No CSVs found.")
        return

    top3 = get_top3_station_names(all_csvs)
    print(f"🏆 Top 3 Stations: {top3}")

    print("📥 Filtering and writing...")
    write_top3_data(all_csvs, top3)

    print("🧹 Cleaning up...")
    shutil.rmtree(TEMP_FOLDER)

    hourly_df = clean_and_aggregate(OUTPUT_FILE)
    supervised_df = transform_to_supervised(hourly_df, WINDOW_SIZE, STEP_SIZE)
    supervised_df.write_parquet(PARQUET_OUTPUT)
    print(f"💾 Saved to: {PARQUET_OUTPUT}")
    print(f"📊 Final rows: {supervised_df.shape[0]:,}")
    print(f"⏱️ Total time: {time.time() - start_all:.2f} sec")

if __name__ == "__main__":
    main()
