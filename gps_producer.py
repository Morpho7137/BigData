import os
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd

DATA_DIR = "/mnt/d/BigDataPj/data/sorted"
KAFKA_BROKER = "localhost:9092"
TOPIC = "gps-stream"

def load_gps_file(path):
    gps_data = pd.read_csv(
        path,
        sep=' ',
        header=None,
        names=['taxi_id', 'timestamp', 'lon', 'lat'],
        quotechar='"'
    )

    gps_data['timestamp'] = pd.to_datetime(
        gps_data['timestamp'],
        format="%Y-%m-%d %H:%M:%S",
        errors='coerce'
    )

    gps_data = gps_data.dropna(subset=['timestamp'])
    return gps_data

def load_all_files():
    files = sorted(os.listdir(DATA_DIR))
    all_data = []

    for f in files:
        full_path = os.path.join(DATA_DIR, f)
        if not f.endswith(".txt"):
            continue

        df = load_gps_file(full_path)
        all_data.append(df)

    if len(all_data) == 0:
        return pd.DataFrame()

    merged = pd.concat(all_data, ignore_index=True)
    merged = merged.sort_values(by="timestamp")
    return merged

def main():
    print("GPS DATA PRODUCER - SIMULATING LIVE STREAM")
    print("=" * 80)

    gps_data = load_all_files()

    print(f"Total GPS points: {len(gps_data)}")
    if len(gps_data) > 0:
        print(f"Time range: {gps_data['timestamp'].min()} to {gps_data['timestamp'].max()}")
    else:
        print("Time range: NaT to NaT")

    print("\nStarting GPS stream simulation...")
    print(f"Sending to Kafka topic: '{TOPIC}'")
    print("-" * 80)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for _, row in gps_data.iterrows():
        record = {
            "taxi_id": int(row["taxi_id"]),
            "timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            "longitude": float(row["lon"]),
            "latitude": float(row["lat"])
        }

        producer.send(TOPIC, record)
        time.sleep(0.01)

    producer.flush()
    print("✓ GPS producer completed.")

if __name__ == "__main__":
    main()
