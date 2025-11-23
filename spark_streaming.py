#!/usr/bin/env python3
"""
Spark Structured Streaming - Complete Implementation
- Adaptive map matching
- Speed calculation with Haversine (ANY consecutive points)
- 5-minute time windows
- Congestion classification from thresholds file
- PyArrow enabled for Pandas 2.0+ compatibility
"""

import os
import json
from pyspark.sql import SparkSession
import pandas as pd


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "gps-stream"
ROAD_NETWORK = "/mnt/d/BigDataPj/data/beijing_roads/beijing_roads_final.geojson"
THRESHOLDS_FILE = "/mnt/d/BigDataPj/output/congestion_thresholds.json"
OUTPUT_DIR = "/mnt/d/BigDataPj/output/streaming_results"
CHECKPOINT_DIR = "/tmp/spark_checkpoint_gps"

WINDOW_MINUTES = 5


def load_thresholds():
    try:
        with open(THRESHOLDS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Thresholds file not found: {THRESHOLDS_FILE}")
        exit(1)

print("=" * 80)
print("SPARK STREAMING - CONGESTION DETECTION")
print("=" * 80)
print()

thresholds = load_thresholds()

SEVERE_THRESHOLD = thresholds['severe_congestion']
HEAVY_THRESHOLD = thresholds['heavy_congestion']
MODERATE_THRESHOLD = thresholds['moderate_congestion']

print(f"✓ Thresholds loaded from: {THRESHOLDS_FILE}")
print(f"  SEVERE:    < {SEVERE_THRESHOLD:.2f} km/h")
print(f"  HEAVY:     {SEVERE_THRESHOLD:.2f} - {HEAVY_THRESHOLD:.2f} km/h")
print(f"  MODERATE:  {HEAVY_THRESHOLD:.2f} - {MODERATE_THRESHOLD:.2f} km/h")
print(f"  FREE_FLOW: ≥ {MODERATE_THRESHOLD:.2f} km/h")
print()


spark = SparkSession.builder \
    .appName("GPSCongestion") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✓ PyArrow enabled for Pandas 2.0+ compatibility")


print("Loading adaptive map matcher...")
from adaptive_matcher import AdaptiveRoadNetworkMatcher
from speed_calculator import calculate_speed_kmh

global_matcher = AdaptiveRoadNetworkMatcher(ROAD_NETWORK)
print("✓ Adaptive map matcher loaded with road-type-specific thresholds")
print()


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import from_json, col

gps_schema = StructType([
    StructField("taxi_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

print("Connecting to Kafka...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

gps_df = df.select(
    from_json(col("value").cast("string"), gps_schema).alias("gps")
).select("gps.*")

gps_df = gps_df.withColumn(
    "timestamp_dt",
    col("timestamp").cast("timestamp")
)

print("✓ Connected to Kafka")
print()


def process_batch(batch_df, batch_id):
    """
    Complete pipeline:
    1. Adaptive map matching (road-type-specific thresholds)
    2. Speed calculation (Haversine, ANY consecutive points)
    3. 5-minute time windowing
    4. Segment aggregation
    5. Congestion classification
    """
    
    if batch_df.isEmpty():
        return
    
    print(f"\n{'='*80}")
    print(f"BATCH {batch_id}: {batch_df.count()} GPS points")
    print(f"{'='*80}")
    
    # Convert to Pandas (using PyArrow )
    pdf = batch_df.toPandas()
    pdf['timestamp_dt'] = pd.to_datetime(pdf['timestamp'])
    
    # Adaptive Map Matching
    print(f"Step 1: Adaptive map matching...")
    matched_rows = []
    
    for _, row in pdf.iterrows():
        match = global_matcher.match_point(row['latitude'], row['longitude'])
        
        if match:
            matched_rows.append({
                'taxi_id': row['taxi_id'],
                'timestamp_dt': row['timestamp_dt'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'segment_id': str(match['segment_id']),
                'road_type': match['road_type'],
                'road_name': match['road_name'],
                'distance_m': match['distance_m'],
                'match_strategy': match.get('match_strategy', 'unknown')
            })
    
    if len(matched_rows) == 0:
        print(f"  ⚠️  No successful matches")
        return
    
    matched_pdf = pd.DataFrame(matched_rows)
    print(f"  ✓ Matched: {len(matched_pdf)}/{len(pdf)} ({len(matched_pdf)/len(pdf)*100:.1f}%)")
    
    # Calculate Speeds (Haversine) 
    print(f"\nStep 2: Calculating speeds (Haversine - all consecutive points)...")
    matched_pdf = matched_pdf.sort_values(['taxi_id', 'timestamp_dt'])
    
    speed_rows = []
    
    for taxi_id in matched_pdf['taxi_id'].unique():
        taxi_data = matched_pdf[matched_pdf['taxi_id'] == taxi_id].reset_index(drop=True)
        
        for i in range(1, len(taxi_data)):
            prev = taxi_data.iloc[i-1]
            curr = taxi_data.iloc[i]
            
            
            speed = calculate_speed_kmh(
                prev['latitude'], prev['longitude'], prev['timestamp_dt'],
                curr['latitude'], curr['longitude'], curr['timestamp_dt']
            )
            
            if speed is not None and 0.5 < speed < 150:
                speed_rows.append({
                    'timestamp_dt': curr['timestamp_dt'],
                    'segment_id': curr['segment_id'],
                    'road_type': curr['road_type'],
                    'road_name': curr['road_name'],
                    'latitude': curr['latitude'],
                    'longitude': curr['longitude'],
                    'speed_kmh': speed
                })
    
    if len(speed_rows) == 0:
        print(f"  ⚠️  No valid speeds calculated")
        return
    
    speed_pdf = pd.DataFrame(speed_rows)
    print(f"  ✓ {len(speed_pdf)} speeds calculated")
    print(f"    Range: {speed_pdf['speed_kmh'].min():.1f} - {speed_pdf['speed_kmh'].max():.1f} km/h")
    print(f"    Mean:  {speed_pdf['speed_kmh'].mean():.1f} km/h")
    
    # Create 5-minute windows
    print(f"\nStep 3: Creating {WINDOW_MINUTES}-minute time windows...")
    speed_pdf['window_start'] = speed_pdf['timestamp_dt'].dt.floor(f'{WINDOW_MINUTES}min')
    speed_pdf['window_end'] = speed_pdf['window_start'] + pd.Timedelta(minutes=WINDOW_MINUTES)
    
    # Aggregate by (segment + window)
    print(f"\nStep 4: Aggregating by segment and window...")
    agg = speed_pdf.groupby(['segment_id', 'window_start', 'window_end', 'road_type', 'road_name']).agg({
        'speed_kmh': 'mean',
        'latitude': 'mean',
        'longitude': 'mean',
        'timestamp_dt': 'count'
    }).rename(columns={
        'speed_kmh': 'avg_speed_kmh',
        'timestamp_dt': 'point_count'
    }).reset_index()
    
    print(f"  ✓ {len(agg)} segment-window combinations")
    
    # Classify Congestion
    print(f"\nStep 5: Classifying congestion...")
    
    def classify(speed):
        if speed < SEVERE_THRESHOLD:
            return 'SEVERE'
        elif speed < HEAVY_THRESHOLD:
            return 'HEAVY'
        elif speed < MODERATE_THRESHOLD:
            return 'MODERATE'
        else:
            return 'FREE_FLOW'
    
    agg['congestion_level'] = agg['avg_speed_kmh'].apply(classify)
    
    # Statistics
    congestion_counts = agg['congestion_level'].value_counts()
    print(f"  Congestion distribution:")
    for level in ['FREE_FLOW', 'MODERATE', 'HEAVY', 'SEVERE']:
        count = congestion_counts.get(level, 0)
        print(f"    {level:12}: {count:3} segments")
    
    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = f"{OUTPUT_DIR}/batch_{batch_id}.csv"
    agg.to_csv(output_file, index=False)
    print(f"\n✓ Saved to: {output_file}")
    print(f"{'='*80}\n")


os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

print(f"Starting streaming with complete pipeline...")
print(f"  Output: {OUTPUT_DIR}")
print(f"  Checkpoint: {CHECKPOINT_DIR}")
print(f"  Window: {WINDOW_MINUTES} minutes")
print(f"  Trigger: 30 seconds")
print()

query = gps_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", f"file://{CHECKPOINT_DIR}") \
    .trigger(processingTime='30 seconds') \
    .start()

print("✓ Streaming started - Complete pipeline active")
print("=" * 80)

query.awaitTermination()
