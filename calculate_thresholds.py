#!/usr/bin/env python3
"""
Calculate Congestion Thresholds Only
Simplified script focused only on threshold calculation
Uses reusable grid_aggregator module
"""

import os
import glob
import json
import pandas as pd
import numpy as np
from speed_calculator import calculate_speed_kmh
from grid_aggregator import aggregate_by_grid_window, filter_aggregated




DATA_DIR = "/mnt/d/BigDataPj/data/release/taxi_log_2008_by_id"
OUTPUT_DIR = "/mnt/d/BigDataPj/output"
THRESHOLDS_FILE = os.path.join(OUTPUT_DIR, "congestion_thresholds.json")

NUM_FILES = 100

# Filter parameters
MIN_POINTS_PER_WINDOW = 3
MIN_SPEED_KMH = 5.0
MAX_SPEED_KMH = 120

# Bias adjustment
BIAS_OFFSET = 6  # km/h



print("=" * 80)
print("CALCULATE CONGESTION THRESHOLDS")
print("=" * 80)
print()

# Load taxi files
taxi_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.txt")))[:NUM_FILES]
print(f"Loading {len(taxi_files)} taxi files...")

all_data = []
for file in taxi_files:
    df = pd.read_csv(file, header=None, 
                     names=['taxi_id', 'timestamp', 'longitude', 'latitude'])
    all_data.append(df)

gps_data = pd.concat(all_data, ignore_index=True)
gps_data['timestamp'] = pd.to_datetime(gps_data['timestamp'])
gps_data = gps_data.sort_values(['taxi_id', 'timestamp']).reset_index(drop=True)

print(f"✓ Loaded {len(gps_data):,} GPS points")
print()

# Calculate speeds
print("Calculating speeds...")
gps_data['speed_kmh'] = np.nan

for taxi_id in gps_data['taxi_id'].unique():
    taxi_data = gps_data[gps_data['taxi_id'] == taxi_id].sort_values('timestamp')
    
    for i in range(1, len(taxi_data)):
        curr_idx = taxi_data.iloc[i].name
        prev = taxi_data.iloc[i-1]
        curr = taxi_data.iloc[i]
        
        speed = calculate_speed_kmh(
            prev['latitude'], prev['longitude'], prev['timestamp'],
            curr['latitude'], curr['longitude'], curr['timestamp']
        )
        
        if speed is not None:
            gps_data.loc[curr_idx, 'speed_kmh'] = speed

gps_data = gps_data.dropna(subset=['speed_kmh'])
print(f"✓ Calculated {len(gps_data):,} valid speeds")
print()

# Aggregate by grid and window
print("Aggregating by grid cell and time window...")
aggregated = aggregate_by_grid_window(gps_data)
print(f"✓ Created {len(aggregated):,} aggregations")
print()

# Apply filters
print("Applying reliability filters...")
filtered = filter_aggregated(aggregated, MIN_POINTS_PER_WINDOW, MIN_SPEED_KMH, MAX_SPEED_KMH)
print(f"✓ After filtering: {len(filtered):,} valid aggregations")
print()

# Calculate thresholds
print("Calculating percentile thresholds...")
windowed_speeds = filtered['avg_speed'].values

p25 = np.percentile(windowed_speeds, 25)
p50 = np.percentile(windowed_speeds, 50)
p75 = np.percentile(windowed_speeds, 75)

# Apply bias
severe_adj = p25
heavy_adj = p50 + BIAS_OFFSET
moderate_adj = p75 + BIAS_OFFSET
freeflow_adj = p75 + BIAS_OFFSET

thresholds = {
    "raw_percentiles": {
        "p25": round(p25, 2),
        "p50": round(p50, 2),
        "p75": round(p75, 2)
    },
    "severe_congestion": round(severe_adj, 2),
    "heavy_congestion": round(heavy_adj, 2),
    "moderate_congestion": round(moderate_adj, 2),
    "free_flow": round(freeflow_adj, 2),
    "statistics": {
        "mean": round(float(np.mean(windowed_speeds)), 2),
        "std": round(float(np.std(windowed_speeds)), 2),
        "min": round(float(np.min(windowed_speeds)), 2),
        "max": round(float(np.max(windowed_speeds)), 2),
        "sample_size": len(windowed_speeds)
    },
    "bias_offset": BIAS_OFFSET
}

print(f"\nRaw Percentiles:")
print(f"  p25: {p25:.2f} km/h")
print(f"  p50: {p50:.2f} km/h")
print(f"  p75: {p75:.2f} km/h")
print()

print(f"Congestion Thresholds (with +{BIAS_OFFSET} km/h bias):")
print(f"  SEVERE    (< p25):      < {severe_adj:.2f} km/h")
print(f"  HEAVY     (< p50+bias): < {heavy_adj:.2f} km/h")
print(f"  MODERATE  (< p75+bias): < {moderate_adj:.2f} km/h")
print(f"  FREE_FLOW (≥ p75+bias): ≥ {freeflow_adj:.2f} km/h")
print()

# Save thresholds
os.makedirs(OUTPUT_DIR, exist_ok=True)
with open(THRESHOLDS_FILE, 'w') as f:
    json.dump(thresholds, f, indent=2)

print(f"✓ Thresholds saved to: {THRESHOLDS_FILE}")
print("=" * 80)
