#!/usr/bin/env python3
"""
Sort GPS data by timestamp and filter to first 4 days
Process ALL taxi files for full deployment
"""

import os
import glob
import pandas as pd
from datetime import datetime

SOURCE_DIR = "/mnt/d/BigDataPj/data/release/taxi_log_2008_by_id"
OUTPUT_DIR = "/mnt/d/BigDataPj/data/sorted"

# Date range: First 4 days (Feb 2-5, 2008)
START_DATE = "2008-02-02"
END_DATE = "2008-02-05 23:59:59"

print("=" * 80)
print("FULL DATASET PROCESSING")
print("=" * 80)
print(f"Date range: {START_DATE} to {END_DATE}")
print()

# Clean output directory
if os.path.exists(OUTPUT_DIR):
    print(f"Cleaning old sorted data from: {OUTPUT_DIR}")
    import shutil
    shutil.rmtree(OUTPUT_DIR)
    print("✓ Deleted old directory")
    print()

os.makedirs(OUTPUT_DIR, exist_ok=True)


taxi_files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.txt")))

print(f"Found {len(taxi_files)} taxi files")
print(f"Processing all files...")
print()

total_original = 0
total_filtered = 0
total_taxis_with_data = 0

for i, file in enumerate(taxi_files, 1):
    taxi_id = int(os.path.basename(file).replace(".txt", ""))
    
    try:
        # Read
        df = pd.read_csv(file, header=None, 
                         names=['taxi_id', 'timestamp', 'longitude', 'latitude'])
        
        original_count = len(df)
        total_original += original_count
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter to date range
        df = df[(df['timestamp'] >= START_DATE) & (df['timestamp'] <= END_DATE)]
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        filtered_count = len(df)
        
        # Only write if has data in this date range
        if filtered_count > 0:
            total_filtered += filtered_count
            total_taxis_with_data += 1
            
            # Write to output directory (no header, space-separated)
            output_file = os.path.join(OUTPUT_DIR, f"{taxi_id}.txt")
            df.to_csv(output_file, header=False, index=False, sep=' ')
        
        # Progress output
        if i % 500 == 0 or i == 1:
            print(f"  [{i:5d}/{len(taxi_files)}] Taxi {taxi_id:5d}: {original_count:6d} → {filtered_count:6d} points")
    
    except Exception as e:
        print(f"  ❌ Error processing taxi {taxi_id}: {e}")
        continue

print()
print("=" * 80)
print("PROCESSING COMPLETE")
print("=" * 80)
print(f"Total taxi files processed: {len(taxi_files)}")
print(f"Taxis with data in date range: {total_taxis_with_data}")
print(f"Total points: {total_original:,} → {total_filtered:,}")
print(f"Reduction: {(1 - total_filtered/total_original)*100:.1f}%")
print(f"Saved to: {OUTPUT_DIR}")
print("=" * 80)
