#!/usr/bin/env python3
"""
Split segments with proper attribute type handling for GeoJSON compatibility
"""

import osmnx as ox
import geopandas as gpd
from shapely.geometry import LineString
from shapely.ops import substring
import math

print("=" * 80)
print("SPLITTING SEGMENTS WITH ATTRIBUTE TYPE FIXING")
print("=" * 80)
print()

# Load graph
print("Loading road network...")
G = ox.load_graphml("/mnt/d/BigDataPj/data/beijing_roads/beijing_roads.graphml")
nodes, edges = ox.graph_to_gdfs(G)

print(f"✓ Loaded {len(edges):,} segments")
print()

# Convert to metric CRS
print("Converting to metric CRS (EPSG:3857)...")
edges_metric = edges.to_crs("EPSG:3857")
print(f"✓ Converted to: {edges_metric.crs}")
print()

# Configuration
MAX_SEGMENT_LENGTH = 300
MIN_SEGMENT_LENGTH = 10

print(f"Configuration:")
print(f"  Maximum segment length: {MAX_SEGMENT_LENGTH}m")
print(f"  Minimum segment length: {MIN_SEGMENT_LENGTH}m")
print()


def split_linestring(line, max_length, min_length):
    """Split a LineString into chunks using Shapely's substring."""
    total_length = line.length
    
    if total_length <= max_length and total_length >= min_length:
        return [line]
    
    if total_length < min_length:
        return []
    
    num_segments = math.ceil(total_length / max_length)
    segment_length = total_length / num_segments
    
    segments = []
    for i in range(num_segments):
        start_dist = i * segment_length
        end_dist = min((i + 1) * segment_length, total_length)
        
        try:
            seg = substring(line, start_dist, end_dist, normalized=False)
            if seg and not seg.is_empty and seg.length >= min_length:
                segments.append(seg)
        except Exception as e:
            continue
    
    return segments


# Process segments
print("Processing and standardizing attributes...")

new_edges = []
split_count = 0
kept_count = 0
discarded_count = 0

# Define attributes to preserve and standardize
attributes_to_preserve = ['highway', 'name', 'ref', 'oneway', 'bridge', 
                          'lanes', 'maxspeed', 'tunnel', 'access', 
                          'junction', 'width', 'service']

for counter, (idx, row) in enumerate(edges_metric.iterrows(), start=1):
    chunks = split_linestring(row['geometry'], MAX_SEGMENT_LENGTH, MIN_SEGMENT_LENGTH)
    
    if len(chunks) == 0:
        discarded_count += 1
    elif len(chunks) == 1:
        new_row = row.copy()
        new_row['geometry'] = chunks[0]
        new_row['length'] = chunks[0].length
        new_row['segment_id'] = str(idx)
        
        # Convert all attribute fields to strings (handle None)
        for attr in attributes_to_preserve:
            if attr in new_row.index:
                val = new_row[attr]
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    new_row[attr] = ""  # Empty string instead of None
                elif isinstance(val, list):
                    new_row[attr] = ",".join(str(v) for v in val)  # Join lists
                else:
                    new_row[attr] = str(val)  # Convert to string
        
        new_edges.append(new_row)
        kept_count += 1
    else:
        for chunk_id, chunk in enumerate(chunks):
            new_row = row.copy()
            new_row['geometry'] = chunk
            new_row['length'] = chunk.length
            new_row['segment_id'] = f"{idx}_{chunk_id}"
            
            # Convert all attribute fields to strings
            for attr in attributes_to_preserve:
                if attr in new_row.index:
                    val = new_row[attr]
                    if val is None or (isinstance(val, float) and math.isnan(val)):
                        new_row[attr] = ""
                    elif isinstance(val, list):
                        new_row[attr] = ",".join(str(v) for v in val)
                    else:
                        new_row[attr] = str(val)
            
            new_edges.append(new_row)
        split_count += 1
    
    if counter % 50000 == 0:
        print(f"  Processed {counter:,} / {len(edges_metric):,} segments...")

edges_final_metric = gpd.GeoDataFrame(new_edges, crs=edges_metric.crs)

print(f"\n✓ Processing complete")
print(f"  Segments kept unchanged: {kept_count:,}")
print(f"  Segments split: {split_count:,}")
print(f"  Segments discarded: {discarded_count:,}")
print()

print(f"Results:")
print(f"  Original total: {len(edges):,}")
print(f"  Final total: {len(edges_final_metric):,}")
print()

# Validation
print("Validation:")
print(f"  Max length: {edges_final_metric['length'].max():.2f}m")
print(f"  Min length: {edges_final_metric['length'].min():.2f}m")
print()

# Check attribute preservation
print("Attribute preservation check:")
for attr in attributes_to_preserve:
    if attr in edges_final_metric.columns:
        non_empty = (edges_final_metric[attr] != "").sum()
        print(f"  {attr:12} : {non_empty:,} non-empty values")
    else:
        print(f"  {attr:12} : NOT PRESENT")

print()

# Convert back to WGS84
print("Converting back to WGS84...")
edges_final = edges_final_metric.to_crs("EPSG:4326")

# Save
output_path = "/mnt/d/BigDataPj/data/beijing_roads/beijing_roads_final.geojson"
edges_final.to_file(output_path, driver="GeoJSON")
print(f"✓ Saved to: {output_path}")
print()

print("=" * 80)
print("✓ SEGMENT PROCESSING COMPLETE")
print("=" * 80)
