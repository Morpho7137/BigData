#!/usr/bin/env python3
"""
Download Beijing road network - Alternative method
"""

import osmnx as ox
import os

print("Downloading Beijing road network...")
print()

# Try place-based download instead of bbox
try:
    # By place name
    G = ox.graph_from_place('Beijing, China', network_type='drive')
    print(f"✓ Downloaded using place name")
except:
    print("Place name failed, trying bbox...")
    # Smaller bbox for testing
    G = ox.graph_from_bbox(
        bbox=(39.95, 39.90, 116.45, 116.35),
        network_type='drive',
        simplify=True
    )
    print(f"✓ Downloaded using bbox")

print(f"  Nodes: {len(G.nodes):,}")
print(f"  Edges: {len(G.edges):,}")

# Save
OUTPUT_DIR = "/mnt/d/BigDataPj/data/beijing_roads"
os.makedirs(OUTPUT_DIR, exist_ok=True)

ox.save_graphml(G, filepath=f"{OUTPUT_DIR}/beijing_roads.graphml")
print(f"✓ Saved to {OUTPUT_DIR}/beijing_roads.graphml")
