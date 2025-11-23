#!/usr/bin/env python3

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import sys
sys.path.append('/mnt/d/BigDataPj/code')
from speed_calculator import haversine_distance


def haversine_distance_m(lat1, lon1, lat2, lon2):
    """Haversine distance in meters (wraps speed_calculator function)"""
    return haversine_distance(lat1, lon1, lat2, lon2) * 1000


def point_to_line_distance(point_lat, point_lon, linestring):
    """
    Calculate minimum distance from a point to a LineString using Haversine.
    
    Args:
        point_lat, point_lon: GPS point coordinates
        linestring: Shapely LineString geometry (WGS84)
    
    Returns:
        Minimum distance in meters
    """
    coords = list(linestring.coords)
    min_distance = float('inf')
    
    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i]
        lon2, lat2 = coords[i + 1]
        
        # Distance to segment endpoints
        dist_start = haversine_distance_m(point_lat, point_lon, lat1, lon1)
        dist_end = haversine_distance_m(point_lat, point_lon, lat2, lon2)
        
        seg_length = haversine_distance_m(lat1, lon1, lat2, lon2)
        
        if seg_length < 0.1:
            min_distance = min(min_distance, dist_start)
            continue
        
        # Project point onto line segment (perpendicular foot)
        t = max(0, min(1, (
            (point_lon - lon1) * (lon2 - lon1) + 
            (point_lat - lat1) * (lat2 - lat1)
        ) / (
            (lon2 - lon1)**2 + (lat2 - lat1)**2
        )))
        
        closest_lon = lon1 + t * (lon2 - lon1)
        closest_lat = lat1 + t * (lat2 - lat1)
        
        dist = haversine_distance_m(point_lat, point_lon, closest_lat, closest_lon)
        min_distance = min(min_distance, dist)
    
    return min_distance


class AdaptiveRoadNetworkMatcher:
    def __init__(self, geojson_file):
        """Initialize with segmented road network GeoJSON."""
        print("Loading segmented road network...")
        self.segments = gpd.read_file(geojson_file)
        
        if self.segments.crs is None:
            self.segments.set_crs("EPSG:4326", inplace=True)
        elif self.segments.crs != "EPSG:4326":
            self.segments = self.segments.to_crs("EPSG:4326")
        
        if 'segment_id' not in self.segments.columns:
            self.segments['segment_id'] = range(len(self.segments))
        
        print("Building spatial index...")
        self.spatial_idx = self.segments.sindex
        
        # Road-type-specific thresholds 
        self.thresholds = {
            'motorway': 15,
            'motorway_link': 15,
            'trunk': 15,
            'trunk_link': 15,
            'primary': 20,
            'primary_link': 20,
            'secondary': 20,
            'secondary_link': 20,
            'tertiary': 30,
            'tertiary_link': 30,
            'unclassified': 50,
            'residential': 120,  # Higher threshold for residential
            'service': 100,
            'living_street': 100,
            'default': 50
        }
        
        print(f"✓ Loaded {len(self.segments):,} road segments")
        print(f"  Using Haversine distance (imported from speed_calculator)")
        print()
        print("Road-type-specific thresholds:")
        for road_type in ['motorway', 'trunk', 'primary', 'secondary', 
                          'tertiary', 'residential']:
            print(f"  {road_type:15} : {self.thresholds[road_type]:3}m")
        print()


    def match_point(self, lat, lon, k=10):
        """
        Match GPS point using adaptive threshold based on nearby road types.
        """
        point = Point(lon, lat)
        
        # Large buffer to find all nearby roads
        buffer_deg = 150 / 111000  # 150m
        bbox = (lon - buffer_deg, lat - buffer_deg, 
                lon + buffer_deg, lat + buffer_deg)
        
        candidate_indices = list(self.spatial_idx.intersection(bbox))
        
        if not candidate_indices:
            return None
        
        candidates = self.segments.iloc[candidate_indices].copy()
        
        if len(candidates) == 0:
            return None
        
        # Calculate Haversine distance to each candidate
        distances = []
        for idx, row in candidates.iterrows():
            dist = point_to_line_distance(lat, lon, row.geometry)
            distances.append(dist)
        
        candidates['distance_m'] = distances
        candidates = candidates.sort_values('distance_m')
        
        # Try matching with road-type-specific thresholds
        # Priority: major roads first
        priority_order = ['motorway', 'trunk', 'primary', 'secondary', 
                         'tertiary', 'unclassified', 'residential', 'service']
        
        # First pass: try high-priority roads with strict thresholds
        for road_type in priority_order:
            threshold = self.thresholds.get(road_type, self.thresholds['default'])
            
            matching = candidates[
                (candidates['highway'] == road_type) & 
                (candidates['distance_m'] <= threshold)
            ]
            
            if len(matching) > 0:
                best = matching.iloc[0]
                
                # Calculate segment length with Haversine
                coords = list(best.geometry.coords)
                segment_length = 0
                for i in range(len(coords) - 1):
                    lon1, lat1 = coords[i]
                    lon2, lat2 = coords[i + 1]
                    segment_length += haversine_distance_m(lat1, lon1, lat2, lon2)
                
                return {
                    'segment_id': best['segment_id'],
                    'distance_m': float(best['distance_m']),
                    'road_name': best.get('name', ''),
                    'road_type': best.get('highway', ''),
                    'segment_length_m': segment_length,
                    'geometry': best.geometry,
                    'match_strategy': f'priority_{road_type}'
                }
        
        # Second pass: accept any match within relaxed threshold
        closest = candidates.iloc[0]
        threshold = self.thresholds.get(
            closest['highway'], 
            self.thresholds['default']
        )
        
        if closest['distance_m'] <= threshold:
            coords = list(closest.geometry.coords)
            segment_length = 0
            for i in range(len(coords) - 1):
                lon1, lat1 = coords[i]
                lon2, lat2 = coords[i + 1]
                segment_length += haversine_distance_m(lat1, lon1, lat2, lon2)
            
            return {
                'segment_id': closest['segment_id'],
                'distance_m': float(closest['distance_m']),
                'road_name': closest.get('name', ''),
                'road_type': closest.get('highway', ''),
                'segment_length_m': segment_length,
                'geometry': closest.geometry,
                'match_strategy': f'relaxed_{closest["highway"]}'
            }
        
        return None


    def match_trajectory(self, gps_points_df, verbose=True):
        """Match entire GPS trajectory with adaptive thresholds."""
        results = []
        total = len(gps_points_df)
        matched_count = 0
        strategy_counts = {}
        
        for idx, row in gps_points_df.iterrows():
            match = self.match_point(row['latitude'], row['longitude'])
            
            if match:
                results.append({
                    'timestamp': row['timestamp'],
                    'original_lat': row['latitude'],
                    'original_lon': row['longitude'],
                    'taxi_id': row.get('taxi_id', None),
                    **match
                })
                matched_count += 1
                
                strategy = match.get('match_strategy', 'unknown')
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
            if verbose and (idx + 1) % 100 == 0:
                print(f"  Processed {idx + 1}/{total} points "
                      f"(matched: {matched_count}/{idx + 1} = "
                      f"{matched_count/(idx+1)*100:.1f}%)")
        
        if verbose:
            print(f"\n✓ Final match rate: {matched_count}/{total} = "
                  f"{matched_count/total*100:.1f}%")
            print(f"\nMatching strategies used:")
            for strategy, count in sorted(strategy_counts.items(), 
                                         key=lambda x: x[1], reverse=True):
                print(f"  {strategy:30} : {count:4} matches")
        
        return pd.DataFrame(results)
