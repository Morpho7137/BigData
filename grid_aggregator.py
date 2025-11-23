#!/usr/bin/env python3
"""
Grid Aggregation Utility Module
Provides shared functions for grid-based aggregation
Used by both batch threshold calculation and Spark streaming
"""

import pandas as pd
import numpy as np


GRID_PRECISION = 3  # 0.001 degrees = ~111 meters
WINDOW_MINUTES = 5


def assign_grid_cell(lat, lon, precision=GRID_PRECISION):
    """
    Assign GPS point to grid cell
    Grid cell ID: "{lat}_{lon}" with specified precision
    """
    grid_lat = round(lat, precision)
    grid_lon = round(lon, precision)
    return f"{grid_lat}_{grid_lon}"


def aggregate_by_grid_window(gps_df):
    """
    Aggregate GPS speeds by grid cell and time window
    
    Args:
        gps_df: DataFrame with columns:
                - speed_kmh: calculated speed
                - timestamp: datetime
                - latitude, longitude: GPS coordinates
                - taxi_id: taxi identifier
    
    Returns:
        DataFrame with aggregated speeds by (grid_id, window)
    """
    # Assign grid cells
    gps_df['grid_id'] = gps_df.apply(
        lambda row: assign_grid_cell(row['latitude'], row['longitude'], GRID_PRECISION),
        axis=1
    )
    
    # Create time windows
    gps_df['window'] = gps_df['timestamp'].dt.floor(f'{WINDOW_MINUTES}min')
    
    # Aggregate by grid and window
    aggregated = gps_df.groupby(['grid_id', 'window']).agg({
        'speed_kmh': ['mean', 'count'],
        'latitude': 'first',
        'longitude': 'first',
        'taxi_id': 'nunique'
    }).reset_index()
    
    # Flatten column names
    aggregated.columns = ['grid_id', 'window', 'avg_speed', 'point_count', 'lat', 'lon', 'unique_taxis']
    
    return aggregated


def filter_aggregated(aggregated, min_points=3, min_speed=5.0, max_speed=120):
    """
    Apply reliability filters to aggregated data
    
    Args:
        aggregated: DataFrame from aggregate_by_grid_window()
        min_points: Minimum GPS points per window
        min_speed: Minimum speed threshold (km/h)
        max_speed: Maximum speed threshold (km/h)
    
    Returns:
        Filtered DataFrame
    """
    filtered = aggregated[aggregated['point_count'] >= min_points]
    filtered = filtered[(filtered['avg_speed'] >= min_speed) & 
                        (filtered['avg_speed'] <= max_speed)]
    
    return filtered


def classify_congestion(speed, thresholds):
    """
    Classify a speed into congestion level
    
    Args:
        speed: Speed in km/h
        thresholds: Dictionary with keys:
                   - severe_congestion
                   - heavy_congestion
                   - moderate_congestion
                   - free_flow
    
    Returns:
        String: 'SEVERE', 'HEAVY', 'MODERATE', or 'FREE_FLOW'
    """
    if speed < thresholds['severe_congestion']:
        return 'SEVERE'
    elif speed < thresholds['heavy_congestion']:
        return 'HEAVY'
    elif speed < thresholds['moderate_congestion']:
        return 'MODERATE'
    else:
        return 'FREE_FLOW'
