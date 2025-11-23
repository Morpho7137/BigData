#!/usr/bin/env python3
"""
Speed Calculator Utility Module
Provides shared functions for speed calculation (Haversine formula)
Used by both batch processing and Spark streaming scripts
"""

from math import radians, sin, cos, sqrt, atan2


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate distance between two GPS points using Haversine formula
    
    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)
    
    Returns:
        Distance in kilometers
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    # Earth radius in kilometers
    radius = 6371.0
    distance = radius * c
    
    return distance


def calculate_speed_kmh(lat1, lon1, timestamp1, lat2, lon2, timestamp2):
    """
    Calculate speed between two GPS points
    
    Args:
        lat1, lon1: First point coordinates (degrees)
        timestamp1: First point timestamp (datetime object)
        lat2, lon2: Second point coordinates (degrees)
        timestamp2: Second point timestamp (datetime object)
    
    Returns:
        Speed in km/h (float), or None if invalid
    """
    # Calculate distance
    distance_km = haversine_distance(lat1, lon1, lat2, lon2)
    
    # Calculate time difference in hours
    time_diff_seconds = (timestamp2 - timestamp1).total_seconds()
    time_diff_hours = time_diff_seconds / 3600.0
    
    # Avoid division by zero
    if time_diff_hours <= 0:
        return None
    
    # Calculate speed
    speed = distance_km / time_diff_hours
    
    # Filter valid speeds (0-200 km/h)
    if 0 < speed < 200:
        return speed
    else:
        return None
