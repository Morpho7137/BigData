#!/usr/bin/env python3
"""
Route Planning Module - Optimized with spatial index for fast routing
"""

import geopandas as gpd
import networkx as nx
from shapely.geometry import Point
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from speed_calculator import haversine_distance

class RoutePlanner:
    def __init__(self, road_network_path):
        self.road_network_path = road_network_path
        self.roads = None
        self.graph_cache = {}  # Cache graphs by bounding box
        
    def load_roads(self):
        """Load road network with spatial index for fast queries"""
        if self.roads is None:
            import time
            print("Loading road network for route planning...")
            start = time.time()
            self.roads = gpd.read_file(self.road_network_path)
            load_time = time.time() - start
            print(f"✓ Loaded {len(self.roads)} segments in {load_time:.2f}s")
            
            # Build spatial index (R-tree) 
            print("Building spatial index...")
            index_start = time.time()
            _ = self.roads.sindex  # Creates spatial index
            index_time = time.time() - index_start
            print(f"✓ Spatial index built in {index_time:.2f}s")
            
            total_time = time.time() - start
            print(f"✓ Total initialization: {total_time:.2f}s")
        return self.roads
    
    def get_dynamic_buffer(self, start_coords, end_coords):
        """Calculate buffer size based on route distance - REDUCED for speed"""
        distance_km = haversine_distance(start_coords[1], start_coords[0], 
                                        end_coords[1], end_coords[0])
        
        if distance_km < 5:
            return 0.03  # ~3km buffer for short routes
        elif distance_km < 20:
            return 0.05  # ~5km buffer for medium routes
        elif distance_km < 50:
            return 0.08  # ~8km buffer for long routes
        else:
            return 0.1   # ~10km buffer for very long routes 
    
    def find_nearest_segment(self, lat, lon, max_distance_degrees=0.0005):
        """Find nearest road segment to clicked point"""
        roads = self.load_roads()
        point = Point(lon, lat)
        
        roads_copy = roads.copy()
        roads_copy['distance'] = roads_copy.geometry.distance(point)
        
        nearest = roads_copy[roads_copy['distance'] < max_distance_degrees].nsmallest(1, 'distance')
        
        if len(nearest) == 0:
            nearest = roads_copy.nsmallest(1, 'distance')
        
        if len(nearest) > 0:
            segment = nearest.iloc[0]
            center = segment.geometry.centroid
            return {
                'segment_id': segment['segment_id'],
                'lat': center.y,
                'lon': center.x,
                'road_name': segment.get('name', 'Unknown'),
                'road_type': segment.get('highway', 'Unknown')
            }
        
        return None
    
    def build_graph_with_bbox(self, congestion_data, start_coords, end_coords, buffer):
        """Build weighted graph with bounding box optimization and one-way support"""
        import time
        roads = self.load_roads()
        
        # Create bounding box
        min_lat = min(start_coords[1], end_coords[1]) - buffer
        max_lat = max(start_coords[1], end_coords[1]) + buffer
        min_lon = min(start_coords[0], end_coords[0]) - buffer
        max_lon = max(start_coords[0], end_coords[0]) + buffer
        
        # Filter roads to bounding box - NOW FAST with spatial index
        print(f"Filtering roads to bounding box...")
        filter_start = time.time()
        roads_filtered = roads.cx[min_lon:max_lon, min_lat:max_lat]
        filter_time = time.time() - filter_start
        print(f"✓ Filtered to {len(roads_filtered)} segments in {filter_time:.2f}s")
        
        # Build graph
        G = nx.DiGraph()
        
        # Create speed lookup from congestion data (FAST dict)
        speed_dict = {}
        if congestion_data is not None:
            speed_dict = dict(zip(congestion_data['segment_id'], congestion_data['avg_speed_kmh']))
        
        # Add edges
        print("Building graph...")
        graph_start = time.time()
        
        for idx, road in roads_filtered.iterrows():
            segment_id = road['segment_id']
            geom = road['geometry']
            
            coords = list(geom.coords)
            if len(coords) < 2:
                continue
            
            start_node = coords[0]
            end_node = coords[-1]
            
            # Get speed (from congestion or default)
            speed = speed_dict.get(segment_id, 30.0)
            speed = max(speed, 5.0)
            
            # Calculate distance using haversine_distance
            distance_km = haversine_distance(start_node[1], start_node[0], 
                                            end_node[1], end_node[0])
            
            # Weight = travel time in hours
            weight = distance_km / speed
            
            # Check if road is one-way (handle string booleans from GeoJSON)
            oneway = road.get('oneway', False)
            
            # Convert string boolean to actual boolean
            if isinstance(oneway, str):
                oneway = oneway.lower() in ['true', '1', 'yes']
            
            # Add forward edge (always)
            G.add_edge(start_node, end_node,
                      weight=weight,
                      segment_id=segment_id,
                      distance=distance_km)
            
            # Add reverse edge ONLY if NOT one-way
            if not oneway:
                G.add_edge(end_node, start_node,
                          weight=weight,
                          segment_id=segment_id,
                          distance=distance_km)
        
        graph_time = time.time() - graph_start
        print(f"✓ Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges in {graph_time:.2f}s")
        return G
    
    def heuristic(self, node1, node2):
        """Heuristic for A* (straight-line distance)"""
        return haversine_distance(node1[1], node1[0], node2[1], node2[0])
    
    def calculate_route(self, start_segment_id, end_segment_id, congestion_data=None):
        """Calculate fastest route between two segments using A* algorithm"""
        import time
        route_start = time.time()
        
        roads = self.load_roads()
        
        # Get start and end segment geometries
        start_road = roads[roads['segment_id'] == start_segment_id]
        end_road = roads[roads['segment_id'] == end_segment_id]
        
        if len(start_road) == 0 or len(end_road) == 0:
            return None
        
        # Get center coordinates
        start_center = start_road.iloc[0].geometry.centroid
        end_center = end_road.iloc[0].geometry.centroid
        start_coords = (start_center.x, start_center.y)
        end_coords = (end_center.x, end_center.y)
        
        # Get dynamic buffer based on distance
        buffer = self.get_dynamic_buffer(start_coords, end_coords)
        
        # Build optimized graph
        G = self.build_graph_with_bbox(congestion_data, start_coords, end_coords, buffer)
        
        # Get actual start/end nodes from segments
        start_coords_list = list(start_road.iloc[0].geometry.coords)
        end_coords_list = list(end_road.iloc[0].geometry.coords)
        
        start_node = start_coords_list[0]
        end_node = end_coords_list[-1]
        
        try:
            # Use A* algorithm instead of Dijkstra (MUCH FASTER for long routes)
            print("Running A* pathfinding...")
            pathfind_start = time.time()
            
            path_nodes = nx.astar_path(
                G, 
                start_node, 
                end_node, 
                heuristic=self.heuristic,
                weight='weight'
            )
            
            pathfind_time = time.time() - pathfind_start
            print(f"✓ A* pathfinding took {pathfind_time:.2f}s")
            
            # Extract segment IDs from path
            segment_ids = []
            for i in range(len(path_nodes) - 1):
                edge_data = G.get_edge_data(path_nodes[i], path_nodes[i+1])
                if edge_data:
                    segment_ids.append(edge_data['segment_id'])
            
            # Remove consecutive duplicates (backtracking on same segment)
            cleaned_segments = []
            prev_seg = None
            for seg in segment_ids:
                if seg != prev_seg:
                    cleaned_segments.append(seg)
                    prev_seg = seg
            
            total_time = time.time() - route_start
            print(f"✓ Total route calculation: {total_time:.2f}s for {len(cleaned_segments)} segments")
            
            return cleaned_segments
            
        except (nx.NetworkXNoPath, nx.NodeNotFound, nx.NetworkXError) as e:
            print(f"❌ No path found between segments: {e}")
            return None
    
    def get_segment_center(self, segment_id):
        """Get center point of a segment"""
        roads = self.load_roads()
        segment = roads[roads['segment_id'] == segment_id]
        
        if len(segment) > 0:
            center = segment.iloc[0].geometry.centroid
            return {'lat': center.y, 'lon': center.x}
        
        return None
    
    def get_route_geometry(self, segment_ids):
        """Get GeoJSON for route segments"""
        if not segment_ids or len(segment_ids) == 0:
            return None
            
        roads = self.load_roads()
        route_segments = roads[roads['segment_id'].isin(segment_ids)]
        
        if len(route_segments) > 0:
            import json
            return json.loads(route_segments.to_json())
        
        return None
