#!/usr/bin/env python3
"""
Flask Backend 
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import geopandas as gpd
import glob
import os
import json
from route_planner import RoutePlanner
from taxi_simulator import TaxiSimulator

app = Flask(__name__)

# Configuration
ROAD_NETWORK = "/mnt/d/BigDataPj/data/beijing_roads/beijing_roads_final.geojson"
RESULTS_DIR = "/mnt/d/BigDataPj/output/streaming_results"

# Simulation configuration
SIMULATION_MINUTES_PER_BATCH = 5  
AVG_TAXI_SPEED_KMH = 30  # Average taxi speed
AVG_SEGMENT_LENGTH_KM = 0.1  # ~100 meters per segment

# Calculate taxi movement per batch
TAXI_SEGMENTS_PER_BATCH = int((AVG_TAXI_SPEED_KMH / 60) * SIMULATION_MINUTES_PER_BATCH / AVG_SEGMENT_LENGTH_KM)

# Initialize route planner and LOAD ROADS at startup
print("=" * 60)
print("Initializing route planner...")
planner = RoutePlanner(ROAD_NETWORK)
print("Loading road network (this takes ~30 seconds)...")
roads_cache = planner.load_roads()
print(f"✓ Road network loaded: {len(roads_cache)} segments")
print("=" * 60)

# Initialize taxi simulator
taxi = TaxiSimulator()

# Cache current route geometry to avoid recalculating every time
cached_route_geojson = None

print(f"Simulation configuration:")
print(f"  Batch interval: 12 seconds (wall-clock)")
print(f"  Simulation time per batch: {SIMULATION_MINUTES_PER_BATCH} minutes")
print(f"  Taxi segments per batch: {TAXI_SEGMENTS_PER_BATCH}")
print(f"  Expected taxi speed: {AVG_TAXI_SPEED_KMH} km/h (simulation time)")
print("=" * 60)
print("Dashboard ready")

def load_roads_lazy():
    """Roads already loaded at startup"""
    return roads_cache

def get_latest_batch():
    """Get most recent batch file"""
    batch_files = glob.glob(os.path.join(RESULTS_DIR, "batch_*.csv"))
    if not batch_files:
        return None
    return max(batch_files, key=os.path.getctime)

@app.route('/')
def index():
    """Serve dashboard"""
    return render_template('dashboard.html')

@app.route('/api/congestion')
def get_congestion():
    """Get current congestion - OPTIMIZED with simulation-time-aligned taxi"""
    global cached_route_geojson
    
    try:
        latest_batch = get_latest_batch()
        
        if not latest_batch:
            return jsonify({"error": "No batch data available"}), 404
        
        # Load results
        results = pd.read_csv(latest_batch)
        batch_num = os.path.basename(latest_batch).replace("batch_", "").replace(".csv", "")
        
        # Get segments from results
        result_segment_ids = set(results['segment_id'].unique())
        all_roads = load_roads_lazy()
        roads_subset = all_roads[all_roads['segment_id'].isin(result_segment_ids)]
        
        # Merge congestion data
        congestion_data = roads_subset.merge(
            results[['segment_id', 'congestion_level', 'avg_speed_kmh']], 
            on='segment_id', 
            how='inner'
        )
        
        # Add road_name and road_type from roads data
        congestion_data['road_name'] = congestion_data['segment_id'].map(
            all_roads.set_index('segment_id')['name']
        ).fillna('Unknown')
        
        congestion_data['road_type'] = congestion_data['segment_id'].map(
            all_roads.set_index('segment_id')['highway']
        ).fillna('Unknown')
        
        geojson = json.loads(congestion_data.to_json())
        
        # Time window
        window_start = results['window_start'].iloc[0] if 'window_start' in results.columns else None
        window_end = results['window_end'].iloc[0] if 'window_end' in results.columns else None
        
        # Update taxi if active 
        taxi_data = None
        if taxi.is_active():
            print(f"\nTaxi is active (Batch {batch_num}, Simulation window: {window_start})")
            try:
                # Create congestion dict 
                congestion_dict = dict(zip(results['segment_id'], results['congestion_level']))
                
                # Check if route needs recalculation 
                if taxi.should_recalculate_route(congestion_dict):
                    print("⚠️ Severe congestion ahead - recalculating route...")
                    new_route = planner.calculate_route(
                        taxi.current_segment_id,
                        taxi.destination_segment_id,
                        results
                    )
                    
                    if new_route and len(new_route) > 0:
                        taxi.update_route(new_route)
                        # Update cached geometry
                        cached_route_geojson = planner.get_route_geometry(new_route)
                        print(f"✓ Route recalculated: {len(new_route)} segments")
                
                # Calculate movement 
                movement_info = taxi.calculate_movement(
                    congestion_dict, 
                    base_segments=TAXI_SEGMENTS_PER_BATCH  
                )
                
                if movement_info is None:
                    print("❌ Movement calculation returned None")
                else:
                    print(f"Taxi moved: segment={movement_info['new_segment_id']}, progress={movement_info['progress']}")
                    
                    # Get position
                    new_position = planner.get_segment_center(movement_info['new_segment_id'])
                    
                    if new_position is None:
                        print(f"❌ Could not get position for segment {movement_info['new_segment_id']}")
                    elif cached_route_geojson is None:
                        print(f"❌ Cached route geometry is None")
                    else:
                        # Get geometry for REMAINING route only (clean up passed segments)
                        remaining_route_geojson = planner.get_route_geometry(movement_info['remaining_route'])
                        
                        taxi_data = {
                            'position': new_position,
                            'route': remaining_route_geojson if remaining_route_geojson else cached_route_geojson,
                            'progress': movement_info['progress'],
                            'reached': movement_info['reached']
                        }
                        
                        print(f"✓ Taxi at: lat={new_position['lat']:.4f}, lon={new_position['lon']:.4f}")
                        
            except Exception as e:
                print(f"❌ Error updating taxi: {e}")
                import traceback
                traceback.print_exc()
        
        # Stats
        stats = {
            'batch': batch_num,
            'total_segments': len(congestion_data),
            'free_flow': len(congestion_data[congestion_data['congestion_level'] == 'FREE_FLOW']),
            'moderate': len(congestion_data[congestion_data['congestion_level'] == 'MODERATE']),
            'heavy': len(congestion_data[congestion_data['congestion_level'] == 'HEAVY']),
            'severe': len(congestion_data[congestion_data['congestion_level'] == 'SEVERE']),
            'window_start': window_start,
            'window_end': window_end
        }
        
        response_data = {
            'geojson': geojson,
            'stats': stats,
            'taxi': taxi_data
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"ERROR in /api/congestion: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/find-segment', methods=['POST'])
def find_segment():
    """Find nearest road segment to clicked point"""
    try:
        data = request.json
        segment = planner.find_nearest_segment(data['lat'], data['lon'])
        
        if segment:
            return jsonify(segment)
        return jsonify({"error": "No segment found"}), 404
    except Exception as e:
        print(f"Error in find_segment: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/calculate-route', methods=['POST'])
def calculate_route_api():
    """Calculate route without starting taxi"""
    global cached_route_geojson
    
    try:
        data = request.json
        start_segment_id = data['start']['segment_id']
        dest_segment_id = data['destination']['segment_id']
        
        print(f"Calculating route from {start_segment_id} to {dest_segment_id}")
        
        # Get current congestion
        latest_batch = get_latest_batch()
        if not latest_batch:
            return jsonify({"error": "No congestion data"}), 404
        
        results = pd.read_csv(latest_batch)
        
        # Calculate route
        route = planner.calculate_route(start_segment_id, dest_segment_id, results)
        
        if route is None or len(route) == 0:
            return jsonify({"error": "No route found"}), 404
        
        # Get route geometry and cache it
        cached_route_geojson = planner.get_route_geometry(route)
        
        print(f"Route calculated: {len(route)} segments")
        
        return jsonify({
            "success": True, 
            "route_length": len(route),
            "route": cached_route_geojson
        })
    except Exception as e:
        print(f"Error in calculate_route: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/toggle-taxi', methods=['POST'])
def toggle_taxi():
    """Start or stop taxi simulation - OPTIMIZED with route reuse"""
    global cached_route_geojson
    
    try:
        data = request.json
        action = data.get('action')
        
        print(f"\n{'='*60}")
        print(f"TOGGLE TAXI REQUEST: {action}")
        print(f"{'='*60}")
        
        if action == 'stop':
            taxi.stop()
            print("✓ Taxi stopped")
            return jsonify({"success": True, "status": "stopped"})
        
        elif action == 'start':
            start_segment_id = data['start']['segment_id']
            dest_segment_id = data['destination']['segment_id']
            
            print(f"Start segment: {start_segment_id}")
            print(f"Destination segment: {dest_segment_id}")
            
            # Check if we already have a cached route 
            if cached_route_geojson is not None:
                print("✓ Using cached route (already calculated)")
                
                # Extract segment IDs from cached GeoJSON
                route = [feature['properties']['segment_id'] 
                        for feature in cached_route_geojson['features']]
                
                if route and len(route) > 0:
                    # Start taxi with cached route (INSTANT!)
                    taxi.start(start_segment_id, dest_segment_id, route)
                    print(f"✓ Taxi started with cached route: {len(route)} segments")
                    print(f"Taxi state: active={taxi.is_active()}, index={taxi.route_index}, segment={taxi.current_segment_id}")
                    
                    return jsonify({
                        "success": True, 
                        "status": "started",
                        "route_length": len(route)
                    })
            
            # If no cached route, calculate it (fallback)
            print("No cached route - calculating new route...")
            
            # Get current congestion
            latest_batch = get_latest_batch()
            if not latest_batch:
                print("❌ No batch data available")
                return jsonify({"error": "No congestion data"}), 404
            
            print(f"Using batch: {latest_batch}")
            results = pd.read_csv(latest_batch)
            print(f"Loaded {len(results)} congestion records")
            
            # Calculate route
            print("Calculating route...")
            route = planner.calculate_route(start_segment_id, dest_segment_id, results)
            
            if route is None:
                print("❌ Route calculation returned None")
                return jsonify({"error": "No route found"}), 404
            
            if len(route) == 0:
                print("❌ Route calculation returned empty list")
                return jsonify({"error": "No route found"}), 404
            
            print(f"✓ Route calculated: {len(route)} segments")
            print(f"First 5 segments: {route[:5]}")
            
            # Cache route geometry
            print("Getting route geometry...")
            cached_route_geojson = planner.get_route_geometry(route)
            
            if cached_route_geojson is None:
                print("❌ Route geometry is None")
                return jsonify({"error": "Failed to get route geometry"}), 500
            
            print(f"✓ Route geometry cached: {len(cached_route_geojson.get('features', []))} features")
            
            # Start taxi
            print("Starting taxi simulator...")
            taxi.start(start_segment_id, dest_segment_id, route)
            print(f"✓ Taxi simulator started")
            print(f"Taxi state: active={taxi.is_active()}, index={taxi.route_index}, segment={taxi.current_segment_id}")
            
            return jsonify({
                "success": True, 
                "status": "started",
                "route_length": len(route)
            })
        
        return jsonify({"error": "Invalid action"}), 400
        
    except Exception as e:
        print(f"\n❌ ERROR in toggle_taxi: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
