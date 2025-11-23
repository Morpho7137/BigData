#!/usr/bin/env python3
"""
Taxi Simulator Module - Optimized with route caching
"""

class TaxiSimulator:
    def __init__(self):
        self.active = False
        self.current_segment_id = None
        self.route = []
        self.route_index = 0
        self.destination_segment_id = None
        self.last_congestion_hash = None  # Track congestion changes
    
    def start(self, start_segment_id, destination_segment_id, route):
        """Initialize taxi simulation"""
        self.active = True
        self.current_segment_id = start_segment_id
        self.destination_segment_id = destination_segment_id
        self.route = route
        self.route_index = 0
        self.last_congestion_hash = None
    
    def stop(self):
        """Stop taxi simulation"""
        self.active = False
        self.route = []
        self.route_index = 0
        self.last_congestion_hash = None
    
    def is_active(self):
        """Check if taxi is running"""
        return self.active
    
    def should_recalculate_route(self, congestion_dict):
        """Check if route should be recalculated based on congestion changes"""
        if not self.route or self.route_index >= len(self.route):
            return False
        
        # Look ahead at next 20 segments
        look_ahead = min(20, len(self.route) - self.route_index)
        upcoming_segments = self.route[self.route_index:self.route_index + look_ahead]
        
        # Create hash of upcoming segment congestion levels
        congestion_ahead = []
        for seg_id in upcoming_segments:
            congestion_ahead.append(congestion_dict.get(seg_id, 'UNKNOWN'))
        
        current_hash = hash(tuple(congestion_ahead))
        
        # If congestion changed significantly, recalculate
        if self.last_congestion_hash is None:
            self.last_congestion_hash = current_hash
            return False
        
        if current_hash != self.last_congestion_hash:
            print(f"Congestion changed ahead - recalculating route")
            self.last_congestion_hash = current_hash
            return True
        
        return False
    
    def calculate_movement(self, congestion_dict, base_segments=10):
        """
        Calculate how many segments taxi can move based on congestion
        
        Args:
            congestion_dict: Dict mapping segment_id -> congestion_level (FAST lookup)
            base_segments: Base movement per batch (default 10)
            
        Returns:
            dict with movement info
        """
        if not self.active or not self.route:
            return None
        
        current_idx = self.route_index
        look_ahead = min(base_segments, len(self.route) - current_idx)
        penalty = 0.0
        
        # Look ahead at next segments to calculate penalty
        for i in range(current_idx, min(current_idx + look_ahead, len(self.route))):
            segment_id = self.route[i]
            congestion = congestion_dict.get(segment_id)
            
            if congestion:
                # Apply penalty based on congestion
                if congestion == 'SEVERE':
                    penalty += 2.0
                elif congestion == 'HEAVY':
                    penalty += 1.0
                elif congestion == 'MODERATE':
                    penalty += 0.5
        
        # Calculate actual movement (minimum 1 segment)
        actual_movement = max(1, int(base_segments - penalty))
        new_idx = min(current_idx + actual_movement, len(self.route) - 1)
        
        # Check if reached destination
        reached = (new_idx >= len(self.route) - 1)
        
        if reached:
            self.active = False
        
        # Update state
        self.route_index = new_idx
        self.current_segment_id = self.route[new_idx]
        
        return {
            'new_index': new_idx,
            'new_segment_id': self.route[new_idx],
            'segments_moved': actual_movement,
            'penalty': int(penalty),
            'reached': reached,
            'progress': f"{new_idx + 1}/{len(self.route)}",
            'remaining_route': self.route[new_idx:]  # Only segments ahead
        }
    
    def update_route(self, new_route):
        """Update route preserving current segment position"""
        if not new_route or len(new_route) == 0:
            return
        
        # Try to find current segment in new route
        if self.current_segment_id in new_route:
            # Find index of current segment in new route
            try:
                self.route_index = new_route.index(self.current_segment_id)
                self.route = new_route
                print(f"Route updated, continuing from segment {self.current_segment_id} at index {self.route_index}")
            except ValueError:
                # Segment not in new route, keep old route
                print(f"Current segment not in new route, keeping old route")
        else:
            # Current segment not in new route anymore, reset to start
            self.route = new_route
            self.route_index = 0
            print(f"Current segment removed from route, restarting from beginning")

