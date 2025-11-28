
```
## File Structure & Functions

### Data Preprocessing (Offline - Run Once)
- `split_segments.py` - Download OSM data, normalize road segments (10-300m), save GeoJSON
- `sort_gps_data.py` - Filter GPS by date range (Feb 2-5, 2008), sort by timestamp, output taxi files
- `calculate_thresholds.py` - Sample 100 taxis, compute speed percentiles (p25/p50/p75), create 4 congestion levels

### Core Processing (Real-Time Streaming)
- `gps_producer.py` - Read sorted GPS files, stream to Kafka at 100 pts/sec
- `spark_streaming.py` - Receive GPS from Kafka, map-match, calculate speed, aggregate 5-min windows, classify congestion, write PostgreSQL
- `adaptive_matcher.py` - R-tree spatial index + Haversine distance for map-matching
- `speed_calculator.py` - Haversine formula for distance/speed computation
- `app.py` - Flask API server, serve congestion data + A* routing
- `dashboard.html` - Leaflet map visualization with real-time congestion overlay

### Supporting Files
- `route_planner.py` - A* algorithm for dynamic routing
- `grid_aggregator.py` - Spatial-temporal binning (0.001° grid, 5-min windows)
- `docker-compose.yml` - Kafka + Spark container setup
- `run.sh` - Automated startup script

---

## Execution Sequence

### Step 0: Data Preparation (if needed)
```
cd code
python split_segments.py
python sort_gps_data.py
python calculate_thresholds.py
```

### Step 1: Start Infrastructure
```
cd code
docker-compose up -d  # Kafka + Spark containers
sleep 30
```

### Step 2: Start Streaming (3 terminals in parallel)
**Terminal A:**
```
python gps_producer.py 
```

**Terminal B:**
```
spark-submit --master local spark_streaming.py
```

**Terminal C:**
```
python app.py
```

### Step 3: Access Dashboard
Open browser → `http://localhost:5000`

---

## File Dependencies Flow

```
[OSM] + [GPS Raw] 
   ↓
split_segments.py → [GeoJSON: 616,269 roads]
sort_gps_data.py → [Sorted GPS: 10,285 taxi files]
calculate_thresholds.py → [Thresholds JSON]
   ↓
gps_producer.py → [Kafka Stream]
   ↓
spark_streaming.py 
   ├→ adaptive_matcher.py
   ├→ speed_calculator.py
   └→ grid_aggregator.py
   ↓
[PostgreSQL Database]
   ↓
app.py (Flask API)
   ↓
dashboard.html (Browser)
```

---

## Key Metrics

- **Map-matching:** 88% success rate
- **Throughput:** 100 GPS pts/sec
- **Latency:** 60 seconds end-to-end
- **Road coverage:** 616,269 segments
- **Congestion levels:** 4 (SEVERE, HEAVY, MODERATE, FREE_FLOW)

---

**Date:** November 2025
```

