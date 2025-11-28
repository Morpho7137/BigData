
# Real-Time Traffic Congestion Analysis & Dynamic Routing System

**Date:** November 2025

---

## 📖 Project Overview
This project implements a real-time big data pipeline that processes GPS taxi trajectories to detect traffic congestion levels and provide dynamic routing using the A* algorithm. The system utilizes **Apache Kafka** for data streaming, **Apache Spark** for processing, and **PostgreSQL** for storage, visualized on a web dashboard.

### Key Metrics
* **Throughput:** 100 GPS points/second
* **Latency:** 60 seconds (End-to-End)
* **Map-Matching Accuracy:** 88% success rate
* **Coverage:** 616,269 road segments
* **Congestion Levels:** 4 (Severe, Heavy, Moderate, Free Flow)

---

## 📂 File Structure & Functions

### 1. Data Preprocessing (Offline)
Scripts used to prepare static data and thresholds before the real-time stream begins.
- `split_segments.py`: Downloads OSM data, normalizes road segments (10-300m), and saves as GeoJSON.
- `sort_gps_data.py`: Filters GPS data by date (Feb 2-5, 2008) and sorts by timestamp.
- `calculate_thresholds.py`: Samples 100 taxis to compute speed percentiles (p25/p50/p75) and define congestion levels.

### 2. Core Processing (Real-Time)
The main pipeline components.
- `gps_producer.py`: Reads sorted GPS files and streams data to Kafka.
- `spark_streaming.py`: Consumes Kafka stream, performs map-matching, aggregates 5-min windows, classifies congestion, and writes to DB.
- `adaptive_matcher.py`: Implements R-tree spatial indexing and Haversine distance for efficient map-matching.
- `speed_calculator.py`: Utility for distance and speed computation using the Haversine formula.
- `grid_aggregator.py`: Handles spatial-temporal binning (0.001° grid, 5-min windows).

### 3. Application & Visualization
- `app.py`: Flask API server serving congestion data and A* routing endpoints.
- `dashboard.html`: Leaflet-based frontend for real-time visualization.
- `route_planner.py`: Implementation of the A* algorithm for dynamic routing based on current congestion.

### 4. Infrastructure
- `docker-compose.yml`: Configuration for Kafka and Spark containers.
- `run.sh`: Automated startup script.

---

# Real-Time Traffic Congestion Analysis & Dynamic Routing System

**Date:** November 2025

---


---

## ⚙️ System Architecture & Data Flow

```text
[OSM & Raw GPS Data]
      |
      +---> (Pre-process) ---> split_segments.py -------+
      |                                                 |
      +---> (Pre-process) ---> sort_gps_data.py         |
                                     |                  |
                                     v                  |
                              gps_producer.py           |
                                     |                  |
                               (Kafka Stream)           |
                                     |                  |
                                     v                  v
                              spark_streaming.py <------+
                                     |
                   +-----------------+-----------------+
                   |                 |                 |
         adaptive_matcher.py  speed_calculator.py  grid_aggregator.py
                   |                 |                 |
                   +-----------------+-----------------+
                                     |
                                     v
                            [PostgreSQL Database]
                                     |
                                     v
                             app.py (Flask API)
                                     |
                                     v
                               dashboard.html
```
-----

## 🚀 Execution Sequence

### Prerequisites

Ensure Docker, Python 3.x, and necessary libraries (`pyspark`, `kafka-python`, `flask`, `psycopg2`, `rtree`) are installed.

### Step 0: Data Preparation

Run these once to generate necessary JSON and sorted data files.

cd code
python split_segments.py
python sort_gps_data.py
python calculate_thresholds.py


### Step 1: Start Infrastructure

Initialize the containerized environment.

cd code
docker-compose up -d
# Wait for containers to fully initialize
sleep 30


### Step 2: Start Streaming Pipeline

Open **3 separate terminals** to run the components in parallel:

**Terminal A (Producer):**

python gps_producer.py


**Terminal B (Spark Processor):**

spark-submit --master local spark_streaming.py


**Terminal C (Web Server):**


python app.py

### Step 3: Access Dashboard

Open your web browser and navigate to:
http://localhost:5000`

-----

<!-- end list -->

