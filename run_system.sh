#!/bin/bash

# Beijing Traffic Congestion System - Master Control Script
# Run all components with one command

set -e  # Exit on error

PROJECT_DIR="/mnt/d/BigDataPj"
CODE_DIR="$PROJECT_DIR/code"
VENV="$PROJECT_DIR/myenv"

echo "=========================================="
echo "Beijing Traffic Congestion System"
echo "=========================================="
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down Python components..."
    kill $(jobs -p) 2>/dev/null || true
    echo "✓ Python processes stopped"
    echo "ℹ️  Kafka/Zookeeper containers left running for faster restart"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Step 1: Clean previous run
echo "Step 1: Cleaning previous run data..."
rm -rf /tmp/spark_checkpoint_gps
rm -rf $PROJECT_DIR/output/streaming_results/*
echo "✓ Cleaned checkpoint and results"
echo ""

# Step 2: Verify Kafka is running
echo "Step 2: Verifying Kafka is running..."
if docker ps | grep -q kafka; then
    echo "✓ Kafka is running"
else
    echo ""
    echo "⚠️  ERROR: Kafka not detected!"
    echo "   Please start Kafka manually first:"
    echo "   cd $CODE_DIR && docker-compose up -d"
    echo ""
    exit 1
fi
echo ""

# Step 3: Activate virtual environment
echo "Step 3: Activating virtual environment..."
source $VENV/bin/activate
echo "✓ Virtual environment activated"
echo ""

# Step 4: Start Flask Dashboard FIRST (loads road network - LONGEST startup time)
echo "Step 4: Starting Flask Dashboard..."
echo "   (This will load 616K road segments - takes ~30 seconds)"
cd $CODE_DIR
python app.py > /tmp/flask.log 2>&1 &
FLASK_PID=$!
echo "✓ Flask Dashboard started (PID: $FLASK_PID)"
echo ""

# Wait for Flask to fully load road network
echo "Waiting for Flask to load road network (30 seconds)..."
sleep 30
echo "✓ Flask ready - road network loaded"
echo "   Access at: http://localhost:5000"
echo ""

# Step 5: Start Spark Streaming - WITH MEMORY CONFIG
echo "Step 5: Starting Spark Streaming..."
cd $CODE_DIR
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000 \
    --conf spark.driver.maxResultSize=2g \
    spark_streaming.py > /tmp/spark.log 2>&1 &
SPARK_PID=$!
echo "✓ Spark Streaming started (PID: $SPARK_PID)"
echo "   Memory: 4GB driver, 4GB executor"
echo ""

# Wait for Spark to initialize
echo "Waiting for Spark to initialize (20 seconds)..."
sleep 20
echo "✓ Spark ready"
echo ""

# Step 6: Start GPS Producer (LAST - after everything is ready)
echo "Step 6: Starting GPS Producer..."
cd $CODE_DIR
python gps_producer.py > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo "✓ GPS Producer started (PID: $PRODUCER_PID)"
echo ""

echo "=========================================="
echo "✓ ALL COMPONENTS RUNNING"
echo "=========================================="
echo ""
echo "Services:"
echo "  🌐 Dashboard:  http://localhost:5000"
echo "  ⚡ Spark:      Processing batches (4GB memory)"
echo "  📡 Producer:   Sending GPS data"
echo "  🔧 Kafka:      localhost:9092 (already running)"
echo ""
echo "Logs (open in new terminal):"
echo "  • Flask:     tail -f /tmp/flask.log"
echo "  • Spark:     tail -f /tmp/spark.log"
echo "  • Producer:  tail -f /tmp/producer.log"
echo ""
echo "Monitor batches:"
echo "  • watch -n 5 'ls -lht /mnt/d/BigDataPj/output/streaming_results/ | head -10'"
echo ""
echo "Debug Spark performance:"
echo "  • tail -f /tmp/spark.log | grep -E 'BATCH|WARN|seconds'"
echo ""
echo "📍 Route Planning: Click 'Start Route Planning' button"
echo ""
echo "Press Ctrl+C to stop (Kafka will stay running)"
echo "=========================================="
echo ""

# Keep script running and monitor processes
PRODUCER_DONE=false

while true; do
    # Check Flask
    if ! kill -0 $FLASK_PID 2>/dev/null; then
        echo "❌ Flask died! Check /tmp/flask.log"
        cleanup
    fi
    
    # Check Spark
    if ! kill -0 $SPARK_PID 2>/dev/null; then
        echo "❌ Spark died! Check /tmp/spark.log"
        echo ""
        echo "Last 30 lines of Spark log:"
        tail -30 /tmp/spark.log
        echo ""
        cleanup
    fi
    
    # Check Producer (only notify once)
    if ! kill -0 $PRODUCER_PID 2>/dev/null; then
        if [ "$PRODUCER_DONE" = false ]; then
            echo ""
            echo "✓ GPS Producer completed - all data sent"
            echo "  Spark will continue processing remaining batches..."
            echo "  Dashboard will keep updating until all batches processed"
            echo ""
            PRODUCER_DONE=true
        fi
    fi
    
    sleep 5
done
