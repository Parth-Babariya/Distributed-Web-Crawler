#!/bin/bash
# Quick start script for distributed crawler
# Usage: ./quick_start.sh [num_workers] [max_pages_per_worker]

set -e  # Exit on error

NUM_WORKERS=${1:-5}
MAX_PAGES=${2:-100}

# Store PIDs
PIDS=()

echo "============================================================"
echo "DISTRIBUTED CRAWLER - QUICK START"
echo "============================================================"
echo "Workers:          $NUM_WORKERS"
echo "Pages per worker: $MAX_PAGES"
echo "Total pages:      $((NUM_WORKERS * MAX_PAGES))"
echo "============================================================"
echo ""

# Check if virtual environment exists
if [ -d ".venv" ]; then
    echo "✅ Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  No virtual environment found. Using system Python."
fi

# Start master in background
echo "🚀 Starting Master node..."
python3 master_node.py &
MASTER_PID=$!
PIDS+=($MASTER_PID)
echo "   Master PID: $MASTER_PID"
sleep 3

# Start workers
echo ""
echo "🚀 Starting $NUM_WORKERS workers..."
for i in $(seq 1 $NUM_WORKERS); do
    python3 -c "from worker_node import WorkerNode; WorkerNode(worker_id='worker-$i').start(max_pages=$MAX_PAGES)" &
    WORKER_PID=$!
    PIDS+=($WORKER_PID)
    echo "   Worker #$i PID: $WORKER_PID"
    sleep 0.5
done

echo ""
echo "============================================================"
echo "✅ System started!"
echo "============================================================"
echo "Master PID:  $MASTER_PID"
echo "Worker PIDs: Check with 'ps aux | grep worker_node'"
echo ""
echo "To stop all: pkill -f 'master_node.py|worker_node.py'"
echo "To monitor:  redis-cli LLEN crawler:extracted_links"
echo "             redis-cli ZCARD crawler:frontier"
echo "============================================================"

# Wait for user interrupt
echo ""
echo "Press Ctrl+C to stop all processes..."

# Cleanup function
cleanup() {
    echo ""
    echo "⚠️  Stopping all processes..."
    
    # Kill all tracked PIDs
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "   Stopping PID: $pid"
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    # Wait a moment for graceful shutdown
    sleep 2
    
    # Force kill any remaining
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "   Force killing PID: $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    
    # Fallback: kill by process name
    pkill -9 -f "master_node.py" 2>/dev/null || true
    pkill -9 -f "worker_node.py" 2>/dev/null || true
    
    echo "✅ All stopped!"
    exit 0
}

# Trap Ctrl+C and cleanup
trap cleanup INT TERM

# Wait for background processes
wait
