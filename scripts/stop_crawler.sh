#!/bin/bash
# Stop all crawler processes
# Usage: ./stop_crawler.sh

echo "============================================================"
echo "STOPPING DISTRIBUTED CRAWLER"
echo "============================================================"

# Find all crawler processes
MASTER_PIDS=$(pgrep -f "master_node.py" 2>/dev/null)
WORKER_PIDS=$(pgrep -f "worker_node.py" 2>/dev/null)

if [ -z "$MASTER_PIDS" ] && [ -z "$WORKER_PIDS" ]; then
    echo "✅ No crawler processes found (already stopped)"
    exit 0
fi

echo "Found processes:"
if [ -n "$MASTER_PIDS" ]; then
    echo "  Master PIDs: $MASTER_PIDS"
fi
if [ -n "$WORKER_PIDS" ]; then
    echo "  Worker PIDs: $WORKER_PIDS"
fi

echo ""
echo "Sending SIGTERM (graceful shutdown)..."
pkill -TERM -f "master_node.py" 2>/dev/null
pkill -TERM -f "worker_node.py" 2>/dev/null

# Wait 3 seconds for graceful shutdown
echo "Waiting 3 seconds for graceful shutdown..."
sleep 3

# Check if any are still running
STILL_RUNNING=$(pgrep -f "master_node.py|worker_node.py" 2>/dev/null)

if [ -n "$STILL_RUNNING" ]; then
    echo ""
    echo "⚠️  Some processes still running, forcing kill..."
    echo "  PIDs: $STILL_RUNNING"
    pkill -9 -f "master_node.py" 2>/dev/null
    pkill -9 -f "worker_node.py" 2>/dev/null
    sleep 1
fi

# Final check
REMAINING=$(pgrep -f "master_node.py|worker_node.py" 2>/dev/null)

if [ -z "$REMAINING" ]; then
    echo ""
    echo "✅ All crawler processes stopped successfully!"
else
    echo ""
    echo "❌ Failed to stop some processes:"
    echo "  PIDs: $REMAINING"
    echo ""
    echo "Try manual kill:"
    echo "  kill -9 $REMAINING"
    exit 1
fi

echo "============================================================"
