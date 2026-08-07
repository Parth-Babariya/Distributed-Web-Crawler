#!/bin/bash

# Distributed Crawler V3 - Stop Script

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

echo "🛑 Stopping Decentralized Crawler V3..."
echo ""

# Check if any workers running
WORKER_COUNT=$(pgrep -f "worker_v3.py" | wc -l)

if [ "$WORKER_COUNT" -eq 0 ]; then
    echo "ℹ️  No workers running"
    exit 0
fi

echo "Found $WORKER_COUNT workers"
echo ""

# Try graceful shutdown first
echo "1️⃣  Attempting graceful shutdown..."
python3 "$PROJECT_ROOT/src/v3/master_v3.py" shutdown 2>/dev/null || true
echo "   Waiting 10 seconds for workers to finish..."
sleep 10

# Check if workers stopped
WORKER_COUNT=$(pgrep -f "worker_v3.py" | wc -l)

if [ "$WORKER_COUNT" -eq 0 ]; then
    echo "   ✅ All workers stopped gracefully"
    exit 0
fi

# Force kill if still running
echo ""
echo "2️⃣  Force killing remaining workers..."
pkill -TERM -f "worker_v3.py"
sleep 2

# Check again
WORKER_COUNT=$(pgrep -f "worker_v3.py" | wc -l)

if [ "$WORKER_COUNT" -eq 0 ]; then
    echo "   ✅ All workers stopped"
else
    echo "   ⚠️  Forcing kill..."
    pkill -KILL -f "worker_v3.py"
    echo "   ✅ Done"
fi

# Clear Redis locks
echo ""
echo "3️⃣  Clearing Redis locks..."
LOCK_COUNT=$(redis-cli KEYS "lock:*" | wc -l)
if [ "$LOCK_COUNT" -gt 0 ]; then
    redis-cli DEL $(redis-cli KEYS "lock:*") > /dev/null
    echo "   ✅ Cleared $LOCK_COUNT locks"
else
    echo "   ℹ️  No locks to clear"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Crawler stopped successfully!"
echo ""
