#!/bin/bash

# Distributed Crawler V3 - Startup Script
# Usage: ./scripts/start_v3.sh [num_workers] [pages_per_worker]

set -e

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Configuration
NUM_WORKERS=${1:-10}
PAGES_PER_WORKER=${2:-100}

# Read seed URLs from file
SEED_FILE="$PROJECT_ROOT/seed_urls.txt"
if [ -f "$SEED_FILE" ]; then
    # Read URLs from file, removing comments and empty lines
    SEED_URLS=$(grep -v '^#' "$SEED_FILE" | grep -v '^$' | tr '\n' ' ')
    URL_COUNT=$(grep -v '^#' "$SEED_FILE" | grep -v '^$' | wc -l)
else
    # Fallback if seed file doesn't exist
    SEED_URLS="https://example.com https://python.org https://github.com/trending"
    URL_COUNT=3
fi

echo "🚀 Starting Decentralized Crawler V3"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Workers: $NUM_WORKERS"
echo "Pages per worker: $PAGES_PER_WORKER"
echo "Seed URLs: $URL_COUNT"
echo ""

# Create logs directory
mkdir -p "$PROJECT_ROOT/logs"

# Check Redis
echo "📡 Checking Redis..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis not running! Start with: redis-server"
    exit 1
fi
echo "✅ Redis OK"

# Check MongoDB
echo "📡 Checking MongoDB..."
if ! mongosh --quiet --eval "db.version()" web_crawler > /dev/null 2>&1; then
    echo "❌ MongoDB not running! Start with: mongod"
    exit 1
fi
echo "✅ MongoDB OK"

# Check dependencies
echo "📦 Checking dependencies..."
if ! python3 -c "import mmh3" > /dev/null 2>&1; then
    echo "❌ mmh3 not installed! Run: pip install mmh3==4.0.1"
    exit 1
fi
echo "✅ Dependencies OK"

# Seed URLs (if frontier empty)
FRONTIER_SIZE=$(redis-cli ZCARD crawler:frontier)
if [ "$FRONTIER_SIZE" -eq 0 ]; then
    echo ""
    echo "🌱 Seeding URLs..."
    python3 "$PROJECT_ROOT/src/v3/master_v3.py" seed $SEED_URLS
    echo "✅ Seeded $(redis-cli ZCARD crawler:frontier) URLs"
else
    echo ""
    echo "ℹ️  Frontier already has $FRONTIER_SIZE URLs (skipping seed)"
fi

# Start workers
echo ""
echo "🤖 Starting $NUM_WORKERS workers..."
echo ""

# Create logs directory
mkdir -p "$PROJECT_ROOT/logs"

for i in $(seq 1 $NUM_WORKERS); do
    WORKER_ID="worker-$i"
    LOG_FILE="$PROJECT_ROOT/logs/$WORKER_ID.log"
    
    python3 "$PROJECT_ROOT/src/v3/worker_v3.py" \
        --worker-id "$WORKER_ID" \
        --max-pages "$PAGES_PER_WORKER" \
        --batch-size 5 \
        > "$LOG_FILE" 2>&1 &
    
    WORKER_PID=$!
    echo "  ✅ Started $WORKER_ID (PID: $WORKER_PID)"
    
    # Stagger starts to avoid thundering herd
    sleep 0.1
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ All workers started!"
echo ""
echo "📊 Monitor progress:"
echo "   python3 src/v3/master_v3.py monitor"
echo ""
echo "📝 View logs:"
echo "   tail -f logs/worker-1.log"
echo ""
echo "🛑 Stop workers:"
echo "   python3 src/v3/master_v3.py shutdown"
echo "   # or: ./scripts/stop_v3.sh"
echo ""
echo "💾 Check results:"
echo "   mongosh web_crawler --eval 'db.pages_metadata.countDocuments()'"
echo ""
