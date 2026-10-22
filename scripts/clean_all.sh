#!/bin/bash
# Complete cleanup script - Wipes MongoDB AND Redis.
# Use this before starting a fresh crawl with new seed URLs.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧹 COMPLETE CRAWLER CLEANUP"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Stop any running workers first
echo "🛑 Stopping workers..."
"$PROJECT_ROOT/scripts/stop_v3.sh"
sleep 2

# Clean MongoDB
echo ""
echo "🗑️  Cleaning MongoDB..."
python3 "$PROJECT_ROOT/src/utils/cleanup_mongodb.py" --confirm

# Clean Redis
echo ""
echo "🗑️  Cleaning Redis..."
# crawler:* covers the per-domain frontier ZSETs, active_domains,
# domain_known, the Bloom filter and the processing index; leases, locks and
# the robots cache live outside that namespace.
for pattern in "crawler:*" "processing_lease:*" "lock:*" "robots_cache:*"; do
    echo "   Deleting $pattern..."
    redis-cli --scan --pattern "$pattern" | xargs -L 100 redis-cli DEL > /dev/null 2>&1
done

# Verify cleanup
FRONTIER_COUNT=$(redis-cli --scan --pattern 'crawler:frontier:*' | wc -l)
BLOOM_EXISTS=$(redis-cli EXISTS crawler:bloom)

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ CLEANUP COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Status:"
echo "  ✅ MongoDB: Cleaned"
echo "  ✅ Redis frontier: $FRONTIER_COUNT domain queues (should be 0)"
echo "  ✅ Redis Bloom Filter: $([ $BLOOM_EXISTS -eq 0 ] && echo 'Deleted' || echo 'Still exists')"
echo ""
echo "Ready for fresh crawl! Run:"
echo "  ./scripts/start_v3.sh"
echo ""
