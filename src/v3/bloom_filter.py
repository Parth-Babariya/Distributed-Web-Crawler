"""
Bloom Filter implementation for URL deduplication.
Uses Redis Bitmaps for distributed, memory-efficient storage.

Memory comparison for 10M URLs:
- Redis Set: ~800 MB
- Bloom Filter: ~14 MB (98% savings!)
"""

import hashlib
from typing import List
from redis import Redis
import logging

try:
    import mmh3  # MurmurHash3 for hash functions
except ImportError:  # pragma: no cover - exercised only when optional wheel is unavailable
    mmh3 = None

logger = logging.getLogger(__name__)


# Atomic check-and-set for the Bloom filter. KEYS[1] is the bitmap key; each
# ARGV is a bit position. Returns 1 if at least one bit was previously zero
# (URL was new), 0 otherwise. Because Redis runs Lua scripts atomically, no
# other worker can interleave between the GETBITs and SETBITs.
_BLOOM_ADD_LUA = """
local was_new = 0
for i = 1, #ARGV do
    if redis.call('GETBIT', KEYS[1], ARGV[i]) == 0 then
        was_new = 1
    end
    redis.call('SETBIT', KEYS[1], ARGV[i], 1)
end
return was_new
"""


class BloomFilter:
    """
    Memory-efficient Bloom Filter using Redis Bitmaps.
    
    Features:
    - 98% memory savings vs Redis Set
    - O(1) lookups
    - Distributed (shared across all workers)
    - Configurable false positive rate
    
    Trade-offs:
    - Small false positive rate (default: 0.1%)
    - Cannot delete items (acceptable for crawlers)
    """
    
    def __init__(self, redis_client: Redis, 
                 key: str = 'crawler:bloom',
                 capacity: int = 10000000,  # 10M URLs
                 error_rate: float = 0.001):  # 0.1% false positive
        """
        Initialize Bloom Filter.
        
        Args:
            redis_client: Redis connection
            key: Redis key for the bitmap
            capacity: Expected number of URLs
            error_rate: Acceptable false positive rate (0.001 = 0.1%)
        """
        self.redis = redis_client
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate
        
        # Calculate optimal parameters
        # m = -(n * ln(p)) / (ln(2)^2)
        # k = (m/n) * ln(2)
        import math
        self.size = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
        self.hash_count = int((self.size / capacity) * math.log(2))
        
        logger.info(f"Bloom Filter initialized: size={self.size:,} bits, "
                   f"hash_count={self.hash_count}, "
                   f"capacity={capacity:,}, error_rate={error_rate}")

        # Register the atomic add script once. Subsequent calls use EVALSHA
        # under the hood, so we pay the script-load cost only once per worker.
        self._add_script = self.redis.register_script(_BLOOM_ADD_LUA)

        # Store metadata
        self.redis.hset(f'{self.key}:info', mapping={
            'size': self.size,
            'hash_count': self.hash_count,
            'capacity': capacity,
            'error_rate': error_rate
        })
    
    def _get_positions(self, url: str) -> list:
        """
        Calculate bit positions for a URL using multiple hash functions.
        
        Args:
            url: URL to hash
            
        Returns:
            List of bit positions
        """
        positions = []
        for i in range(self.hash_count):
            if mmh3:
                # Use MurmurHash3 with different seeds when available.
                hash_val = mmh3.hash(url, i) % self.size
            else:
                # Portable fallback for Python versions where mmh3 wheels are
                # unavailable. Slower than mmh3, but keeps the crawler runnable.
                digest = hashlib.blake2b(
                    url.encode('utf-8'),
                    digest_size=8,
                    person=f"bf{i:06d}".encode('ascii')[:8]
                ).digest()
                hash_val = int.from_bytes(digest, 'big') % self.size
            positions.append(hash_val)
        return positions
    
    def add(self, url: str) -> bool:
        """
        Add URL to Bloom Filter atomically.

        The check (was any bit zero?) and the set (force all bits to one)
        run inside a single Lua script on the Redis server, so two workers
        racing on the same URL cannot both see "new".

        Args:
            url: URL to add

        Returns:
            True if probably new, False if definitely exists
        """
        positions = self._get_positions(url)
        was_new = self._add_script(keys=[self.key], args=positions)
        return bool(was_new)
    
    def contains(self, url: str) -> bool:
        """
        Check if URL probably exists in the filter.
        
        Args:
            url: URL to check
            
        Returns:
            True if probably exists (might be false positive)
            False if definitely does not exist
        """
        positions = self._get_positions(url)
        
        # Check all bits
        pipeline = self.redis.pipeline()
        for pos in positions:
            pipeline.getbit(self.key, pos)
        bits = pipeline.execute()
        
        # All bits must be 1 for URL to exist
        return all(bits)
    
    def add_many(self, urls: List[str]) -> List[bool]:
        """
        Atomically add many URLs in a single Redis round trip.

        Each URL is evaluated by an independent atomic Lua call, but all
        calls are pipelined so the network cost is one round trip for the
        whole batch instead of one per URL.

        Args:
            urls: URLs to add

        Returns:
            List of booleans, same order as input. True means the URL was
            probably new (and is now reserved); False means it was already
            present.
        """
        if not urls:
            return []

        pipeline = self.redis.pipeline(transaction=False)
        for url in urls:
            positions = self._get_positions(url)
            self._add_script(keys=[self.key], args=positions, client=pipeline)

        results = pipeline.execute()
        return [bool(r) for r in results]

    def add_batch(self, urls: list) -> int:
        """
        Backwards-compatible wrapper around add_many.

        Args:
            urls: URLs to add

        Returns:
            Number of URLs that were probably new
        """
        return sum(1 for is_new in self.add_many(urls) if is_new)
    
    def get_stats(self) -> dict:
        """
        Get Bloom Filter statistics.
        
        Returns:
            Dictionary with stats
        """
        # Count set bits (approximate item count)
        # This is expensive, so use sparingly
        info = self.redis.hgetall(f'{self.key}:info')
        
        return {
            'size_bits': int(info.get(b'size', 0)),
            'size_mb': int(info.get(b'size', 0)) / 8 / 1024 / 1024,
            'hash_count': int(info.get(b'hash_count', 0)),
            'capacity': int(info.get(b'capacity', 0)),
            'error_rate': float(info.get(b'error_rate', 0.001))
        }
    
    def clear(self):
        """Clear the Bloom Filter (delete bitmap)."""
        self.redis.delete(self.key)
        self.redis.delete(f'{self.key}:info')
        logger.info(f"Bloom Filter cleared: {self.key}")


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Connect to Redis
    redis_client = Redis(host='localhost', port=6379, decode_responses=False)
    
    # Create Bloom Filter
    bloom = BloomFilter(
        redis_client,
        capacity=10000000,  # 10M URLs
        error_rate=0.001    # 0.1% false positive
    )
    
    # Test URLs
    test_urls = [
        "https://example.com/page1",
        "https://example.com/page2",
        "https://python.org/docs",
        "https://github.com/trending"
    ]
    
    print("\n" + "="*60)
    print("BLOOM FILTER TEST")
    print("="*60)
    
    # Add URLs
    print("\nAdding URLs...")
    for url in test_urls:
        is_new = bloom.add(url)
        print(f"  {url}: {'NEW' if is_new else 'DUPLICATE'}")
    
    # Check URLs
    print("\nChecking URLs...")
    for url in test_urls:
        exists = bloom.contains(url)
        print(f"  {url}: {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Test new URL
    print("\nTesting new URL...")
    new_url = "https://stackoverflow.com/questions"
    exists = bloom.contains(new_url)
    print(f"  {new_url}: {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Add and check again
    bloom.add(new_url)
    exists = bloom.contains(new_url)
    print(f"  {new_url} (after add): {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Show stats
    print("\n" + "="*60)
    print("BLOOM FILTER STATISTICS")
    print("="*60)
    stats = bloom.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Bloom Filter test complete!")
