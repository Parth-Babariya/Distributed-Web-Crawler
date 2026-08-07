"""
Distributed Politeness Manager using Redis locks.
Implements the "Bathroom Key" rule for per-domain crawl delays.

Each domain gets a lock that expires after crawl_delay seconds.
Workers self-regulate without needing a central coordinator.
"""

from redis import Redis
from urllib.parse import urlparse
from typing import Optional
import logging
import time
import uuid

logger = logging.getLogger(__name__)


_RELEASE_AFTER_CRAWL_LUA = """
local owner = redis.call('GET', KEYS[1])
if owner == ARGV[1] or not owner then
    redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[1])
    return 1
end
return 0
"""


class PolitenessManager:
    """
    Distributed per-domain politeness enforcement using Redis locks.
    
    Features:
    - Self-regulating workers (no Master needed)
    - Automatic crawl-delay enforcement
    - Fair domain access across workers
    - Lock auto-expiry (fault tolerant)
    
    How it works:
    1. Worker wants to crawl example.com/page1
    2. Worker tries: SET lock:example.com "1" NX EX 1
    3. If successful → crawl
    4. If failed → re-queue with lower priority (snooze)
    """
    
    def __init__(self, redis_client: Redis, default_delay: float = 0.1,
                 max_request_timeout: float = 15.0,
                 owner_id: str = None):
        """
        Initialize Politeness Manager.

        Args:
            redis_client: Redis connection
            default_delay: Default crawl delay in seconds (if not specified)
            max_request_timeout: Worst-case fetch duration in seconds. The
                lock TTL is set to (crawl_delay + max_request_timeout) when
                acquired, so the lock cannot evaporate mid-download. After
                the fetch finishes, release_after_crawl() shrinks it back
                to crawl_delay so the target server gets exactly that much
                silence before the next request.
        """
        self.redis = redis_client
        self.default_delay = default_delay
        self.max_request_timeout = max_request_timeout
        self.owner_id = owner_id or f"politeness-{uuid.uuid4().hex[:8]}"
        self._release_after_crawl_script = redis_client.register_script(
            _RELEASE_AFTER_CRAWL_LUA
        )
        self.stats = {
            'locks_acquired': 0,
            'locks_failed': 0,
            're_queued': 0
        }

        logger.info(
            f"PolitenessManager initialized (default_delay={default_delay}s, "
            f"max_request_timeout={max_request_timeout}s)"
        )
    
    def _extract_domain(self, url: str) -> str:
        """
        Extract bare netloc from URL.

        Matches the domain key used by the two-tier frontier so the politeness
        lock and the frontier queues share the same identifier (e.g. lock for
        ``example.com:8080`` matches frontier ``crawler:frontier:example.com:8080``).
        http and https on the same host share politeness — they hit the same
        server, so they share the rate limit.
        """
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return "unknown"

    def _get_lock_key(self, domain: str) -> str:
        """Get Redis key for domain lock."""
        return f"lock:{domain}"
    
    def can_crawl_domain(self, domain: str, crawl_delay: Optional[float] = None) -> bool:
        """
        Acquire the politeness lock for a bare domain (no URL needed).

        The lock TTL covers (crawl_delay + max_request_timeout) so a slow
        download cannot outlive its lock and let a second worker open a
        concurrent connection to the same server. The worker MUST call
        release_after_crawl_domain() once the fetch finishes to shrink the
        lock back to just crawl_delay (the actual cooldown).

        Args:
            domain: Bare domain (e.g. ``example.com``)
            crawl_delay: Crawl delay in seconds (None = use default)

        Returns:
            True if lock acquired, False if domain is currently busy
        """
        lock_key = self._get_lock_key(domain)
        delay = crawl_delay or self.default_delay

        ttl_ms = max(int((delay + self.max_request_timeout) * 1000), 1)
        acquired = self.redis.set(lock_key, self.owner_id, nx=True, px=ttl_ms)

        if acquired:
            self.stats['locks_acquired'] += 1
            logger.debug(
                f"Lock acquired for {domain} (delay={delay}s, ttl={ttl_ms}ms)"
            )
            return True
        self.stats['locks_failed'] += 1
        logger.debug(f"Lock failed for {domain} (domain busy)")
        return False

    def release_after_crawl_domain(self, domain: str,
                                   crawl_delay: Optional[float] = None):
        """
        Shrink the politeness lock to just crawl_delay after the fetch
        finishes, so the cooldown clock starts from connection-close.

        Only the worker that acquired the lock may shorten it. If a stalled
        worker resumes after another worker acquired a newer lock, it must not
        overwrite that newer owner's TTL.

        Args:
            domain: Bare domain whose lock to shorten
            crawl_delay: Cooldown in seconds (None = use default)
        """
        lock_key = self._get_lock_key(domain)
        delay = crawl_delay or self.default_delay
        cooldown_ms = max(int(delay * 1000), 1)
        updated = self._release_after_crawl_script(
            keys=[lock_key],
            args=[self.owner_id, cooldown_ms],
        )
        if updated:
            logger.debug(f"Cooldown set for {domain} ({delay}s after fetch)")
        else:
            logger.debug(
                f"Skipped cooldown update for {domain}; lock has another owner"
            )
        return bool(updated)

    def can_crawl(self, url: str, crawl_delay: Optional[float] = None) -> bool:
        """URL-flavoured wrapper around can_crawl_domain."""
        return self.can_crawl_domain(self._extract_domain(url), crawl_delay)

    def release_after_crawl(self, url: str, crawl_delay: Optional[float] = None):
        """URL-flavoured wrapper around release_after_crawl_domain."""
        self.release_after_crawl_domain(self._extract_domain(url), crawl_delay)
    
    def get_lock_ttl(self, url: str) -> Optional[int]:
        """
        Get remaining TTL for domain lock.
        
        Args:
            url: URL to check
            
        Returns:
            Remaining seconds until lock expires, or None if not locked
        """
        domain = self._extract_domain(url)
        lock_key = self._get_lock_key(domain)
        ttl = self.redis.ttl(lock_key)
        
        if ttl > 0:
            return ttl
        return None
    
    def force_release_lock(self, url: str):
        """
        Force release lock for a domain (use with caution).
        
        Args:
            url: URL whose domain lock to release
        """
        domain = self._extract_domain(url)
        lock_key = self._get_lock_key(domain)
        self.redis.delete(lock_key)
        logger.warning(f"Force released lock for {domain}")
    
    def get_crawl_delay_for_domain(self, domain: str) -> float:
        """
        Look up crawl delay for a bare domain.

        Checks the robots.txt cache first, then the per-domain state hash,
        then falls back to ``self.default_delay``.
        """
        delay_key = f"crawler:robots:delay:{domain}"
        cached_delay = self.redis.get(delay_key)
        if cached_delay:
            try:
                return float(cached_delay)
            except (TypeError, ValueError):
                pass

        state_key = f"crawler:domain_state:{domain}"
        state = self.redis.hgetall(state_key)
        if state and b'crawl_delay' in state:
            try:
                return float(state[b'crawl_delay'])
            except (TypeError, ValueError):
                pass

        return self.default_delay

    def get_crawl_delay(self, url: str) -> float:
        """URL-flavoured wrapper around get_crawl_delay_for_domain."""
        return self.get_crawl_delay_for_domain(self._extract_domain(url))
    
    def set_crawl_delay(self, url: str, delay: float):
        """
        Set crawl delay for a domain.
        
        Args:
            url: URL of domain
            delay: Crawl delay in seconds
        """
        domain = self._extract_domain(url)
        
        # Store in domain state
        state_key = f"crawler:domain_state:{domain}"
        self.redis.hset(state_key, 'crawl_delay', delay)
        
        # Also store in robots cache format
        delay_key = f"crawler:robots:delay:{domain}"
        self.redis.setex(delay_key, 86400, delay)  # Cache for 24 hours
        
        logger.info(f"Set crawl delay for {domain}: {delay}s")
    
    def get_stats(self) -> dict:
        """Get politeness manager statistics."""
        total_attempts = self.stats['locks_acquired'] + self.stats['locks_failed']
        success_rate = (self.stats['locks_acquired'] / total_attempts * 100) if total_attempts > 0 else 0
        
        return {
            **self.stats,
            'total_attempts': total_attempts,
            'success_rate': f"{success_rate:.1f}%"
        }
    
    def clear_all_locks(self):
        """
        Clear all domain locks (emergency use only).
        Use for system shutdown or reset.
        """
        pattern = "lock:*"
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
            logger.warning(f"Cleared {len(keys)} domain locks")


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    redis_client = Redis(host='localhost', port=6379, decode_responses=False)
    politeness = PolitenessManager(redis_client, default_delay=2.0)

    print("\n" + "="*60)
    print("POLITENESS MANAGER TEST")
    print("="*60)

    url1 = "https://example.com/page1"
    url2 = "https://example.com/page2"
    url3 = "https://python.org/docs"

    print("\n[Test 1] Acquiring lock for example.com...")
    can_crawl = politeness.can_crawl(url1, crawl_delay=2.0)
    print(f"  {url1}: {'CAN CRAWL' if can_crawl else 'LOCKED'}")

    print("\n[Test 2] Same domain immediately (should fail)...")
    can_crawl = politeness.can_crawl(url2, crawl_delay=2.0)
    print(f"  {url2}: {'CAN CRAWL' if can_crawl else 'LOCKED'}")

    print("\n[Test 3] Different domain...")
    can_crawl = politeness.can_crawl(url3, crawl_delay=1.0)
    print(f"  {url3}: {'CAN CRAWL' if can_crawl else 'LOCKED'}")

    print("\n[Test 4] TTL on first lock...")
    ttl = politeness.get_lock_ttl(url1)
    print(f"  Lock expires in: {ttl} seconds" if ttl else "  No lock")

    print("\n[Test 5] Wait for lock to expire and retry...")
    time.sleep(2)
    can_crawl = politeness.can_crawl(url2, crawl_delay=2.0)
    print(f"  {url2}: {'CAN CRAWL' if can_crawl else 'LOCKED'}")

    print("\n" + "="*60)
    print("STATISTICS")
    print("="*60)
    for key, value in politeness.get_stats().items():
        print(f"  {key}: {value}")

    print("\nPoliteness test complete.")
