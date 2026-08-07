"""
Two-tiered URL frontier (Mercator / Heritrix design).

Instead of one global ZSET of URLs, the frontier is split:

- Tier 1: ``crawler:active_domains`` — Redis LIST of domains that have URLs
  ready. Workers pop from here, not URL-by-URL. Round-robin gives every
  domain equal attention regardless of how many URLs it has.
- Tier 2: ``crawler:frontier:<domain>`` — one ZSET per domain, scored by
  priority. URLs only leave their queue when their politeness lock is held.
- Guard: ``crawler:domain_known`` — SET that prevents the same domain from
  being pushed onto Tier 1 twice.

All three structures must stay coherent. The producer/consumer races between
LPOP/RPUSH/SADD/SREM/ZADD/ZPOPMAX are subtle, so the dequeue and push-back
paths are implemented as Lua scripts. Redis runs Lua atomically (no other
command can interleave), eliminating the race entirely.
"""

from typing import Optional, Tuple
from urllib.parse import urlparse

from redis import Redis


ACTIVE_DOMAINS_KEY = 'crawler:active_domains'
DOMAIN_KNOWN_KEY = 'crawler:domain_known'
FRONTIER_KEY_PREFIX = 'crawler:frontier:'


def extract_domain(url: str) -> str:
    """Return the bare netloc (host[:port]) used as the domain key."""
    return urlparse(url).netloc.lower()


def frontier_key(domain: str) -> str:
    return f'{FRONTIER_KEY_PREFIX}{domain}'


# Enqueue one (domain, priority, url_json). Atomically:
#   1. ZADD url to the per-domain frontier
#   2. SADD domain to the known-set
#   3. If SADD reported "was new", RPUSH the domain onto the active queue
# Returns 1 if this call activated the domain (was previously unknown), 0 otherwise.
_ENQUEUE_LUA = """
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
if redis.call('SADD', KEYS[2], ARGV[1]) == 1 then
    redis.call('RPUSH', KEYS[3], ARGV[1])
    return 1
end
return 0
"""


# Dequeue one URL by:
#   1. LPOP a domain from the active queue
#   2. SREM it from the known-set because it is no longer present in tier 1
#   3. ZPOPMAX one URL from that domain's frontier
# Returns nil if no domain was waiting; {domain, false, false} if the popped
# domain had a stale (empty) frontier; {domain, url_json, priority} on success.
_DEQUEUE_LUA = """
local domain = redis.call('LPOP', KEYS[1])
if not domain then
    return nil
end
redis.call('SREM', KEYS[2], domain)
local fkey = ARGV[1] .. domain
local result = redis.call('ZPOPMAX', fkey, 1)
if #result == 0 then
    return {domain, false, false}
end
return {domain, result[1], result[2]}
"""


# After a worker is done with a domain (either it crawled a URL or hit the
# politeness lock), put the domain back at the tail of the active queue *only
# if* its frontier still has URLs. SADD-then-RPUSH avoids double-pushing the
# domain when another worker has already placed it.
#   KEYS[1] = crawler:domain_known
#   KEYS[2] = crawler:active_domains
#   ARGV[1] = domain
#   ARGV[2] = frontier key prefix
_PUSH_BACK_LUA = """
local fkey = ARGV[2] .. ARGV[1]
local remaining = redis.call('ZCARD', fkey)
if remaining == 0 then
    redis.call('SREM', KEYS[1], ARGV[1])
    return 0
end
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
    redis.call('RPUSH', KEYS[2], ARGV[1])
end
return remaining
"""


class Frontier:
    """Two-tier domain frontier wrapping Redis with atomic Lua ops."""

    def __init__(self, redis_client: Redis):
        self.redis = redis_client
        self._enqueue_script = redis_client.register_script(_ENQUEUE_LUA)
        self._dequeue_script = redis_client.register_script(_DEQUEUE_LUA)
        self._push_back_script = redis_client.register_script(_PUSH_BACK_LUA)

    @staticmethod
    def _to_text(value) -> str:
        return value.decode('utf-8') if isinstance(value, (bytes, bytearray)) else value

    def enqueue(self, url: str, priority: float, url_json: str) -> bool:
        """
        Add one URL to its domain's frontier.

        Returns True if this call activated the domain (i.e. the domain was
        previously absent from active_domains). The caller doesn't usually
        need this signal — it's mostly useful for stats/tests.
        """
        domain = extract_domain(url)
        if not domain:
            return False
        result = self._enqueue_script(
            keys=[frontier_key(domain), DOMAIN_KNOWN_KEY, ACTIVE_DOMAINS_KEY],
            args=[domain, priority, url_json],
        )
        return bool(result)

    def dequeue(self) -> Optional[Tuple[str, Optional[str], Optional[float]]]:
        """
        Pop one URL from the head domain.

        Returns:
          - None if active_domains is empty (worker should sleep and retry)
          - (domain, None, None) if the head domain's frontier was stale
            (worker should immediately try again — domain_known is now clean)
          - (domain, url_json, priority) on success — caller now "owns" the
            domain and is responsible for calling push_back() when done.
        """
        result = self._dequeue_script(
            keys=[ACTIVE_DOMAINS_KEY, DOMAIN_KNOWN_KEY],
            args=[FRONTIER_KEY_PREFIX],
        )
        if not result:
            return None

        domain = self._to_text(result[0])
        url_json_raw = result[1]
        priority_raw = result[2]

        if not url_json_raw:
            # Stale entry — domain was in active_domains but its ZSET was empty.
            # The Lua script already SREM'd it from domain_known.
            return (domain, None, None)

        url_json = self._to_text(url_json_raw)
        priority = float(self._to_text(priority_raw))
        return (domain, url_json, priority)

    def push_back(self, domain: str) -> int:
        """
        Return a domain to the tail of active_domains *if* its frontier still
        has URLs. Idempotent — safe to call when another worker has already
        repushed the domain.

        Returns the number of URLs remaining in that domain's frontier.
        """
        if not domain:
            return 0
        remaining = self._push_back_script(
            keys=[DOMAIN_KNOWN_KEY, ACTIVE_DOMAINS_KEY],
            args=[domain, FRONTIER_KEY_PREFIX],
        )
        return int(remaining)

    def total_size(self) -> int:
        """
        Sum ZCARD across every crawler:frontier:* key. Uses SCAN (not KEYS)
        so it's safe to call against a large keyspace from monitoring code.
        Intended for stats/dashboards, not the hot path.
        """
        total = 0
        cursor = 0
        match = f'{FRONTIER_KEY_PREFIX}*'
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match=match, count=200)
            if keys:
                pipeline = self.redis.pipeline(transaction=False)
                for key in keys:
                    pipeline.zcard(key)
                total += sum(pipeline.execute())
            if cursor == 0:
                break
        return total

    def active_domain_count(self) -> int:
        """Length of active_domains (Tier 1)."""
        return self.redis.llen(ACTIVE_DOMAINS_KEY)

    def known_domain_count(self) -> int:
        """Cardinality of domain_known (should match unique domains in Tier 2)."""
        return self.redis.scard(DOMAIN_KNOWN_KEY)
