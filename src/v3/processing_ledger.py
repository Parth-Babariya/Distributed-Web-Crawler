"""
Per-URL leases for in-flight crawls — clock-drift-safe.

The previous design wrote ``time.time()`` from the acquiring worker into a
single ``crawler:processing`` hash, then asked any worker to compare it
against its own local clock to find "stale" entries. With multiple workers
on different machines, clock drift of even a few seconds causes false
positives — a healthy worker's URL gets re-queued and crawled twice, and
the original worker's later write hits a duplicate-key error or (worse)
silently overwrites another worker's data.

The fix is to use Redis itself as the only source of time:

- ``processing_lease:<url>`` — STRING, value = worker_id, native PEXPIRE.
  When the lease expires the key disappears. No clocks compared.
- ``crawler:processing`` — HASH, url → url_data JSON. Used purely as an
  enumerable index of in-flight URLs (no timestamp inside).

All ownership transitions (claim / release / renew) are Lua scripts so the
GET-compare-DEL sequence is atomic — no other worker can interleave between
"check who owns this" and "act on that ownership."
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from redis import Redis


PROCESSING_HASH_KEY = 'crawler:processing'
LEASE_KEY_PREFIX = 'processing_lease:'

logger = logging.getLogger(__name__)


# Claim a URL: write the lease with TTL and add the URL to the index hash.
# The frontier dequeue already guarantees only one worker pops each URL, so
# we don't need NX here — we always own the lease at this point.
#   KEYS[1] = processing_lease:<url>
#   KEYS[2] = crawler:processing (hash)
#   ARGV[1] = url (hash field name)
#   ARGV[2] = worker_id (lease value)
#   ARGV[3] = url_data JSON (hash field value)
#   ARGV[4] = lease TTL in milliseconds
_CLAIM_LUA = """
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
"""


# Release a URL we still own. Returns 1 on success, 0 if another worker has
# taken over (we're a zombie). Atomicity matters: a non-Lua "GET, then DEL
# if matches" can race with a recovery worker reclaiming the URL.
#   KEYS[1] = processing_lease:<url>
#   KEYS[2] = crawler:processing (hash)
#   ARGV[1] = url
#   ARGV[2] = worker_id
_RELEASE_LUA = """
local owner = redis.call('GET', KEYS[1])
if owner == ARGV[2] then
    redis.call('DEL', KEYS[1])
    redis.call('HDEL', KEYS[2], ARGV[1])
    return 1
end
return 0
"""


# Renew the lease (extend TTL). Useful for very long crawls. Returns 1 if
# we still owned it and renewed, 0 if we've been displaced.
#   KEYS[1] = processing_lease:<url>
#   ARGV[1] = worker_id
#   ARGV[2] = new TTL in milliseconds
_RENEW_LUA = """
local owner = redis.call('GET', KEYS[1])
if owner == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
"""


# Force-clear an entry from the hash. Used by recovery when the lease has
# expired (so there's no owner to verify against). The lease is already gone
# at this point — we just need to clean up the index. Atomic with a sanity
# check: only HDEL if the lease really is missing.
#   KEYS[1] = processing_lease:<url>
#   KEYS[2] = crawler:processing
#   ARGV[1] = url
_RECLAIM_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HDEL', KEYS[2], ARGV[1])
    return 1
end
return 0
"""


class ProcessingLedger:
    """Tracks in-flight URLs with per-URL Redis-native leases."""

    def __init__(self, redis_client: Redis, default_lease_seconds: int = 300):
        self.redis = redis_client
        self.default_lease_seconds = default_lease_seconds
        self.processing_key = PROCESSING_HASH_KEY

        self._claim_script = redis_client.register_script(_CLAIM_LUA)
        self._release_script = redis_client.register_script(_RELEASE_LUA)
        self._renew_script = redis_client.register_script(_RENEW_LUA)
        self._reclaim_script = redis_client.register_script(_RECLAIM_LUA)

    @staticmethod
    def _to_text(value) -> str:
        return value.decode('utf-8') if isinstance(value, (bytes, bytearray)) else value

    def _lease_key(self, url: str) -> str:
        return f'{LEASE_KEY_PREFIX}{url}'

    def claim(self, url: str, url_data: Dict, worker_id: str,
              lease_seconds: Optional[int] = None) -> None:
        """
        Record that ``worker_id`` is now responsible for ``url``.

        Writes:
          - processing_lease:<url> = worker_id (TTL = lease_seconds)
          - crawler:processing[<url>] = json(url_data)

        Both writes are atomic so a recovery scan can't observe the index
        without a corresponding lease (or vice versa) mid-claim.
        """
        ttl_ms = (lease_seconds or self.default_lease_seconds) * 1000
        self._claim_script(
            keys=[self._lease_key(url), self.processing_key],
            args=[url, worker_id, json.dumps(url_data), ttl_ms],
        )

    def verify_owner(self, url: str, worker_id: str) -> bool:
        """
        Return True iff ``worker_id`` still owns ``url``'s lease.

        Call this before any irreversible side-effect (writing to MongoDB,
        sending a webhook, etc.) so a stalled-then-resumed worker cannot
        clobber state owned by a recovery worker.
        """
        owner = self.redis.get(self._lease_key(url))
        if owner is None:
            return False
        return self._to_text(owner) == worker_id

    def renew(self, url: str, worker_id: str,
              lease_seconds: Optional[int] = None) -> bool:
        """
        Extend the lease if we still own it. Returns False if displaced.
        """
        ttl_ms = (lease_seconds or self.default_lease_seconds) * 1000
        result = self._renew_script(
            keys=[self._lease_key(url)],
            args=[worker_id, ttl_ms],
        )
        return bool(result)

    def release(self, url: str, worker_id: str) -> bool:
        """
        Atomically release the URL if we still own it.

        Returns True if we owned the lease and cleaned up. Returns False if
        we've been displaced (another worker took over via recovery) — in
        which case we MUST NOT do anything else with the URL: the new owner
        is now responsible.
        """
        result = self._release_script(
            keys=[self._lease_key(url), self.processing_key],
            args=[url, worker_id],
        )
        return bool(result)

    def in_flight_count(self) -> int:
        """Number of URLs currently in the index (may include some whose
        lease just expired but whose recovery hasn't run yet)."""
        return self.redis.hlen(self.processing_key)

    def recover_stale(self, max_recover: int = 100) -> List[Tuple[str, Dict]]:
        """
        Find URLs whose lease has expired and clean them out of the index.

        Returns a list of ``(url, url_data)`` tuples for the caller to
        re-enqueue into the frontier. Up to ``max_recover`` per call so a
        single recovery pass can't monopolise a worker.
        """
        recovered: List[Tuple[str, Dict]] = []
        cursor = 0

        while True:
            cursor, items = self.redis.hscan(
                self.processing_key, cursor=cursor, count=200
            )
            if items:
                urls = [self._to_text(u) for u in items.keys()]
                jsons = [self._to_text(v) for v in items.values()]

                # Batch the EXISTS checks so we pay one round trip per page
                # of hash entries instead of one per URL.
                pipeline = self.redis.pipeline(transaction=False)
                for url in urls:
                    pipeline.exists(self._lease_key(url))
                exists_results = pipeline.execute()

                for url, url_json, lease_exists in zip(urls, jsons, exists_results):
                    if lease_exists:
                        # Healthy in-flight URL — leave it alone.
                        continue

                    # Lease expired natively. The Lua script double-checks
                    # the lease really is gone (paranoia against concurrent
                    # claim by another worker between EXISTS and HDEL).
                    cleared = self._reclaim_script(
                        keys=[self._lease_key(url), self.processing_key],
                        args=[url],
                    )
                    if not cleared:
                        continue

                    try:
                        url_data = json.loads(url_json)
                    except (TypeError, ValueError):
                        logger.warning(
                            f"Dropping unparseable processing entry for {url}"
                        )
                        continue

                    # Strip ownership/lease fields from a previous owner so
                    # the next worker's claim starts clean.
                    url_data.pop('worker_id', None)
                    url_data.pop('started_at', None)
                    recovered.append((url, url_data))

                    if len(recovered) >= max_recover:
                        return recovered

            if cursor == 0:
                break

        return recovered
