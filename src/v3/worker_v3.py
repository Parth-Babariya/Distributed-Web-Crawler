#!/usr/bin/env python3
"""
Decentralized Worker Node - Version 3.0

Complete rewrite implementing:
1. Direct frontier access (no Master bottleneck)
2. Bloom Filter deduplication (98% memory savings)
3. Distributed politeness (self-regulating)
4. Batch inserts (20x faster)
5. Connection pooling + User-Agent rotation
6. Compression + split collections

This worker is fully autonomous and coordinates with peers through Redis.
"""

import logging
import time
import json
import random
import sys
import os
import asyncio
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from redis import Redis

# Add parent directory to path for shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from bloom_filter import BloomFilter
from politeness import PolitenessManager
from frontier import Frontier, extract_domain as frontier_extract_domain
from processing_ledger import ProcessingLedger
from optimized_storage import OptimizedStorage
from robots_handler_async import AsyncRobotsHandler
from config import CrawlerConfig

try:
    import aiohttp
except ImportError:  # pragma: no cover - requests fallback covers minimal envs
    aiohttp = None

# How long to sleep when active_domains is empty vs. when the head domain
# is currently locked. Locked-and-pushed-back happens in a tight loop only
# if there is exactly one active domain; the small sleep prevents 100% CPU.
_IDLE_SLEEP_SECONDS = 0.1
_LOCKED_SLEEP_SECONDS = 0.05

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PageFetchResult:
    html: Optional[str]
    final_url: str
    content_type: str = ''
    status: int = 0


@dataclass
class FetchOutcome:
    """What one fetch attempt produced, as seen by the crawl pipeline.

    ``permanent`` marks failures that retrying cannot fix (404, non-HTML,
    oversized body); those URLs are dropped instead of re-queued.
    """
    html: Optional[str] = None
    final_url: Optional[str] = None
    error: Optional[str] = None
    permanent: bool = False


class PageTooLargeError(Exception):
    """Response body exceeded the configured size cap."""


# 4xx statuses that are worth retrying: the server timed out the request or
# is rate limiting us, neither of which says anything about the URL itself.
_RETRYABLE_4XX = {408, 429}


def is_permanent_http_status(status: int) -> bool:
    return 400 <= status < 500 and status not in _RETRYABLE_4XX


def _decode_body(body: bytes, charset: Optional[str]) -> str:
    try:
        return body.decode(charset or 'utf-8', errors='replace')
    except LookupError:  # unknown charset label in the Content-Type header
        return body.decode('utf-8', errors='replace')


def _check_declared_length(headers, max_bytes: int):
    declared = headers.get('Content-Length', '')
    if declared.isdigit() and int(declared) > max_bytes:
        raise PageTooLargeError(f"Content-Length {declared} exceeds {max_bytes}")


class AiohttpPageFetcher:
    """Persistent aiohttp fetcher with DNS cache and connection pooling.

    The event loop runs on its own daemon thread. ``fetch`` is a blocking,
    thread-safe call, so the worker's fetch threads can all multiplex their
    requests over one ClientSession whose TCP pools and DNS cache stay hot
    across many crawls.
    """

    def __init__(self, dns_cache_ttl: int = 300,
                 pool_limit: int = 100,
                 limit_per_host: int = 4,
                 max_bytes: int = CrawlerConfig.MAX_PAGE_BYTES):
        if aiohttp is None:
            raise RuntimeError("aiohttp is not installed")
        self.dns_cache_ttl = dns_cache_ttl
        self.pool_limit = pool_limit
        self.limit_per_host = limit_per_host
        self.max_bytes = max_bytes
        self.loop = asyncio.new_event_loop()
        self.session = None
        self._loop_thread = threading.Thread(
            target=self.loop.run_forever,
            name='aiohttp-page-fetcher',
            daemon=True,
        )
        self._loop_thread.start()

    async def _ensure_session(self):
        if self.session is not None and not self.session.closed:
            return

        connector = aiohttp.TCPConnector(
            ttl_dns_cache=self.dns_cache_ttl,
            use_dns_cache=True,
            limit=self.pool_limit,
            limit_per_host=self.limit_per_host,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(
            total=13.05,
            connect=3.05,
            sock_read=10,
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )

    async def _fetch(self, url: str, user_agent: str) -> PageFetchResult:
        await self._ensure_session()

        async with self.session.get(
            url,
            headers={'User-Agent': user_agent},
            allow_redirects=True,
        ) as response:
            content_type = response.headers.get('Content-Type', '')
            final_url = str(response.url)
            if 'text/html' not in content_type.lower():
                return PageFetchResult(
                    html=None,
                    final_url=final_url,
                    content_type=content_type,
                    status=response.status,
                )

            response.raise_for_status()
            # Read at most max_bytes so one huge page cannot exhaust worker
            # memory or produce a document over MongoDB's 16MB limit.
            _check_declared_length(response.headers, self.max_bytes)
            body = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                body.extend(chunk)
                if len(body) > self.max_bytes:
                    raise PageTooLargeError(f"Body exceeds {self.max_bytes} bytes")

            return PageFetchResult(
                html=_decode_body(bytes(body), response.charset),
                final_url=final_url,
                content_type=content_type,
                status=response.status,
            )

    def fetch(self, url: str, user_agent: str) -> PageFetchResult:
        # The ClientTimeout above bounds how long this can block.
        return asyncio.run_coroutine_threadsafe(
            self._fetch(url, user_agent), self.loop
        ).result()

    async def _close_session(self):
        if self.session is not None and not self.session.closed:
            await self.session.close()

    def close(self):
        asyncio.run_coroutine_threadsafe(self._close_session(), self.loop).result(timeout=10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._loop_thread.join(timeout=10)
        self.loop.close()


class DecentralizedWorker:
    """
    Autonomous worker that handles entire crawl pipeline:
    1. Pull URL from frontier
    2. Check politeness (distributed lock)
    3. Fetch page (with connection pooling)
    4. Parse links
    5. Validate against Bloom Filter
    6. Add new links to frontier
    7. Compress and batch-store content
    
    No Master needed for crawling loop!
    """
    
    # User-Agent rotation pool
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
    ]

    max_page_bytes = CrawlerConfig.MAX_PAGE_BYTES

    def __init__(self, worker_id: str = None,
                 redis_host: str = None, redis_port: int = None,
                 mongodb_uri: str = None,
                 batch_size: int = 50,
                 processing_timeout: int = 300,
                 recovery_interval: int = None,
                 concurrency: int = None):
        """
        Initialize decentralized worker.

        Args:
            worker_id: Unique worker identifier
            redis_host: Redis host
            redis_port: Redis port
            mongodb_uri: MongoDB connection URI
            batch_size: Pages to batch before MongoDB insert
            concurrency: Max page fetches in flight (each on a different
                domain, since the politeness lock admits one per domain)
        """
        import uuid
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.concurrency = max(1, concurrency or CrawlerConfig.FETCH_CONCURRENCY)
        redis_host = redis_host or CrawlerConfig.REDIS_HOST
        redis_port = redis_port or CrawlerConfig.REDIS_PORT
        mongodb_uri = mongodb_uri or CrawlerConfig.get_mongo_url()
        
        # Redis connection
        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=False  # Binary for Bloom Filter
        )
        
        # Initialize components
        self.bloom_filter = BloomFilter(
            self.redis,
            capacity=10000000,  # 10M URLs
            error_rate=0.001    # 0.1% false positive
        )
        
        self.politeness = PolitenessManager(
            self.redis,
            default_delay=0.1,
            owner_id=self.worker_id,
        )
        self.frontier = Frontier(self.redis)

        self.storage = OptimizedStorage(
            mongodb_uri=mongodb_uri,
            batch_size=batch_size
        )
        self.processing_timeout = processing_timeout
        self.ledger = ProcessingLedger(
            self.redis,
            default_lease_seconds=processing_timeout,
        )
        # Throttle recover_stale_urls so we don't HSCAN crawler:processing on
        # every loop iteration. Each call is O(in-flight URLs); on a busy
        # system the loop runs hundreds of times per second per worker.
        # The leases already have native TTLs in Redis — recovery is just a
        # clean-up sweep, it doesn't need to be tight.
        self.recovery_interval = (
            recovery_interval
            if recovery_interval is not None
            else CrawlerConfig.RECOVERY_INTERVAL
        )
        self._last_recovery_at = 0.0
        self.max_retries = 3
        # Stats counter for "we tried to do something but discovered another
        # worker had taken over the URL" — non-zero indicates clock skew or
        # processing_timeout being too short for actual crawl latency.
        self.zombie_aborts = 0
        # Per-page-id snapshot of the url_data that produced it. We keep
        # this so that if a flush returns "failed" page_ids (transaction
        # aborted) we can re-enqueue the original URLs with their priority
        # and retry counter intact. Cleared after each flush.
        self._pending_url_data: Dict = {}
        # Stat: pages durably stored (incremented when a flush commits).
        self.stats_content_duplicates = 0
        # Keep one stable User-Agent per domain for this worker. This preserves
        # HTTP keep-alive behavior and avoids presenting multiple browser
        # identities over the same pooled connection.
        self.domain_user_agents: Dict[str, str] = {}
        
        # Async robots handler for parallel fetching (10x speedup!)
        self.robots_handler_async = AsyncRobotsHandler(
            redis_host=redis_host,
            redis_port=redis_port,
            user_agent=self._random_user_agent(),
            cache_ttl=86400  # 24 hours
        )
        
        # HTTP fetchers. aiohttp is preferred for page HTML because its
        # persistent TCPConnector caches DNS answers and pools sockets. The
        # requests session remains as a fallback for minimal environments and
        # for unit tests that inject a fake session.
        self.session = self._create_session()
        self.page_fetcher = self._create_page_fetcher()
        
        # Statistics
        self.stats = {
            'pages_crawled': 0,
            'links_extracted': 0,
            'links_added': 0,
            'links_duplicate': 0,
            'links_robots_blocked': 0,
            'domain_locked_skips': 0,
            'recovered': 0,
            'errors': 0,
            'timeouts': 0
        }
        
        self.running = True
        logger.info(f"Worker {self.worker_id} initialized (decentralized mode)")

    def _create_page_fetcher(self) -> Optional[AiohttpPageFetcher]:
        """Create the preferred page fetcher with DNS caching."""
        if aiohttp is None:
            logger.warning("aiohttp unavailable; falling back to requests fetcher")
            return None

        if os.getenv('CRAWLER_USE_AIOHTTP_FETCH', '1').lower() in {'0', 'false', 'no'}:
            logger.info("aiohttp page fetcher disabled by CRAWLER_USE_AIOHTTP_FETCH")
            return None

        dns_ttl = int(os.getenv('DNS_CACHE_TTL_SECONDS', '300'))
        pool_limit = int(os.getenv('PAGE_FETCH_POOL_LIMIT', '100'))
        limit_per_host = int(os.getenv('PAGE_FETCH_LIMIT_PER_HOST', '4'))
        return AiohttpPageFetcher(
            dns_cache_ttl=dns_ttl,
            pool_limit=pool_limit,
            limit_per_host=limit_per_host,
            max_bytes=self.max_page_bytes,
        )
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with connection pooling and retries."""
        session = requests.Session()
        
        # Configure retry strategy
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
        
        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=retry
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def _random_user_agent(self) -> str:
        """Get random User-Agent."""
        return random.choice(self.USER_AGENTS)

    def _user_agent_for_url(self, url: str) -> str:
        """Return a stable User-Agent for the URL's domain."""
        domain = frontier_extract_domain(url) or self._extract_domain(url)
        if not domain:
            return self._random_user_agent()

        if not hasattr(self, 'domain_user_agents'):
            self.domain_user_agents = {}

        if domain not in self.domain_user_agents:
            self.domain_user_agents[domain] = self._random_user_agent()
        return self.domain_user_agents[domain]
    
    def _extract_domain(self, url: str) -> str:
        """Extract bare lowercase netloc from URL.

        Lowercased so the value matches what frontier.extract_domain and
        politeness._extract_domain return — all three must agree on the
        domain identifier or storage stats, lock keys, and frontier keys
        end up split across "Example.COM" vs "example.com" buckets.
        """
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return "unknown"

    def normalize_url(self, url: str) -> str:
        """Canonicalize URLs enough to avoid common crawl duplicates."""
        from urllib.parse import parse_qsl, urlencode

        parsed = urlparse(url.strip())
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        if scheme == 'http' and netloc.endswith(':80'):
            netloc = netloc[:-3]
        elif scheme == 'https' and netloc.endswith(':443'):
            netloc = netloc[:-4]

        path = parsed.path or '/'

        # Sort query parameters and strip common tracking params so URLs that
        # only differ in parameter order or marketing tags collapse to one.
        if parsed.query:
            tracking_prefixes = ('utm_', 'fbclid', 'gclid', 'mc_cid', 'mc_eid')
            params = [
                (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                if not any(k.lower().startswith(p) for p in tracking_prefixes)
            ]
            params.sort()
            query = urlencode(params, doseq=True)
        else:
            query = ''

        return urlunparse((scheme, netloc, path, '', query, ''))
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format."""
        try:
            parsed = urlparse(url)
            if parsed.scheme not in ['http', 'https']:
                return False
            if not parsed.netloc:
                return False
            # Exclude common file extensions
            excluded = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.exe', '.mp4', '.avi']
            if any(url.lower().endswith(ext) for ext in excluded):
                return False
            # Exclude very long URLs
            if len(url) > 500:
                return False
            return True
        except:
            return False
    
    def _calculate_priority(self, url: str, depth: int, parent_url: str = None) -> float:
        """
        Calculate priority score for URL.
        
        Args:
            url: URL to score
            depth: Crawl depth
            parent_url: Parent URL
            
        Returns:
            Priority score (higher = more important)
        """
        priority = 100.0
        
        # Depth penalty (deeper = lower priority)
        priority -= depth * 5
        
        # Boost index pages
        if url.endswith('/') or url.endswith('/index.html'):
            priority += 5
        
        # Boost content pages
        if any(keyword in url.lower() for keyword in ['/blog/', '/article/', '/post/', '/docs/']):
            priority += 3
        
        # Penalize login/signup pages
        if any(keyword in url.lower() for keyword in ['/login', '/signup', '/register', '/auth']):
            priority -= 10
        
        # Penalize very long URLs
        if len(url) > 200:
            priority -= 10
        
        return max(priority, 1.0)
    
    def pull_url_from_frontier(self) -> Optional[Dict]:
        """
        Pop one URL using the two-tier domain frontier.

        Flow:
          1. Frontier.dequeue() pops a domain off active_domains and pulls
             one URL off that domain's per-domain ZSET, atomically (Lua).
          2. If the domain's politeness lock is held, push the domain back
             to the tail of active_domains and return None — caller will
             sleep briefly and try the next domain. *No URL is touched.*
          3. Otherwise, record the URL in the processing ledger and return
             url_data so the worker can fetch it.

        The caller (start loop) is responsible for calling
        ``self.frontier.push_back(domain)`` after the crawl finishes so
        the domain rejoins active_domains for the next URL.

        Returns:
            url_data dict on success; None if no domain was ready or the
            head domain was locked (caller should retry / sleep).
        """
        try:
            entry = self.frontier.dequeue()
            if entry is None:
                return None

            domain, url_json, priority = entry
            if url_json is None:
                # Stale active_domains entry; the Lua script already cleaned
                # up domain_known. Tell the caller to retry immediately.
                return None

            url_data = json.loads(url_json)
            url = self.normalize_url(url_data['url'])
            url_data['url'] = url
            url_data['domain'] = domain

            crawl_delay = self.politeness.get_crawl_delay_for_domain(domain)

            if not self.politeness.can_crawl_domain(domain, crawl_delay):
                # Lock held by another worker (or by us during cooldown).
                # Re-queue the URL we just popped at its original priority,
                # then put the domain back at the tail of active_domains.
                # The URL never had its politeness check granted, so this
                # is the correct invariant per the textbook.
                self.frontier.enqueue(url, priority, url_json)
                self.frontier.push_back(domain)
                self.stats['domain_locked_skips'] += 1
                logger.debug(
                    f"[{self.worker_id}] Domain locked, deferred: {domain}"
                )
                return None

            # Lock acquired - return URL. The processing ledger writes a
            # per-URL lease with native Redis TTL (clock-drift-safe). The
            # crawl_delay is stashed in url_data so the worker can hand it
            # to release_after_crawl_domain() once the fetch finishes,
            # without re-querying Redis.
            url_data['priority'] = priority
            url_data['crawl_delay'] = crawl_delay
            self.ledger.claim(url, url_data, self.worker_id)
            return url_data

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error pulling URL: {e}")
            return None

    def mark_completed(self, url: str) -> bool:
        """
        Release the lease on ``url``. Returns False if we were displaced by
        a recovery worker (zombie case) — the new owner is now responsible
        for the URL and we must not touch it further.
        """
        owned = self.ledger.release(url, self.worker_id)
        if not owned:
            self.zombie_aborts += 1
            logger.warning(
                f"[{self.worker_id}] Zombie release on {url} — "
                f"another worker took over while we were running"
            )
        return owned

    def mark_failed(self, url_data: Dict, permanent: bool = False):
        """Retry failed URLs a few times, then drop them from processing.

        Permanent failures (404, non-HTML, oversized) are dropped right away;
        retrying them only spends politeness budget on a known outcome.

        If we no longer own the lease (a recovery worker took the URL),
        skip both the release and the re-enqueue — the new owner is now
        responsible.
        """
        url = url_data['url']
        owned = self.ledger.release(url, self.worker_id)
        if not owned:
            self.zombie_aborts += 1
            logger.warning(
                f"[{self.worker_id}] Zombie failure on {url} — "
                f"another worker took over; not re-queuing"
            )
            return

        if permanent:
            logger.debug(f"[{self.worker_id}] Dropping permanently failed URL: {url}")
            return

        retries = int(url_data.get('retries', 0)) + 1
        if retries > self.max_retries:
            logger.warning(f"[{self.worker_id}] Dropping failed URL after retries: {url}")
            return

        url_data['retries'] = retries
        url_data.pop('crawl_delay', None)
        priority = max(float(url_data.get('priority', 1.0)) - 10.0, 1.0)
        self.frontier.enqueue(url, priority, json.dumps(url_data))

    def recover_stale_urls(self, force: bool = False):
        """
        Re-queue URLs whose worker died (lease expired natively in Redis).

        Throttled: at most one HSCAN sweep every ``recovery_interval``
        seconds (default 10s, from CrawlerConfig.RECOVERY_INTERVAL). The
        leases themselves are server-side TTLs in Redis, so a delay in
        sweeping doesn't cause data loss — it only delays re-enqueue of
        a dead worker's URLs by at most the recovery_interval. Pass
        ``force=True`` to bypass the throttle (e.g. on shutdown).

        Uses ProcessingLedger.recover_stale, which enumerates the in-flight
        index hash and checks each URL's per-key lease via EXISTS — no
        cross-machine clock comparison. Recovered URLs are pushed back into
        the two-tier frontier with a small priority decay so a permanently
        broken URL drifts down the queue over time.
        """
        now = time.time()
        if not force and now - self._last_recovery_at < self.recovery_interval:
            return
        self._last_recovery_at = now

        recovered_entries = self.ledger.recover_stale(max_recover=200)
        if not recovered_entries:
            return

        for url, url_data in recovered_entries:
            url_data.pop('crawl_delay', None)
            priority = max(float(url_data.get('priority', 1.0)) - 1.0, 1.0)
            self.frontier.enqueue(url, priority, json.dumps(url_data))

        count = len(recovered_entries)
        self.stats['recovered'] += count
        logger.warning(f"[{self.worker_id}] Recovered {count} stale URLs")
    
    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch page with aggressive timeout and User-Agent rotation.

        Args:
            url: URL to fetch

        Returns:
            HTML content or None on error
        """
        outcome = self._fetch_outcome(url, self._user_agent_for_url(url))
        self._record_fetch_outcome(url, outcome)
        if outcome.html is not None:
            self.last_final_url = self.normalize_url(outcome.final_url or url)
        return outcome.html

    def _fetch_outcome(self, url: str, user_agent: str) -> FetchOutcome:
        """Fetch ``url`` and classify the result. Never raises.

        Touches no worker state, so it is safe to run on fetch threads.
        """
        page_fetcher = getattr(self, 'page_fetcher', None)
        if page_fetcher is None:
            return self._fetch_outcome_requests(url, user_agent)

        try:
            result = page_fetcher.fetch(url, user_agent)
        except (asyncio.TimeoutError, TimeoutError):
            return FetchOutcome(error='timeout')
        except PageTooLargeError:
            return FetchOutcome(error='too_large', permanent=True)
        except Exception as e:
            # aiohttp.ClientResponseError carries the HTTP status.
            status = getattr(e, 'status', None)
            if isinstance(status, int) and status >= 400:
                return FetchOutcome(
                    error=f'http_{status}',
                    permanent=is_permanent_http_status(status),
                )
            return FetchOutcome(error=f'error: {e}')

        if result.html is None:
            return FetchOutcome(
                final_url=result.final_url,
                error=f'not_html ({result.status} {result.content_type})',
                permanent=True,
            )
        return FetchOutcome(html=result.html, final_url=result.final_url)

    def _fetch_outcome_requests(self, url: str, user_agent: str) -> FetchOutcome:
        """Synchronous requests fallback used when aiohttp is unavailable."""
        try:
            response = self.session.get(
                url,
                timeout=(3.05, 10),  # (connect, read) timeout
                headers={'User-Agent': user_agent},
                allow_redirects=True,
                stream=True,  # read the body incrementally under the size cap
            )
        except requests.Timeout:
            return FetchOutcome(error='timeout')
        except Exception as e:
            return FetchOutcome(error=f'error: {e}')

        try:
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type.lower():
                return FetchOutcome(
                    final_url=response.url,
                    error=f'not_html ({content_type})',
                    permanent=True,
                )

            response.raise_for_status()
            _check_declared_length(response.headers, self.max_page_bytes)
            body = bytearray()
            for chunk in response.iter_content(64 * 1024):
                body.extend(chunk)
                if len(body) > self.max_page_bytes:
                    raise PageTooLargeError(f"Body exceeds {self.max_page_bytes} bytes")
            return FetchOutcome(
                html=_decode_body(bytes(body), response.encoding),
                final_url=response.url,
            )
        except PageTooLargeError:
            return FetchOutcome(error='too_large', permanent=True)
        except requests.HTTPError:
            status = response.status_code
            return FetchOutcome(
                error=f'http_{status}',
                permanent=is_permanent_http_status(status),
            )
        except requests.Timeout:
            return FetchOutcome(error='timeout')
        except Exception as e:
            return FetchOutcome(error=f'error: {e}')
        finally:
            response.close()

    def _record_fetch_outcome(self, url: str, outcome: FetchOutcome):
        """Update stats/logs for a fetch. Runs on the main thread only."""
        if outcome.error is None:
            return
        if outcome.error == 'timeout':
            self.stats['timeouts'] += 1
        elif not outcome.error.startswith('not_html'):
            self.stats['errors'] += 1
        kind = 'permanent' if outcome.permanent else 'transient'
        logger.debug(f"[{self.worker_id}] Fetch failed ({kind}, {outcome.error}): {url}")
    
    def parse_and_extract_links(self, html: str, base_url: str) -> List[str]:
        """
        Parse HTML and extract links.
        
        Args:
            html: HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href')
                if not href:
                    continue
                
                # Resolve relative URLs
                absolute_url = self.normalize_url(urljoin(base_url, href))
                
                # Validate
                if self._is_valid_url(absolute_url):
                    links.append(absolute_url)
            
            self.stats['links_extracted'] += len(links)
            return links
            
        except Exception as e:
            logger.error(f"[{self.worker_id}] Error parsing HTML: {e}")
            return []

    def extract_text(self, html: str) -> Dict[str, str]:
        """
        Extract title and readable text from HTML for indexing.

        Args:
            html: HTML content

        Returns:
            Dict with title and text
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            for element in soup(['script', 'style', 'noscript', 'template']):
                element.decompose()

            title = soup.title.get_text(" ", strip=True) if soup.title else ''
            text = soup.get_text(" ", strip=True)
            text = " ".join(text.split())

            return {
                'title': title[:500],
                'text': text
            }
        except Exception as e:
            logger.error(f"[{self.worker_id}] Error extracting text: {e}")
            return {'title': '', 'text': ''}
    
    def process_links(self, links: List[str], parent_url: str, depth: int):
        """
        Process extracted links: Bloom Filter check + add to frontier.
        
        Uses async robots.txt checking so multiple domains can be checked
        concurrently and cached in Redis.
        
        Args:
            links: List of URLs to process
            parent_url: Parent page URL
            depth: Current depth
        """
        if not links:
            return
        
        # 1. Filter valid URLs first (fast)
        valid_links = []
        seen_on_page = set()
        for link in links:
            if not self._is_valid_url(link):
                continue

            normalized = self.normalize_url(link)
            if normalized in seen_on_page:
                self.stats['links_duplicate'] += 1
                continue

            seen_on_page.add(normalized)
            valid_links.append(normalized)
        
        # 2. Reserve slots atomically in one Redis round trip. The Lua-backed
        #    add_many checks-and-sets each URL atomically on the server, so
        #    two workers racing on the same link cannot both see "new".
        results = self.bloom_filter.add_many(valid_links)
        new_links = [link for link, is_new in zip(valid_links, results) if is_new]
        self.stats['links_duplicate'] += len(valid_links) - len(new_links)

        if not new_links:
            return

        # 3. Check robots.txt for all links in parallel.
        robots_results = asyncio.run(self.robots_handler_async.can_fetch_batch(new_links))

        # 4. Add allowed links to the two-tier frontier. Frontier.enqueue
        #    atomically writes to crawler:frontier:<domain>, crawler:domain_known
        #    and crawler:active_domains via Lua so the SET/LIST stay coherent
        #    even when many workers are enqueueing the same domain at once.
        for link in new_links:
            can_fetch = robots_results.get(link, True)

            if not can_fetch:
                self.stats['links_robots_blocked'] += 1
                continue

            priority = self._calculate_priority(link, depth + 1, parent_url)

            url_data = {
                'url': link,
                'parent': parent_url,
                'depth': depth + 1,
                'added_at': time.time(),
            }

            self.frontier.enqueue(link, priority, json.dumps(url_data))
            self.stats['links_added'] += 1
    
    def _complete_crawl(self, url_data: Dict, outcome: FetchOutcome):
        """Finish one fetched URL: store or fail it, then free its domain.

        The politeness lock currently covers (crawl_delay +
        max_request_timeout); shrink it back to crawl_delay first so the
        cooldown clock starts from connection-close, not connection-open.
        """
        domain = url_data.get('domain') or frontier_extract_domain(url_data['url'])
        self.politeness.release_after_crawl_domain(domain, url_data.get('crawl_delay'))

        # On success the URL is now queued in the storage batch — the lease
        # is NOT released yet. The release happens in _process_flush_result,
        # after Mongo has durably written the page. On failure (network
        # error, zombie, non-HTML), release/re-enqueue immediately.
        if not self._finish_crawl(url_data, outcome):
            self.mark_failed(url_data, permanent=outcome.permanent)

        # Drain any committed/duplicate/failed work from a flush that may
        # have happened above (or trigger a flush when the batch fills up).
        self._maybe_flush()

        # Return the domain to the tail of active_domains so other workers
        # (or this one) can pick it up for its next URL. push_back is a
        # no-op if the per-domain frontier became empty.
        self.frontier.push_back(domain)

    def _finish_crawl(self, url_data: Dict, outcome: FetchOutcome) -> bool:
        """
        Post-fetch crawl pipeline for one URL: parse, enqueue links, batch.

        Args:
            url_data: URL data dictionary
            outcome: result of fetching url_data['url']

        Returns:
            True if successful, False otherwise
        """
        url = url_data['url']
        depth = url_data.get('depth', 0)

        try:
            self._record_fetch_outcome(url, outcome)
            html = outcome.html
            if not html:
                return False

            # Zombie check. If our lease expired during the fetch (slow
            # network, GC pause) and a recovery worker has already reclaimed
            # the URL, abort before any irreversible side-effect: don't add
            # extracted links to the frontier, don't write to Mongo, don't
            # bump pages_crawled. The new owner will redo this work cleanly.
            if not self.ledger.verify_owner(url, self.worker_id):
                self.zombie_aborts += 1
                logger.warning(
                    f"[{self.worker_id}] Lost lease for {url} during fetch; aborting"
                )
                return False

            page_url = self.normalize_url(outcome.final_url or url)
            self.bloom_filter.add(page_url)
            
            # 2. Parse and extract links
            links = self.parse_and_extract_links(html, page_url)
            document = self.extract_text(html)
            
            # 3. Process links (add to frontier)
            self.process_links(links, page_url, depth)
            
            # 4. Queue page for batch insert. We do NOT release the lease
            #    here — that has to wait until flush_batch durably writes
            #    the page to Mongo (see _process_flush_result). If the
            #    worker dies between here and the next flush, the lease
            #    will expire naturally and a recovery worker will re-queue
            #    the URL — no data loss.
            page_domain = self._extract_domain(page_url)
            page_id = self.storage.add_page(
                url=page_url,
                html=html,
                links=links,
                domain=page_domain,
                depth=depth,
                worker_id=self.worker_id,
                title=document['title'],
                text=document['text']
            )
            if page_id is None:
                # Storage rejected pre-batch; nothing pending — release now.
                self.mark_completed(url)
                return True

            # Stash the original url_data keyed by page_id so a "failed"
            # flush result can re-enqueue with the right priority/retries.
            self._pending_url_data[page_id] = {
                'url': url,
                'parent': url_data.get('parent', ''),
                'depth': depth,
                'priority': url_data.get('priority', 1.0),
                'retries': url_data.get('retries', 0),
                'added_at': time.time(),
            }

            logger.info(f"[{self.worker_id}] Crawled: {url} "
                       f"({len(links)} links extracted, queued as {page_id})")

            return True

        except Exception as e:
            logger.error(f"[{self.worker_id}] Error crawling {url}: {e}")
            self.stats['errors'] += 1
            return False

    def _process_flush_result(self, result):
        """
        Handle the FlushResult returned by storage.flush_batch.

        - committed: page is durable in Mongo → release lease, bump pages_crawled
        - duplicate: rejected by unique index (URL or content_hash already
          in Mongo) → release lease, but bump a separate "content_duplicates"
          stat instead of pages_crawled
        - failed: transaction or infra error → release lease and re-enqueue
          the URL to the frontier so another worker picks it up
        """
        if not result:
            return

        for page_id in result.committed:
            url_data = self._pending_url_data.pop(page_id, None)
            lease_url = (
                url_data.get('url')
                if url_data is not None
                else result.page_id_to_url.get(page_id)
            )
            if lease_url is not None:
                self.mark_completed(lease_url)
                self.stats['pages_crawled'] += 1

        for page_id in result.duplicate:
            url_data = self._pending_url_data.pop(page_id, None)
            lease_url = (
                url_data.get('url')
                if url_data is not None
                else result.page_id_to_url.get(page_id)
            )
            if lease_url is not None:
                self.mark_completed(lease_url)
                self.stats_content_duplicates += 1
                logger.debug(
                    f"[{self.worker_id}] Content duplicate for {lease_url}; "
                    f"already in Mongo, lease released"
                )

        for page_id in result.failed:
            url_data = self._pending_url_data.pop(page_id, None)
            url = result.page_id_to_url.get(page_id)
            if url is None:
                continue
            if url_data is None:
                # Shouldn't happen — fall back to a plain release with
                # no re-enqueue rather than losing the URL silently.
                self.mark_completed(url)
                continue
            # mark_failed handles release + re-enqueue + retry decay.
            self.mark_failed(url_data)
            logger.warning(
                f"[{self.worker_id}] Flush failed for {url}; re-queued"
            )

    def _maybe_flush(self, force: bool = False):
        """
        Flush the storage batch when it's full (or when ``force`` is set).
        Wrapper that also drives _process_flush_result.
        """
        if not force and not self.storage.batch_is_full():
            return
        result = self.storage.flush_batch()
        self._process_flush_result(result)
    
    def start(self, max_pages: int = None, idle_timeout: int = 60):
        """
        Start worker crawling loop.
        
        Args:
            max_pages: Maximum pages to crawl (None = unlimited)
            idle_timeout: Seconds to wait before stopping if frontier empty
        """
        logger.info(f"[{self.worker_id}] Starting decentralized worker...")
        logger.info(f"  Max pages: {max_pages or 'unlimited'}")
        logger.info(f"  Idle timeout: {idle_timeout}s")
        logger.info(f"  Fetch concurrency: {self.concurrency}")

        idle_since = None
        # Fetches run on a thread pool; everything that touches Redis, Mongo
        # or worker state (claiming, parsing, link enqueue, batching, lease
        # release) stays on this thread when a fetch completes.
        in_flight: Dict[Future, Dict] = {}
        fetch_pool = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix=f'{self.worker_id}-fetch',
        )

        try:
            while self.running:
                if self.redis.get('crawler:shutdown'):
                    logger.info(f"[{self.worker_id}] Shutdown signal received")
                    break

                # Check max pages
                if max_pages and self.stats['pages_crawled'] >= max_pages:
                    logger.info(f"[{self.worker_id}] Reached max pages: {max_pages}")
                    break

                self.recover_stale_urls()
                dispatched = self._dispatch_fetches(fetch_pool, in_flight, max_pages)

                if in_flight:
                    idle_since = None
                    done, _ = wait(
                        in_flight,
                        timeout=_LOCKED_SLEEP_SECONDS,
                        return_when=FIRST_COMPLETED,
                    )
                    for future in done:
                        self._complete_crawl(in_flight.pop(future), self._future_outcome(future))
                    continue

                # Nothing in flight. Flush a partial batch now: its URLs hold
                # leases in the processing ledger, which would otherwise keep
                # the idle check below from ever firing, and at the max-pages
                # cap they are what's left to commit.
                if self.storage.pending_count():
                    self._maybe_flush(force=True)
                    continue

                if dispatched:
                    continue

                # We got nothing. Three reasons:
                #   - active_domains was empty (no work anywhere)
                #   - head domain was locked (push-back already happened)
                #   - stale active_domains entry (already cleaned)
                # In all cases we just retry; idle detection uses
                # active_domain_count + processing as the "is there any
                # work pending anywhere" signal.
                active_domains = self.frontier.active_domain_count()
                processing_size = self.ledger.in_flight_count()

                if active_domains == 0 and processing_size == 0:
                    if idle_since is None:
                        idle_since = time.time()
                    elif time.time() - idle_since >= idle_timeout:
                        logger.info(f"[{self.worker_id}] Frontier empty for {idle_timeout}s, stopping")
                        break
                else:
                    idle_since = None

                # Short sleep when a locked head domain just got pushed
                # back (avoid hot-loop), longer sleep when truly idle.
                time.sleep(_LOCKED_SLEEP_SECONDS if active_domains else _IDLE_SLEEP_SECONDS)

        except KeyboardInterrupt:
            logger.info(f"[{self.worker_id}] Interrupted by user")
        finally:
            # Finish fetches already started so their URLs are stored or
            # re-queued now instead of waiting out their leases.
            for future in list(in_flight):
                self._complete_crawl(in_flight.pop(future), self._future_outcome(future))
            fetch_pool.shutdown(wait=True)
            self._shutdown()

    def _dispatch_fetches(self, fetch_pool: ThreadPoolExecutor,
                          in_flight: Dict[Future, Dict],
                          max_pages: Optional[int]) -> int:
        """Claim URLs and start fetching them until every slot is busy.

        Stops at the first empty/locked pull rather than spinning through
        the frontier; the loop comes back within _LOCKED_SLEEP_SECONDS.
        Pages already fetched, batched or in flight count toward max_pages
        so the worker does not overshoot its cap by `concurrency` pages.
        """
        slots = self.concurrency - len(in_flight)
        if max_pages:
            projected = (
                self.stats['pages_crawled']
                + self.storage.pending_count()
                + len(in_flight)
            )
            slots = min(slots, max_pages - projected)

        dispatched = 0
        while dispatched < slots:
            url_data = self.pull_url_from_frontier()
            if not url_data:
                break
            url = url_data['url']
            logger.info(f"[{self.worker_id}] Crawling: {url}")
            future = fetch_pool.submit(self._fetch_outcome, url, self._user_agent_for_url(url))
            in_flight[future] = url_data
            dispatched += 1
        return dispatched

    @staticmethod
    def _future_outcome(future: Future) -> FetchOutcome:
        try:
            return future.result()
        except Exception as e:  # _fetch_outcome catches everything; belt and braces
            return FetchOutcome(error=f'error: {e}')
    
    def _shutdown(self):
        """Cleanup and show statistics."""
        logger.info(f"[{self.worker_id}] Shutting down...")

        # Force-flush any pending pages and process the result so leases
        # are released (committed/duplicate) or URLs re-enqueued (failed)
        # before the worker exits. Without this, a clean shutdown with a
        # half-full batch would leave URLs locked in the ledger until
        # their lease expires.
        self._maybe_flush(force=True)

        # Get storage stats BEFORE closing
        storage_stats = self.storage.get_stats()
        
        # Close connections
        self.storage.close()
        page_fetcher = getattr(self, 'page_fetcher', None)
        if page_fetcher is not None:
            page_fetcher.close()
        self.session.close()
        
        # Show statistics
        logger.info(f"\n{'='*60}")
        logger.info(f"Worker {self.worker_id} Final Statistics")
        logger.info(f"{'='*60}")
        logger.info(f"Pages crawled:        {self.stats['pages_crawled']:,}")
        logger.info(f"Links extracted:      {self.stats['links_extracted']:,}")
        logger.info(f"Links added:          {self.stats['links_added']:,}")
        logger.info(f"Links duplicate:      {self.stats['links_duplicate']:,}")
        logger.info(f"Links robots blocked: {self.stats['links_robots_blocked']:,}")
        logger.info(f"Domain locked skips:  {self.stats['domain_locked_skips']:,}")
        logger.info(f"Recovered stale:      {self.stats['recovered']:,}")
        logger.info(f"Zombie aborts:        {self.zombie_aborts:,}")
        logger.info(f"Content duplicates:   {self.stats_content_duplicates:,}")
        logger.info(f"Errors:               {self.stats['errors']:,}")
        logger.info(f"Timeouts:             {self.stats['timeouts']:,}")
        logger.info(f"{'='*60}")
        
        # Storage stats (already retrieved above)
        if storage_stats['pages_stored'] > 0:
            savings = (1 - storage_stats['compression_ratio']) * 100
            logger.info(f"\nStorage Statistics:")
            logger.info(f"  Pages stored:     {storage_stats['pages_stored']:,}")
            logger.info(f"  Compression:      {savings:.1f}% saved")
            logger.info(f"  Space saved:      {storage_stats['space_saved_mb']:.1f} MB")
        
        logger.info(f"\n✅ Worker {self.worker_id} stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Decentralized Crawler Worker')
    parser.add_argument('--worker-id', type=str, help='Worker ID')
    parser.add_argument('--max-pages', type=int, default=100, help='Max pages to crawl')
    parser.add_argument('--redis-host', type=str, default=CrawlerConfig.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=CrawlerConfig.REDIS_PORT, help='Redis port')
    parser.add_argument('--mongodb', type=str, default=CrawlerConfig.get_mongo_url(), help='MongoDB URI')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for inserts')
    parser.add_argument('--idle-timeout', type=int, default=60, help='Idle timeout seconds')
    parser.add_argument('--processing-timeout', type=int, default=300, help='Seconds before stale processing URLs are recovered')
    parser.add_argument('--recovery-interval', type=int, default=CrawlerConfig.RECOVERY_INTERVAL, help='Min seconds between stale-recovery sweeps')
    parser.add_argument('--concurrency', type=int, default=CrawlerConfig.FETCH_CONCURRENCY, help='Max concurrent page fetches (one per domain)')

    args = parser.parse_args()

    # Create and start worker
    worker = DecentralizedWorker(
        worker_id=args.worker_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        mongodb_uri=args.mongodb,
        batch_size=args.batch_size,
        processing_timeout=args.processing_timeout,
        recovery_interval=args.recovery_interval,
        concurrency=args.concurrency,
    )
    
    worker.start(max_pages=args.max_pages, idle_timeout=args.idle_timeout)


if __name__ == '__main__':
    main()
