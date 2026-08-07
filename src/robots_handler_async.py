"""
Asynchronous robots.txt handler with parallel fetching.

This module provides a robots.txt handler that fetches multiple domain
robots.txt files in parallel and caches results in Redis.

Key features:
- Parallel async fetching with aiohttp
- Redis caching (shared across workers)
- Local in-memory caching (per-worker)
- Aggressive timeouts (3 seconds max)
- Error handling (timeouts, SSL errors, etc.)
"""

import asyncio
import time
import logging
import urllib.request
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from redis import Redis
from typing import Dict, List, Optional, Tuple

try:
    import aiohttp
except ImportError:  # pragma: no cover - fallback for minimal environments
    aiohttp = None

logger = logging.getLogger(__name__)


class AsyncRobotsHandler:
    """
    Async robots.txt handler that fetches multiple domains in parallel.
    
    Performance:
    - Multiple domain checks are fetched concurrently and cached.
    
    Usage:
        handler = AsyncRobotsHandler()
        results = await handler.can_fetch_batch(['http://example.com/page1', ...])
    """
    
    def __init__(self, redis_host='localhost', redis_port=6379, 
                 user_agent='Mozilla/5.0 (compatible; WebCrawler/3.0)',
                 cache_ttl=3600):
        """
        Initialize async robots.txt handler.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            user_agent: User agent string for requests
            cache_ttl: Cache duration in seconds (default 1 hour)
        """
        self.redis = Redis(host=redis_host, port=redis_port, 
                          decode_responses=True)
        self.user_agent = user_agent
        self.cache = {}  # Local in-memory cache
        self.cache_ttl = cache_ttl
        
        logger.info(f"AsyncRobotsHandler initialized (cache_ttl={cache_ttl}s)")
    
    async def can_fetch_batch(self, urls: List[str]) -> Dict[str, bool]:
        """
        Check if multiple URLs can be fetched concurrently.
        
        Instead of checking each URL sequentially, it groups URLs by domain
        and fetches uncached robots.txt files in parallel.
        
        Args:
            urls: List of URLs to check
            
        Returns:
            Dict mapping URL -> can_fetch (True/False)
        """
        if not urls:
            return {}
        
        # Group URLs by domain to avoid duplicate fetches
        domain_to_urls = {}
        for url in urls:
            domain = self._extract_domain(url)
            if domain:
                if domain not in domain_to_urls:
                    domain_to_urls[domain] = []
                domain_to_urls[domain].append(url)
        
        # Get robots.txt parsers for all domains in PARALLEL
        domains = list(domain_to_urls.keys())
        robots_parsers = await self._fetch_robots_batch(domains)
        
        # Check each URL against its domain's robots.txt
        results = {}
        for url in urls:
            domain = self._extract_domain(url)
            if not domain:
                results[url] = True
                continue
                
            parser = robots_parsers.get(domain)
            
            if not parser:
                # No robots.txt or error = allow crawling
                results[url] = True
            else:
                # Check if this specific URL is allowed
                results[url] = parser.can_fetch(self.user_agent, url)
        
        return results
    
    async def _fetch_robots_batch(self, domains: List[str]) -> Dict[str, Optional[RobotFileParser]]:
        """
        Fetch robots.txt for multiple domains concurrently.
        
        Flow:
        1. Check local cache (instant)
        2. Check Redis cache (fast)
        3. Fetch remaining domains in parallel
        4. Cache results in Redis and locally
        
        Args:
            domains: List of domain names (e.g., ['example.com', 'google.com'])
            
        Returns:
            Dict mapping domain -> RobotFileParser (or None if no robots.txt)
        """
        parsers = {}
        domains_to_fetch = []
        
        # Check caches first (fast!)
        for domain in domains:
            # Check local in-memory cache
            if domain in self.cache:
                cached_time = self.cache[domain].get('fetched_at', 0)
                if time.time() - cached_time < self.cache_ttl:
                    parsers[domain] = self.cache[domain]['parser']
                    continue
            
            # Check Redis cache (shared across workers)
            cached_data = self.redis.hgetall(f'robots_cache:{domain}')
            if cached_data and 'content' in cached_data:
                try:
                    parser = RobotFileParser()
                    parser.parse(cached_data['content'].split('\n'))
                    parsers[domain] = parser
                    
                    # Update local cache
                    self.cache[domain] = {
                        'parser': parser,
                        'fetched_at': time.time()
                    }
                    continue
                except Exception as e:
                    logger.warning(f"Error parsing cached robots.txt for {domain}: {e}")
            
            # Need to fetch from website
            domains_to_fetch.append(domain)
        
        # Fetch all uncached domains in PARALLEL
        if domains_to_fetch:
            logger.info(f"🚀 Fetching robots.txt for {len(domains_to_fetch)} domains in parallel...")
            start_time = time.time()
            
            if aiohttp is None:
                for domain in domains_to_fetch:
                    parser, content = self._fetch_robots_for_domain_sync(domain)
                    parsers[domain] = parser
                    if content:
                        self.redis.hset(f'robots_cache:{domain}', mapping={
                            'content': content,
                            'fetched_at': str(time.time())
                        })
                        self.redis.expire(f'robots_cache:{domain}', self.cache_ttl)
                    self.cache[domain] = {
                        'parser': parser,
                        'fetched_at': time.time()
                    }
            else:
                async with aiohttp.ClientSession() as session:
                    # Create tasks for ALL domains simultaneously
                    tasks = [
                        self._fetch_robots_for_domain(session, domain)
                        for domain in domains_to_fetch
                    ]

                    # Wait for ALL to complete (with individual timeouts)
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process results
                    for domain, result in zip(domains_to_fetch, results):
                        if isinstance(result, Exception):
                            logger.error(f"Error fetching robots.txt for {domain}: {result}")
                            parsers[domain] = None  # Allow crawling on error
                        else:
                            parser, content = result
                            parsers[domain] = parser

                            # Cache in Redis (shared) and locally
                            if content:
                                self.redis.hset(f'robots_cache:{domain}', mapping={
                                    'content': content,
                                    'fetched_at': str(time.time())
                                })
                                self.redis.expire(f'robots_cache:{domain}', self.cache_ttl)

                            self.cache[domain] = {
                                'parser': parser,
                                'fetched_at': time.time()
                            }
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Fetched {len(domains_to_fetch)} robots.txt in {elapsed:.2f}s "
                       f"({elapsed/len(domains_to_fetch):.2f}s avg)")
        
        return parsers

    def _cache_crawl_delay(self, domain: str, content: str):
        """Cache crawl-delay in the key format used by PolitenessManager."""
        crawl_delay = self._extract_crawl_delay(content)
        if not crawl_delay:
            return

        self.redis.setex(
            f'crawler:robots:delay:{domain}',
            self.cache_ttl,
            str(crawl_delay)
        )
        self.redis.hset(
            f'crawler:domain_state:{domain}',
            'crawl_delay',
            str(crawl_delay)
        )

    def _fetch_robots_for_domain_sync(self, domain: str) -> Tuple[Optional[RobotFileParser], Optional[str]]:
        """Fetch robots.txt synchronously when aiohttp is not installed."""
        for protocol in ['https', 'http']:
            robots_url = f'{protocol}://{domain}/robots.txt'

            try:
                request = urllib.request.Request(
                    robots_url,
                    headers={'User-Agent': self.user_agent}
                )
                with urllib.request.urlopen(request, timeout=3) as response:
                    if response.status == 200:
                        content = response.read().decode('utf-8', errors='replace')
                        parser = RobotFileParser()
                        parser.parse(content.split('\n'))
                        self._cache_crawl_delay(domain, content)
                        return (parser, content)

                    if response.status in [404, 403]:
                        return (None, None)

            except Exception:
                continue

        return (None, None)
    
    async def _fetch_robots_for_domain(self, session, 
                                      domain: str) -> Tuple[Optional[RobotFileParser], Optional[str]]:
        """
        Fetch robots.txt for a single domain (async).
        
        Args:
            session: aiohttp session for HTTP requests
            domain: Domain name (e.g., 'example.com')
            
        Returns:
            Tuple of (RobotFileParser or None, content or None)
        """
        # Try HTTPS first, then HTTP
        for protocol in ['https', 'http']:
            robots_url = f'{protocol}://{domain}/robots.txt'
            
            try:
                # Aggressive timeout (3 seconds max per domain!)
                timeout = aiohttp.ClientTimeout(total=3, connect=1)
                
                async with session.get(
                    robots_url, 
                    timeout=timeout,
                    allow_redirects=True,
                    headers={'User-Agent': self.user_agent}
                ) as response:
                    if response.status == 200:
                        content = await response.text()
                        
                        # Parse robots.txt
                        parser = RobotFileParser()
                        parser.parse(content.split('\n'))
                        
                        self._cache_crawl_delay(domain, content)
                        
                        logger.debug(f"✅ Fetched robots.txt for {domain} ({protocol})")
                        return (parser, content)
                    
                    elif response.status in [404, 403]:
                        # No robots.txt = allow all
                        logger.debug(f"No robots.txt for {domain} ({response.status})")
                        return (None, None)
            
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout fetching robots.txt for {domain} ({protocol})")
                continue  # Try next protocol
                
            except aiohttp.ClientError as e:
                logger.debug(f"Connection error for {domain} ({protocol}): {e}")
                continue  # Try next protocol
                
            except Exception as e:
                logger.error(f"❌ Error fetching robots.txt for {domain} ({protocol}): {e}")
                continue
        
        # Both protocols failed = allow crawling
        logger.debug(f"No robots.txt accessible for {domain} (allow all)")
        return (None, None)
    
    def _extract_domain(self, url: str) -> Optional[str]:
        """
        Extract domain from URL.
        
        Args:
            url: Full URL (e.g., 'https://example.com/page')
            
        Returns:
            Domain name (e.g., 'example.com') or None if invalid
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if not domain:
                # Handle malformed URLs
                domain = parsed.path.split('/')[0] if parsed.path else None
            return domain
        except Exception as e:
            logger.warning(f"Error extracting domain from {url}: {e}")
            return None
    
    def _extract_crawl_delay(self, robots_content: str) -> Optional[float]:
        """
        Extract crawl-delay directive from robots.txt content.
        
        Args:
            robots_content: Raw robots.txt content
            
        Returns:
            Crawl delay in seconds, or None if not specified
        """
        try:
            for line in robots_content.split('\n'):
                line = line.strip().lower()
                if line.startswith('crawl-delay:'):
                    delay_str = line.split(':', 1)[1].strip()
                    return float(delay_str)
        except Exception as e:
            logger.warning(f"Error extracting crawl-delay: {e}")
        
        return None
    
    def get_crawl_delay(self, domain: str) -> float:
        """
        Get crawl delay for a domain (from cache or default).
        
        Args:
            domain: Domain name
            
        Returns:
            Crawl delay in seconds (default 1.0)
        """
        try:
            cached_delay = self.redis.get(f'crawler:robots:delay:{domain}')
            if cached_delay:
                return float(cached_delay)
            state_delay = self.redis.hget(
                f'crawler:domain_state:{domain}',
                'crawl_delay',
            )
            if state_delay:
                return float(state_delay)
        except Exception:
            pass
        
        return 1.0  # Default crawl delay


# Synchronous wrapper for backward compatibility
def can_fetch_sync(handler: AsyncRobotsHandler, urls: List[str]) -> Dict[str, bool]:
    """
    Synchronous wrapper for can_fetch_batch.
    
    Usage:
        handler = AsyncRobotsHandler()
        results = can_fetch_sync(handler, ['http://example.com/page1', ...])
    """
    return asyncio.run(handler.can_fetch_batch(urls))
