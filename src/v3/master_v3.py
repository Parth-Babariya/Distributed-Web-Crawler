#!/usr/bin/env python3
"""
Master Node V3 - Monitoring & Seeding Only

The Master no longer processes links!
Workers handle everything autonomously.

Master's new roles:
1. Seed initial URLs
2. Monitor system statistics
3. Graceful shutdown signal
"""

import logging
import time
import json
import sys
import os
from typing import List
from urllib.parse import urlparse, urlunparse
from redis import Redis
from pymongo import MongoClient

# Add parent directory to path for shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from bloom_filter import BloomFilter
from frontier import Frontier
from config import CrawlerConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def normalize_url(url: str) -> str:
    """Canonicalize seed URLs to match worker-side deduplication."""
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    if scheme == 'http' and netloc.endswith(':80'):
        netloc = netloc[:-3]
    elif scheme == 'https' and netloc.endswith(':443'):
        netloc = netloc[:-4]

    path = parsed.path or '/'
    return urlunparse((scheme, netloc, path, '', parsed.query, ''))


class MasterV3:
    """
    Lightweight Master for monitoring and management.
    
    The crawling loop is fully decentralized!
    Workers validate, deduplicate, and schedule URLs themselves.
    """
    
    def __init__(self, redis_host: str = None, redis_port: int = None,
                 mongodb_uri: str = None):
        """
        Initialize Master V3.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            mongodb_uri: MongoDB URI
        """
        redis_host = redis_host or CrawlerConfig.REDIS_HOST
        redis_port = redis_port or CrawlerConfig.REDIS_PORT
        mongodb_uri = mongodb_uri or CrawlerConfig.get_mongo_url()

        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=False
        )
        
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client['web_crawler']
        
        self.bloom_filter = BloomFilter(
            self.redis,
            capacity=10000000,
            error_rate=0.001
        )

        self.frontier = Frontier(self.redis)

        logger.info("Master V3 initialized (monitoring mode)")
    
    def seed_urls(self, urls: List[str], priority: float = 100.0):
        """
        Add seed URLs to frontier.
        
        Args:
            urls: List of seed URLs
            priority: Priority score for seeds (default: 100 = high)
        """
        logger.info(f"Seeding {len(urls)} URLs...")
        
        added = 0
        for url in urls:
            url = normalize_url(url)
            # Check if already in Bloom Filter
            if self.bloom_filter.contains(url):
                logger.debug(f"Seed URL already seen: {url}")
                continue
            
            # Add to Bloom Filter
            self.bloom_filter.add(url)
            
            # Add to two-tier frontier (Tier 2 ZSET + Tier 1 active_domains
            # + domain_known SET, atomically via Lua).
            url_data = {
                'url': url,
                'parent': '',
                'depth': 0,
                'added_at': time.time()
            }
            self.frontier.enqueue(url, priority, json.dumps(url_data))

            added += 1
            logger.info(f"Seeded: {url}")
        
        logger.info(f"✅ Seeded {added} URLs to frontier")
        return added
    
    def get_system_stats(self) -> dict:
        """Get real-time system statistics."""
        # Two-tier frontier: total URLs across all per-domain ZSETs (uses
        # SCAN, not KEYS — safe for large keyspaces) plus the count of
        # domains currently waiting in active_domains.
        frontier_size = self.frontier.total_size()
        active_domains = self.frontier.active_domain_count()
        known_domains = self.frontier.known_domain_count()
        processing_size = self.redis.hlen('crawler:processing')
        
        # Bloom Filter stats
        bloom_stats = self.bloom_filter.get_stats()
        
        # MongoDB stats
        pages_stored = self.db.pages_metadata.count_documents({})
        
        # Domain distribution
        pipeline = [
            {'$group': {'_id': '$domain', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]
        top_domains = list(self.db.pages_metadata.aggregate(pipeline))
        
        # Storage stats
        storage_pipeline = [
            {'$group': {
                '_id': None,
                'total_size': {'$sum': '$content_size'},
                'total_compressed': {'$sum': '$compressed_size'}
            }}
        ]
        storage_result = list(self.db.pages_metadata.aggregate(storage_pipeline))
        storage_stats = storage_result[0] if storage_result else {}
        
        return {
            'frontier_size': frontier_size,
            'active_domains': active_domains,
            'known_domains': known_domains,
            'processing_size': processing_size,
            'bloom_size_mb': bloom_stats['size_mb'],
            'bloom_capacity': bloom_stats['capacity'],
            'pages_stored': pages_stored,
            'top_domains': top_domains,
            'total_size_mb': storage_stats.get('total_size', 0) / 1024 / 1024,
            'compressed_size_mb': storage_stats.get('total_compressed', 0) / 1024 / 1024,
            'space_saved_mb': (
                (storage_stats.get('total_size', 0) -
                 storage_stats.get('total_compressed', 0)) / 1024 / 1024
            )
        }
    
    def monitor(self, interval: int = 5):
        """
        Monitor system in real-time.
        
        Args:
            interval: Update interval in seconds
        """
        logger.info("Starting monitoring mode...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                stats = self.get_system_stats()
                
                print("\n" + "="*70)
                print(f"System Statistics - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*70)
                print(f"Frontier size:       {stats['frontier_size']:,} URLs "
                      f"across {stats['known_domains']:,} domains")
                print(f"Active domains:      {stats['active_domains']:,} ready")
                print(f"Processing size:     {stats['processing_size']:,} URLs")
                print(f"Bloom Filter:        {stats['bloom_size_mb']:.1f} MB "
                      f"(capacity: {stats['bloom_capacity']:,})")
                print(f"Pages stored:        {stats['pages_stored']:,}")
                print(f"Storage (original):  {stats['total_size_mb']:.1f} MB")
                print(f"Storage (compressed):{stats['compressed_size_mb']:.1f} MB")
                print(f"Space saved:         {stats['space_saved_mb']:.1f} MB "
                      f"({(stats['space_saved_mb']/max(stats['total_size_mb'],1)*100):.1f}%)")
                
                if stats['top_domains']:
                    print("\nTop 10 Domains:")
                    for i, domain in enumerate(stats['top_domains'][:10], 1):
                        print(f"  {i:2}. {domain['_id']}: {domain['count']:,} pages")
                
                print("="*70)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("\nMonitoring stopped")
    
    def shutdown_workers(self):
        """
        Signal all workers to gracefully shutdown.
        Workers check this flag periodically.
        """
        self.redis.set('crawler:shutdown', '1', ex=300)
        logger.info("Shutdown signal sent to all workers")
    
    def clear_shutdown_signal(self):
        """Clear shutdown signal."""
        self.redis.delete('crawler:shutdown')
        logger.info("Shutdown signal cleared")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Master Node V3 (Monitoring)')
    parser.add_argument('--redis-host', type=str, default=CrawlerConfig.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=CrawlerConfig.REDIS_PORT, help='Redis port')
    parser.add_argument('--mongodb', type=str, default=CrawlerConfig.get_mongo_url(), help='MongoDB URI')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Seed command
    seed_parser = subparsers.add_parser('seed', help='Seed URLs')
    seed_parser.add_argument('urls', nargs='+', help='URLs to seed')
    seed_parser.add_argument('--priority', type=float, default=100.0, help='Priority score')
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor system')
    monitor_parser.add_argument('--interval', type=int, default=5, help='Update interval (seconds)')
    
    # Shutdown command
    subparsers.add_parser('shutdown', help='Signal workers to shutdown')
    
    # Stats command
    subparsers.add_parser('stats', help='Show current statistics')
    
    args = parser.parse_args()
    
    # Create Master
    master = MasterV3(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        mongodb_uri=args.mongodb
    )
    
    # Execute command
    if args.command == 'seed':
        master.seed_urls(args.urls, priority=args.priority)
        
    elif args.command == 'monitor':
        master.monitor(interval=args.interval)
        
    elif args.command == 'shutdown':
        master.shutdown_workers()
        
    elif args.command == 'stats':
        stats = master.get_system_stats()
        print("\n" + "="*70)
        print("Current System Statistics")
        print("="*70)
        for key, value in stats.items():
            if key == 'top_domains':
                print(f"\n{key}:")
                for domain in value[:10]:
                    print(f"  {domain['_id']}: {domain['count']:,}")
            else:
                print(f"{key}: {value}")
        print("="*70)
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
