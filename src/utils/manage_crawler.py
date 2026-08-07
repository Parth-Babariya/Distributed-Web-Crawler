#!/usr/bin/env python3
"""
Monitoring and management utilities for the distributed crawler.

Provides tools for:
- Real-time queue monitoring
- Worker status tracking
- Queue management (clear, reset, etc.)
- Statistics reporting
"""

import time
import sys
import argparse
from datetime import datetime

from distributed_crawler import RedisQueueManager, MongoDBStorage


class CrawlerMonitor:
    """Monitor crawler queues and statistics in real-time."""
    
    def __init__(self):
        """Initialize monitoring components."""
        self.queue_manager = RedisQueueManager()
        self.storage = MongoDBStorage()
    
    def display_stats(self, refresh_rate: int = 2):
        """
        Display real-time statistics.
        
        Args:
            refresh_rate: Refresh interval in seconds
        """
        print("\nDistributed Crawler Monitor")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                # Clear screen (Unix/Linux)
                print("\033[2J\033[H", end='')
                
                # Header
                print("="*70)
                print(f"Crawler Statistics - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*70)
                
                # Queue statistics
                queue_stats = self.queue_manager.get_stats()
                print("\n📊 Queue Statistics:")
                print(f"  URLs in queue:     {queue_stats['urls_in_queue']:,}")
                print(f"  URLs processing:   {queue_stats['urls_processing']:,}")
                print(f"  URLs visited:      {queue_stats['urls_visited']:,}")
                print(f"  Unique content:    {queue_stats['unique_content']:,}")
                
                # Storage statistics
                storage_stats = self.storage.get_stats()
                print("\n💾 Storage Statistics:")
                print(f"  Total pages:       {storage_stats['total_pages']:,}")
                print(f"  Total size:        {storage_stats['total_size_mb']:.2f} MB")
                
                # Calculate rates
                in_queue = queue_stats['urls_in_queue']
                processing = queue_stats['urls_processing']
                visited = queue_stats['urls_visited']
                
                # Progress indicator
                if visited > 0:
                    completion = (visited / (visited + in_queue + processing)) * 100
                    print(f"\n📈 Progress:")
                    print(f"  Completion:        {completion:.2f}%")
                
                print("\n" + "="*70)
                print(f"Refresh rate: {refresh_rate}s | Press Ctrl+C to exit")
                
                time.sleep(refresh_rate)
                
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
    
    def display_processing_queue(self):
        """Display currently processing URLs."""
        print("\n📋 Currently Processing URLs:\n")
        
        processing = self.queue_manager.redis_client.hgetall(
            self.queue_manager.PROCESSING_QUEUE
        )
        
        if not processing:
            print("  No URLs currently being processed.")
            return
        
        for url, data in processing.items():
            try:
                data_dict = eval(data)
                worker_id = data_dict.get('worker_id', 'unknown')
                timestamp = data_dict.get('timestamp', 0)
                elapsed = int(time.time() - timestamp)
                
                print(f"  • {url}")
                print(f"    Worker: {worker_id} | Elapsed: {elapsed}s")
            except Exception as e:
                print(f"  • {url} (error parsing data)")
    
    def recover_stale_urls(self):
        """Manually trigger stale URL recovery."""
        print("\n🔄 Recovering stale URLs...")
        
        recovered = self.queue_manager.recover_stale_urls()
        
        if recovered > 0:
            print(f"✓ Recovered {recovered} stale URLs")
        else:
            print("✓ No stale URLs found")


class CrawlerManager:
    """Manage crawler queues and data."""
    
    def __init__(self):
        """Initialize management components."""
        self.queue_manager = RedisQueueManager()
        self.storage = MongoDBStorage()
    
    def clear_queues(self, confirm: bool = True):
        """
        Clear all Redis queues.
        
        Args:
            confirm: Require user confirmation
        """
        if confirm:
            response = input("⚠️  Clear all queues? This cannot be undone! (yes/no): ")
            if response.lower() != 'yes':
                print("Cancelled.")
                return
        
        print("\n🗑️  Clearing queues...")
        
        redis = self.queue_manager.redis_client
        
        # Delete all crawler keys
        keys = [
            self.queue_manager.URL_QUEUE,
            self.queue_manager.PROCESSING_QUEUE,
            self.queue_manager.VISITED_SET,
            self.queue_manager.CONTENT_HASH_SET
        ]
        
        for key in keys:
            redis.delete(key)
        
        print("✓ All queues cleared")
    
    def add_seed_urls(self, urls_file: str):
        """
        Add seed URLs from a file.
        
        Args:
            urls_file: Path to file containing URLs (one per line)
        """
        print(f"\n📝 Loading seed URLs from {urls_file}...")
        
        try:
            with open(urls_file, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            added = self.queue_manager.add_seed_urls(urls)
            print(f"✓ Added {added} seed URLs")
            
        except FileNotFoundError:
            print(f"✗ File not found: {urls_file}")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def export_urls(self, output_file: str):
        """
        Export visited URLs to a file.
        
        Args:
            output_file: Output file path
        """
        print(f"\n💾 Exporting visited URLs to {output_file}...")
        
        try:
            redis = self.queue_manager.redis_client
            urls = redis.smembers(self.queue_manager.VISITED_SET)
            
            with open(output_file, 'w') as f:
                for url in urls:
                    f.write(f"{url}\n")
            
            print(f"✓ Exported {len(urls)} URLs")
            
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def clear_database(self, confirm: bool = True):
        """
        Clear MongoDB database.
        
        Args:
            confirm: Require user confirmation
        """
        if confirm:
            response = input("⚠️  Clear entire database? This cannot be undone! (yes/no): ")
            if response.lower() != 'yes':
                print("Cancelled.")
                return
        
        print("\n🗑️  Clearing database...")
        
        result = self.storage.pages_collection.delete_many({})
        print(f"✓ Deleted {result.deleted_count} pages")


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(
        description='Distributed Web Crawler - Management Utilities'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor crawler statistics')
    monitor_parser.add_argument(
        '--refresh',
        type=int,
        default=2,
        help='Refresh rate in seconds (default: 2)'
    )
    
    # Status command
    subparsers.add_parser('status', help='Show current status')
    
    # Processing command
    subparsers.add_parser('processing', help='Show processing queue')
    
    # Recover command
    subparsers.add_parser('recover', help='Recover stale URLs')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear queues')
    clear_parser.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation'
    )
    
    # Seed command
    seed_parser = subparsers.add_parser('seed', help='Add seed URLs from file')
    seed_parser.add_argument('file', help='File containing URLs (one per line)')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export visited URLs')
    export_parser.add_argument('file', help='Output file path')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    try:
        if args.command == 'monitor':
            monitor = CrawlerMonitor()
            monitor.display_stats(refresh_rate=args.refresh)
            
        elif args.command == 'status':
            monitor = CrawlerMonitor()
            queue_stats = monitor.queue_manager.get_stats()
            storage_stats = monitor.storage.get_stats()
            
            print("\n📊 Crawler Status\n")
            print(f"URLs in queue:     {queue_stats['urls_in_queue']:,}")
            print(f"URLs processing:   {queue_stats['urls_processing']:,}")
            print(f"URLs visited:      {queue_stats['urls_visited']:,}")
            print(f"Unique content:    {queue_stats['unique_content']:,}")
            print(f"Total pages:       {storage_stats['total_pages']:,}")
            print(f"Storage size:      {storage_stats['total_size_mb']:.2f} MB\n")
            
        elif args.command == 'processing':
            monitor = CrawlerMonitor()
            monitor.display_processing_queue()
            
        elif args.command == 'recover':
            monitor = CrawlerMonitor()
            monitor.recover_stale_urls()
            
        elif args.command == 'clear':
            manager = CrawlerManager()
            manager.clear_queues(confirm=not args.force)
            
        elif args.command == 'seed':
            manager = CrawlerManager()
            manager.add_seed_urls(args.file)
            
        elif args.command == 'export':
            manager = CrawlerManager()
            manager.export_urls(args.file)
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
