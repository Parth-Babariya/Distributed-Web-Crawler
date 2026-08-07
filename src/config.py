"""
Configuration file for Distributed Web Crawler

Centralized configuration management for all crawler components.
"""

import os


class CrawlerConfig:
    """Configuration class for distributed crawler."""
    
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    
    # MongoDB Configuration
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_DB = os.getenv('MONGO_DB', 'web_crawler')
    MONGO_USER = os.getenv('MONGO_USER', None)
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', None)
    MONGO_URI = os.getenv('MONGO_URI', None)
    MONGO_REPLICA_SET = os.getenv('MONGO_REPLICA_SET', None)
    MONGO_DIRECT_CONNECTION = os.getenv('MONGO_DIRECT_CONNECTION', None)
    
    # Crawler Configuration
    USER_AGENT = os.getenv(
        'USER_AGENT',
        'DistributedCrawler/1.0 (+https://github.com/yourproject/crawler)'
    )
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10))
    CRAWL_DELAY = float(os.getenv('CRAWL_DELAY', 0.5))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    
    # Queue Configuration
    PROCESSING_TIMEOUT = int(os.getenv('PROCESSING_TIMEOUT', 300))
    RECOVERY_INTERVAL = int(os.getenv('RECOVERY_INTERVAL', 10))
    
    # Worker Configuration
    MAX_PAGES_PER_WORKER = int(os.getenv('MAX_PAGES_PER_WORKER', 0))  # 0 = unlimited
    WORKER_IDLE_SLEEP = int(os.getenv('WORKER_IDLE_SLEEP', 5))
    
    # URL Filtering
    ALLOWED_SCHEMES = ['http', 'https']
    EXCLUDED_EXTENSIONS = [
        '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip',
        '.mp4', '.avi', '.mov', '.doc', '.docx', '.xls',
        '.xlsx', '.ppt', '.pptx', '.mp3', '.wav', '.tar',
        '.gz', '.rar', '.7z', '.exe', '.dmg', '.iso'
    ]
    
    # Content Type Filtering
    SUPPORTED_CONTENT_TYPES = [
        'text/html',
        'application/xhtml+xml'
    ]
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Seed URLs (can be overridden)
    SEED_URLS = [
        'https://example.com',
    ]
    
    @classmethod
    def get_redis_url(cls):
        """Generate Redis connection URL."""
        if cls.REDIS_PASSWORD:
            return f"redis://:{cls.REDIS_PASSWORD}@{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
    
    @classmethod
    def get_mongo_url(cls):
        """Generate MongoDB connection URL."""
        if cls.MONGO_URI:
            return cls.MONGO_URI

        if cls.MONGO_USER and cls.MONGO_PASSWORD:
            base_url = f"mongodb://{cls.MONGO_USER}:{cls.MONGO_PASSWORD}@{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}"
        else:
            base_url = f"mongodb://{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}"

        options = []
        if cls.MONGO_REPLICA_SET:
            options.append(f"replicaSet={cls.MONGO_REPLICA_SET}")
        elif cls.MONGO_DIRECT_CONNECTION is not None:
            options.append(f"directConnection={cls.MONGO_DIRECT_CONNECTION.lower()}")
        elif cls.MONGO_HOST in {'localhost', '127.0.0.1'}:
            options.append("directConnection=true")

        if options:
            return f"{base_url}?{'&'.join(options)}"
        return base_url
    
    @classmethod
    def print_config(cls):
        """Print current configuration (masks sensitive data)."""
        print("\n" + "="*60)
        print("Distributed Crawler Configuration")
        print("="*60)
        print(f"Redis: {cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}")
        replica_set = f" replicaSet={cls.MONGO_REPLICA_SET}" if cls.MONGO_REPLICA_SET else ""
        print(f"MongoDB: {cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}{replica_set}")
        print(f"Request Timeout: {cls.REQUEST_TIMEOUT}s")
        print(f"Crawl Delay: {cls.CRAWL_DELAY}s")
        print(f"Processing Timeout: {cls.PROCESSING_TIMEOUT}s")
        print(f"Max Pages Per Worker: {cls.MAX_PAGES_PER_WORKER or 'Unlimited'}")
        print("="*60 + "\n")
