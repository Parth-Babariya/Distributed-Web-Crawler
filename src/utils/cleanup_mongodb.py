#!/usr/bin/env python3
"""
MongoDB WiredTiger cleanup script.

This script safely cleans up MongoDB .wt files and journal data.
Use this when you want to reset the crawler database or clean up old data.

WARNING: This will DELETE all crawled data!
"""

import os
import shutil
import logging
from pathlib import Path
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MongoDBCleaner:
    """Clean up MongoDB WiredTiger files safely."""
    
    # MongoDB WiredTiger file patterns
    WT_PATTERNS = [
        '*.wt',           # WiredTiger data files
        '*.turtle',       # WiredTiger metadata
        '*.bson',         # BSON files
        'WiredTiger*',    # WiredTiger system files
        '_mdb_catalog.wt' # MongoDB catalog
    ]
    
    JOURNAL_DIR = 'journal'
    
    def __init__(self, workspace_path: str = None):
        """
        Initialize cleaner.
        
        Args:
            workspace_path: Path to workspace with MongoDB files (default: current directory)
        """
        self.workspace_path = Path(workspace_path or os.getcwd())
        logger.info(f"MongoDB Cleaner initialized for: {self.workspace_path}")
    
    def find_wt_files(self) -> list:
        """
        Find all WiredTiger files in workspace.
        
        Returns:
            List of Path objects for .wt and related files
        """
        wt_files = []
        
        for pattern in self.WT_PATTERNS:
            files = list(self.workspace_path.glob(pattern))
            wt_files.extend(files)
        
        logger.info(f"Found {len(wt_files)} WiredTiger files")
        return wt_files
    
    def find_journal_dir(self) -> Path:
        """
        Find MongoDB journal directory.
        
        Returns:
            Path to journal directory or None
        """
        journal_path = self.workspace_path / self.JOURNAL_DIR
        if journal_path.exists() and journal_path.is_dir():
            logger.info(f"Found journal directory: {journal_path}")
            return journal_path
        return None
    
    def list_files(self, show_sizes: bool = True):
        """
        List all MongoDB files that would be deleted.
        
        Args:
            show_sizes: Show file sizes
        """
        print("\n" + "="*70)
        print("MongoDB Files Found:")
        print("="*70)
        
        wt_files = self.find_wt_files()
        journal_dir = self.find_journal_dir()
        
        total_size = 0
        
        # List WiredTiger files
        print("\nWiredTiger Files:")
        for file_path in sorted(wt_files):
            if file_path.is_file():
                size = file_path.stat().st_size
                total_size += size
                if show_sizes:
                    size_mb = size / (1024 * 1024)
                    print(f"  - {file_path.name} ({size_mb:.2f} MB)")
                else:
                    print(f"  - {file_path.name}")
        
        # List journal files
        if journal_dir:
            print("\nJournal Files:")
            for file_path in sorted(journal_dir.iterdir()):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    total_size += size
                    if show_sizes:
                        size_mb = size / (1024 * 1024)
                        print(f"  - journal/{file_path.name} ({size_mb:.2f} MB)")
                    else:
                        print(f"  - journal/{file_path.name}")
        
        print("\n" + "="*70)
        print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
        print("="*70 + "\n")
    
    def clean(self, dry_run: bool = False, force: bool = False) -> dict:
        """
        Clean up MongoDB WiredTiger files.
        
        Args:
            dry_run: If True, only show what would be deleted
            force: If True, skip confirmation prompt
            
        Returns:
            Dictionary with cleanup statistics
        """
        wt_files = self.find_wt_files()
        journal_dir = self.find_journal_dir()
        
        if not wt_files and not journal_dir:
            logger.info("No MongoDB files found to clean")
            return {'files_deleted': 0, 'dirs_deleted': 0, 'size_freed': 0}
        
        # Show what will be deleted
        self.list_files(show_sizes=True)
        
        if dry_run:
            logger.info("DRY RUN - No files were deleted")
            return {'files_deleted': 0, 'dirs_deleted': 0, 'size_freed': 0, 'dry_run': True}
        
        # Confirmation prompt
        if not force:
            print("⚠️  WARNING: This will DELETE all MongoDB data!")
            print("   Make sure MongoDB is stopped before proceeding.")
            response = input("\nAre you sure you want to delete these files? (yes/no): ")
            
            if response.lower() not in ['yes', 'y']:
                logger.info("Cleanup cancelled by user")
                return {'cancelled': True}
        
        # Delete files
        files_deleted = 0
        dirs_deleted = 0
        size_freed = 0
        
        logger.info("Starting cleanup...")
        
        # Delete WiredTiger files
        for file_path in wt_files:
            try:
                if file_path.is_file():
                    size = file_path.stat().st_size
                    file_path.unlink()
                    files_deleted += 1
                    size_freed += size
                    logger.debug(f"Deleted: {file_path.name}")
            except Exception as e:
                logger.error(f"Error deleting {file_path}: {e}")
        
        # Delete journal directory
        if journal_dir and journal_dir.exists():
            try:
                shutil.rmtree(journal_dir)
                dirs_deleted += 1
                logger.info(f"Deleted journal directory: {journal_dir}")
            except Exception as e:
                logger.error(f"Error deleting journal directory: {e}")
        
        stats = {
            'files_deleted': files_deleted,
            'dirs_deleted': dirs_deleted,
            'size_freed_mb': size_freed / (1024 * 1024)
        }
        
        logger.info(f"Cleanup complete!")
        logger.info(f"  Files deleted: {files_deleted}")
        logger.info(f"  Directories deleted: {dirs_deleted}")
        logger.info(f"  Space freed: {stats['size_freed_mb']:.2f} MB")
        
        return stats
    
    def check_mongodb_running(self) -> bool:
        """
        Check if MongoDB is currently running.
        
        Returns:
            True if MongoDB appears to be running
        """
        try:
            import pymongo
            client = pymongo.MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
            client.server_info()
            return True
        except:
            return False
    
    def safe_clean(self, dry_run: bool = False) -> dict:
        """
        Safely clean MongoDB files with automatic checks.
        
        Args:
            dry_run: If True, only show what would be deleted
            
        Returns:
            Dictionary with cleanup statistics
        """
        # Check if MongoDB is running
        if self.check_mongodb_running():
            logger.error("❌ MongoDB is currently RUNNING!")
            logger.error("   Please stop MongoDB before cleaning files:")
            logger.error("   sudo systemctl stop mongod")
            logger.error("   OR")
            logger.error("   killall mongod")
            return {'error': 'MongoDB is running'}
        
        logger.info("✅ MongoDB is not running - safe to clean")
        return self.clean(dry_run=dry_run, force=False)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Clean up MongoDB WiredTiger files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List files without deleting
  python cleanup_mongodb.py --list
  
  # Dry run (show what would be deleted)
  python cleanup_mongodb.py --dry-run
  
  # Clean files (with confirmation)
  python cleanup_mongodb.py --clean
  
  # Force clean without confirmation (DANGEROUS!)
  python cleanup_mongodb.py --clean --force
  
  # Clean specific directory
  python cleanup_mongodb.py --clean --path /path/to/mongodb/data
        """
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List MongoDB files without deleting'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Clean up MongoDB files'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force cleanup without confirmation (use with caution!)'
    )
    
    parser.add_argument(
        '--path',
        type=str,
        default=None,
        help='Path to MongoDB data directory (default: current directory)'
    )
    
    args = parser.parse_args()
    
    # Create cleaner
    cleaner = MongoDBCleaner(workspace_path=args.path)
    
    # Execute command
    if args.list:
        cleaner.list_files(show_sizes=True)
    
    elif args.dry_run:
        cleaner.safe_clean(dry_run=True)
    
    elif args.clean:
        result = cleaner.safe_clean(dry_run=False)
        if result.get('error'):
            sys.exit(1)
    
    else:
        # No arguments - show help and list files
        parser.print_help()
        print("\n")
        cleaner.list_files(show_sizes=True)
        print("Use --clean to delete these files")


if __name__ == "__main__":
    main()
