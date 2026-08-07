#!/usr/bin/env python3
"""
Fix MongoDB collection synchronization issues.

This script fixes the mismatch between pages_metadata and pages_content
collections by removing orphaned documents.
"""

from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_collection_sync(mongodb_uri='mongodb://localhost:27017/',
                       database='web_crawler'):
    """
    Fix sync between metadata and content collections.
    
    Removes:
    1. Orphaned content docs (no corresponding metadata)
    2. Orphaned metadata docs (no corresponding content)
    """
    
    client = MongoClient(mongodb_uri)
    db = client[database]
    
    metadata = db['pages_metadata']
    content = db['pages_content']
    
    logger.info("=" * 60)
    logger.info("MongoDB Collection Sync Fix")
    logger.info("=" * 60)
    
    # Get counts before
    metadata_count_before = metadata.count_documents({})
    content_count_before = content.count_documents({})
    
    logger.info(f"\nBefore:")
    logger.info(f"  Metadata docs:  {metadata_count_before}")
    logger.info(f"  Content docs:   {content_count_before}")
    logger.info(f"  Difference:     {abs(metadata_count_before - content_count_before)}")
    
    # Get all page IDs from both collections
    logger.info("\n🔍 Analyzing collections...")
    
    metadata_ids = set(doc['_id'] for doc in metadata.find({}, {'_id': 1}))
    content_page_ids = set(doc['page_id'] for doc in content.find({}, {'page_id': 1}))
    
    logger.info(f"  Unique metadata IDs: {len(metadata_ids)}")
    logger.info(f"  Unique content IDs:  {len(content_page_ids)}")
    
    # Find orphaned documents
    orphaned_content = content_page_ids - metadata_ids  # Content without metadata
    orphaned_metadata = metadata_ids - content_page_ids  # Metadata without content
    
    logger.info(f"\n🔍 Found issues:")
    logger.info(f"  Orphaned content docs:  {len(orphaned_content)} (no metadata)")
    logger.info(f"  Orphaned metadata docs: {len(orphaned_metadata)} (no content)")
    
    # Fix orphaned content (content without metadata)
    if orphaned_content:
        logger.info(f"\n🗑️  Removing {len(orphaned_content)} orphaned content docs...")
        result = content.delete_many({'page_id': {'$in': list(orphaned_content)}})
        logger.info(f"  ✅ Deleted {result.deleted_count} orphaned content docs")
    
    # Fix orphaned metadata (metadata without content)
    if orphaned_metadata:
        logger.info(f"\n🗑️  Removing {len(orphaned_metadata)} orphaned metadata docs...")
        result = metadata.delete_many({'_id': {'$in': list(orphaned_metadata)}})
        logger.info(f"  ✅ Deleted {result.deleted_count} orphaned metadata docs")
    
    # Get counts after
    metadata_count_after = metadata.count_documents({})
    content_count_after = content.count_documents({})
    
    logger.info(f"\nAfter:")
    logger.info(f"  Metadata docs:  {metadata_count_after}")
    logger.info(f"  Content docs:   {content_count_after}")
    logger.info(f"  Difference:     {abs(metadata_count_after - content_count_after)}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    if metadata_count_after == content_count_after:
        logger.info("✅ Collections are now synchronized!")
    else:
        logger.warning("⚠️  Collections still have mismatch (may need manual inspection)")
    logger.info("=" * 60)
    
    # Show storage stats
    metadata_size = db.command("collStats", "pages_metadata")['size']
    content_size = db.command("collStats", "pages_content")['size']
    
    logger.info(f"\n📊 Storage Stats:")
    logger.info(f"  Metadata size:  {metadata_size / 1024:.2f} KB")
    logger.info(f"  Content size:   {content_size / 1024:.2f} KB")
    logger.info(f"  Total size:     {(metadata_size + content_size) / 1024:.2f} KB")
    
    client.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix MongoDB collection sync')
    parser.add_argument('--mongodb', default='mongodb://localhost:27017/',
                       help='MongoDB URI')
    parser.add_argument('--database', default='web_crawler',
                       help='Database name')
    
    args = parser.parse_args()
    
    fix_collection_sync(args.mongodb, args.database)
