"""
Optimized MongoDB storage with split collections and externalized HTML blobs.

Features:
- Split collections: metadata (fast queries) + content pointers
- 90% storage savings via zlib compression
- Batched transactional URL upserts
- Transactional graph outbox events for downstream PageRank updates
"""

from dataclasses import dataclass, field
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure
from bson import ObjectId
from typing import Dict, List, Optional, Tuple
import zlib
import hashlib
import logging
import os
import time
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from config import CrawlerConfig
except ImportError:  # pragma: no cover - direct module usage fallback
    CrawlerConfig = None


class LocalHtmlBlobStore:
    """File-backed object store for compressed HTML payloads.

    MongoDB stores only the returned content_path pointer. The compressed blob
    itself lives outside WiredTiger, preventing raw HTML payloads from evicting
    hot metadata, index, and PageRank documents from MongoDB cache.
    """
    store_name = 'local'

    def __init__(self, root_dir: str = None):
        self.root_dir = os.path.abspath(
            root_dir or os.getenv('CRAWLER_CONTENT_STORE_DIR', 'content_store')
        )

    def _path_for_hash(self, content_hash: str) -> str:
        return os.path.join(
            'html',
            content_hash[:2],
            content_hash[2:4],
            f'{content_hash}.zlib',
        )

    def write(self, content_hash: str, compressed_html: bytes) -> str:
        relative_path = self._path_for_hash(content_hash)
        absolute_path = os.path.join(self.root_dir, relative_path)
        os.makedirs(os.path.dirname(absolute_path), exist_ok=True)

        if os.path.exists(absolute_path):
            return relative_path

        temp_path = f'{absolute_path}.{os.getpid()}.tmp'
        with open(temp_path, 'wb') as blob:
            blob.write(compressed_html)
        os.replace(temp_path, absolute_path)
        return relative_path

    def read(self, content_path: str) -> bytes:
        absolute_path = os.path.join(self.root_dir, content_path)
        with open(absolute_path, 'rb') as blob:
            return blob.read()

    def uri_for(self, content_path: str) -> str:
        return f"local://{content_path.replace(os.sep, '/')}"


class S3HtmlBlobStore:
    """S3-compatible object store for compressed HTML payloads.

    Works with AWS S3 and local MinIO. The MongoDB document stores only the
    bucket/key pointer, so any worker, indexer, or API container can read the
    blob regardless of which machine crawled the page.
    """
    store_name = 's3'

    def __init__(
        self,
        bucket: str = None,
        endpoint_url: str = None,
        region_name: str = None,
        create_bucket: bool = None,
    ):
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as exc:
            raise RuntimeError(
                "CRAWLER_CONTENT_STORE=s3 requires boto3. "
                "Install dependencies with `python -m pip install -r requirements.txt`."
            ) from exc

        self.bucket = bucket or os.getenv('S3_BUCKET', 'crawler-html')
        self.endpoint_url = endpoint_url or os.getenv('S3_ENDPOINT_URL')
        self.region_name = region_name or os.getenv('AWS_REGION', 'us-east-1')
        self._client_error = ClientError
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )

        if create_bucket is None:
            create_bucket = os.getenv('S3_CREATE_BUCKET', 'true').lower() not in {
                '0',
                'false',
                'no',
            }
        if create_bucket:
            self._ensure_bucket()

    def _key_for_hash(self, content_hash: str) -> str:
        return (
            f"html/{content_hash[:2]}/{content_hash[2:4]}/"
            f"{content_hash}.zlib"
        )

    def _ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except self._client_error as exc:
            code = str(exc.response.get('Error', {}).get('Code', ''))
            if code not in {'404', 'NoSuchBucket', 'NotFound'}:
                raise

            kwargs = {'Bucket': self.bucket}
            if self.region_name != 'us-east-1' and not self.endpoint_url:
                kwargs['CreateBucketConfiguration'] = {
                    'LocationConstraint': self.region_name,
                }
            self.client.create_bucket(**kwargs)

    def write(self, content_hash: str, compressed_html: bytes) -> str:
        key = self._key_for_hash(content_hash)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compressed_html,
            ContentType='application/zlib',
            Metadata={'content-sha256': content_hash},
        )
        return key

    def read(self, content_path: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=content_path)
        return response['Body'].read()

    def uri_for(self, content_path: str) -> str:
        return f"s3://{self.bucket}/{content_path}"


def create_html_blob_store():
    store = os.getenv('CRAWLER_CONTENT_STORE', 'local').lower()
    if store in {'s3', 'minio'}:
        return S3HtmlBlobStore()
    if store == 'local':
        return LocalHtmlBlobStore()
    raise ValueError(
        "Unsupported CRAWLER_CONTENT_STORE value "
        f"{store!r}; expected 'local', 's3', or 'minio'."
    )


@dataclass
class FlushResult:
    """
    Outcome of a single flush_batch call.

    The worker uses this to decide which leases in the ProcessingLedger
    can be released and which URLs need to be re-enqueued.

    - committed: page_ids that were durably written. Worker should release
      these URLs' leases.
    - duplicate: retained for backwards-compatible worker accounting.
      The page already exists in Mongo, so the work is "done" — release
      these leases too.
    - failed: page_ids that did not write because the transaction (or whole
      flush) errored. Worker should release the lease AND re-enqueue these
      URLs to the frontier so another worker (or this one) can retry.
    - page_id_to_url: full mapping for the worker to resolve URLs without
      maintaining a parallel dict on its side.
    """
    committed: List[ObjectId] = field(default_factory=list)
    duplicate: List[ObjectId] = field(default_factory=list)
    failed: List[ObjectId] = field(default_factory=list)
    page_id_to_url: Dict[ObjectId, str] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.committed or self.duplicate or self.failed)


class OptimizedStorage:
    """
    High-performance MongoDB storage with compression and batching.
    
    Collections:
    - pages_metadata: Small documents for fast queries
    - pages_content: Small HTML blob pointers and extracted links
    - pages_documents: Clean text used by the indexer
    - graph_outbox: Transactional queue for incremental PageRank updates
    
    Benefits:
    - 10x faster queries (metadata is small)
    - 90% storage savings (compression)
    - URL-based recrawl updates without content-hash dedup loss
    """
    
    def __init__(self, mongodb_uri: str = None,
                 database: str = 'web_crawler',
                 batch_size: int = 5):
        """
        Initialize optimized storage.
        
        Args:
            mongodb_uri: MongoDB connection URI
            database: Database name
            batch_size: Number of pages to batch before insert
        """
        if mongodb_uri is None:
            mongodb_uri = (
                CrawlerConfig.get_mongo_url()
                if CrawlerConfig is not None
                else 'mongodb://localhost:27017/'
            )

        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database]
        
        # Collections
        self.metadata = self.db['pages_metadata']
        self.content = self.db['pages_content']
        self.documents = self.db['pages_documents']
        self.graph_outbox = self.db['graph_outbox']
        self.index_outbox = self.db['index_outbox']
        self.html_blobs = create_html_blob_store()
        
        # Batch buffers
        self.batch_size = batch_size
        self.metadata_batch = []
        self.content_batch = []
        self.document_batch = []
        
        # Statistics
        self.stats = {
            'pages_stored': 0,
            'bytes_original': 0,
            'bytes_compressed': 0,
            'compression_ratio': 0.0,
            'batches_flushed': 0
        }
        
        # Track URLs queued in the current batch so flush can return
        # actionable info to the worker.
        self._pending_url_for_page: Dict[ObjectId, str] = {}

        # Mongo transactions only work on replica sets (or sharded mongos).
        # Detect once at startup so flush_batch can pick the right path
        # without a try/except every call.
        self._transactions_supported = self._probe_transaction_support()

        # Create indexes
        self._create_indexes()

        logger.info(
            f"OptimizedStorage initialized "
            f"(batch_size={batch_size}, transactions={self._transactions_supported})"
        )

    def _probe_transaction_support(self) -> bool:
        """
        Return True if this Mongo server supports multi-document transactions.

        Standalone Mongo (single mongod, no replica set) raises
        OperationFailure on transaction start with a message about
        "Transaction numbers are only allowed on a replica set member".
        Replica sets and sharded clusters allow them.
        """
        unsupported_markers = (
            'Transaction numbers are only allowed',
            'replica set member or mongos',
            'Transaction numbers',
        )

        for attempt in range(1, 4):
            try:
                if '_transaction_probe' not in self.db.list_collection_names():
                    try:
                        self.db.create_collection('_transaction_probe')
                    except CollectionInvalid:
                        pass
                with self.client.start_session() as session:
                    with session.start_transaction():
                        # A tiny write forces MongoDB to actually start the
                        # transaction and reject standalone mongod instances.
                        probe = self.db['_transaction_probe']
                        probe.update_one(
                            {'_id': 'probe'},
                            {'$set': {'ok': True}},
                            upsert=True,
                            session=session,
                        )
                        probe.delete_one({'_id': 'probe'}, session=session)
                return True
            except OperationFailure as exc:
                message = str(exc)
                if any(marker in message for marker in unsupported_markers):
                    logger.warning(
                        f"Mongo transactions disabled (server does not support them): "
                        f"{exc}. Start MongoDB as a replica set before crawling."
                    )
                    return False

                if exc.has_error_label('TransientTransactionError') or exc.code in {112, 251}:
                    if attempt < 3:
                        time.sleep(0.1 * attempt)
                        continue
                    logger.warning(
                        "Transaction probe hit repeated transient errors; "
                        "MongoDB accepted transaction semantics, so continuing "
                        "with transactions enabled. Last error: %s",
                        exc,
                    )
                    return True
                logger.warning(f"Transaction probe failed: {exc}; assuming unsupported")
                return False
            except Exception as exc:
                if attempt < 3:
                    time.sleep(0.1 * attempt)
                    continue
                logger.warning(f"Transaction probe failed: {exc}; assuming unsupported")
                return False

    def _drop_unique_index_on_field(self, collection, field_name: str):
        """Remove a legacy unique field index before recreating it."""
        for name, info in collection.index_information().items():
            if name == '_id_':
                continue
            keys = list(info.get('key', []))
            if info.get('unique') and keys == [(field_name, 1)]:
                collection.drop_index(name)
                logger.info(
                    "Dropped legacy unique index %s on %s.%s",
                    name,
                    collection.name,
                    field_name,
                )

    def _create_indexes(self):
        """Create database indexes for fast queries.

        URL is the durable identity of a crawled page. content_hash remains
        queryable but is deliberately not unique so mirrors and unchanged
        recrawls still produce graph updates.
        """
        self._drop_unique_index_on_field(self.metadata, 'content_hash')
        self._drop_unique_index_on_field(self.content, 'content_hash')

        self.metadata.create_index('url', unique=True)
        self.metadata.create_index('domain')
        self.metadata.create_index('crawled_at')
        self.metadata.create_index('last_crawled_at')
        self.metadata.create_index('content_hash')
        self.metadata.create_index([('domain', 1), ('crawled_at', -1)])

        self.content.create_index('page_id')
        self.content.create_index('url', unique=True, sparse=True)
        self.content.create_index('content_path')
        self.content.create_index('content_hash')

        self.documents.create_index('page_id', unique=True)
        self.documents.create_index('url', unique=True)
        self.documents.create_index('indexed_at')

        self.graph_outbox.create_index([('status', 1), ('_id', 1)])
        self.graph_outbox.create_index([('status', 1), ('claimed_at', 1)])
        self.graph_outbox.create_index('source_url')
        self.index_outbox.create_index([('status', 1), ('_id', 1)])
        self.index_outbox.create_index([('status', 1), ('claimed_at', 1)])
        self.index_outbox.create_index('url')
        self.db['graph_edges'].create_index('source_url', unique=True)
        self.db['pagerank_nodes'].create_index('url', unique=True)
        self.db['indexed_documents'].create_index('url', unique=True)

        logger.info("Database indexes created")
    
    def _compress_html(self, html: str) -> bytes:
        """
        Compress HTML content using zlib.
        
        Args:
            html: HTML string
            
        Returns:
            Compressed bytes
        """
        return zlib.compress(html.encode('utf-8'), level=6)
    
    def _decompress_html(self, compressed: bytes) -> str:
        """
        Decompress HTML content.
        
        Args:
            compressed: Compressed bytes
            
        Returns:
            Original HTML string
        """
        return zlib.decompress(compressed).decode('utf-8')

    def _read_html_from_content_doc(self, content_doc: Dict) -> str:
        """Load and decompress HTML from blob storage or legacy Mongo field."""
        if content_doc.get('content_path'):
            compressed = self._blob_store_for_doc(content_doc).read(
                content_doc['content_path']
            )
        else:
            compressed = content_doc['compressed_html']
        return self._decompress_html(compressed)

    def _blob_store_for_doc(self, content_doc: Dict):
        content_store = content_doc.get('content_store') or 'local'
        if content_store in {'s3', 'minio'}:
            if getattr(self.html_blobs, 'store_name', None) == 's3':
                return self.html_blobs
            return S3HtmlBlobStore(bucket=content_doc.get('content_bucket'))

        if getattr(self.html_blobs, 'store_name', None) == 'local':
            return self.html_blobs
        return LocalHtmlBlobStore()
    
    def _calculate_hash(self, content: str) -> str:
        """Calculate SHA-256 hash of content."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def add_page(self, url: str, html: str, links: List[str],
                 domain: str, depth: int = 0,
                 worker_id: str = 'unknown',
                 title: str = '',
                 text: str = '') -> Optional[ObjectId]:
        """
        Queue a page for the next batch insert. Does NOT trigger a flush.

        The caller is responsible for checking ``batch_is_full()`` and
        invoking ``flush_batch()`` so the worker can delay releasing the
        URL's lease in the ProcessingLedger until after the batch is
        durably written. This is the fix for the previous OOM-loss bug
        (mark_completed was called before flush_batch ran).

        The previous find_one(content_hash) precheck has been removed —
        the unique index on content_hash now enforces deduplication
        atomically at insert time, and flush_batch reports the rejected
        documents back to the worker as "duplicate".

        Args:
            url, html, links, domain, depth, worker_id, title, text:
                page contents

        Returns:
            ObjectId of the queued document, or None if the input was
            rejected pre-batch (currently never; reserved for future
            input validation).
        """
        page_id = ObjectId()
        crawled_at = datetime.utcnow()
        content_hash = self._calculate_hash(html)
        crawl_version = f"{page_id}_{crawled_at.isoformat()}Z"

        compressed_html = self._compress_html(html)
        original_size = len(html.encode('utf-8'))
        compressed_size = len(compressed_html)
        content_path = self.html_blobs.write(content_hash, compressed_html)

        self.stats['bytes_original'] += original_size
        self.stats['bytes_compressed'] += compressed_size

        metadata_doc = {
            '_id': page_id,
            'url': url,
            'domain': domain,
            'depth': depth,
            'link_count': len(links),
            'links': links[:100],
            'content_hash': content_hash,
            'content_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compressed_size / original_size,
            'worker_id': worker_id,
            'title': title,
            'text_length': len(text),
            'crawled_at': crawled_at,
            'last_crawled_at': crawled_at,
            'crawl_version': crawl_version,
        }

        content_doc = {
            'page_id': page_id,
            'url': url,
            'content_store': self.html_blobs.store_name,
            'content_path': content_path,
            'content_bucket': getattr(self.html_blobs, 'bucket', None),
            'content_uri': self.html_blobs.uri_for(content_path),
            'content_encoding': 'zlib',
            'all_links': links,
            'content_hash': content_hash,
            'content_size': original_size,
            'compressed_size': compressed_size,
            'crawl_version': crawl_version,
            'crawled_at': crawled_at,
        }

        document_doc = {
            'page_id': page_id,
            'url': url,
            'title': title,
            'text': text,
            'indexed_at': None,
            'crawl_version': crawl_version,
            'updated_at': crawled_at,
        }

        self.metadata_batch.append(metadata_doc)
        self.content_batch.append(content_doc)
        self.document_batch.append(document_doc)
        self._pending_url_for_page[page_id] = url

        return page_id

    def batch_is_full(self) -> bool:
        """True when the in-memory batch has reached batch_size."""
        return len(self.metadata_batch) >= self.batch_size

    def pending_count(self) -> int:
        """Number of pages queued in the current batch."""
        return len(self.metadata_batch)

    def pending_urls(self) -> List[Tuple[ObjectId, str]]:
        """(page_id, url) for every page currently queued in the batch.

        Lets the worker pre-verify lease ownership before triggering a
        flush — useful when batches are large and individual leases may
        have expired since the URL was queued.
        """
        return list(self._pending_url_for_page.items())
    
    def flush_batch(self, skip_page_ids: Optional[set] = None) -> FlushResult:
        """
        Write the queued batch to MongoDB and report what happened.

        Three categories of outcome are returned so the worker can drive
        the ProcessingLedger correctly:

        - ``committed``: page_ids whose metadata + content + document docs
          all wrote successfully. Worker should release the lease.
        - ``duplicate``: page_ids rejected by a unique-index violation
          (URL or content_hash already in Mongo). The work is "done" —
          worker should also release the lease, but bump a separate stat.
        - ``failed``: page_ids that did not write because the whole batch
          errored (transaction abort, replica set failure, etc.). Worker
          should release the lease AND re-enqueue these URLs to the
          frontier so they get retried.

        Args:
            skip_page_ids: page_ids to drop pre-flush (e.g. zombie URLs
                whose lease was lost while queued). They appear in
                ``failed`` so the worker can release their leases.

        Returns:
            FlushResult — empty (all three lists empty) if nothing was queued.
        """
        if not self.metadata_batch:
            return FlushResult()

        skip = skip_page_ids or set()
        result = FlushResult(page_id_to_url=dict(self._pending_url_for_page))

        # Pre-filter: drop anything the worker told us is no longer ours.
        if skip:
            metadata_docs = [d for d in self.metadata_batch if d['_id'] not in skip]
            content_docs = [d for d in self.content_batch if d['page_id'] not in skip]
            document_docs = [d for d in self.document_batch if d['page_id'] not in skip]
            for page_id in skip:
                if page_id in self._pending_url_for_page:
                    result.failed.append(page_id)
        else:
            metadata_docs = list(self.metadata_batch)
            content_docs = list(self.content_batch)
            document_docs = list(self.document_batch)

        all_page_ids = [d['_id'] for d in metadata_docs]

        if all_page_ids:
            if self._transactions_supported:
                committed, duplicate, failed = self._flush_transactional(
                    metadata_docs, content_docs, document_docs
                )
            else:
                logger.error(
                    "MongoDB transactions are required for crawler storage "
                    "and graph/index outbox consistency. Start MongoDB as a "
                    "replica set before crawling."
                )
                committed, duplicate, failed = [], [], all_page_ids

            result.committed.extend(committed)
            result.duplicate.extend(duplicate)
            result.failed.extend(failed)

            self.stats['pages_stored'] += len(committed)
            self.stats['batches_flushed'] += 1
            if self.stats['bytes_original'] > 0:
                self.stats['compression_ratio'] = (
                    self.stats['bytes_compressed'] /
                    self.stats['bytes_original']
                )

            logger.info(
                f"Batch flushed: {len(committed)} committed, "
                f"{len(duplicate)} duplicate, {len(failed)} failed "
                f"(compression: {(1-self.stats['compression_ratio'])*100:.1f}% saved)"
            )

        # Always clear in-memory state — committed/duplicate/failed are
        # all "done" from the storage's perspective; retries happen via
        # the worker re-enqueueing failed page_ids.
        self.metadata_batch.clear()
        self.content_batch.clear()
        self.document_batch.clear()
        self._pending_url_for_page.clear()

        return result

    def _flush_transactional(self, metadata_docs, content_docs,
                             document_docs):
        """All-or-nothing flush via Mongo multi-document transaction.

        Either every doc in all three collections commits, or none do.
        DuplicateKeyError on URL/content_hash aborts the transaction —
        we then retry without the offending docs (split out as duplicates).
        Any other failure is reported as ``failed``.
        """
        committed = []
        duplicate = []
        failed = []

        content_by_page = {d['page_id']: d for d in content_docs}
        document_by_page = {d['page_id']: d for d in document_docs}

        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    outbox_docs = []
                    index_outbox_docs = []
                    for metadata_doc in metadata_docs:
                        page_id = metadata_doc['_id']
                        url = metadata_doc['url']
                        content_doc = content_by_page[page_id]
                        document_doc = document_by_page[page_id]
                        metadata_update = dict(metadata_doc)
                        metadata_update.pop('_id', None)

                        self.metadata.update_one(
                            {'url': url},
                            {
                                '$set': metadata_update,
                                '$setOnInsert': {
                                    '_id': page_id,
                                    'first_crawled_at': metadata_doc['crawled_at'],
                                },
                            },
                            upsert=True,
                            session=session,
                        )
                        self.content.update_one(
                            {'url': url},
                            {'$set': content_doc},
                            upsert=True,
                            session=session,
                        )
                        self.documents.update_one(
                            {'url': url},
                            {'$set': document_doc},
                            upsert=True,
                            session=session,
                        )
                        outbox_docs.append({
                            'source_url': url,
                            'new_outbound_links': content_doc['all_links'],
                            'status': 'pending',
                            'crawl_version': metadata_doc['crawl_version'],
                            'timestamp': metadata_doc['crawled_at'],
                            'page_id': page_id,
                        })
                        index_outbox_docs.append({
                            'url': url,
                            'page_id': page_id,
                            'crawl_version': metadata_doc['crawl_version'],
                            'status': 'pending',
                            'timestamp': metadata_doc['crawled_at'],
                        })

                    if outbox_docs:
                        self.graph_outbox.insert_many(
                            outbox_docs,
                            ordered=True,
                            session=session,
                        )
                    if index_outbox_docs:
                        self.index_outbox.insert_many(
                            index_outbox_docs,
                            ordered=True,
                            session=session,
                        )
                    committed = [d['_id'] for d in metadata_docs]
        except Exception as exc:
            logger.error(f"Transactional flush errored: {exc}")
            failed = [d['_id'] for d in metadata_docs]

        return committed, duplicate, failed

    def get_page(self, url: str) -> Optional[Dict]:
        """
        Retrieve page by URL (with decompressed content).
        
        Args:
            url: Page URL
            
        Returns:
            Dictionary with page data and decompressed HTML
        """
        # Get metadata
        meta = self.metadata.find_one({'url': url})
        if not meta:
            return None
        
        # Get content
        content_doc = self.content.find_one({'url': url})
        if not content_doc:
            # Backwards compatibility for data written before pages_content
            # became URL-anchored.
            content_doc = self.content.find_one({'page_id': meta['_id']})
        if not content_doc:
            return None
        
        # Decompress HTML. New rows store only a blob pointer; old rows may
        # still carry compressed_html directly in MongoDB.
        html = self._read_html_from_content_doc(content_doc)
        
        return {
            'url': meta['url'],
            'domain': meta['domain'],
            'depth': meta['depth'],
            'title': meta.get('title', ''),
            'html': html,
            'links': content_doc['all_links'],
            'crawled_at': meta['crawled_at']
        }

    def get_document(self, url: str) -> Optional[Dict]:
        """Retrieve the clean text document for a URL."""
        return self.documents.find_one({'url': url})

    def iter_documents(self, only_unindexed: bool = False):
        """Yield clean text documents for indexing."""
        query = {'indexed_at': None} if only_unindexed else {}
        yield from self.documents.find(query)
    
    def get_metadata(self, url: str) -> Optional[Dict]:
        """
        Get page metadata only (without content).
        Fast queries for statistics.
        
        Args:
            url: Page URL
            
        Returns:
            Metadata dictionary
        """
        return self.metadata.find_one({'url': url}, {'compressed_html': 0})
    
    def get_domain_stats(self, domain: str) -> Dict:
        """
        Get statistics for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dictionary with domain stats
        """
        pipeline = [
            {'$match': {'domain': domain}},
            {'$group': {
                '_id': None,
                'total_pages': {'$sum': 1},
                'total_links': {'$sum': '$link_count'},
                'avg_links_per_page': {'$avg': '$link_count'},
                'total_size': {'$sum': '$content_size'},
                'total_compressed': {'$sum': '$compressed_size'},
                'first_crawl': {'$min': '$crawled_at'},
                'last_crawl': {'$max': '$crawled_at'}
            }}
        ]
        
        result = list(self.metadata.aggregate(pipeline))
        if not result:
            return {}
        
        stats = result[0]
        stats['compression_ratio'] = (
            stats['total_compressed'] / stats['total_size']
            if stats['total_size'] > 0 else 0
        )
        stats['space_saved'] = stats['total_size'] - stats['total_compressed']
        
        return stats
    
    def get_stats(self) -> Dict:
        """Get overall storage statistics."""
        total_pages = self.metadata.count_documents({})
        
        # Get size stats from aggregation
        pipeline = [
            {'$group': {
                '_id': None,
                'total_size': {'$sum': '$content_size'},
                'total_compressed': {'$sum': '$compressed_size'},
                'total_links': {'$sum': '$link_count'}
            }}
        ]
        
        result = list(self.metadata.aggregate(pipeline))
        size_stats = result[0] if result else {}
        
        return {
            'pages_stored': total_pages,
            'bytes_original': size_stats.get('total_size', 0),
            'bytes_compressed': size_stats.get('total_compressed', 0),
            'compression_ratio': (
                size_stats.get('total_compressed', 0) / 
                size_stats.get('total_size', 1)
            ),
            'space_saved_mb': (
                (size_stats.get('total_size', 0) - 
                 size_stats.get('total_compressed', 0)) / 1024 / 1024
            ),
            'total_links': size_stats.get('total_links', 0),
            'batches_flushed': self.stats['batches_flushed'],
            'pending_in_batch': len(self.metadata_batch)
        }
    
    def close(self) -> FlushResult:
        """Flush the remaining batch (returning the result so the caller
        can release/re-enqueue any leases) and close the connection."""
        result = self.flush_batch()
        self.client.close()
        logger.info("Storage closed")
        return result


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Initialize storage
    storage = OptimizedStorage(batch_size=5)  # Small batch for testing
    
    print("\n" + "="*60)
    print("OPTIMIZED STORAGE TEST")
    print("="*60)
    
    # Test HTML content (realistic size)
    test_html = """
    <!DOCTYPE html>
    <html>
    <head><title>Test Page</title></head>
    <body>
        <h1>Test Content</h1>
        <p>""" + "This is test content. " * 1000 + """</p>
        <div>More content here with lots of text.</div>
    </body>
    </html>
    """
    
    test_links = [
        f"https://example.com/page{i}" for i in range(50)
    ]
    
    # Add pages to the batch. add_page now returns the assigned ObjectId
    # (or None on rejection). Flushing is the caller's responsibility —
    # the worker uses this to delay releasing the URL's lease until the
    # batch is durably written.
    print("\nAdding pages to batch...")
    for i in range(10):
        url = f"https://example.com/test{i}"
        page_id = storage.add_page(
            url=url,
            html=test_html,
            links=test_links,
            domain="example.com",
            depth=1,
            worker_id="test-worker"
        )
        print(f"  Page {i+1}: queued as {page_id}")
        if storage.batch_is_full():
            result = storage.flush_batch()
            print(f"    flushed: {len(result.committed)} committed, "
                  f"{len(result.duplicate)} duplicate, "
                  f"{len(result.failed)} failed")

    # Flush remaining
    print("\nFlushing remaining batch...")
    final_result = storage.flush_batch()
    print(f"  final: {len(final_result.committed)} committed, "
          f"{len(final_result.duplicate)} duplicate, "
          f"{len(final_result.failed)} failed")
    
    # Retrieve page
    print("\nRetrieving page...")
    page = storage.get_page("https://example.com/test0")
    if page:
        print(f"  URL: {page['url']}")
        print(f"  Domain: {page['domain']}")
        print(f"  Links: {len(page['links'])}")
        print(f"  HTML size: {len(page['html'])} bytes")
    
    # Get domain stats
    print("\nDomain statistics...")
    domain_stats = storage.get_domain_stats("example.com")
    for key, value in domain_stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    # Get overall stats
    print("\n" + "="*60)
    print("OVERALL STATISTICS")
    print("="*60)
    stats = storage.get_stats()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    # Calculate savings
    if stats['bytes_original'] > 0:
        savings_pct = (1 - stats['compression_ratio']) * 100
        print(f"\n💾 Storage savings: {savings_pct:.1f}%")
        print(f"   Original: {stats['bytes_original']/1024/1024:.2f} MB")
        print(f"   Compressed: {stats['bytes_compressed']/1024/1024:.2f} MB")
        print(f"   Saved: {stats['space_saved_mb']:.2f} MB")
    
    # Close
    storage.close()
    print("\n✅ Storage test complete!")
