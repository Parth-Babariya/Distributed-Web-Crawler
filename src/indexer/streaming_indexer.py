#!/usr/bin/env python3
"""Transactional streaming indexer for pages_documents.

Crawler storage writes index_outbox events in the same MongoDB transaction as
the page document. This worker claims those durable events, updates the
``term_postings`` collection incrementally, and marks the exact crawl_version
done. No Redis Pub/Sub gap, no lost messages when the indexer is offline.

The legacy embedded-array ``inverted_index`` collection has been removed
because $push/$addToSet on a per-term document hits MongoDB's 16MB BSON
limit on common English terms in any real-world crawl.
"""

import argparse
import logging
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, Optional

from pymongo import DeleteOne, MongoClient, ReturnDocument, UpdateOne

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import CrawlerConfig
from indexer.mapreduce_indexer import MapReduceIndexer

logger = logging.getLogger(__name__)


class StreamingIndexer:
    """Consumes index_outbox events and updates term_postings safely."""

    def __init__(
        self,
        mongodb_uri: str = None,
        database: str = None,
        worker_id: str = None,
        claim_timeout_seconds: int = 300,
    ):
        self.client = MongoClient(mongodb_uri or CrawlerConfig.get_mongo_url())
        self.db = self.client[database or CrawlerConfig.MONGO_DB]
        self.documents = self.db['pages_documents']
        # term_postings: one document per (term, url). Replaces the legacy
        # inverted_index whose embedded-array layout broke at 16MB on hot terms.
        self.postings = self.db['term_postings']
        self.outbox = self.db['index_outbox']
        self.state = self.db['indexed_documents']
        self.worker_id = worker_id or f"stream-indexer-{uuid.uuid4().hex[:8]}"
        self.claim_timeout = timedelta(seconds=claim_timeout_seconds)
        self.tokenizer = MapReduceIndexer.__new__(MapReduceIndexer)
        self._create_indexes()

    def _create_indexes(self):
        self.documents.create_index('url', unique=True)
        self.documents.create_index('indexed_at')
        self.documents.create_index('crawl_version')
        self.postings.create_index([('term', 1), ('url', 1)], unique=True)
        self.postings.create_index([('term', 1), ('term_frequency', -1)])
        self.postings.create_index('url')
        self.outbox.create_index([('status', 1), ('_id', 1)])
        self.outbox.create_index([('status', 1), ('claimed_at', 1)])
        self.outbox.create_index('url')
        self.state.create_index('url', unique=True)

    def reclaim_stale_events(self) -> int:
        cutoff = datetime.utcnow() - self.claim_timeout
        result = self.outbox.update_many(
            {
                'status': 'processing',
                'claimed_at': {'$lt': cutoff},
            },
            {
                '$set': {'status': 'pending'},
                '$unset': {'claimed_at': '', 'worker_id': ''},
            },
        )
        if result.modified_count:
            logger.warning("Reclaimed %s stale index events", result.modified_count)
        return result.modified_count

    def claim_event(self) -> Optional[Dict]:
        return self.outbox.find_one_and_update(
            {'status': 'pending'},
            {
                '$set': {
                    'status': 'processing',
                    'claimed_at': datetime.utcnow(),
                    'worker_id': self.worker_id,
                }
            },
            sort=[('_id', 1)],
            return_document=ReturnDocument.AFTER,
        )

    def _mark_event_pending(self, event_id):
        self.outbox.update_one(
            {'_id': event_id, 'status': 'processing', 'worker_id': self.worker_id},
            {
                '$set': {'status': 'pending'},
                '$unset': {'claimed_at': '', 'worker_id': ''},
            },
        )

    def _term_counts(self, document: Dict) -> Dict[str, int]:
        return dict(Counter(self.tokenizer.tokenize(document.get('text', ''))))

    def _legacy_indexed_terms(self, url: str, session=None) -> Dict[str, int]:
        """Recover prior postings for URL when indexed_documents lacks state.

        After a legacy batch MapReduce build, the term_postings collection
        is populated but the per-URL state in indexed_documents may not be.
        Reading the existing postings for the URL lets us correctly compute
        which terms to remove on recrawl.
        """
        terms = {}
        postings_cursor = self.postings.find(
            {'url': url},
            {'term': 1, 'term_frequency': 1},
            session=session,
        )
        for item in postings_cursor:
            terms[item['term']] = int(item.get('term_frequency', 0))
        return terms

    def _old_term_counts(self, url: str, session=None) -> Dict[str, int]:
        state = self.state.find_one({'url': url}, {'term_counts': 1}, session=session)
        if state is not None:
            return {
                term: int(count)
                for term, count in (state.get('term_counts') or {}).items()
            }
        return self._legacy_indexed_terms(url, session=session)

    def _mark_done(self, event_id, fields: Dict, session=None):
        self.outbox.update_one(
            {'_id': event_id},
            {
                '$set': {
                    'status': 'done',
                    'done_at': datetime.utcnow(),
                    **fields,
                },
                '$unset': {'claimed_at': '', 'worker_id': ''},
            },
            session=session,
        )

    def process_event(self, event: Dict) -> bool:
        event_id = event['_id']
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    current = self.outbox.find_one(
                        {
                            '_id': event_id,
                            'status': 'processing',
                            'worker_id': self.worker_id,
                        },
                        session=session,
                    )
                    if current is None:
                        return False

                    url = current['url']
                    crawl_version = str(current.get('crawl_version', ''))
                    document = self.documents.find_one({'url': url}, session=session)
                    if document is None:
                        self._mark_done(
                            event_id,
                            {'skipped_reason': 'missing_document'},
                            session=session,
                        )
                        return True

                    current_version = str(document.get('crawl_version', ''))
                    if crawl_version and current_version != crawl_version:
                        self._mark_done(
                            event_id,
                            {
                                'skipped_reason': 'stale_document_version',
                                'current_crawl_version': current_version,
                            },
                            session=session,
                        )
                        return True

                    state = self.state.find_one(
                        {'url': url},
                        {'last_indexed_version': 1},
                        session=session,
                    )
                    if state and state.get('last_indexed_version') == crawl_version:
                        self._mark_done(
                            event_id,
                            {'skipped_reason': 'already_indexed'},
                            session=session,
                        )
                        return True

                    old_counts = self._old_term_counts(url, session=session)
                    new_counts = self._term_counts(document)
                    changed_terms = sorted(set(old_counts) | set(new_counts))
                    now = datetime.utcnow()

                    if changed_terms:
                        # Drop stale (term, url) postings from the previous
                        # crawl_version of this URL.
                        posting_delete_ops = [
                            DeleteOne({'term': term, 'url': url})
                            for term in old_counts
                        ]
                        if posting_delete_ops:
                            self.postings.bulk_write(
                                posting_delete_ops,
                                ordered=False,
                                session=session,
                            )

                        # Upsert (term, url) postings for the new crawl_version.
                        posting_upsert_ops = [
                            UpdateOne(
                                {'term': term, 'url': url},
                                {
                                    '$set': {
                                        'term': term,
                                        'page_id': document.get('page_id'),
                                        'url': url,
                                        'title': document.get('title', ''),
                                        'term_frequency': int(count),
                                        'updated_at': now,
                                    },
                                    '$setOnInsert': {'created_at': now},
                                },
                                upsert=True,
                            )
                            for term, count in new_counts.items()
                        ]
                        if posting_upsert_ops:
                            self.postings.bulk_write(
                                posting_upsert_ops,
                                ordered=False,
                                session=session,
                            )

                    self.state.update_one(
                        {'url': url},
                        {
                            '$set': {
                                'url': url,
                                'page_id': document.get('page_id'),
                                'title': document.get('title', ''),
                                'term_counts': new_counts,
                                'last_indexed_version': crawl_version,
                                'indexed_at': now,
                            },
                            '$setOnInsert': {'created_at': now},
                        },
                        upsert=True,
                        session=session,
                    )
                    self.documents.update_one(
                        {'url': url},
                        {
                            '$set': {
                                'indexed_at': now,
                                'indexed_crawl_version': crawl_version,
                            }
                        },
                        session=session,
                    )
                    self._mark_done(
                        event_id,
                        {
                            'terms_indexed': len(new_counts),
                            'terms_changed': len(changed_terms),
                        },
                        session=session,
                    )
            return True
        except Exception as exc:
            logger.exception("Failed to process index event %s: %s", event_id, exc)
            self._mark_event_pending(event_id)
            return False

    def process_pending_events(self, max_events: int = 20) -> int:
        processed = 0
        for _ in range(max_events):
            event = self.claim_event()
            if event is None:
                break
            if self.process_event(event):
                processed += 1
        return processed

    def run_forever(self, sleep_seconds: float = 1.0, max_events: int = 20):
        logger.info("Starting streaming indexer %s", self.worker_id)
        while True:
            self.reclaim_stale_events()
            processed = self.process_pending_events(max_events=max_events)
            if not processed:
                time.sleep(sleep_seconds)

    def close(self):
        self.client.close()


def parse_args():
    parser = argparse.ArgumentParser(description='Transactional streaming indexer')
    parser.add_argument('--mongodb', default=None, help='MongoDB URI')
    parser.add_argument('--database', default=None, help='MongoDB database name')
    parser.add_argument('--worker-id', default=None, help='Stable worker ID')
    parser.add_argument('--claim-timeout', type=int, default=300, help='Seconds before processing claims are reclaimed')
    parser.add_argument('--sleep', type=float, default=1.0, help='Idle sleep seconds')
    parser.add_argument('--max-events', type=int, default=20, help='Events processed per loop')
    parser.add_argument('--once', action='store_true', help='Run one reclaim/process cycle and exit')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    args = parse_args()
    indexer = StreamingIndexer(
        mongodb_uri=args.mongodb,
        database=args.database,
        worker_id=args.worker_id,
        claim_timeout_seconds=args.claim_timeout,
    )
    try:
        if args.once:
            reclaimed = indexer.reclaim_stale_events()
            processed = indexer.process_pending_events(max_events=args.max_events)
            print({'reclaimed': reclaimed, 'events_processed': processed})
            return
        indexer.run_forever(sleep_seconds=args.sleep, max_events=args.max_events)
    finally:
        indexer.close()


if __name__ == '__main__':
    main()
