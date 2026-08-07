#!/usr/bin/env python3
"""
MapReduce-style inverted index builder.

The mapper turns each crawled document into term -> posting records.
The reducer aggregates those postings into an inverted index collection.
"""

import argparse
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List

from pymongo import MongoClient, UpdateOne

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import CrawlerConfig

logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_'-]{1,48}", re.IGNORECASE)
STOP_WORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
    'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'or', 'that',
    'the', 'to', 'was', 'were', 'will', 'with', 'you', 'your'
}
DOUBLE_CONSONANTS = {'bb', 'dd', 'ff', 'gg', 'll', 'mm', 'nn', 'pp', 'rr', 'tt'}


class MapReduceIndexer:
    """Builds and queries a MongoDB-backed inverted index."""

    def __init__(self, mongodb_uri: str = None, database: str = None, batch_size: int = 500):
        self.client = MongoClient(mongodb_uri or CrawlerConfig.get_mongo_url())
        self.db = self.client[database or CrawlerConfig.MONGO_DB]
        self.documents = self.db['pages_documents']
        # term_postings: one document per (term, url) pair. Replaces the
        # legacy embedded-array inverted_index, which hit MongoDB's 16MB
        # BSON document limit on common terms during real crawls.
        self.postings = self.db['term_postings']
        self.batch_size = batch_size
        self._create_indexes()

    def _create_indexes(self):
        self.documents.create_index('indexed_at')
        self.postings.create_index([('term', 1), ('url', 1)], unique=True)
        self.postings.create_index([('term', 1), ('term_frequency', -1)])
        self.postings.create_index('url')

    def tokenize(self, text: str) -> List[str]:
        """Normalize text into searchable stem terms."""
        tokens = []
        for match in TOKEN_RE.finditer(text or ''):
            token = match.group(0).lower().strip("_'-")
            if len(token) < 2 or token in STOP_WORDS:
                continue
            stemmed = self.stem_token(token)
            if len(stemmed) < 2 or stemmed in STOP_WORDS:
                continue
            tokens.append(stemmed)
        return tokens

    @staticmethod
    def stem_token(token: str) -> str:
        """Apply a small deterministic English stemmer.

        This intentionally stays dependency-free for Docker-only installs while
        normalizing the high-value crawler/search variants: crawled, crawling,
        crawler -> crawl.
        """
        token = token.lower().strip("_'-")
        if len(token) <= 3:
            return token

        if token.endswith("'s"):
            token = token[:-2]

        for suffix in ('ingly', 'edly'):
            if len(token) > len(suffix) + 2 and token.endswith(suffix):
                return MapReduceIndexer._clean_stem(token[:-len(suffix)])

        for suffix in ('ing', 'ers', 'er', 'ed'):
            if len(token) > len(suffix) + 2 and token.endswith(suffix):
                stem = token[:-len(suffix)]
                return MapReduceIndexer._clean_stem(stem)

        if len(token) > 5 and token.endswith('ies'):
            return token[:-3] + 'y'

        if len(token) > 4 and token.endswith(('ses', 'xes', 'zes', 'ches', 'shes')):
            return MapReduceIndexer._clean_stem(token[:-2])

        if (
            len(token) > 4
            and token.endswith('s')
            and not token.endswith(('ss', 'us', 'is'))
        ):
            return token[:-1]

        return token

    @staticmethod
    def _clean_stem(stem: str) -> str:
        if len(stem) >= 2 and stem[-2:] in DOUBLE_CONSONANTS:
            stem = stem[:-1]
        return stem

    def map_document(self, document: Dict) -> Dict[str, Dict]:
        """Map one document to term posting records."""
        term_counts = Counter(self.tokenize(document.get('text', '')))
        postings = {}

        for term, count in term_counts.items():
            postings[term] = {
                'page_id': document['page_id'],
                'url': document['url'],
                'title': document.get('title', ''),
                'term_frequency': count
            }

        return postings

    def reduce_postings(self, mapped: Iterable[Dict[str, Dict]]) -> Dict[str, List[Dict]]:
        """Reduce mapper output into term -> postings list."""
        reduced = defaultdict(list)
        for doc_postings in mapped:
            for term, posting in doc_postings.items():
                reduced[term].append(posting)
        return reduced

    def build_index(self, only_unindexed: bool = False, reset: bool = False) -> Dict:
        """
        Build or refresh the inverted index.

        Args:
            only_unindexed: index only documents with indexed_at=None
            reset: drop term_postings before rebuilding
        """
        if reset:
            self.postings.delete_many({})
            self.documents.update_many({}, {'$set': {'indexed_at': None}})

        query = {'indexed_at': None} if only_unindexed else {}
        cursor = self.documents.find(query)

        mapped_batch = []
        indexed_doc_ids = []
        docs_seen = 0
        terms_written = 0

        for document in cursor:
            mapped_batch.append(self.map_document(document))
            indexed_doc_ids.append(document['_id'])
            docs_seen += 1

            if len(mapped_batch) >= self.batch_size:
                terms_written += self._flush(mapped_batch, indexed_doc_ids)
                mapped_batch.clear()
                indexed_doc_ids.clear()

        if mapped_batch:
            terms_written += self._flush(mapped_batch, indexed_doc_ids)

        return {
            'documents_indexed': docs_seen,
            'terms_written': terms_written,
            'index_postings_total': self.postings.count_documents({}),
        }

    def _flush(self, mapped_batch: List[Dict[str, Dict]], document_ids: List) -> int:
        """Flush a batch of mapped postings into term_postings.

        We write one document per (term, url) pair via upsert. The legacy
        embedded-array inverted_index has been removed because $addToSet on
        a per-term document hits MongoDB's 16MB BSON limit on common terms
        in any real-world crawl.
        """
        reduced = self.reduce_postings(mapped_batch)
        now = datetime.utcnow()
        posting_operations = []

        for term, postings in reduced.items():
            for posting in postings:
                posting_operations.append(UpdateOne(
                    {'term': term, 'url': posting['url']},
                    {
                        '$set': {
                            'term': term,
                            'page_id': posting.get('page_id'),
                            'url': posting['url'],
                            'title': posting.get('title', ''),
                            'term_frequency': int(posting.get('term_frequency', 0)),
                            'updated_at': now,
                        },
                        '$setOnInsert': {
                            'created_at': now,
                        },
                    },
                    upsert=True,
                ))

        if posting_operations:
            self.postings.bulk_write(posting_operations, ordered=False)

        self.documents.update_many(
            {'_id': {'$in': document_ids}},
            {'$set': {'indexed_at': now}}
        )

        return len(reduced)

    def close(self):
        self.client.close()


def main():
    parser = argparse.ArgumentParser(description='Build inverted index from crawled documents')
    parser.add_argument('--mongodb', default=None, help='MongoDB URI')
    parser.add_argument('--database', default=None, help='MongoDB database name')
    parser.add_argument('--only-unindexed', action='store_true', help='Index only new documents')
    parser.add_argument('--reset', action='store_true', help='Rebuild from scratch')
    parser.add_argument('--batch-size', type=int, default=500, help='Documents per MapReduce batch')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    indexer = MapReduceIndexer(
        mongodb_uri=args.mongodb,
        database=args.database,
        batch_size=args.batch_size
    )

    try:
        stats = indexer.build_index(
            only_unindexed=args.only_unindexed,
            reset=args.reset
        )
        print(stats)
    finally:
        indexer.close()


if __name__ == '__main__':
    main()
