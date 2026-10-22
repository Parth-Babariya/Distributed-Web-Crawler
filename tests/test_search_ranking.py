"""End-to-end ranking checks: streaming indexer -> term_postings -> search.

Needs a MongoDB replica set (the indexer uses transactions). Uses MONGO_URI,
defaulting to the docker-compose port mapping; skipped when unreachable.

    docker compose up -d mongodb
    python -m unittest tests.test_search_ranking
"""

import os
import sys
import unittest
from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))

from indexer.streaming_indexer import StreamingIndexer
from search.search_api import SearchService

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/?directConnection=true')
DATABASE = 'search_ranking_test'
FILLER = 'lorem ipsum dolor sit amet consectetur adipiscing elit'


def mongo_available() -> bool:
    try:
        MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).admin.command('ping')
        return True
    except PyMongoError:
        return False


@unittest.skipUnless(mongo_available(), 'MongoDB replica set not reachable')
class SearchRankingTest(unittest.TestCase):
    PAGES = {
        # Common term repeated vs. rare term once (IDF).
        '/common': ('Common', 'kiwi kiwi kiwi ' + FILLER),
        '/rare': ('Rare', 'zebra ' + FILLER),
        '/k1': ('K1', 'kiwi ' + FILLER),
        '/k2': ('K2', 'kiwi ' + FILLER),
        '/k3': ('K3', 'kiwi ' + FILLER),
        # Same single mention, different lengths (length normalization).
        '/short': ('Short', 'mango ' + FILLER),
        '/long': ('Long', 'mango ' + ' '.join([FILLER] * 12)),
        # Same body, term only in one title (title boost).
        '/titled': ('Papaya Guide', 'papaya ' + FILLER),
        '/untitled': ('Guide', 'papaya ' + FILLER),
        # Conjunctive queries.
        '/both': ('Both', 'quince guava ' + FILLER),
        '/only-quince': ('Quince', 'quince quince ' + FILLER),
        # Identical text, different authority.
        '/hub': ('Lychee', 'lychee ' + FILLER),
        '/leaf': ('Lychee', 'lychee ' + FILLER),
    }

    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(MONGO_URI)
        cls.client.drop_database(DATABASE)
        db = cls.client[DATABASE]
        for path, (title, text) in cls.PAGES.items():
            page_id = ObjectId()
            version = f'{page_id}_v1'
            db.pages_documents.insert_one({
                'page_id': page_id,
                'url': cls.url(path),
                'title': title,
                'text': text,
                'crawl_version': version,
            })
            db.index_outbox.insert_one({
                'url': cls.url(path),
                'page_id': page_id,
                'crawl_version': version,
                'status': 'pending',
                'timestamp': datetime.utcnow(),
            })
        db.pagerank_nodes.insert_many([
            {'url': cls.url('/hub'), 'rank': 25.0, 'residual': 0.0},
            {'url': cls.url('/leaf'), 'rank': 0.15, 'residual': 0.0},
        ])

        indexer = StreamingIndexer(mongodb_uri=MONGO_URI, database=DATABASE)
        try:
            while indexer.process_pending_events(max_events=100):
                pass
        finally:
            indexer.close()
        cls.service = SearchService(mongodb_uri=MONGO_URI, database=DATABASE)

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(DATABASE)
        cls.client.close()

    @staticmethod
    def url(path):
        return f'https://fruit.test{path}'

    def ranked_urls(self, query, mode='any'):
        response = self.service.search(query, limit=10, mode=mode)
        return [item['url'] for item in response['results']]

    def test_postings_carry_bm25_fields(self):
        posting = self.client[DATABASE].term_postings.find_one(
            {'term': 'papaya', 'url': self.url('/titled')}
        )
        self.assertTrue(posting['in_title'])
        self.assertEqual(posting['doc_length'], 9)

    def test_rare_term_outweighs_repeated_common_term(self):
        self.assertEqual(self.ranked_urls('kiwi zebra')[0], self.url('/rare'))

    def test_shorter_document_wins_for_equal_term_frequency(self):
        self.assertEqual(
            self.ranked_urls('mango'),
            [self.url('/short'), self.url('/long')],
        )

    def test_title_match_is_boosted(self):
        self.assertEqual(self.ranked_urls('papaya')[0], self.url('/titled'))

    def test_all_mode_requires_every_term(self):
        self.assertEqual(
            self.ranked_urls('quince guava', mode='all'),
            [self.url('/both')],
        )
        self.assertEqual(
            set(self.ranked_urls('quince guava', mode='any')),
            {self.url('/both'), self.url('/only-quince')},
        )

    def test_all_mode_with_unknown_term_is_empty(self):
        self.assertEqual(self.ranked_urls('quince notpresentxyz', mode='all'), [])

    def test_pagerank_breaks_text_ties(self):
        response = self.service.search('lychee', limit=10)
        hub, leaf = response['results']
        self.assertEqual(hub['url'], self.url('/hub'))
        self.assertAlmostEqual(hub['term_score'], leaf['term_score'])
        self.assertGreater(hub['score'], leaf['score'])

    def test_unknown_term_returns_no_results(self):
        self.assertEqual(self.ranked_urls('notpresentxyz'), [])


if __name__ == '__main__':
    unittest.main()
