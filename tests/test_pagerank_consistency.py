"""Incremental PageRank must converge to the same ranks as full recompute.

Needs a MongoDB replica set (transactions). Uses MONGO_URI, defaulting to the
docker-compose port mapping; the test is skipped when MongoDB is unreachable
(unless REQUIRE_MONGO is set, as in CI).

    docker compose up -d mongodb
    python -m unittest tests.test_pagerank_consistency
"""

import os
import random
import sys
import unittest
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))

from indexer.pagerank_worker import IncrementalPageRankWorker

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/?directConnection=true')
DATABASE = 'pagerank_consistency_test'


def mongo_available() -> bool:
    try:
        MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).admin.command('ping')
        return True
    except PyMongoError:
        # CI sets REQUIRE_MONGO so a broken database fails the build
        # instead of silently skipping these tests.
        if os.getenv('REQUIRE_MONGO'):
            raise
        return False


@unittest.skipUnless(mongo_available(), 'MongoDB replica set not reachable')
class PageRankConsistencyTest(unittest.TestCase):
    def setUp(self):
        self.client = MongoClient(MONGO_URI)
        self.client.drop_database(DATABASE)

    def tearDown(self):
        self.client.drop_database(DATABASE)
        self.client.close()

    def _worker(self, seed_urls=None):
        return IncrementalPageRankWorker(
            mongodb_uri=MONGO_URI,
            database=DATABASE,
            worker_id='consistency-test',
            epsilon=1e-10,
            seed_urls=seed_urls,
        )

    def _emit(self, version, source, links):
        self.client[DATABASE].graph_outbox.insert_one({
            'source_url': source,
            'new_outbound_links': links,
            'status': 'pending',
            'crawl_version': f'v{version:04d}',
            'timestamp': datetime.utcnow(),
        })

    @staticmethod
    def _drain(worker):
        while worker.process_pending_events(max_events=500) or worker.propagate_residuals(max_pushes=5000):
            pass

    def _assert_matches_power_iteration(self, worker, final_graph):
        nodes = set(final_graph)
        for links in final_graph.values():
            nodes.update(links)
        nodes.update(worker.seed_urls)
        expected, _, _ = worker.power_iteration(nodes, final_graph, 1e-12, 1000)

        actual = {
            doc['url']: doc['rank']
            for doc in self.client[DATABASE].pagerank_nodes.find({}, {'url': 1, 'rank': 1})
        }
        self.assertEqual(set(actual), set(expected))
        for url, rank in expected.items():
            self.assertAlmostEqual(actual[url], rank, delta=1e-6, msg=url)

    def _run_random_crawl(self, worker, seed=7):
        rng = random.Random(seed)
        pages = [f'https://site{i % 4}.test/p{i}' for i in range(25)]
        final_graph = {}
        version = 0
        # Crawl every page, then recrawl a third of them with changed links,
        # including pages that become (or stop being) dangling.
        for page in pages + rng.sample(pages, 8):
            version += 1
            links = rng.sample(pages, rng.choice([0, 1, 2, 4])) if rng.random() > 0.15 else []
            links = [link for link in links if link != page]
            final_graph[page] = links
            self._emit(version, page, links)
            if version % 5 == 0:
                # Interleave pushes with events like the live worker does.
                worker.propagate_residuals(max_pushes=50)
        self._drain(worker)
        return final_graph

    def test_incremental_matches_power_iteration(self):
        worker = self._worker()
        try:
            final_graph = self._run_random_crawl(worker)
            self._assert_matches_power_iteration(worker, final_graph)
        finally:
            worker.close()

    def test_incremental_matches_power_iteration_with_seed_teleport(self):
        worker = self._worker(seed_urls=['https://site0.test/p0', 'https://site1.test/p1'])
        try:
            final_graph = self._run_random_crawl(worker, seed=11)
            self._assert_matches_power_iteration(worker, final_graph)
        finally:
            worker.close()

    def test_full_recompute_keeps_incremental_scale(self):
        """A recompute followed by new crawl events stays on one scale."""
        worker = self._worker()
        try:
            final_graph = self._run_random_crawl(worker)
            worker.full_recompute(convergence_epsilon=1e-12, max_iterations=1000)
            self._emit(9999, 'https://new.test/a', ['https://new.test/b'])
            final_graph['https://new.test/a'] = ['https://new.test/b']
            self._drain(worker)
            self._assert_matches_power_iteration(worker, final_graph)
            # A page nobody links to sits at the teleport floor, not at 1.0.
            node = self.client[DATABASE].pagerank_nodes.find_one({'url': 'https://new.test/a'})
            self.assertAlmostEqual(node['rank'], 1 - worker.damping, delta=1e-6)
        finally:
            worker.close()


if __name__ == '__main__':
    unittest.main()
