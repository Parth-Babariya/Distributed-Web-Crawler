#!/usr/bin/env python3
"""Incremental PageRank worker backed by MongoDB graph_outbox.

The crawler writes graph_outbox events transactionally with page data. This
worker consumes those durable events, updates the historical graph_edges
ledger, and propagates PageRank deltas through pagerank_nodes using residual
pushes. Query-time search only reads the precomputed rank values.

Scale and formulation
---------------------
Ranks use the *unnormalized* PageRank equation

    x = (1 - d) * t + d * A^T x + d * D(x)

where ``t`` is the teleport vector (1 for every node, or 1 for seed URLs
only when ``--seed-url`` is given), ``A`` is the row-normalized link matrix
and ``D(x)`` routes the rank of dangling nodes to the seed URLs (or drops it
when no seeds are configured). Ranks therefore sit on a fixed scale (a page
with no in-links has rank ``1 - d``) instead of summing to 1.

This choice keeps every update *local*: adding a node only seeds that node's
own residual with ``(1 - d) * t[node]``, and no update ever has to touch all
nodes. A normalized/uniform teleport would change every node's teleport mass
whenever the graph grows, which forces an O(N) write per new node.

``full_recompute`` solves the same equation by power iteration, so the
incremental and batch paths converge to the same fixed point.
"""

import argparse
import logging
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pymongo import MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import PyMongoError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import CrawlerConfig

logger = logging.getLogger(__name__)


class IncrementalPageRankWorker:
    """Consumes graph_outbox events and maintains incremental PageRank."""

    def __init__(
        self,
        mongodb_uri: str = None,
        database: str = None,
        worker_id: str = None,
        damping: float = 0.85,
        epsilon: float = 1e-6,
        claim_timeout_seconds: int = 300,
        seed_urls: Optional[Sequence[str]] = None,
    ):
        self.client = MongoClient(mongodb_uri or CrawlerConfig.get_mongo_url())
        self.db = self.client[database or CrawlerConfig.MONGO_DB]
        self.outbox = self.db['graph_outbox']
        self.edges = self.db['graph_edges']
        self.nodes = self.db['pagerank_nodes']
        self.worker_id = worker_id or f"pagerank-{uuid.uuid4().hex[:8]}"
        self.damping = damping
        # Residuals with |r| <= epsilon are not pushed. Ranks are O(1) on the
        # unnormalized scale, so an absolute threshold is meaningful.
        self.epsilon = epsilon
        self.claim_timeout = timedelta(seconds=claim_timeout_seconds)
        self.seed_urls = self._unique_urls(seed_urls or [])
        self._seed_set = set(self.seed_urls)
        self._create_indexes()

    def _create_indexes(self):
        self.outbox.create_index([('status', 1), ('_id', 1)])
        self.outbox.create_index([('status', 1), ('claimed_at', 1)])
        self.outbox.create_index('source_url')
        self.edges.create_index('source_url', unique=True)
        self.nodes.create_index('url', unique=True)
        self.nodes.create_index('residual')
        self.nodes.create_index('residual_claimed_at')

    @staticmethod
    def _unique_urls(urls: Iterable[str]) -> List[str]:
        seen = set()
        unique = []
        for url in urls or []:
            if not url or url in seen:
                continue
            seen.add(url)
            unique.append(url)
        return unique

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
            logger.warning("Reclaimed %s stale PageRank events", result.modified_count)
        return result.modified_count

    def reclaim_stale_residual_claims(self) -> int:
        cutoff = datetime.utcnow() - self.claim_timeout
        result = self.nodes.update_many(
            {'residual_claimed_at': {'$lt': cutoff}},
            {'$unset': {'residual_claimed_at': '', 'residual_worker_id': ''}},
        )
        if result.modified_count:
            logger.warning("Released %s stale residual claims", result.modified_count)
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

    @staticmethod
    def _is_transient_transaction_error(exc: Exception) -> bool:
        """MongoDB can raise retryable WriteConflict during hot residual updates."""
        if isinstance(exc, PyMongoError):
            try:
                if exc.has_error_label('TransientTransactionError'):
                    return True
            except AttributeError:
                pass
            if getattr(exc, 'code', None) in (112, 251):
                return True
        return False

    def teleport_mass(self, url: str) -> float:
        """Residual a node starts with: (1 - d) * t[url]."""
        if self._seed_set and url not in self._seed_set:
            return 0.0
        return 1.0 - self.damping

    def dangling_targets(self) -> Dict[str, float]:
        """Where a dangling node's rank flows: seeds, or nowhere (dropped)."""
        if not self.seed_urls:
            return {}
        weight = 1.0 / len(self.seed_urls)
        return {url: weight for url in self.seed_urls}

    def _outflow(self, rank: float, links: List[str]) -> Dict[str, float]:
        """Rank mass a node with ``rank`` sends to each target."""
        if links:
            share = self.damping * rank / len(links)
            return {url: share for url in links}
        return {
            url: self.damping * rank * weight
            for url, weight in self.dangling_targets().items()
        }

    def _ensure_node(self, url: str, session=None):
        self._add_residual_delta(url, 0.0, session=session)

    def _get_rank(self, url: str, session=None) -> float:
        node = self.nodes.find_one({'url': url}, {'rank': 1}, session=session)
        if node is None:
            self._ensure_node(url, session=session)
            return 0.0
        return float(node.get('rank', 0.0))

    def _add_residual_delta(self, url: str, delta: float, session=None):
        """Add ``delta`` to url's residual, creating the node if needed.

        A new node starts at rank 0 with its teleport mass as residual — the
        initial state of forward-push PageRank. The pipeline update lets one
        upsert both initialize and increment, which $setOnInsert + $inc on
        the same field cannot.
        """
        if not url:
            return
        now = datetime.utcnow()
        self.nodes.update_one(
            {'url': url},
            [{
                '$set': {
                    'rank': {'$ifNull': ['$rank', 0.0]},
                    'residual': {
                        '$add': [
                            {'$ifNull': ['$residual', self.teleport_mass(url)]},
                            float(delta),
                        ]
                    },
                    'created_at': {'$ifNull': ['$created_at', now]},
                    'last_updated': now,
                }
            }],
            upsert=True,
            session=session,
        )

    def _contribution_deltas(
        self,
        source_rank: float,
        old_links: List[str],
        new_links: List[str],
    ) -> Dict[str, float]:
        """Residual corrections when a source's out-links change.

        The source has already pushed ``source_rank`` through its old links;
        withdraw that mass from the old targets and send it to the new ones.
        """
        deltas = defaultdict(float)
        for url, mass in self._outflow(source_rank, old_links).items():
            deltas[url] -= mass
        for url, mass in self._outflow(source_rank, new_links).items():
            deltas[url] += mass
        return dict(deltas)

    def _process_event_once(self, event: Dict) -> bool:
        event_id = event['_id']
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

                source_url = current['source_url']
                crawl_version = str(current.get('crawl_version', ''))
                new_links = self._unique_urls(current.get('new_outbound_links', []))
                edge_doc = self.edges.find_one(
                    {'source_url': source_url},
                    session=session,
                )
                old_version = str(edge_doc.get('last_crawl_version', '')) if edge_doc else ''

                if old_version and crawl_version and crawl_version <= old_version:
                    self.outbox.update_one(
                        {'_id': event_id},
                        {
                            '$set': {
                                'status': 'done',
                                'done_at': datetime.utcnow(),
                                'skipped_reason': 'stale_crawl_version',
                            },
                            '$unset': {'claimed_at': '', 'worker_id': ''},
                        },
                        session=session,
                    )
                    return True

                old_links = self._unique_urls(edge_doc.get('outbound_urls', [])) if edge_doc else []
                source_rank = self._get_rank(source_url, session=session)
                deltas = self._contribution_deltas(source_rank, old_links, new_links)

                # Every new link target becomes a graph node (even when the
                # source has no rank to give yet), and the source node is
                # written so a concurrent residual push on it conflicts with
                # this transaction instead of reading the old out-links.
                for url in [source_url, *new_links]:
                    deltas.setdefault(url, 0.0)
                for target_url, delta in deltas.items():
                    self._add_residual_delta(target_url, delta, session=session)

                now = datetime.utcnow()
                self.edges.update_one(
                    {'source_url': source_url},
                    {
                        '$set': {
                            'source_url': source_url,
                            'outbound_urls': new_links,
                            'outdegree': len(new_links),
                            'last_updated': now,
                            'last_crawl_version': crawl_version,
                        }
                    },
                    upsert=True,
                    session=session,
                )
                self.outbox.update_one(
                    {'_id': event_id},
                    {
                        '$set': {
                            'status': 'done',
                            'done_at': now,
                        },
                        '$unset': {'claimed_at': '', 'worker_id': ''},
                    },
                    session=session,
                )
        return True

    def process_event(self, event: Dict) -> bool:
        event_id = event['_id']
        for attempt in range(6):
            try:
                return self._process_event_once(event)
            except Exception as exc:
                if self._is_transient_transaction_error(exc) and attempt < 5:
                    delay = min(0.05 * (2 ** attempt), 1.0)
                    logger.warning(
                        "Retrying transient PageRank transaction conflict for %s "
                        "(attempt %s/6)",
                        event_id,
                        attempt + 2,
                    )
                    time.sleep(delay)
                    continue

                logger.exception("Failed to process PageRank event %s: %s", event_id, exc)
                self._mark_event_pending(event_id)
                return False
        return False

    def claim_residual_node(self) -> Optional[Dict]:
        return self.nodes.find_one_and_update(
            {
                'residual_claimed_at': {'$exists': False},
                '$or': [
                    {'residual': {'$gt': self.epsilon}},
                    {'residual': {'$lt': -self.epsilon}},
                ],
            },
            {
                '$set': {
                    'residual_claimed_at': datetime.utcnow(),
                    'residual_worker_id': self.worker_id,
                }
            },
            sort=[('last_updated', 1)],
            return_document=ReturnDocument.AFTER,
        )

    def push_residual(self, node: Dict) -> bool:
        url = node['url']
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    current = self.nodes.find_one(
                        {
                            'url': url,
                            'residual_worker_id': self.worker_id,
                        },
                        session=session,
                    )
                    if current is None:
                        return False

                    residual = float(current.get('residual', 0.0))
                    if abs(residual) <= self.epsilon:
                        self.nodes.update_one(
                            {'url': url},
                            {'$unset': {'residual_claimed_at': '', 'residual_worker_id': ''}},
                            session=session,
                        )
                        return False

                    edge_doc = self.edges.find_one({'source_url': url}, session=session)
                    successors = self._unique_urls(edge_doc.get('outbound_urls', [])) if edge_doc else []

                    self.nodes.update_one(
                        {'url': url},
                        {
                            '$inc': {'rank': residual},
                            '$set': {
                                'residual': 0.0,
                                'last_updated': datetime.utcnow(),
                            },
                            '$unset': {
                                'residual_claimed_at': '',
                                'residual_worker_id': '',
                            },
                        },
                        session=session,
                    )

                    for target_url, mass in self._outflow(residual, successors).items():
                        self._add_residual_delta(target_url, mass, session=session)
            return True
        except Exception as exc:
            logger.exception("Failed to push residual for %s: %s", url, exc)
            self.nodes.update_one(
                {'url': url, 'residual_worker_id': self.worker_id},
                {'$unset': {'residual_claimed_at': '', 'residual_worker_id': ''}},
            )
            return False

    def propagate_residuals(self, max_pushes: int = 100) -> int:
        pushed = 0
        for _ in range(max_pushes):
            node = self.claim_residual_node()
            if node is None:
                break
            if self.push_residual(node):
                pushed += 1
        return pushed

    def process_pending_events(self, max_events: int = 10) -> int:
        processed = 0
        for _ in range(max_events):
            event = self.claim_event()
            if event is None:
                break
            if self.process_event(event):
                processed += 1
        return processed

    def run_forever(self, sleep_seconds: float = 1.0, max_events: int = 10,
                    max_pushes: int = 100):
        logger.info("Starting PageRank worker %s", self.worker_id)
        while True:
            self.reclaim_stale_events()
            self.reclaim_stale_residual_claims()
            events = self.process_pending_events(max_events=max_events)
            pushes = self.propagate_residuals(max_pushes=max_pushes)
            if not events and not pushes:
                time.sleep(sleep_seconds)

    def power_iteration(self, nodes: Set[str], outgoing: Dict[str, List[str]],
                        convergence_epsilon: float = 1e-8,
                        max_iterations: int = 100) -> Tuple[Dict[str, float], int, float]:
        """Solve the module-level PageRank equation by power iteration.

        Pure function of the graph (no database access), so it doubles as the
        reference the incremental residual-push path is tested against.
        Returns (ranks, iterations_run, final_l1_delta).
        """
        teleport = {url: self.teleport_mass(url) for url in nodes}
        ranks = dict(teleport)
        l1_delta = 0.0
        iteration = 0

        for iteration in range(1, max_iterations + 1):
            new_ranks = dict(teleport)
            for source in nodes:
                for target, mass in self._outflow(ranks[source], outgoing.get(source, [])).items():
                    new_ranks[target] = new_ranks.get(target, 0.0) + mass
            l1_delta = sum(abs(new_ranks[url] - ranks.get(url, 0.0)) for url in new_ranks)
            ranks = new_ranks
            if l1_delta < convergence_epsilon:
                break

        return ranks, iteration, l1_delta

    def full_recompute(self, convergence_epsilon: float = 1e-8,
                       max_iterations: int = 100) -> Dict:
        """Run converged power iteration from graph_edges and reset residuals."""
        edge_docs = list(self.edges.find({}, {'source_url': 1, 'outbound_urls': 1}))
        nodes = set()
        outgoing = {}

        for edge in edge_docs:
            source = edge['source_url']
            links = self._unique_urls(edge.get('outbound_urls', []))
            nodes.add(source)
            nodes.update(links)
            outgoing[source] = links

        # Iterate instead of distinct(): distinct's result is capped at 16MB.
        nodes.update(doc['url'] for doc in self.nodes.find({}, {'url': 1}))
        nodes.update(self.seed_urls)
        if not nodes:
            return {'nodes': 0, 'iterations': 0, 'l1_delta': 0.0}

        ranks, iteration, l1_delta = self.power_iteration(
            nodes, outgoing, convergence_epsilon, max_iterations
        )

        now = datetime.utcnow()
        operations = [
            UpdateOne(
                {'url': url},
                {
                    '$set': {
                        'url': url,
                        'rank': rank,
                        'residual': 0.0,
                        'last_updated': now,
                        'recomputed_at': now,
                    },
                    '$setOnInsert': {'created_at': now},
                    '$unset': {'residual_claimed_at': '', 'residual_worker_id': ''},
                },
                upsert=True,
            )
            for url, rank in ranks.items()
        ]
        if operations:
            self.nodes.bulk_write(operations, ordered=False)

        return {
            'nodes': len(nodes),
            'iterations': iteration,
            'l1_delta': l1_delta,
        }

    def close(self):
        self.client.close()


def parse_args():
    parser = argparse.ArgumentParser(description='Incremental PageRank worker')
    parser.add_argument('--mongodb', default=None, help='MongoDB URI')
    parser.add_argument('--database', default=None, help='MongoDB database name')
    parser.add_argument('--worker-id', default=None, help='Stable worker ID')
    parser.add_argument('--damping', type=float, default=0.85, help='PageRank damping factor')
    parser.add_argument('--epsilon', type=float, default=1e-6, help='Absolute residual threshold below which nodes are not pushed')
    parser.add_argument('--claim-timeout', type=int, default=300, help='Seconds before processing claims are reclaimed')
    parser.add_argument('--sleep', type=float, default=1.0, help='Idle sleep seconds')
    parser.add_argument('--max-events', type=int, default=10, help='Events processed per loop')
    parser.add_argument('--max-pushes', type=int, default=100, help='Residual pushes per loop')
    parser.add_argument('--seed-url', action='append', default=[], help='Personalization seed URL; repeatable')
    parser.add_argument('--once', action='store_true', help='Run one maintenance/process/pass cycle and exit')
    parser.add_argument('--recompute', action='store_true', help='Run full power-iteration recompute and exit')
    parser.add_argument('--convergence-epsilon', type=float, default=1e-8, help='Full recompute L1 convergence epsilon')
    parser.add_argument('--max-iterations', type=int, default=100, help='Full recompute max iterations')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    args = parse_args()
    worker = IncrementalPageRankWorker(
        mongodb_uri=args.mongodb,
        database=args.database,
        worker_id=args.worker_id,
        damping=args.damping,
        epsilon=args.epsilon,
        claim_timeout_seconds=args.claim_timeout,
        seed_urls=args.seed_url,
    )

    try:
        if args.recompute:
            print(worker.full_recompute(
                convergence_epsilon=args.convergence_epsilon,
                max_iterations=args.max_iterations,
            ))
            return

        if args.once:
            worker.reclaim_stale_events()
            worker.reclaim_stale_residual_claims()
            events = worker.process_pending_events(max_events=args.max_events)
            pushes = worker.propagate_residuals(max_pushes=args.max_pushes)
            print({'events_processed': events, 'residual_pushes': pushes})
            return

        worker.run_forever(
            sleep_seconds=args.sleep,
            max_events=args.max_events,
            max_pushes=args.max_pushes,
        )
    finally:
        worker.close()


if __name__ == '__main__':
    main()
