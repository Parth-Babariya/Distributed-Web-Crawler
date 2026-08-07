#!/usr/bin/env python3
"""
Deterministic multi-process system test.

This test proves that independent worker processes coordinate through Redis and
persist output in MongoDB. It also verifies indexing and the FastAPI search API.
"""

import json
import multiprocessing
import os
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from pymongo import MongoClient
from redis import Redis

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / 'results'
LOG_DIR = RESULTS_DIR / 'distributed_test_logs'
WEB_PORT = 9191
API_PORT = 8010
MONGO_URI = os.getenv(
    'EVAL_MONGO_URI',
    'mongodb://localhost:27017/web_crawler?directConnection=true',
)
DATABASE_NAME = os.getenv('EVAL_MONGO_DB', 'web_crawler')


def process_env():
    env = os.environ.copy()
    env['PYTHONPATH'] = str(PROJECT_ROOT / 'src')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env.setdefault('REDIS_HOST', 'localhost')
    env.setdefault('REDIS_PORT', '6379')
    env.setdefault('MONGO_URI', MONGO_URI)
    env.setdefault('MONGO_DB', DATABASE_NAME)
    env.setdefault('RECOVERY_INTERVAL', '1')
    env.setdefault('CRAWLER_CONTENT_STORE', 's3')
    env.setdefault('S3_BUCKET', 'crawler-html')
    env.setdefault('S3_ENDPOINT_URL', 'http://localhost:9000')
    env.setdefault('AWS_ACCESS_KEY_ID', 'minioadmin')
    env.setdefault('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    env.setdefault('AWS_REGION', 'us-east-1')
    return env


class MiniWebHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        routes = {
            '/': self.home,
            '/alpha': self.alpha,
            '/beta': self.beta,
            '/cycle-a': self.cycle_a,
            '/cycle-b': self.cycle_b,
            '/slow': self.slow,
            '/asset.txt': self.asset,
            '/robots.txt': self.robots,
            '/redirect': self.redirect,
        }
        handler = routes.get(self.path.split('#', 1)[0])
        if handler:
            handler()
        else:
            self.send_error(404)

    def robots(self):
        self.send_text('User-agent: *\nAllow: /\nCrawl-delay: 0.001\n')

    def home(self):
        self.send_html("""
        <html>
          <head>
            <title>Fixture Home</title>
            <script>script_noise_should_not_index</script>
            <style>style_noise_should_not_index</style>
          </head>
          <body>
            <h1>Distributed crawler fixture</h1>
            <a href="/alpha">Alpha relative</a>
            <a href="/alpha#duplicate-fragment">Alpha duplicate with fragment</a>
            <a href="http://127.0.0.1:9191/beta">Beta absolute</a>
            <a href="/cycle-a">Cycle start</a>
            <a href="/broken">Broken link</a>
            <a href="/slow">Slow page</a>
            <a href="/asset.txt">Non HTML resource</a>
            <a href="/redirect">Redirect page</a>
          </body>
        </html>
        """)

    def alpha(self):
        self.send_html("""
        <html><head><title>Alpha Ranking Page</title></head>
        <body>
          <p>kiwi kiwi kiwi kiwi kiwi crawler ranking alpha.</p>
          <a href="/cycle-a">cycle</a>
        </body></html>
        """)

    def beta(self):
        self.send_html("""
        <html><head><title>Beta Unique Page</title></head>
        <body>
          <p>nebulaunique crawler search beta.</p>
          <a href="/alpha">alpha</a>
        </body></html>
        """)

    def cycle_a(self):
        self.send_html('<html><head><title>Cycle A</title></head><body><a href="/cycle-b">B</a></body></html>')

    def cycle_b(self):
        self.send_html('<html><head><title>Cycle B</title></head><body><a href="/cycle-a">A</a></body></html>')

    def slow(self):
        time.sleep(1.5)
        self.send_html('<html><head><title>Slow Page</title></head><body>slowpage crawler content</body></html>')

    def asset(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'plain text asset should not be stored as HTML')

    def redirect(self):
        self.send_response(302)
        self.send_header('Location', '/beta')
        self.end_headers()

    def send_html(self, body):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def send_text(self, body):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def log_message(self, format, *args):
        return


def serve_mini_web():
    server = ThreadingHTTPServer(('127.0.0.1', WEB_PORT), MiniWebHandler)
    server.serve_forever()


CRAWLER_COLLECTIONS = (
    # Page corpus written by the crawler in one Mongo transaction
    'pages_metadata',
    'pages_content',
    'pages_documents',
    # Durable outbox queues consumed by streaming workers
    'index_outbox',
    'graph_outbox',
    # Streaming indexer state
    'term_postings',
    'indexed_documents',
    # Streaming PageRank state
    'graph_edges',
    'pagerank_nodes',
    # Legacy embedded-postings collection — still cleaned to drop stale data
    # from previous batch-indexer runs even though we no longer write to it.
    'inverted_index',
)


def clean_datastores():
    """Wipe every Redis key and Mongo collection the crawler writes to.

    The two-tier frontier rewrite renamed the old single ZSET
    ``crawler:frontier`` to per-domain ``crawler:frontier:<domain>`` keys, so
    the broad ``crawler:*`` SCAN here covers the new frontier shape too.
    Also wipes ``processing_lease:*`` (per-URL leases written by the
    ProcessingLedger) which live OUTSIDE the ``crawler:`` namespace.
    """
    redis = Redis(host='localhost', port=6379, decode_responses=False)
    mongo = MongoClient(MONGO_URI)
    db = mongo[DATABASE_NAME]

    for pattern in ('crawler:*', 'lock:*', 'robots_cache:*', 'processing_lease:*'):
        for key in redis.scan_iter(pattern):
            redis.delete(key)

    for collection in CRAWLER_COLLECTIONS:
        db[collection].delete_many({})


def two_tier_frontier_size(redis_client) -> int:
    """Sum ZCARD across all ``crawler:frontier:<domain>`` keys.

    The legacy single ``crawler:frontier`` ZSET no longer exists; the
    two-tier frontier shards URLs by domain, so monitoring code has to
    SCAN-and-sum.
    """
    total = 0
    for key in redis_client.scan_iter(match='crawler:frontier:*', count=200):
        total += redis_client.zcard(key)
    return total


def wait_for_http(url, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code < 500:
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f'timed out waiting for {url}')


def run_command(args, timeout=60):
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env=process_env(),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=True,
    )


def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)
    clean_datastores()

    web_process = multiprocessing.Process(target=serve_mini_web)
    web_process.start()
    api_process = None
    worker_processes = []

    try:
        wait_for_http(f'http://127.0.0.1:{WEB_PORT}/')

        seed_url = f'http://127.0.0.1:{WEB_PORT}/'
        seed_result = run_command([
            sys.executable,
            'src/v3/master_v3.py',
            'seed',
            seed_url,
        ])

        for worker_id in ['dist-worker-1', 'dist-worker-2', 'dist-worker-3']:
            log_path = LOG_DIR / f'{worker_id}.log'
            log_file = log_path.open('w', encoding='utf-8')
            process = subprocess.Popen(
                [
                    sys.executable,
                    'src/v3/worker_v3.py',
                    '--worker-id',
                    worker_id,
                    '--max-pages',
                    '5',
                    '--idle-timeout',
                    '8',
                    '--batch-size',
                    '1',
                    '--processing-timeout',
                    '2',
                ],
                cwd=PROJECT_ROOT,
                env=process_env(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            worker_processes.append((process, log_file))

        time.sleep(1.0)
        killed_pid = worker_processes[0][0].pid
        worker_processes[0][0].terminate()

        for process, log_file in worker_processes:
            try:
                process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=5)
            log_file.close()

        # Drive the streaming index/PageRank pipeline that the production
        # system actually uses. The crawler emits index_outbox + graph_outbox
        # events transactionally with each page write; one --once pass over
        # each consumer drains everything that was buffered while the workers
        # ran. --max-events=10000 / --max-pushes=10000 picks numbers larger
        # than any plausible event count from this fixture so the single
        # pass really does drain the queue.
        index_result = run_command([
            sys.executable,
            'src/indexer/streaming_indexer.py',
            '--once',
            '--max-events',
            '10000',
        ])

        pagerank_result = run_command([
            sys.executable,
            'src/indexer/pagerank_worker.py',
            '--once',
            '--max-events',
            '10000',
            '--max-pushes',
            '500',
        ], timeout=180)

        api_log = (LOG_DIR / 'search_api.log').open('w', encoding='utf-8')
        api_process = subprocess.Popen(
            [
                sys.executable,
                '-m',
                'uvicorn',
                'search.search_api:app',
                '--host',
                '127.0.0.1',
                '--port',
                str(API_PORT),
            ],
            cwd=PROJECT_ROOT,
            env=process_env(),
            stdout=api_log,
            stderr=subprocess.STDOUT,
        )
        wait_for_http(f'http://127.0.0.1:{API_PORT}/health')

        kiwi_response = requests.get(
            f'http://127.0.0.1:{API_PORT}/search',
            params={'q': 'kiwi crawler', 'limit': 5},
            timeout=10,
        )
        unique_response = requests.get(
            f'http://127.0.0.1:{API_PORT}/search',
            params={'q': 'nebulaunique', 'limit': 5},
            timeout=10,
        )
        unknown_response = requests.get(
            f'http://127.0.0.1:{API_PORT}/search',
            params={'q': 'notpresentterm', 'limit': 5},
            timeout=10,
        )

        redis = Redis(host='localhost', port=6379, decode_responses=False)
        mongo = MongoClient(MONGO_URI)
        db = mongo[DATABASE_NAME]

        metadata = list(db.pages_metadata.find({}, {'url': 1, 'title': 1, 'worker_id': 1, '_id': 0}))
        documents = list(db.pages_documents.find({}, {'url': 1, 'title': 1, 'text': 1, '_id': 0}))
        worker_ids = sorted({doc.get('worker_id') for doc in metadata if doc.get('worker_id')})
        crawled_urls = sorted(doc['url'] for doc in metadata)
        alpha_count = sum(1 for url in crawled_urls if url.endswith('/alpha'))
        text_blob = ' '.join(doc.get('text', '') for doc in documents)

        payload = {
            'seed_stdout': seed_result.stdout,
            'index_stdout': index_result.stdout,
            'pagerank_stdout': pagerank_result.stdout,
            'web_process_pid': web_process.pid,
            'killed_worker_pid': killed_pid,
            'worker_process_ids': [process.pid for process, _ in worker_processes],
            'worker_ids_seen_in_storage': worker_ids,
            'crawled_urls': crawled_urls,
            'pages_metadata_count': db.pages_metadata.count_documents({}),
            'pages_documents_count': db.pages_documents.count_documents({}),
            # term_postings is the live search index (one doc per term-url);
            # the legacy embedded-array inverted_index is no longer written.
            'term_postings_count': db.term_postings.count_documents({}),
            'pagerank_nodes_count': db.pagerank_nodes.count_documents({}),
            'graph_outbox_pending': db.graph_outbox.count_documents({'status': 'pending'}),
            'index_outbox_pending': db.index_outbox.count_documents({'status': 'pending'}),
            # Two-tier frontier: per-domain ZSETs summed via SCAN. The legacy
            # global crawler:frontier ZSET no longer exists.
            'frontier_size': two_tier_frontier_size(redis),
            'active_domains_size': redis.llen('crawler:active_domains'),
            'processing_size': redis.hlen('crawler:processing'),
            'kiwi_search': kiwi_response.json(),
            'unique_search': unique_response.json(),
            'unknown_search': unknown_response.json(),
            'script_noise_indexed': 'script_noise_should_not_index' in text_blob,
            'style_noise_indexed': 'style_noise_should_not_index' in text_blob,
            'alpha_url_count': alpha_count,
            'logs': {
                path.name: path.read_text(encoding='utf-8', errors='replace')
                for path in LOG_DIR.glob('dist-worker-*.log')
            },
        }

        # Crawler distributed-system invariants
        assert payload['pages_metadata_count'] >= 4, payload
        assert len(worker_ids) >= 2, payload
        assert payload['processing_size'] == 0, payload
        assert alpha_count == 1, payload

        # Index sanity: text extraction strips scripts/styles
        assert not payload['script_noise_indexed'], payload
        assert not payload['style_noise_indexed'], payload

        # Streaming pipeline drained — no events left in pending state
        assert payload['index_outbox_pending'] == 0, payload
        assert payload['graph_outbox_pending'] == 0, payload

        # Streaming indexer wrote real postings; PageRank produced node ranks
        assert payload['term_postings_count'] > 0, payload
        assert payload['pagerank_nodes_count'] > 0, payload

        # Search ranking reaches the right pages via the streaming index
        assert payload['kiwi_search']['results'], payload
        assert payload['kiwi_search']['results'][0]['url'].endswith('/alpha'), payload
        assert payload['unique_search']['results'][0]['url'].endswith('/beta'), payload
        assert payload['unknown_search']['results'] == [], payload

        output_file = RESULTS_DIR / 'distributed_system_test_results.json'
        output_file.write_text(json.dumps(payload, indent=2, default=str), encoding='utf-8')
        print(json.dumps({
            'status': 'passed',
            'output_file': str(output_file),
            'pages': payload['pages_metadata_count'],
            'workers_seen': worker_ids,
            'top_kiwi_result': payload['kiwi_search']['results'][0],
            'top_unique_result': payload['unique_search']['results'][0],
        }, indent=2))

    finally:
        if api_process:
            api_process.terminate()
            try:
                api_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                api_process.kill()
        for process, log_file in worker_processes:
            if process.poll() is None:
                process.terminate()
            if not log_file.closed:
                log_file.close()
        web_process.terminate()
        web_process.join(timeout=5)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
