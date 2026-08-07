#!/usr/bin/env python3
"""
Run reproducible evaluation experiments for the distributed web crawler.

The experiments use a deterministic local website and real Redis/MongoDB
services. Worker scaling uses independent OS subprocesses running worker_v3.py,
not in-process simulated workers.
"""

import csv
import json
import math
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean, median

import requests
from pymongo import MongoClient
from redis import Redis

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / 'src'
RAW_DIR = PROJECT_ROOT / 'results' / 'raw'
PROCESSED_DIR = PROJECT_ROOT / 'results' / 'processed'
LOG_DIR = RAW_DIR / 'worker_logs'

WEB_BASE_PORT = 9292
DOMAIN_COUNT = 20
API_PORT = 8020
PAGE_COUNT = 600
TARGET_PAGES = 200
WORKER_COUNTS = list(range(1, 21))
INDEX_DOC_COUNTS = [50, 100, 200, 400, 800]
QUERY_RUNS = 30
MONGO_URI = os.getenv(
    'EVAL_MONGO_URI',
    'mongodb://localhost:27017/web_crawler?directConnection=true',
)
DATABASE_NAME = os.getenv('EVAL_MONGO_DB', 'web_crawler')
# Legacy alias used by run_resource_scaling_1_20.py
WEB_PORT = WEB_BASE_PORT


class EvaluationSiteHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        routes = {
            '/': self.home,
            '/robots.txt': self.robots,
            '/asset.txt': self.asset,
            '/broken': self.broken,
            '/redirect': self.redirect,
        }
        if path in routes:
            routes[path]()
            return

        if path.startswith('/page/'):
            try:
                page_id = int(path.rsplit('/', 1)[1])
            except ValueError:
                self.send_error(404)
                return
            self.page(page_id)
            return

        self.send_error(404)

    def robots(self):
        self.send_text('User-agent: *\nAllow: /\nCrawl-delay: 0.005\n')

    def home(self):
        # Determine which port we are running on
        host_header = self.headers.get('Host', '')
        try:
            own_port = int(host_header.split(':')[-1])
        except (ValueError, IndexError):
            own_port = WEB_BASE_PORT

        # Build cross-domain links to all fixture domains
        cross_links = []
        for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT):
            cross_links.append(f'<a href="http://127.0.0.1:{port}/page/0">domain {port} root</a>')

        body = f"""
        <html>
          <head>
            <title>Evaluation Fixture Home (port {own_port})</title>
            <script>script_noise_should_not_index</script>
            <style>style_noise_should_not_index</style>
          </head>
          <body>
            <h1>Distributed crawler evaluation fixture (port {own_port})</h1>
            <a href="/page/0">root page</a>
            <a href="/page/0#duplicate-fragment">duplicate root page</a>
            <a href="/page/1">second page</a>
            <a href="/page/2">third page</a>
            <a href="/broken">broken page</a>
            <a href="/asset.txt">non html asset</a>
            <a href="/redirect">redirect page</a>
            {' '.join(cross_links)}
          </body>
        </html>
        """
        self.send_html(body)

    def page(self, page_id):
        if page_id < 0 or page_id >= PAGE_COUNT:
            self.send_error(404)
            return

        # Simulate realistic network latency (makes workload I/O-bound)
        time.sleep(0.01)

        # Determine which port we are running on for cross-domain links
        host_header = self.headers.get('Host', '')
        try:
            own_port = int(host_header.split(':')[-1])
        except (ValueError, IndexError):
            own_port = WEB_BASE_PORT

        links = []
        for offset in (1, 2, 3):
            child = page_id * 2 + offset
            if child < PAGE_COUNT:
                links.append(f'<a href="/page/{child}">child {child}</a>')

        # Cross-domain links: every 3rd page links to a different domain
        if page_id % 3 == 0:
            target_port = WEB_BASE_PORT + ((own_port - WEB_BASE_PORT + 1) % DOMAIN_COUNT)
            cross_page = (page_id + 7) % PAGE_COUNT
            links.append(f'<a href="http://127.0.0.1:{target_port}/page/{cross_page}">cross domain</a>')

        if page_id > 3:
            links.append(f'<a href="/page/{page_id // 2}#cycle">cycle duplicate</a>')
        if page_id % 10 == 0:
            links.append('<a href="/broken">broken</a>')
        if page_id % 15 == 0:
            links.append('<a href="/asset.txt">asset</a>')
        if page_id == 5:
            links.append('<a href="/redirect">redirect</a>')

        repeated = 'kiwi ' * (1 + page_id % 5)
        rare = f'unique_term_{page_id}'
        size_blob = ' '.join([f'token{page_id % 13}', 'crawler', 'benchmark'] * (5 + page_id % 7))
        body = f"""
        <html>
          <head><title>Evaluation Page {page_id}</title></head>
          <body>
            <h1>Evaluation distributed crawler page {page_id}</h1>
            <p>{repeated} crawler search benchmark group_{page_id % 8} {rare}</p>
            <p>{size_blob}</p>
            {' '.join(links)}
          </body>
        </html>
        """
        self.send_html(body)

    def asset(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'plain text asset')

    def broken(self):
        self.send_error(404)

    def redirect(self):
        self.send_response(302)
        # Redirect to a cross-domain page
        host_header = self.headers.get('Host', '')
        try:
            own_port = int(host_header.split(':')[-1])
        except (ValueError, IndexError):
            own_port = WEB_BASE_PORT
        target_port = WEB_BASE_PORT + ((own_port - WEB_BASE_PORT + 1) % DOMAIN_COUNT)
        self.send_header('Location', f'http://127.0.0.1:{target_port}/page/7')
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


def serve_fixture(port=None):
    """Start a fixture server on the given port (default: WEB_BASE_PORT)."""
    listen_port = port if port is not None else WEB_BASE_PORT
    server = ThreadingHTTPServer(('127.0.0.1', listen_port), EvaluationSiteHandler)
    server.serve_forever()


def start_all_fixture_servers():
    """Launch DOMAIN_COUNT fixture servers on consecutive ports."""
    processes = []
    for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT):
        proc = multiprocessing.Process(target=serve_fixture, args=(port,), daemon=True)
        proc.start()
        processes.append(proc)
    # Wait until all servers are responsive
    for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT):
        wait_for_http(f'http://127.0.0.1:{port}/')
    return processes


def stop_all_fixture_servers(processes):
    """Terminate all fixture server processes."""
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.join(timeout=5)


def ensure_dirs():
    for directory in (RAW_DIR, PROCESSED_DIR, LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


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


def env():
    base = os.environ.copy()
    base['PYTHONPATH'] = str(SRC_DIR)
    base['PYTHONDONTWRITEBYTECODE'] = '1'
    base.setdefault('REDIS_HOST', 'localhost')
    base.setdefault('REDIS_PORT', '6379')
    base.setdefault('MONGO_URI', MONGO_URI)
    base.setdefault('MONGO_DB', DATABASE_NAME)
    base.setdefault('RECOVERY_INTERVAL', '1')
    # Exercise the distributed blob-store path during host-side evaluation.
    # The Compose MinIO service is published at localhost:9000.
    base.setdefault('CRAWLER_CONTENT_STORE', 's3')
    base.setdefault('S3_BUCKET', 'crawler-html')
    base.setdefault('S3_ENDPOINT_URL', 'http://localhost:9000')
    base.setdefault('AWS_ACCESS_KEY_ID', 'minioadmin')
    base.setdefault('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    base.setdefault('AWS_REGION', 'us-east-1')
    return base


def clean_datastores(redis_client, db):
    for pattern in ('crawler:*', 'lock:*', 'robots_cache:*'):
        for key in redis_client.scan_iter(pattern):
            redis_client.delete(key)

    for name in (
        'pages_metadata',
        'pages_content',
        'pages_documents',
        'index_outbox',
        'graph_outbox',
        'term_postings',
        'indexed_documents',
        'graph_edges',
        'pagerank_nodes',
        # Legacy collection may still exist on older checkouts; keep it clean
        # so no stale data can be mistaken for current streaming-index output.
        'inverted_index',
    ):
        db[name].delete_many({})


def frontier_total(redis_client):
    total = 0
    for key in redis_client.scan_iter('crawler:frontier:*', count=200):
        total += redis_client.zcard(key)
    return total


def drain_streaming_pipeline(max_rounds=200):
    """Drain index_outbox and graph_outbox with the real streaming workers."""
    sys.path.insert(0, str(SRC_DIR))
    from indexer.pagerank_worker import IncrementalPageRankWorker
    from indexer.streaming_indexer import StreamingIndexer

    indexer = StreamingIndexer(
        mongodb_uri=MONGO_URI,
        database=DATABASE_NAME,
        worker_id='eval-streaming-indexer',
        claim_timeout_seconds=5,
    )
    pagerank = IncrementalPageRankWorker(
        mongodb_uri=MONGO_URI,
        database=DATABASE_NAME,
        worker_id='eval-pagerank-worker',
        claim_timeout_seconds=5,
    )
    try:
        index_events = 0
        graph_events = 0
        residual_pushes = 0
        for _ in range(max_rounds):
            processed_index = indexer.process_pending_events(max_events=100)
            processed_graph = pagerank.process_pending_events(max_events=100)
            pushed = pagerank.propagate_residuals(max_pushes=500)
            index_events += processed_index
            graph_events += processed_graph
            residual_pushes += pushed
            if not processed_index and not processed_graph and not pushed:
                break
        return {
            'index_events_processed': index_events,
            'graph_events_processed': graph_events,
            'residual_pushes': residual_pushes,
        }
    finally:
        indexer.close()
        pagerank.close()


def parse_worker_stats(log_path):
    text = log_path.read_text(encoding='utf-8', errors='replace') if log_path.exists() else ''
    fields = {
        'pages_crawled': r'Pages crawled:\s+([\d,]+)',
        'links_extracted': r'Links extracted:\s+([\d,]+)',
        'links_added': r'Links added:\s+([\d,]+)',
        'links_duplicate': r'Links duplicate:\s+([\d,]+)',
        'recovered': r'Recovered stale:\s+([\d,]+)',
        'errors': r'Errors:\s+([\d,]+)',
        'timeouts': r'Timeouts:\s+([\d,]+)',
    }
    stats = {}
    for key, pattern in fields.items():
        matches = re.findall(pattern, text)
        stats[key] = int(matches[-1].replace(',', '')) if matches else 0
    return stats


def run_command(args, timeout=60):
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env=env(),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=True,
    )


def run_scaling_experiment(redis_client, db):
    rows = []
    worker_rows = []

    for workers in WORKER_COUNTS:
        print(f'Running crawl scaling experiment with {workers} workers')
        clean_datastores(redis_client, db)
        # Seed all domains so workers can crawl in parallel across domains
        seed_urls = [f'http://127.0.0.1:{port}/' for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT)]
        run_command([sys.executable, 'src/v3/master_v3.py', 'seed'] + seed_urls)

        processes = []
        started = time.perf_counter()
        for index in range(1, workers + 1):
            worker_id = f'eval-{workers}-{index}'
            log_path = LOG_DIR / f'scaling_w{workers}_{worker_id}.log'
            log_file = log_path.open('w', encoding='utf-8')
            process = subprocess.Popen(
                [
                    sys.executable,
                    'src/v3/worker_v3.py',
                    '--worker-id', worker_id,
                    '--max-pages', str(TARGET_PAGES * 10),
                    '--idle-timeout', '12',
                    '--batch-size', '5',
                    '--processing-timeout', '2',
                ],
                cwd=PROJECT_ROOT,
                env=env(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            processes.append((process, log_file, log_path, worker_id))
            time.sleep(0.01)

        deadline = time.perf_counter() + 90
        while time.perf_counter() < deadline:
            metadata_count = db.pages_metadata.count_documents({})
            alive = [process for process, _, _, _ in processes if process.poll() is None]
            if metadata_count >= TARGET_PAGES or not alive:
                break
            time.sleep(0.1)

        redis_client.set('crawler:shutdown', '1', ex=30)

        for process, log_file, _, _ in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=5)
            log_file.close()

        total_time = time.perf_counter() - started
        metadata_count = db.pages_metadata.count_documents({})
        documents_count = db.pages_documents.count_documents({})

        totals = {
            'links_extracted': 0,
            'links_added': 0,
            'links_duplicate': 0,
            'recovered': 0,
            'errors': 0,
            'timeouts': 0,
        }

        for _, _, log_path, worker_id in processes:
            stats = parse_worker_stats(log_path)
            for key in totals:
                totals[key] += stats.get(key, 0)
            worker_rows.append({
                'experiment_workers': workers,
                'worker_id': worker_id,
                'pages_crawled': stats.get('pages_crawled', 0),
                'links_extracted': stats.get('links_extracted', 0),
                'links_added': stats.get('links_added', 0),
                'links_duplicate': stats.get('links_duplicate', 0),
                'errors': stats.get('errors', 0),
                'timeouts': stats.get('timeouts', 0),
            })

        worker_contrib = list(db.pages_metadata.aggregate([
            {'$group': {'_id': '$worker_id', 'pages': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]))

        row = {
            'workers': workers,
            'target_pages': TARGET_PAGES,
            'crawled_pages': metadata_count,
            'documents_stored': documents_count,
            'total_time_seconds': total_time,
            'failed_fetch_attempts': totals['errors'] + totals['timeouts'],
            'duplicate_urls_filtered': totals['links_duplicate'],
            'links_extracted': totals['links_extracted'],
            'links_added': totals['links_added'],
            'recovered_urls': totals['recovered'],
            'frontier_size_after_run': frontier_total(redis_client),
            'processing_size_after_run': redis_client.hlen('crawler:processing'),
            'pages_per_second': metadata_count / total_time if total_time else 0,
            'worker_contribution_json': json.dumps(worker_contrib, default=str),
        }
        rows.append(row)
        print(json.dumps(row, indent=2))

    baseline = rows[0]['total_time_seconds']
    for row in rows:
        row['speedup'] = baseline / row['total_time_seconds'] if row['total_time_seconds'] else 0
        row['parallel_efficiency'] = row['speedup'] / row['workers'] if row['workers'] else 0

    write_csv(PROCESSED_DIR / 'crawl_scaling.csv', rows)
    write_csv(PROCESSED_DIR / 'worker_contribution.csv', worker_rows)
    return rows, worker_rows


def make_synthetic_document(doc_id):
    repeated = ' '.join(['crawler', 'search', 'benchmark'] * (5 + doc_id % 11))
    rare = f'unique_index_term_{doc_id}'
    group = f'group_{doc_id % 17}'
    text = f'Document {doc_id} {repeated} {rare} {group} kiwi ' * (1 + doc_id % 3)
    return {
        'page_id': f'synthetic-{doc_id}',
        'url': f'http://synthetic.local/doc/{doc_id}',
        'title': f'Synthetic Index Document {doc_id}',
        'text': text,
        'indexed_at': None,
        'crawl_version': f'synthetic-{doc_id}-v1',
    }


def run_indexing_experiment(db):
    sys.path.insert(0, str(SRC_DIR))
    from indexer.mapreduce_indexer import MapReduceIndexer
    from indexer.streaming_indexer import StreamingIndexer

    rows = []
    for doc_count in INDEX_DOC_COUNTS:
        print(f'Running streaming indexing experiment with {doc_count} documents')
        db.pages_documents.delete_many({})
        db.index_outbox.delete_many({})
        db.term_postings.delete_many({})
        db.indexed_documents.delete_many({})

        docs = [make_synthetic_document(i) for i in range(doc_count)]
        db.pages_documents.insert_many(docs)
        db.index_outbox.insert_many([
            {
                'url': doc['url'],
                'page_id': doc['page_id'],
                'crawl_version': doc['crawl_version'],
                'status': 'pending',
                'timestamp': datetime.now(),
            }
            for doc in docs
        ])

        tokenizer = MapReduceIndexer.__new__(MapReduceIndexer)
        total_tokens = sum(len(tokenizer.tokenize(doc['text'])) for doc in docs)
        indexer = StreamingIndexer(
            mongodb_uri=MONGO_URI,
            database=DATABASE_NAME,
            worker_id=f'eval-indexer-{doc_count}',
            claim_timeout_seconds=5,
        )
        started = time.perf_counter()
        processed_events = 0
        for _ in range(100):
            if db.index_outbox.count_documents({'status': 'pending'}) == 0:
                break
            processed_now = indexer.process_pending_events(max_events=100)
            processed_events += processed_now
            if not processed_now:
                raise RuntimeError('streaming indexer made no progress with pending events')
        elapsed = time.perf_counter() - started
        postings = db.term_postings.count_documents({})
        try:
            coll_stats = db.command('collStats', 'term_postings')
            index_size_bytes = int(coll_stats.get('size', 0))
        except Exception:
            index_size_bytes = 0
        indexer.close()

        row = {
            'documents': doc_count,
            'unique_terms': len(db.term_postings.distinct('term')),
            'total_tokens': total_tokens,
            'postings': postings,
            'index_size_bytes': index_size_bytes,
            'indexing_time_seconds': elapsed,
            'events_processed': processed_events,
            'indexer': 'streaming_indexer',
        }
        rows.append(row)
        print(json.dumps(row, indent=2))

    write_csv(PROCESSED_DIR / 'indexing_scaling.csv', rows)
    return rows


def percentile(values, percent):
    if not values:
        return 0
    ordered = sorted(values)
    index = math.ceil((percent / 100) * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def run_query_latency_experiment():
    print('Running query latency experiment through FastAPI')
    api_log = (LOG_DIR / 'query_latency_api.log').open('w', encoding='utf-8')
    api_process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            'uvicorn',
            'search.search_api:app',
            '--host', '127.0.0.1',
            '--port', str(API_PORT),
        ],
        cwd=PROJECT_ROOT,
        env=env(),
        stdout=api_log,
        stderr=subprocess.STDOUT,
    )

    rows = []
    try:
        wait_for_http(f'http://127.0.0.1:{API_PORT}/health')
        queries = [
            ('frequent_term', 'crawler'),
            ('multi_term', 'crawler benchmark'),
            ('rare_term', 'unique_index_term_799'),
            ('unknown_term', 'term_not_present_anywhere'),
        ]

        for query_type, query in queries:
            latencies = []
            result_counts = []
            for _ in range(QUERY_RUNS):
                started = time.perf_counter()
                response = requests.get(
                    f'http://127.0.0.1:{API_PORT}/search',
                    params={'q': query, 'limit': 10},
                    timeout=10,
                )
                elapsed_ms = (time.perf_counter() - started) * 1000
                response.raise_for_status()
                latencies.append(elapsed_ms)
                result_counts.append(len(response.json().get('results', [])))

            row = {
                'query_type': query_type,
                'query': query,
                'runs': QUERY_RUNS,
                'mean_latency_ms': mean(latencies),
                'median_latency_ms': median(latencies),
                'p95_latency_ms': percentile(latencies, 95),
                'results_returned': median(result_counts),
            }
            rows.append(row)
            print(json.dumps(row, indent=2))
    finally:
        api_process.terminate()
        try:
            api_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            api_process.kill()
        api_log.close()

    write_csv(PROCESSED_DIR / 'query_latency.csv', rows)
    return rows


def run_correctness_experiment(db):
    sys.path.insert(0, str(SRC_DIR))
    from indexer.streaming_indexer import StreamingIndexer
    from search.search_api import SearchService

    db.pages_documents.delete_many({})
    db.index_outbox.delete_many({})
    db.term_postings.delete_many({})
    db.indexed_documents.delete_many({})
    db.pagerank_nodes.delete_many({})
    docs = [
        {
            'page_id': 'known-1',
            'url': 'http://known.local/alpha',
            'title': 'Alpha',
            'text': 'kiwi kiwi kiwi crawler alpha',
            'indexed_at': None,
            'crawl_version': 'known-1-v1',
        },
        {
            'page_id': 'known-2',
            'url': 'http://known.local/beta',
            'title': 'Beta',
            'text': 'kiwi crawler beta',
            'indexed_at': None,
            'crawl_version': 'known-2-v1',
        },
        {
            'page_id': 'known-3',
            'url': 'http://known.local/gamma',
            'title': 'Gamma',
            'text': 'nebulaunique gamma search',
            'indexed_at': None,
            'crawl_version': 'known-3-v1',
        },
    ]
    db.pages_documents.insert_many(docs)
    db.index_outbox.insert_many([
        {
            'url': doc['url'],
            'page_id': doc['page_id'],
            'crawl_version': doc['crawl_version'],
            'status': 'pending',
            'timestamp': datetime.now(),
        }
        for doc in docs
    ])
    db.pagerank_nodes.insert_many([
        {'url': doc['url'], 'rank': 1.0, 'residual': 0.0, 'last_updated': datetime.now()}
        for doc in docs
    ])
    indexer = StreamingIndexer(
        mongodb_uri=MONGO_URI,
        database=DATABASE_NAME,
        worker_id='eval-correctness-indexer',
        claim_timeout_seconds=5,
    )
    events_processed = indexer.process_pending_events(max_events=20)
    kiwi_postings = list(db.term_postings.find({'term': 'kiwi'}, {'_id': 0}).sort('url', 1))
    service = SearchService(mongodb_uri=MONGO_URI, database=DATABASE_NAME)
    ranked = service.search('kiwi', limit=3)
    unique = service.search('nebulaunique', limit=3)
    payload = {
        'documents': len(docs),
        'indexer': 'streaming_indexer',
        'events_processed': events_processed,
        'term_postings_count': db.term_postings.count_documents({}),
        'kiwi_postings': kiwi_postings,
        'kiwi_top_result': ranked['results'][0] if ranked['results'] else None,
        'unique_top_result': unique['results'][0] if unique['results'] else None,
        'passed': bool(
            ranked['results']
            and ranked['results'][0]['url'].endswith('/alpha')
            and unique['results']
            and unique['results'][0]['url'].endswith('/gamma')
        ),
    }
    indexer.close()
    service.client.close()
    for filename in ('inverted_index_correctness.json', 'search_correctness.json'):
        (PROCESSED_DIR / filename).write_text(
            json.dumps(payload, indent=2, default=str),
            encoding='utf-8',
        )
    print(json.dumps(payload, indent=2, default=str))
    return payload


def run_fault_tolerance_experiment(redis_client, db):
    print('Running worker failure experiment')
    clean_datastores(redis_client, db)
    seed_urls = [f'http://127.0.0.1:{port}/' for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT)]
    run_command([sys.executable, 'src/v3/master_v3.py', 'seed'] + seed_urls)

    processes = []
    started = time.perf_counter()
    for index in range(1, 5):
        worker_id = f'fault-{index}'
        log_path = LOG_DIR / f'fault_{worker_id}.log'
        log_file = log_path.open('w', encoding='utf-8')
        process = subprocess.Popen(
            [
                sys.executable,
                'src/v3/worker_v3.py',
                '--worker-id', worker_id,
                '--max-pages', '20',
                '--idle-timeout', '8',
                '--batch-size', '5',
                '--processing-timeout', '1',
            ],
            cwd=PROJECT_ROOT,
            env=env(),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        processes.append((process, log_file, log_path, worker_id))
        time.sleep(0.05)

    time.sleep(1.5)
    killed_pid = processes[0][0].pid
    processes[0][0].terminate()
    # Allow enough time for stale processing recovery to trigger
    time.sleep(2.0)

    for process, log_file, _, _ in processes:
        try:
            process.wait(timeout=75)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
        log_file.close()

    if redis_client.hlen('crawler:processing') > 0:
        helper_id = 'fault-recovery-helper'
        log_path = LOG_DIR / f'fault_{helper_id}.log'
        log_file = log_path.open('w', encoding='utf-8')
        helper = subprocess.Popen(
            [
                sys.executable,
                'src/v3/worker_v3.py',
                '--worker-id', helper_id,
                '--max-pages', '20',
                '--idle-timeout', '12',
                '--batch-size', '5',
                '--processing-timeout', '1',
            ],
            cwd=PROJECT_ROOT,
            env=env(),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            helper.wait(timeout=45)
        except subprocess.TimeoutExpired:
            helper.terminate()
            helper.wait(timeout=5)
        finally:
            log_file.close()
        processes.append((helper, log_file, log_path, helper_id))

    elapsed = time.perf_counter() - started
    streaming_stats = drain_streaming_pipeline(max_rounds=200)
    worker_ids = sorted({
        row['_id']
        for row in db.pages_metadata.aggregate([
            {'$group': {'_id': '$worker_id'}}
        ])
        if row['_id']
    })
    recovered = sum(parse_worker_stats(log_path).get('recovered', 0) for _, _, log_path, _ in processes)
    row = {
        'scenario': 'one_worker_terminated',
        'workers_started': 4,
        'killed_worker_pid': killed_pid,
        'workers_seen_in_storage': json.dumps(worker_ids),
        'completed_pages': db.pages_metadata.count_documents({}),
        'documents_stored': db.pages_documents.count_documents({}),
        'completion_time_seconds': elapsed,
        'frontier_size_after_run': frontier_total(redis_client),
        'processing_size_after_run': redis_client.hlen('crawler:processing'),
        'recovered_urls': recovered,
        'index_outbox_pending': db.index_outbox.count_documents({'status': 'pending'}),
        'graph_outbox_pending': db.graph_outbox.count_documents({'status': 'pending'}),
        'term_postings_count': db.term_postings.count_documents({}),
        'pagerank_nodes_count': db.pagerank_nodes.count_documents({}),
        **streaming_stats,
    }
    write_csv(PROCESSED_DIR / 'fault_tolerance.csv', [row])
    print(json.dumps(row, indent=2))
    return row


def write_csv(path, rows):
    if not rows:
        return
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    ensure_dirs()
    if LOG_DIR.exists():
        shutil.rmtree(LOG_DIR)
        LOG_DIR.mkdir(parents=True, exist_ok=True)

    redis_client = Redis(host='localhost', port=6379, decode_responses=False)
    mongo = MongoClient(MONGO_URI)
    db = mongo[DATABASE_NAME]

    # Launch multi-domain fixture servers
    server_processes = start_all_fixture_servers()

    try:
        scaling_rows, worker_rows = run_scaling_experiment(redis_client, db)
        index_rows = run_indexing_experiment(db)
        query_rows = run_query_latency_experiment()
        correctness = run_correctness_experiment(db)
        fault = run_fault_tolerance_experiment(redis_client, db)

        payload = {
            'generated_at': datetime.now().isoformat(),
            'web_base_port': WEB_BASE_PORT,
            'domain_count': DOMAIN_COUNT,
            'api_port': API_PORT,
            'page_count': PAGE_COUNT,
            'target_pages': TARGET_PAGES,
            'worker_counts': WORKER_COUNTS,
            'index_doc_counts': INDEX_DOC_COUNTS,
            'query_runs': QUERY_RUNS,
            'scaling': scaling_rows,
            'worker_contribution': worker_rows,
            'indexing': index_rows,
            'query_latency': query_rows,
            'correctness': correctness,
            'fault_tolerance': fault,
        }
        (RAW_DIR / 'evaluation_results.json').write_text(
            json.dumps(payload, indent=2, default=str),
            encoding='utf-8',
        )
        print(f'Wrote raw results to {RAW_DIR / "evaluation_results.json"}')
    finally:
        stop_all_fixture_servers(server_processes)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
