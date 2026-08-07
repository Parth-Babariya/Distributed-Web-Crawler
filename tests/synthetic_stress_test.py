#!/usr/bin/env python3
"""
Synthetic stress test for the distributed crawler.

This test avoids public websites so results are repeatable. It starts a local
HTTP site with many linked HTML pages, crawls it with different worker counts,
builds the inverted index, runs a search query, and writes metrics/graphs.
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt
import psutil
from pymongo import MongoClient
from redis import Redis

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'src'))
sys.path.insert(0, str(PROJECT_ROOT / 'src' / 'v3'))

from indexer.mapreduce_indexer import MapReduceIndexer
from master_v3 import MasterV3
from search.search_api import SearchService
from worker_v3 import DecentralizedWorker

RESULTS_DIR = PROJECT_ROOT / 'results'
GRAPHS_DIR = RESULTS_DIR / 'graphs'
PORT = 9090
PAGE_COUNT = 160
WORKER_COUNTS = [1, 2, 4, 8]
TARGET_PAGES = 80


class SyntheticSiteHandler(BaseHTTPRequestHandler):
    page_count = PAGE_COUNT

    def do_GET(self):
        if self.path == '/robots.txt':
            self._send_text('User-agent: *\nAllow: /\nCrawl-delay: 0.001\n')
            return

        if self.path in ('/', '/page/0'):
            page_id = 0
        elif self.path.startswith('/page/'):
            try:
                page_id = int(self.path.rsplit('/', 1)[1])
            except ValueError:
                self.send_error(404)
                return
        else:
            self.send_error(404)
            return

        if page_id < 0 or page_id >= self.page_count:
            self.send_error(404)
            return

        links = []
        for offset in (1, 2, 3):
            next_id = page_id * 3 + offset
            if next_id < self.page_count:
                links.append(f'<a href="/page/{next_id}">Synthetic page {next_id}</a>')

        body = f"""
        <html>
          <head><title>Synthetic Page {page_id}</title></head>
          <body>
            <h1>Synthetic distributed crawler page {page_id}</h1>
            <p>
              This benchmark page tests crawler throughput, indexing speed,
              Redis frontier behavior, MongoDB storage, and search latency.
              Topic group {page_id % 7} repeats crawler search benchmark terms.
            </p>
            {' '.join(links)}
          </body>
        </html>
        """
        self._send_html(body)

    def _send_html(self, body):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def _send_text(self, body):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def log_message(self, format, *args):
        return


class SyntheticStressTest:
    def __init__(self):
        self.redis = Redis(host='localhost', port=6379, decode_responses=False)
        self.mongo = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo['web_crawler']
        RESULTS_DIR.mkdir(exist_ok=True)
        GRAPHS_DIR.mkdir(exist_ok=True)

    def clean(self):
        for pattern in ('crawler:*', 'lock:*', 'robots_cache:*'):
            for key in self.redis.scan_iter(pattern):
                self.redis.delete(key)

        for collection in [
            'pages_metadata',
            'pages_content',
            'pages_documents',
            'inverted_index',
        ]:
            self.db[collection].delete_many({})

    def run_workers(self, worker_count):
        pages_per_worker = max(TARGET_PAGES // worker_count, 1)
        threads = []

        def run_worker(index):
            worker = DecentralizedWorker(
                worker_id=f'stress-{worker_count}-{index}',
                batch_size=10
            )
            worker.start(max_pages=pages_per_worker, idle_timeout=6)

        start = time.perf_counter()
        for index in range(worker_count):
            thread = threading.Thread(target=run_worker, args=(index + 1,))
            thread.start()
            threads.append(thread)
            time.sleep(0.05)

        for thread in threads:
            thread.join()

        return time.perf_counter() - start

    def run_one(self, worker_count):
        self.clean()
        seed_url = f'http://127.0.0.1:{PORT}/page/0'
        MasterV3().seed_urls([seed_url])

        cpu_samples = []
        memory_samples = []
        stop_sampling = threading.Event()

        def sample_system():
            while not stop_sampling.is_set():
                cpu_samples.append(psutil.cpu_percent(interval=0.2))
                memory_samples.append(psutil.virtual_memory().used / (1024 * 1024))

        sampler = threading.Thread(target=sample_system)
        sampler.start()

        crawl_elapsed = self.run_workers(worker_count)
        stop_sampling.set()
        sampler.join()

        pages_stored = self.db.pages_metadata.count_documents({})
        documents_stored = self.db.pages_documents.count_documents({})
        frontier_size = self.redis.zcard('crawler:frontier')
        processing_size = self.redis.hlen('crawler:processing')
        redis_memory_mb = self.redis.info('memory')['used_memory'] / (1024 * 1024)

        storage_stats = list(self.db.pages_metadata.aggregate([
            {
                '$group': {
                    '_id': None,
                    'content_size': {'$sum': '$content_size'},
                    'compressed_size': {'$sum': '$compressed_size'},
                }
            }
        ]))
        storage = storage_stats[0] if storage_stats else {}
        content_size = storage.get('content_size', 0)
        compressed_size = storage.get('compressed_size', 0)
        compression_saved_percent = (
            (1 - compressed_size / content_size) * 100 if content_size else 0
        )

        indexer = MapReduceIndexer(batch_size=100)
        index_start = time.perf_counter()
        index_stats = indexer.build_index(reset=True)
        index_elapsed = time.perf_counter() - index_start
        indexer.close()

        search_service = SearchService()
        latencies = []
        result_counts = []
        for _ in range(10):
            search_start = time.perf_counter()
            results = search_service.search('crawler benchmark', limit=10)
            latencies.append((time.perf_counter() - search_start) * 1000)
            result_counts.append(len(results['results']))

        return {
            'worker_count': worker_count,
            'target_pages': TARGET_PAGES,
            'pages_stored': pages_stored,
            'documents_stored': documents_stored,
            'crawl_elapsed_seconds': crawl_elapsed,
            'throughput_pages_per_second': pages_stored / crawl_elapsed if crawl_elapsed else 0,
            'frontier_size_after_run': frontier_size,
            'processing_size_after_run': processing_size,
            'redis_memory_mb': redis_memory_mb,
            'content_size_mb': content_size / (1024 * 1024),
            'compressed_size_mb': compressed_size / (1024 * 1024),
            'compression_saved_percent': compression_saved_percent,
            'index_elapsed_seconds': index_elapsed,
            'index_documents': index_stats['documents_indexed'],
            'index_terms_total': index_stats['index_terms_total'],
            'avg_search_latency_ms': mean(latencies),
            'max_search_latency_ms': max(latencies),
            'avg_search_results': mean(result_counts),
            'avg_cpu_percent': mean(cpu_samples) if cpu_samples else 0,
            'avg_system_memory_mb': mean(memory_samples) if memory_samples else 0,
        }

    def plot(self, results):
        workers = [row['worker_count'] for row in results]

        plots = [
            ('throughput_pages_per_second', 'Throughput by Worker Count', 'Pages / second', 'worker_scaling.png'),
            ('crawl_elapsed_seconds', 'Crawl Time by Worker Count', 'Seconds', 'crawl_time.png'),
            ('index_elapsed_seconds', 'Index Build Time by Worker Count', 'Seconds', 'index_time.png'),
            ('avg_search_latency_ms', 'Average Search Latency', 'Milliseconds', 'search_latency.png'),
            ('redis_memory_mb', 'Redis Memory Usage', 'MB', 'redis_memory.png'),
            ('compression_saved_percent', 'MongoDB Compression Savings', 'Percent', 'compression_savings.png'),
        ]

        for metric, title, ylabel, filename in plots:
            values = [row[metric] for row in results]
            plt.figure(figsize=(8, 5))
            plt.plot(workers, values, marker='o', linewidth=2)
            plt.title(title)
            plt.xlabel('Worker count')
            plt.ylabel(ylabel)
            plt.grid(True, alpha=0.3)
            plt.xticks(workers)
            plt.tight_layout()
            plt.savefig(GRAPHS_DIR / filename, dpi=160)
            plt.close()

    def run(self):
        server = ThreadingHTTPServer(('127.0.0.1', PORT), SyntheticSiteHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        try:
            results = []
            for worker_count in WORKER_COUNTS:
                print(f'Running stress test with {worker_count} workers...')
                row = self.run_one(worker_count)
                results.append(row)
                print(json.dumps(row, indent=2))

            payload = {
                'test_date': datetime.now().isoformat(),
                'page_count': PAGE_COUNT,
                'target_pages': TARGET_PAGES,
                'worker_counts': WORKER_COUNTS,
                'results': results,
            }

            output_file = RESULTS_DIR / 'synthetic_stress_results.json'
            output_file.write_text(json.dumps(payload, indent=2), encoding='utf-8')
            self.plot(results)

            print(f'Wrote metrics: {output_file}')
            print(f'Wrote graphs: {GRAPHS_DIR}')
        finally:
            server.shutdown()


def main():
    logging.basicConfig(level=logging.WARNING)
    test = SyntheticStressTest()
    test.run()


if __name__ == '__main__':
    main()
