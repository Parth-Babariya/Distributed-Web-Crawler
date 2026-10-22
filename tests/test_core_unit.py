import os
import sys
import tempfile
import unittest
import requests

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src', 'v3'))

from indexer.mapreduce_indexer import MapReduceIndexer
from optimized_storage import LocalHtmlBlobStore, OptimizedStorage
from robots_handler_async import AsyncRobotsHandler
from search.search_api import SearchService
from worker_v3 import DecentralizedWorker, PageFetchResult


class CoreCrawlerTests(unittest.TestCase):
    def test_fetch_timeout_returns_none_and_counts_timeout(self):
        class TimeoutSession:
            def get(self, *args, **kwargs):
                raise requests.Timeout()

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.session = TimeoutSession()
        worker.stats = {'timeouts': 0, 'errors': 0}
        worker._random_user_agent = lambda: 'unit-test'

        self.assertIsNone(worker.fetch_page('http://example.com/slow'))
        self.assertEqual(worker.stats['timeouts'], 1)

    def test_non_html_response_returns_none(self):
        class Response:
            headers = {'Content-Type': 'application/json'}
            url = 'http://example.com/data.json'

            def raise_for_status(self):
                raise AssertionError('should not be called for non-HTML')

            def close(self):
                pass

        class Session:
            def get(self, *args, **kwargs):
                return Response()

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.session = Session()
        worker.stats = {'timeouts': 0, 'errors': 0}
        worker._random_user_agent = lambda: 'unit-test'

        self.assertIsNone(worker.fetch_page('http://example.com/data.json'))

    def test_aiohttp_fetcher_result_sets_final_url(self):
        class Fetcher:
            def fetch(self, url, user_agent):
                return PageFetchResult(
                    html='<html><body>ok</body></html>',
                    final_url='HTTP://Example.COM:80/final#section',
                    content_type='text/html',
                    status=200,
                )

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.page_fetcher = Fetcher()
        worker.stats = {'timeouts': 0, 'errors': 0}
        worker._random_user_agent = lambda: 'unit-test'

        html = worker.fetch_page('http://example.com/start')

        self.assertEqual(html, '<html><body>ok</body></html>')
        self.assertEqual(worker.last_final_url, 'http://example.com/final')

    @staticmethod
    def _requests_worker(status=200, headers=None, chunks=(b'<html>ok</html>',), max_bytes=1024):
        class Response:
            def __init__(self):
                self.status_code = status
                self.headers = {'Content-Type': 'text/html; charset=utf-8', **(headers or {})}
                self.url = 'http://example.com/page'
                self.encoding = 'utf-8'
                self.closed = False

            def raise_for_status(self):
                if status >= 400:
                    raise requests.HTTPError(f'{status}')

            def iter_content(self, chunk_size):
                yield from chunks

            def close(self):
                self.closed = True

        class Session:
            def get(self, *args, **kwargs):
                self.response = Response()
                return self.response

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.session = Session()
        worker.max_page_bytes = max_bytes
        return worker

    def test_http_4xx_is_permanent_but_5xx_408_429_are_transient(self):
        for status, permanent in [(404, True), (403, True), (410, True),
                                  (408, False), (429, False), (503, False)]:
            worker = self._requests_worker(status=status)
            outcome = worker._fetch_outcome('http://example.com/page', 'ua')
            self.assertIsNone(outcome.html)
            self.assertEqual(outcome.permanent, permanent, status)
            self.assertTrue(worker.session.response.closed)

    def test_non_html_is_permanent(self):
        worker = self._requests_worker(headers={'Content-Type': 'application/pdf'})
        outcome = worker._fetch_outcome('http://example.com/doc', 'ua')
        self.assertTrue(outcome.permanent)

    def test_oversized_declared_length_is_rejected_before_reading(self):
        def never_read():
            raise AssertionError('body should not be read')
            yield

        worker = self._requests_worker(headers={'Content-Length': '5000'}, chunks=never_read())
        outcome = worker._fetch_outcome('http://example.com/big', 'ua')
        self.assertEqual(outcome.error, 'too_large')
        self.assertTrue(outcome.permanent)

    def test_oversized_streamed_body_is_rejected(self):
        worker = self._requests_worker(chunks=[b'x' * 600, b'x' * 600])
        outcome = worker._fetch_outcome('http://example.com/big', 'ua')
        self.assertEqual(outcome.error, 'too_large')

    def test_body_within_cap_is_decoded(self):
        worker = self._requests_worker(chunks=[b'<p>caf', b'\xc3\xa9</p>'])
        outcome = worker._fetch_outcome('http://example.com/page', 'ua')
        self.assertEqual(outcome.html, '<p>café</p>')
        self.assertEqual(outcome.final_url, 'http://example.com/page')

    def test_aiohttp_response_error_status_is_classified(self):
        class ResponseError(Exception):
            def __init__(self, status):
                super().__init__(status)
                self.status = status

        class Fetcher:
            def __init__(self, status):
                self.status = status

            def fetch(self, url, user_agent):
                raise ResponseError(self.status)

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.page_fetcher = Fetcher(404)
        self.assertTrue(worker._fetch_outcome('http://example.com/x', 'ua').permanent)
        worker.page_fetcher = Fetcher(502)
        self.assertFalse(worker._fetch_outcome('http://example.com/x', 'ua').permanent)

    def test_mark_failed_drops_permanent_and_requeues_transient(self):
        class Ledger:
            def release(self, url, worker_id):
                return True

        class FakeFrontier:
            def __init__(self):
                self.enqueued = []

            def enqueue(self, url, priority, url_json):
                self.enqueued.append(url)

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.ledger = Ledger()
        worker.frontier = FakeFrontier()
        worker.max_retries = 3

        worker.mark_failed({'url': 'http://example.com/gone', 'priority': 50}, permanent=True)
        worker.mark_failed({'url': 'http://example.com/flaky', 'priority': 50})

        self.assertEqual(worker.frontier.enqueued, ['http://example.com/flaky'])

    def test_dispatch_counts_batched_and_in_flight_pages_toward_max_pages(self):
        class Storage:
            def pending_count(self):
                return 2

        class Pool:
            def __init__(self):
                self.submitted = []

            def submit(self, fn, url, user_agent):
                self.submitted.append(url)
                return object()

        urls = iter([{'url': f'http://site{i}.test/'} for i in range(10)])
        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.concurrency = 4
        worker.stats = {'pages_crawled': 5}
        worker.storage = Storage()
        worker.domain_user_agents = {}
        worker._random_user_agent = lambda: 'ua'
        worker.pull_url_from_frontier = lambda: next(urls)
        pool = Pool()
        in_flight = {object(): {'url': 'http://busy.test/'}}

        # 5 stored + 2 batched + 1 in flight leaves room for 1 more under 9.
        self.assertEqual(worker._dispatch_fetches(pool, in_flight, max_pages=9), 1)
        # Without a cap, the remaining concurrency slots are filled.
        self.assertEqual(worker._dispatch_fetches(pool, in_flight, max_pages=None), 2)
        self.assertEqual(len(in_flight), 4)

    def test_dispatch_stops_at_first_empty_pull(self):
        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.concurrency = 8
        worker.pull_url_from_frontier = lambda: None
        self.assertEqual(worker._dispatch_fetches(None, {}, max_pages=None), 0)

    def test_user_agent_is_stable_per_domain(self):
        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.domain_user_agents = {}
        agents = iter(['agent-a', 'agent-b'])
        worker._random_user_agent = lambda: next(agents)

        first = worker._user_agent_for_url('https://example.com/one')
        second = worker._user_agent_for_url('https://example.com/two')
        third = worker._user_agent_for_url('https://other.example/one')

        self.assertEqual(first, 'agent-a')
        self.assertEqual(second, 'agent-a')
        self.assertEqual(third, 'agent-b')

    def test_url_normalization_strips_fragments_and_default_ports(self):
        worker = DecentralizedWorker.__new__(DecentralizedWorker)

        self.assertEqual(
            worker.normalize_url('HTTP://Example.COM:80/path?q=1#section'),
            'http://example.com/path?q=1'
        )
        self.assertEqual(
            worker.normalize_url('https://Example.COM:443'),
            'https://example.com/'
        )

    def test_link_extraction_resolves_relative_and_removes_fragments(self):
        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.stats = {'links_extracted': 0}

        links = worker.parse_and_extract_links(
            '<a href="/alpha#top">A</a><a href="beta">B</a><a href="mailto:x@y.com">Mail</a>',
            'http://example.com/base/page'
        )

        self.assertIn('http://example.com/alpha', links)
        self.assertIn('http://example.com/base/beta', links)
        self.assertNotIn('mailto:x@y.com', links)

    def test_process_links_deduplicates_same_page_links_before_enqueue(self):
        class Bloom:
            def __init__(self):
                self.urls = set()

            def contains(self, url):
                return url in self.urls

            def add(self, url):
                if url in self.urls:
                    return False
                self.urls.add(url)
                return True

            def add_many(self, urls):
                return [self.add(url) for url in urls]

        class Robots:
            async def can_fetch_batch(self, urls):
                return {url: True for url in urls}

        class FakeFrontier:
            """Captures (url, priority, json) tuples handed to enqueue.

            Mirrors the real Frontier.enqueue contract used by the worker
            after the two-tier rewrite — process_links no longer touches
            redis.zadd directly, it goes through self.frontier.
            """

            def __init__(self):
                self.enqueued = []

            def enqueue(self, url, priority, url_json):
                self.enqueued.append((url, priority, url_json))
                return True

        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'
        worker.bloom_filter = Bloom()
        worker.robots_handler_async = Robots()
        worker.frontier = FakeFrontier()
        worker.stats = {
            'links_duplicate': 0,
            'links_robots_blocked': 0,
            'links_added': 0,
        }

        worker.process_links(
            [
                'http://example.com/alpha#top',
                'http://example.com/alpha#bottom',
                'http://example.com/beta',
            ],
            'http://example.com/',
            0
        )

        self.assertEqual(worker.stats['links_added'], 2)
        self.assertEqual(worker.stats['links_duplicate'], 1)
        # Both unique URLs reached the two-tier frontier exactly once.
        enqueued_urls = {url for url, _, _ in worker.frontier.enqueued}
        self.assertEqual(
            enqueued_urls,
            {'http://example.com/alpha', 'http://example.com/beta'},
        )

    def test_extract_text_removes_scripts_and_title(self):
        worker = DecentralizedWorker.__new__(DecentralizedWorker)
        worker.worker_id = 'unit-test'

        document = worker.extract_text("""
            <html>
              <head><title>Example Title</title><script>ignoreMe()</script></head>
              <body><h1>Hello Search</h1><p>Distributed crawler text.</p></body>
            </html>
        """)

        self.assertEqual(document['title'], 'Example Title')
        self.assertIn('Hello Search', document['text'])
        self.assertIn('Distributed crawler text.', document['text'])
        self.assertNotIn('ignoreMe', document['text'])

    def test_indexer_tokenize_filters_stop_words(self):
        indexer = MapReduceIndexer.__new__(MapReduceIndexer)

        tokens = indexer.tokenize('The distributed crawler indexes crawler pages analysis.')

        self.assertNotIn('the', tokens)
        self.assertEqual(tokens.count('crawl'), 2)
        self.assertIn('distribut', tokens)
        self.assertIn('index', tokens)
        self.assertIn('page', tokens)
        self.assertIn('analysis', tokens)

    def test_indexer_tokenize_stems_common_crawler_variants(self):
        indexer = MapReduceIndexer.__new__(MapReduceIndexer)

        tokens = indexer.tokenize('crawler crawling crawled crawlers')

        self.assertEqual(tokens, ['crawl', 'crawl', 'crawl', 'crawl'])

    def test_robots_crawl_delay_cache_uses_bare_domain_key(self):
        class FakeRedis:
            def __init__(self):
                self.values = {}
                self.hashes = {}

            def setex(self, key, ttl, value):
                self.values[key] = (ttl, value)

            def hset(self, key, field, value):
                self.hashes[(key, field)] = value

        handler = AsyncRobotsHandler.__new__(AsyncRobotsHandler)
        handler.redis = FakeRedis()
        handler.cache_ttl = 3600

        handler._cache_crawl_delay('example.com', 'User-agent: *\nCrawl-delay: 7')

        self.assertEqual(
            handler.redis.values['crawler:robots:delay:example.com'],
            (3600, '7.0'),
        )
        self.assertEqual(
            handler.redis.hashes[('crawler:domain_state:example.com', 'crawl_delay')],
            '7.0',
        )
        self.assertNotIn('crawler:robots:delay:https://example.com', handler.redis.values)

    def test_map_reduce_groups_postings_by_term(self):
        indexer = MapReduceIndexer.__new__(MapReduceIndexer)

        mapped = [
            {
                'crawler': {'url': 'https://a.test', 'term_frequency': 2},
                'search': {'url': 'https://a.test', 'term_frequency': 1},
            },
            {
                'crawler': {'url': 'https://b.test', 'term_frequency': 1},
            },
        ]

        reduced = indexer.reduce_postings(mapped)

        self.assertEqual(len(reduced['crawler']), 2)
        self.assertEqual(len(reduced['search']), 1)

    def test_search_uses_term_postings_aggregation_and_limits_in_database(self):
        """Search must run the ranking aggregation against term_postings and
        push sort/limit into MongoDB so FastAPI never holds full posting
        lists in memory. The legacy inverted_index pipeline (which used
        $unwind on an embedded postings array) has been removed because it
        couldn't survive MongoDB's 16MB BSON document limit on common terms.
        """
        class FakePostings:
            def __init__(self):
                self.pipeline = None
                self.allow_disk_use = False

            def count_documents(self, query):
                return 1

            def aggregate(self, pipeline, allowDiskUse=False):
                self.pipeline = pipeline
                self.allow_disk_use = allowDiskUse
                return iter([
                    {
                        'url': 'https://example.com/a',
                        'title': 'A',
                        'score': 2.0,
                        'term_score': 2,
                        'authority_score': 1.0,
                        'matched_terms': ['crawl'],
                    }
                ])

        class FakeIndexedDocuments:
            def aggregate(self, pipeline):
                return iter([{'_id': None, 'doc_count': 10, 'avg_doc_length': 50.0}])

        class FakeDocuments:
            def __init__(self):
                self.query = None

            def find(self, query, projection):
                self.query = query
                return [{'url': 'https://example.com/a', 'text': 'Crawler text.'}]

        service = SearchService.__new__(SearchService)
        service.postings = FakePostings()
        service.documents = FakeDocuments()
        service.indexed_documents = FakeIndexedDocuments()
        service.indexer = MapReduceIndexer.__new__(MapReduceIndexer)

        response = service.search('crawler', limit=1)

        # term_postings is one-doc-per-(term,url), so $unwind is unnecessary
        # and would be a regression to the legacy embedded-array layout.
        self.assertNotIn({'$unwind': '$postings'}, service.postings.pipeline)
        self.assertIn({'$limit': 1}, service.postings.pipeline)
        self.assertTrue(service.postings.allow_disk_use)
        self.assertEqual(
            service.documents.query,
            {'url': {'$in': ['https://example.com/a']}},
        )
        self.assertEqual(response['results'][0]['url'], 'https://example.com/a')
        self.assertEqual(response['results'][0]['score'], 2.0)

    def test_content_doc_uses_blob_pointer_not_mongo_html_blob(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedStorage.__new__(OptimizedStorage)
            storage.html_blobs = LocalHtmlBlobStore(tmpdir)
            storage.metadata_batch = []
            storage.content_batch = []
            storage.document_batch = []
            storage._pending_url_for_page = {}
            storage.stats = {
                'bytes_original': 0,
                'bytes_compressed': 0,
            }

            storage.add_page(
                url='https://example.com/page',
                html='<html><body>hello crawler</body></html>',
                links=['https://example.com/next'],
                domain='example.com',
                title='Example',
                text='hello crawler',
            )

            content_doc = storage.content_batch[0]

            self.assertIn('content_path', content_doc)
            self.assertNotIn('compressed_html', content_doc)
            self.assertTrue(os.path.exists(os.path.join(tmpdir, content_doc['content_path'])))
            self.assertEqual(
                storage._read_html_from_content_doc(content_doc),
                '<html><body>hello crawler</body></html>',
            )

    def test_content_doc_records_configured_shared_blob_store(self):
        class FakeSharedBlobStore:
            store_name = 's3'
            bucket = 'crawler-html'

            def write(self, content_hash, compressed_html):
                self.compressed_html = compressed_html
                return f'html/{content_hash}.zlib'

            def read(self, content_path):
                return self.compressed_html

            def uri_for(self, content_path):
                return f's3://{self.bucket}/{content_path}'

        storage = OptimizedStorage.__new__(OptimizedStorage)
        storage.html_blobs = FakeSharedBlobStore()
        storage.metadata_batch = []
        storage.content_batch = []
        storage.document_batch = []
        storage._pending_url_for_page = {}
        storage.stats = {
            'bytes_original': 0,
            'bytes_compressed': 0,
        }

        storage.add_page(
            url='https://example.com/shared',
            html='<html><body>shared crawler blob</body></html>',
            links=[],
            domain='example.com',
            title='Shared',
            text='shared crawler blob',
        )

        content_doc = storage.content_batch[0]

        self.assertEqual(content_doc['content_store'], 's3')
        self.assertEqual(content_doc['content_bucket'], 'crawler-html')
        self.assertTrue(content_doc['content_uri'].startswith('s3://crawler-html/'))
        self.assertEqual(
            storage._read_html_from_content_doc(content_doc),
            '<html><body>shared crawler blob</body></html>',
        )


if __name__ == '__main__':
    unittest.main()
