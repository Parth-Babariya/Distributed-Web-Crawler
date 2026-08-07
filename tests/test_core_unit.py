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
            text = '{}'

            def raise_for_status(self):
                raise AssertionError('should not be called for non-HTML')

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
                    }
                ])

        class FakeDocuments:
            def __init__(self):
                self.query = None

            def find(self, query, projection):
                self.query = query
                return [{'url': 'https://example.com/a', 'text': 'Crawler text.'}]

        service = SearchService.__new__(SearchService)
        service.postings = FakePostings()
        service.documents = FakeDocuments()
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
