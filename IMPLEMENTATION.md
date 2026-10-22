# Implementation

This document describes the current implementation of the distributed crawler and the demo topology used by `docker-compose.yml`.

## Runtime Topology

```text
seed
  -> Redis frontier / Bloom / leases / politeness
  -> crawler-worker replicas
  -> MinIO/S3 compressed HTML objects
  -> MongoDB transactional page collections
  -> durable outboxes
  -> streaming-indexer replicas
  -> pagerank-worker replicas
  -> search-api
```

## Docker Services

| Service | Role |
| --- | --- |
| `redis` | URL frontier, Bloom filter, in-flight URL leases, politeness locks, robots crawl-delay cache |
| `mongodb` | Single-node replica set `rs0` for transactions and persistent crawler/index/PageRank state |
| `minio` | S3-compatible shared object store for compressed HTML |
| `crawler-worker` | Fetches pages, extracts text/links, writes page data and outbox events |
| `streaming-indexer` | Consumes `index_outbox` and updates search indexes |
| `pagerank-worker` | Consumes `graph_outbox`, updates graph ledger, and propagates PageRank residuals |
| `search-api` | FastAPI query service |
| `seed` | One-shot helper that loads `seed_urls.txt` |

## Crawler Coordination

The crawler is decentralized. There is no master bottleneck in the hot path.

The Redis frontier is split into two tiers:

- `crawler:active_domains`: Redis list of domains with pending work.
- `crawler:frontier:<domain>`: per-domain priority sorted set.

`src/v3/frontier.py` uses Redis Lua scripts for enqueue, dequeue, and push-back so the active-domain list, known-domain set, and per-domain frontier stay coherent under many workers.

URL deduplication uses `src/v3/bloom_filter.py`. The Bloom filter is backed by a Redis bitmap. Adds are Lua-backed check-and-set operations so two workers cannot both reserve the same newly discovered URL.

In-flight crawl ownership uses `src/v3/processing_ledger.py`:

- `processing_lease:<url>` stores the owner worker ID with Redis-native TTL.
- `crawler:processing` indexes in-flight URLs for recovery.

Recovery scans the index and re-enqueues URLs whose Redis lease expired. This avoids cross-machine clock comparisons.

## Politeness

`src/v3/politeness.py` enforces per-domain crawl delay through Redis locks.

Locks are owner-aware:

- The lock value is the worker ID.
- Only the owner may shrink the lock to the post-fetch cooldown.
- If a stalled worker resumes after another worker acquired a newer lock, it cannot overwrite that newer lock.

`src/robots_handler_async.py` fetches robots.txt concurrently and caches `Crawl-delay` under the same bare-domain key used by the politeness manager.

## Storage

`src/v3/optimized_storage.py` stores page data in MongoDB and raw HTML in a shared object store.

MongoDB collections:

- `pages_metadata`: URL identity, domain, title, size fields, crawl version.
- `pages_content`: object-store pointer, content hash, outbound links.
- `pages_documents`: clean extracted text for indexing and snippets.
- `index_outbox`: durable queue for the streaming indexer.
- `graph_outbox`: durable queue for PageRank.

Compressed HTML is not stored in MongoDB. It is written through a blob-store abstraction:

- `LocalHtmlBlobStore`: local debugging fallback.
- `S3HtmlBlobStore`: S3/MinIO distributed path.

For Docker Compose, `CRAWLER_CONTENT_STORE=s3` and `S3_ENDPOINT_URL=http://minio:9000` make every container read/write the same MinIO bucket.

Page writes use MongoDB transactions. The crawler writes metadata, content pointer, clean document, `index_outbox`, and `graph_outbox` in the same transaction. If the transaction fails, no outbox event is emitted for a page that was not saved.

## Streaming Index

`src/indexer/streaming_indexer.py` consumes `index_outbox`.

Fault-tolerant lifecycle:

```text
pending -> processing -> done
```

Workers claim with `find_one_and_update`, setting `worker_id` and `claimed_at`. Stale `processing` events are reclaimed after a timeout.

Index state:

- `term_postings`: normalized postings, one document per `(term, url)`, carrying `term_frequency`, `doc_length` and an `in_title` flag for BM25.
- `indexed_documents`: per-URL term-count state used to remove stale postings on recrawl, plus `doc_length` for corpus statistics.

The legacy embedded-postings `inverted_index` collection is no longer written: per-term documents hit MongoDB's 16MB limit on common terms.

Tokenization and stemming live in `src/indexer/mapreduce_indexer.py` and are shared by indexing and search.

## Incremental PageRank

`src/indexer/pagerank_worker.py` consumes `graph_outbox`.

State collections:

- `graph_edges`: historical outbound-link ledger per source URL.
- `pagerank_nodes`: rank and residual per URL.

The worker calculates contribution deltas from edge changes, adds residuals to affected nodes, and pushes residuals through successors until below the configured threshold.

Ranks use the unnormalized equation `x = (1-d)·t + d·Aᵀx`: each node (or each seed URL, with `--seed-url`) carries teleport mass `1-d`, and dangling nodes route their rank to the seeds or drop it. A new node starts at rank 0 with residual `(1-d)·t`, so every update touches only the nodes it changes — no O(N) personalization vector. A page nobody links to has rank `1-d`. `--recompute` solves the same equation, so incremental and batch ranks agree (checked by `tests/test_pagerank_consistency.py`).

Fault-tolerant lifecycle:

- Outbox events are atomically claimed.
- Stale event claims are reclaimed.
- Residual node claims are also reclaimable.
- A full recompute mode is available through power iteration:

```bash
docker compose run --rm pagerank-worker python pagerank_worker.py --recompute
```

## Search

`src/search/search_api.py` serves `/search`.

Search uses the same tokenizer/stemmer as the indexer. Ranking is performed inside MongoDB aggregation:

1. Count each term's document frequency and read corpus statistics (document count, average length) from `indexed_documents`, cached for 30s.
2. Match query terms in `term_postings` and score each posting with Okapi BM25 (`k1=1.2`, `b=0.75`), x1.5 for title matches.
3. Group by URL and sum; with `mode=all`, keep only URLs that matched every term.
4. Keep the best `10 x limit` (at least 100) candidates by BM25.
5. Lookup PageRank for those candidates only and score `bm25 * (1 + 0.5 * ln(1 + rank / (1-d)))`.
6. Sort and limit in MongoDB, then fetch snippets only for final top URLs.

This avoids loading huge postings lists into FastAPI memory.

## Docker Demo Commands

Fresh bounded demo:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build redis mongodb minio streaming-indexer pagerank-worker search-api
docker compose --profile tools run --rm seed
docker compose run --rm crawler-worker python src/v3/worker_v3.py --worker-id demo-worker --max-pages 30 --idle-timeout 45 --batch-size 5
docker compose exec -T streaming-indexer python streaming_indexer.py --once --max-events 1000
docker compose exec -T pagerank-worker python pagerank_worker.py --once --max-events 1000 --max-pushes 2000
curl "http://localhost:8000/search?q=cricket&limit=5"
```

Scaled live demo:

```bash
docker compose up -d --build
docker compose --profile tools run --rm seed
docker compose up -d --scale crawler-worker=3 --scale streaming-indexer=2 --scale pagerank-worker=2
docker compose logs -f crawler-worker streaming-indexer pagerank-worker
```

Stop crawlers after the demo has enough data:

```bash
docker compose stop crawler-worker
```

## Verification

Unit tests:

```bash
python -m unittest tests.test_core_unit
```

Compile/import check:

```bash
python -m compileall -q src tests scripts
```

Compose validation:

```bash
docker compose config
docker compose build search-api crawler-worker streaming-indexer pagerank-worker seed
```

Runtime checks:

```bash
docker compose ps
curl http://localhost:8000/health
docker compose exec -T mongodb mongosh --quiet --eval "rs.status().members.map(m => m.name + ':' + m.stateStr).join(',')"
```

## Fault Tolerance Summary

| Failure | Current behavior |
| --- | --- |
| Crawler worker dies mid-fetch | URL lease expires in Redis and another worker can recover it |
| Crawler worker dies before storage flush | URL lease expires; page was not marked complete |
| Mongo page transaction fails | URL is re-enqueued; outbox remains clean |
| Indexer dies after claiming event | Watchdog returns event to `pending` |
| PageRank worker dies after claiming event | Watchdog returns event to `pending` |
| PageRank worker dies while pushing residual | Residual claim is reclaimed |
| API receives common query term | MongoDB sorts/limits results before FastAPI receives them |
| Worker writes raw HTML | Blob is stored in shared MinIO/S3, not per-machine disk |

## Known Limits

The Compose setup is suitable for local demos and application-level distributed behavior. It is not infrastructure HA:

- Redis is a single container, not Sentinel or Cluster.
- MongoDB is a single-node replica set for transactions, not a three-node HA replica set.
- MinIO is single-node, not distributed MinIO.

For production machine-failure tolerance, deploy Redis Sentinel/Cluster, a multi-node MongoDB replica set, and distributed MinIO or real S3.
