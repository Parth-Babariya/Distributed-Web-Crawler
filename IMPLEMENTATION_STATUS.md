# Implementation Status

The current implementation is Dockerized end to end for a local distributed demo. See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the detailed architecture and fault-tolerance notes.

## Current Coverage

- Distributed crawler workers coordinate through Redis.
- Redis frontier, Bloom filter, processing leases, and politeness locks use atomic server-side operations where correctness requires it.
- MongoDB runs as a replica set and supports multi-document transactions.
- Page writes emit `index_outbox` and `graph_outbox` events in the same transaction as the page data.
- Raw compressed HTML is stored in MinIO/S3 through `S3HtmlBlobStore`; MongoDB stores only object pointers.
- Streaming indexer consumes `index_outbox` and updates `term_postings`, `inverted_index`, and `indexed_documents`.
- Incremental PageRank worker consumes `graph_outbox`, updates `graph_edges`, and maintains `pagerank_nodes`.
- Search API uses MongoDB aggregation to sort and limit results before returning data to FastAPI.
- Docker Compose includes Redis, MongoDB, MinIO, crawler workers, streaming indexer, PageRank worker, search API, and seed helper.

## Primary Demo Commands

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

## Verification Used

```bash
python -m unittest tests.test_core_unit
python -m compileall -q src tests scripts
docker compose config
docker compose build search-api crawler-worker streaming-indexer pagerank-worker seed
curl http://localhost:8000/health
```

Runtime checks previously passed with:

```text
redis: healthy
mongodb: healthy, mongodb:27017:PRIMARY
minio: healthy
crawler-worker: running
streaming-indexer: running
pagerank-worker: running
search-api: running
```

## Remaining Production Limits

The Compose stack provides application-level fault tolerance for local demos, but the infrastructure services are still single-node:

- Redis is not Sentinel/Cluster.
- MongoDB is a single-node replica set, not a three-node HA replica set.
- MinIO is single-node, not distributed MinIO.

For production machine-failure tolerance, deploy HA versions of those backing services.
