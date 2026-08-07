# Distributed Web Crawler

A Dockerized distributed web crawler with Redis coordination, MongoDB storage,
MinIO/S3 HTML blob storage, streaming indexing, incremental PageRank, and a
FastAPI search API.

The first section is the recommended automatic Docker flow: no manual index
drain, no manual PageRank drain, and no manual crawler stop.

## Run Order

| Order | What to run | Why |
|---|---|---|
| 1 | **Automatic Docker Run** | Runs the full system automatically with Docker. |
| 2 | **Search Edge Cases** | Checks top-k search and unknown-term behavior. |
| 3 | **Docker Desktop Inspection** | Checks containers, logs, MongoDB state, and MinIO state. |
| 4 | **Automated Unit Tests** | Checks core logic correctness. |
| 5 | **Automated Distributed Fault Test** | Checks worker-kill recovery and deterministic search correctness. |
| 6 | **Optional Manual Fault Tests** | Extra failure checks when needed. |
| 7 | **Report/Result Scripts** | Regenerates graphs and tables. |

Important: `tests/distributed_system_test.py` cleans Redis/MongoDB test state.
Run it after the automatic Docker run, or run the Docker flow again afterward.

## Components

| Service | Role |
|---|---|
| `redis` | URL frontier, Bloom filter, crawl leases, domain locks, robots cache. |
| `mongodb` | Durable database. Runs as replica set `rs0` so transactions work. |
| `minio` | Shared S3-compatible object store for compressed raw HTML. |
| `crawler-worker` | Fetches pages, extracts text/links, writes page data and events. |
| `streaming-indexer` | Automatically consumes index events and updates `term_postings`. |
| `pagerank-worker` | Automatically consumes graph events and updates PageRank state. |
| `search-api` | FastAPI service for `/health` and `/search`. |
| `seed` | One-shot helper that loads `seed_urls.txt` into Redis. |

## Default Run Behavior

`docker-compose.yml` is configured for a bounded Docker run:

```yaml
crawler-worker:
  command: ["python", "src/v3/worker_v3.py", "--max-pages", "30", "--idle-timeout", "120", "--batch-size", "5"]
  restart: "no"
```

Meaning:

| Setting | Meaning |
|---|---|
| `--max-pages 30` | Each crawler container stores at most 30 pages, then exits. |
| `--idle-timeout 120` | If no crawlable URL appears for 120 seconds, the worker exits. |
| `--batch-size 5` | Pages are saved in small transaction batches. |
| `restart: "no"` | Docker does not restart a worker after it finishes. |

If you run three workers, the system can store up to roughly `3 x 30 = 90`
pages. It may store fewer if URLs are duplicates, blocked, unreachable, or the
frontier becomes idle.

## Automatic Docker Run

Run every command from the project root:

```powershell
cd C:\Users\Asus\Desktop\Distributed-Web-Crawler-main\Distributed-Web-Crawler-main
```

### Quick command list

Copy-paste these commands in order:

```powershell
docker compose down -v --remove-orphans
docker compose up -d --build redis mongodb minio streaming-indexer pagerank-worker search-api
docker compose ps
docker compose exec -T mongodb mongosh --quiet --eval "rs.status().members.map(m => m.name + ':' + m.stateStr).join(',')"
docker compose --profile tools run --rm seed
docker compose up -d --scale crawler-worker=3
docker compose logs -f crawler-worker streaming-indexer pagerank-worker
```

When logs indicate the workers have finished, press `Ctrl+C` and continue:

```powershell
docker compose ps -a crawler-worker
Start-Sleep -Seconds 20
docker compose exec -T mongodb mongosh --quiet --eval "const d=db.getSiblingDB('web_crawler'); printjson({pages:d.pages_metadata.countDocuments(), documents:d.pages_documents.countDocuments(), postings:d.term_postings.countDocuments(), indexedDocs:d.indexed_documents.countDocuments(), indexPending:d.index_outbox.countDocuments({status:'pending'}), indexProcessing:d.index_outbox.countDocuments({status:'processing'}), graphPending:d.graph_outbox.countDocuments({status:'pending'}), graphProcessing:d.graph_outbox.countDocuments({status:'processing'}), graphDone:d.graph_outbox.countDocuments({status:'done'}), edges:d.graph_edges.countDocuments(), pagerank:d.pagerank_nodes.countDocuments()})"
curl.exe http://localhost:8000/health
curl.exe "http://localhost:8000/search?q=cricket&limit=5"
```

If the search term has no results, list indexed terms and search one of them:

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('web_crawler').term_postings.distinct('term').slice(0,20)"
curl.exe "http://localhost:8000/search?q=<term_from_previous_command>&limit=5"
```

The detailed explanation for each command is below.

### Step 1: Start clean

```powershell
docker compose down -v --remove-orphans
```

Why: removes old containers and old Redis/MongoDB/MinIO volumes.

### Step 2: Start infrastructure, background processors, and API

```powershell
docker compose up -d --build redis mongodb minio streaming-indexer pagerank-worker search-api
```

Why: starts the services that should be alive before crawling begins.

What is automatic now:

- `streaming-indexer` automatically consumes `index_outbox`.
- `pagerank-worker` automatically consumes `graph_outbox`.
- `search-api` is ready to serve queries.

### Step 3: Check service status

```powershell
docker compose ps
```

Expected: `redis`, `mongodb`, `minio`, `streaming-indexer`,
`pagerank-worker`, and `search-api` are running/healthy.

### Step 4: Verify MongoDB replica set

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "rs.status().members.map(m => m.name + ':' + m.stateStr).join(',')"
```

Expected:

```text
mongodb:27017:PRIMARY
```

Why: MongoDB transactions require a replica set primary.

### Step 5: Seed starting URLs

```powershell
docker compose --profile tools run --rm seed
```

Why: reads `seed_urls.txt` and inserts starting URLs into Redis. Workers then
pull URLs from Redis and discover more links while crawling.

### Step 6: Start three bounded crawler workers

```powershell
docker compose up -d --scale crawler-worker=3
```

Why: starts three independent crawler containers. Each one uses the default
bounded command from `docker-compose.yml` and exits automatically after up to
30 stored pages or 120 seconds idle.

### Step 7: Watch progress

```powershell
docker compose logs -f crawler-worker streaming-indexer pagerank-worker
```

What to look for:

- crawler workers fetching/storing pages
- streaming indexer processing index events
- PageRank worker processing graph events

Press `Ctrl+C` to stop watching logs. This does **not** stop the containers.

### Step 8: Confirm crawler workers finished automatically

```powershell
docker compose ps -a crawler-worker
```

Expected after the crawl finishes: crawler worker containers are `Exited`.
That is good. It means they hit their page limit or idle timeout.

### Step 9: Give background processors a short moment

```powershell
Start-Sleep -Seconds 20
```

Why: indexing and PageRank are asynchronous. The crawler creates events first;
the indexer and PageRank worker consume them in the background.

### Step 10: Check final pipeline counts

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "const d=db.getSiblingDB('web_crawler'); printjson({pages:d.pages_metadata.countDocuments(), documents:d.pages_documents.countDocuments(), postings:d.term_postings.countDocuments(), indexedDocs:d.indexed_documents.countDocuments(), indexPending:d.index_outbox.countDocuments({status:'pending'}), indexProcessing:d.index_outbox.countDocuments({status:'processing'}), graphPending:d.graph_outbox.countDocuments({status:'pending'}), graphProcessing:d.graph_outbox.countDocuments({status:'processing'}), graphDone:d.graph_outbox.countDocuments({status:'done'}), edges:d.graph_edges.countDocuments(), pagerank:d.pagerank_nodes.countDocuments()})"
```

Healthy result:

| Field | What it proves |
|---|---|
| `pages > 0` | Crawler stored page metadata. |
| `documents > 0` | Clean text was stored. |
| `postings > 0` | Streaming indexer built the search index. |
| `edges > 0` | PageRank worker stored graph edges. |
| `pagerank > 0` | PageRank scores exist. |
| `indexPending = 0` | Index events are fully processed. |
| `graphPending = 0` | Graph events are fully processed. |

If `indexPending` or `graphPending` is not zero, wait and run the same check
again:

```powershell
Start-Sleep -Seconds 20
```

This is not manual draining. The already-running background containers are
doing the work automatically.

### Step 11: Query the Search API

Check API health:

```powershell
curl.exe http://localhost:8000/health
```

Search:

```powershell
curl.exe "http://localhost:8000/search?q=cricket&limit=5"
curl.exe "http://localhost:8000/search?q=football&limit=5"
curl.exe "http://localhost:8000/search?q=music&limit=5"
```

If these words are not in the crawled pages, list indexed terms:

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('web_crawler').term_postings.distinct('term').slice(0,20)"
```

Then query one returned term:

```powershell
curl.exe "http://localhost:8000/search?q=<term_from_mongodb>&limit=5"
```

## Docker Desktop Inspection

Open Docker Desktop:

1. Go to **Containers**.
2. Open the Compose project.
3. Check running services:
   - `redis`
   - `mongodb`
   - `minio`
   - `streaming-indexer`
   - `pagerank-worker`
   - `search-api`
4. Check crawler containers:
   - `crawler-worker-1`
   - `crawler-worker-2`
   - `crawler-worker-3`
5. It is fine if crawler workers are `Exited` after the run. They are
   bounded workers.
6. Open logs for crawler/indexer/PageRank to inspect processing.

MinIO console:

```text
http://localhost:9001
username: minioadmin
password: minioadmin
```

Check bucket:

```text
crawler-html
```

This proves raw HTML is stored in shared object storage, not on a worker's
local disk.

## What Happens Internally

1. `seed` inserts starting URLs into Redis.
2. `crawler-worker` containers pull URLs from Redis.
3. Workers check robots.txt and domain politeness locks.
4. Workers download HTML and extract clean text plus outbound links.
5. Compressed raw HTML goes to MinIO.
6. MongoDB stores metadata, clean text, content pointers, and durable events.
7. `streaming-indexer` automatically consumes index events and writes
   `term_postings`.
8. `pagerank-worker` automatically consumes graph events and writes
   `graph_edges` / `pagerank_nodes`.
9. `search-api` reads the prebuilt index and PageRank values to return ranked
   results.

## Test Cases

Run these after the automatic Docker run, or run them in a separate terminal
session.

Important: the distributed fault test resets crawler data. If you still need to
check the MongoDB counts from the automatic Docker run after this test, run the
Docker flow again.

### Test 1: Unit tests

When to run: after the automatic Docker run, or before it if you only want a quick
logic check.

```powershell
python -m unittest tests.test_core_unit
```

Checks core logic such as Bloom/frontier behavior, parsing helpers, and
storage-related utilities.

Expected: all tests pass.

### Test 2: Compose and import checks

When to run: before the Docker run if you want to check the build first, or
after the run if time is limited.

```powershell
docker compose config
docker compose build search-api crawler-worker streaming-indexer pagerank-worker seed
python -m compileall -q src tests scripts
```

Checks Docker Compose syntax, image builds, and Python syntax/imports.

Expected: all commands finish without errors.

### Test 3: End-to-end distributed fault test

When to run: after the automatic Docker run, because this test cleans and reuses the
datastores.

Make sure backing services are up:

```powershell
docker compose up -d redis mongodb minio
```

Run:

```powershell
python tests/distributed_system_test.py
```

What it does:

- starts a deterministic local website
- starts multiple crawler worker subprocesses
- kills one worker during the crawl
- verifies other workers continue
- drains index/PageRank inside the test
- starts FastAPI on a test port
- verifies known search results
- verifies unknown terms return empty results

Expected final output:

```json
{
  "status": "passed"
}
```

Generated proof:

```powershell
Get-Content results\distributed_system_test_results.json
```

Important fields:

| Field | Meaning |
|---|---|
| `killed_worker_pid` | A worker was intentionally killed. |
| `workers_seen_in_storage` | Multiple workers successfully stored pages. |
| `processing_size` | Redis in-flight leases were recovered/cleared. |
| `index_outbox_pending` | Index queue was drained. |
| `graph_outbox_pending` | Graph queue was drained. |
| `kiwi_search` / `unique_search` | Known terms returned expected pages. |
| `unknown_search` | Unknown term returned no results. |

### Test 4: Search edge cases

When to run: immediately after the automatic Docker run, while `search-api` is running
and `term_postings` has data.

Known/common term:

```powershell
curl.exe "http://localhost:8000/search?q=cricket&limit=5"
```

Unknown term:

```powershell
curl.exe "http://localhost:8000/search?q=notpresenttermxyz&limit=5"
```

Expected unknown-term behavior:

```json
"results": []
```

This proves the API does not invent matches.

### Test 5: Duplicate URL/idempotency check

When to run: after the automatic Docker run if you want to check duplicate URL
handling.

Seed again:

```powershell
docker compose --profile tools run --rm seed
```

Run a short bounded worker:

```powershell
docker compose run --rm crawler-worker python src/v3/worker_v3.py --worker-id duplicate-check --max-pages 10 --idle-timeout 30 --batch-size 5
```

Check duplicates:

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "const d=db.getSiblingDB('web_crawler'); const pages=d.pages_metadata.countDocuments(); const distinct=d.pages_metadata.distinct('url').length; printjson({pages:pages, distinctUrls:distinct, duplicateUrlRows:pages-distinct})"
```

Expected:

```text
duplicateUrlRows: 0
```

### Test 6: Optional manual worker crash test

When to run: only if you want a live manual crash. The automated distributed
test already covers worker-kill recovery more reliably.

Start a longer one-off crawler so you have time to kill it:

```powershell
docker compose run -d --name crawler-crash-test crawler-worker python src/v3/worker_v3.py --worker-id crawler-crash-test --max-pages 300 --idle-timeout 300 --batch-size 5
```

Kill it:

```powershell
docker kill crawler-crash-test
```

Check remaining state:

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('web_crawler').pages_metadata.countDocuments()"
docker compose exec -T redis redis-cli HLEN crawler:processing
```

What this proves: committed data is not lost when one crawler dies. Other
workers and shared storage continue to exist.

Cleanup the killed one-off container if Docker keeps it:

```powershell
docker rm crawler-crash-test
```

## Optional Manual Finish / Recovery Commands

These are not part of the automatic Docker run. Use them only if you
intentionally run long-running crawlers, stop background consumers, or want to force a final
deterministic state.

### Stop long-running crawlers

```powershell
docker compose stop crawler-worker
```

### Manually drain index events

```powershell
docker compose stop streaming-indexer
docker compose run --rm --no-deps streaming-indexer python streaming_indexer.py --once --max-events 1000
docker compose up -d streaming-indexer
```

### Manually drain PageRank graph events

```powershell
docker compose stop pagerank-worker
docker compose run --rm --no-deps pagerank-worker python pagerank_worker.py --once --max-events 20 --max-pushes 0 --claim-timeout 1
docker compose up -d pagerank-worker
```

### Optional full PageRank recompute

```powershell
docker compose run --rm --no-deps pagerank-worker python pagerank_worker.py --recompute --convergence-epsilon 1e-8 --max-iterations 100
```

Why optional: incremental PageRank is automatic. Full recompute is a periodic
maintenance/verification job, not something needed after every crawl.

## Generate Results And Graphs

These scripts use host Python and the running Docker services.

Install dependencies if needed:

```powershell
python -m pip install -r requirements.txt
```

Start backing services:

```powershell
docker compose up -d redis mongodb minio
```

Run experiments:

```powershell
python scripts/run_evaluation_experiments.py
python scripts/run_resource_scaling_1_20.py
python scripts/run_report_supplement_metrics.py
python scripts/generate_evaluation_graphs.py
```

Generated files:

| Path | Meaning |
|---|---|
| `results/raw/` | Raw experiment JSON/log files. |
| `results/processed/` | CSV/JSON files used for report tables and graphs. |
| `results/figures/` | Generated figures. |
| `report/figures/` | Figures used by LaTeX. |
| `report/generated_results.tex` | Auto-generated report tables/macros. |
| `report/main.pdf` | Final report PDF. |

Compile the report:

```powershell
pdflatex -interaction=nonstopmode -output-directory report report/main.tex
pdflatex -interaction=nonstopmode -output-directory report report/main.tex
```

## How To Justify Results And Graphs

Explain the chain:

```text
tests/scripts
  -> results/processed/*.csv and *.json
  -> scripts/generate_evaluation_graphs.py
  -> report/figures/*
  -> report/main.pdf
```

The report should not contain invented numbers. Graphs and tables should trace
back to generated CSV/JSON files.

Important formulas:

| Metric | Formula |
|---|---|
| Throughput | `pages_per_second = crawled_pages / total_time_seconds` |
| Speedup | `speedup_N = time_1_worker / time_N_workers` |
| Parallel efficiency | `efficiency_N = speedup_N / N` |
| Search score | `final_score = term_score * pagerank_authority` |
| PageRank convergence | Stop when L1 rank-vector change is below epsilon. |

## Troubleshooting

MongoDB not healthy:

```powershell
docker compose ps --all
docker compose logs --tail=120 mongodb
```

Services stuck as `Created` after MongoDB becomes healthy:

```powershell
docker compose up -d streaming-indexer pagerank-worker search-api
```

Search returns no results:

```powershell
docker compose exec -T mongodb mongosh --quiet --eval "const d=db.getSiblingDB('web_crawler'); printjson({documents:d.pages_documents.countDocuments(), postings:d.term_postings.countDocuments(), indexPending:d.index_outbox.countDocuments({status:'pending'}), graphPending:d.graph_outbox.countDocuments({status:'pending'})})"
```

If pending counts are not zero, wait 20 seconds and check again. The background
workers should drain automatically.

Full reset:

```powershell
docker compose down -v --remove-orphans
docker compose up -d --build redis mongodb minio streaming-indexer pagerank-worker search-api
```

## Notes

- Docker Compose runs multiple application containers on one Docker host.
- This is not a multi-machine Redis/MongoDB/MinIO production cluster.
- Production HA would need Redis Sentinel/Cluster, a multi-node MongoDB replica
  set, and distributed MinIO or real S3.
- The crawler handles static HTML and does not execute JavaScript.
- Public websites may block crawlers, so live internet result counts can vary.
