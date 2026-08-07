# Regenerating Report Figures, Tables, and PDF

The report ([report/main.tex](main.tex)) is designed to compile cleanly even when no figures have been generated yet — missing figures show a labelled placeholder via the `\rendered` macro. This file documents the exact sequence to produce the actual data, render the diagrams, and compile the PDF.

The report architecture changed substantially (two-tier frontier, transactional outbox, streaming indexer, incremental PageRank, MinIO blob storage, atomic Lua coordination paths). All previous CSVs/JSONs and PDF figures have been removed so the report cannot accidentally show stale numbers from the old architecture.

---

## Prerequisites

- **Docker Desktop** with Compose v2 (the `docker compose` subcommand).
- **Python 3.10+** with `pip install -r requirements.txt` for evaluation scripts that run on the host.
- **mermaid-cli (`mmdc`)** for rendering diagrams. Install with `npm install -g @mermaid-js/mermaid-cli`.
- **TeX Live or MiKTeX** with `pdflatex` for compiling `report/main.tex`.

If you don't have `mmdc` installed, the report still compiles — diagrams just show as labelled placeholders.

---

## Step 1 — Bring the Stack Up Cleanly

```bash
docker compose down -v --remove-orphans
docker compose up -d --build redis mongodb minio streaming-indexer pagerank-worker search-api
```

Wait for the MongoDB replica set to elect a primary:

```bash
docker compose exec -T mongodb mongosh --quiet --eval \
  "rs.status().members.map(m => m.name + ':' + m.stateStr).join(',')"
```

You want at least one member showing `PRIMARY`.

---

## Step 2 — Run Tests First (Confirms the Build Is Healthy)

```bash
python -m unittest tests.test_core_unit
```

Expect all tests to pass.

End-to-end distributed system test (spins up its own fixture web server, crawler subprocesses, kill scenario, drain streaming pipeline, query search API):

```bash
python tests/distributed_system_test.py
```

This produces:

- `results/distributed_system_test_results.json` — full assertion payload
- `results/distributed_test_logs/dist-worker-*.log` — per-worker logs
- `results/distributed_test_logs/search_api.log` — API logs

---

## Step 3 — Run Empirical Evaluations

These produce the CSV/JSON files in `results/processed/` that drive the report's tables and graphs.

```bash
# Full empirical suite (worker scaling, indexing, query latency, fault tolerance, etc.)
python scripts/run_evaluation_experiments.py

# Resource scaling sweep from 1 to 20 workers
python scripts/run_resource_scaling_1_20.py

# Auxiliary metrics (Bloom memory math, sustained throughput windows, etc.)
python scripts/run_report_supplement_metrics.py
```

Each script is idempotent: it cleans the relevant Redis/Mongo collections before running, so you can re-run without manual cleanup.

**Note:** the evaluation scripts may need updating to exercise the new architecture (`term_postings` instead of `inverted_index`, `streaming_indexer` instead of batch `mapreduce_indexer`, `pagerank_worker` invocation). See `scripts/` for the current state — if a script throws an error on a missing collection, that's the migration signal.

Expected output files in `results/processed/`:

| File | Source script | What it contains |
|---|---|---|
| `crawl_scaling.csv` | `run_evaluation_experiments.py` | (workers, pages, time_s, pages_per_s, speedup, efficiency) |
| `worker_contribution.csv` | `run_evaluation_experiments.py` | per-worker fetched/stored counts |
| `streaming_indexer_lag.csv` | `run_report_supplement_metrics.py` | `index_outbox` pending count over time |
| `pagerank_convergence.csv` | `run_report_supplement_metrics.py` | full-recompute L1 delta vs iteration |
| `query_latency.csv` | `run_evaluation_experiments.py` | latency by query type |
| `redis_latency.csv` | `run_report_supplement_metrics.py` | per-op latency for Lua scripts and primitives |
| `mongodb_latency.csv` | `run_report_supplement_metrics.py` | transaction commit, outbox claim, postings upsert latency |
| `storage_compression.csv` | `run_evaluation_experiments.py` | raw vs compressed bytes for the crawl subset |
| `bloom_memory_calculation.csv` | `run_report_supplement_metrics.py` | Bloom filter sizing math |
| `memory_efficiency.csv` | `run_report_supplement_metrics.py` | Bloom vs exact-set memory comparison |
| `sustained_throughput_windows.csv` | `run_report_supplement_metrics.py` | windowed throughput summaries |
| `resource_scaling_1_20.csv` | `run_resource_scaling_1_20.py` | CPU, memory, Redis memory at each worker count |
| `fault_tolerance.csv` | `run_evaluation_experiments.py` | kill-scenario summary |
| `inverted_index_correctness.json` | `run_evaluation_experiments.py` | deterministic ranking validation |

---

## Step 4 — Render Graphs from CSVs

```bash
python scripts/generate_evaluation_graphs.py
```

This reads every CSV in `results/processed/` and writes both PDF and PNG to `results/figures/` and `report/figures/` (the report references the latter).

Expected files in `report/figures/`:

```
throughput_vs_workers.pdf, throughput_vs_workers.png
speedup_vs_workers.pdf, speedup_vs_workers.png
parallel_efficiency_vs_workers.pdf, parallel_efficiency_vs_workers.png
crawl_time_vs_workers.pdf, crawl_time_vs_workers.png
duplicates_filtered_vs_workers.pdf, duplicates_filtered_vs_workers.png
worker_contribution_distribution.pdf, worker_contribution_distribution.png
streaming_indexer_lag.pdf, streaming_indexer_lag.png
pagerank_convergence.pdf, pagerank_convergence.png
query_latency_by_type.pdf, query_latency_by_type.png
redis_operation_latency.pdf, redis_operation_latency.png
mongodb_latency.pdf, mongodb_latency.png
fault_tolerance_completed_pages.pdf, fault_tolerance_completed_pages.png
memory_efficiency_bloom_vs_exact.pdf, memory_efficiency_bloom_vs_exact.png
sustained_throughput_windows.pdf, sustained_throughput_windows.png
resource_utilization.pdf, resource_utilization.png
redis_memory_usage.pdf, redis_memory_usage.png
indexing_time_vs_documents.pdf, indexing_time_vs_documents.png
index_growth_vs_documents.pdf, index_growth_vs_documents.png
```

---

## Step 5 — Render the Architecture Diagrams

The report references seven Mermaid diagrams. Render them once with `mmdc`:

```bash
# bash / zsh
for f in report/mermaid/*.mmd; do
  out="report/figures/$(basename "${f%.mmd}").pdf"
  mmdc -i "$f" -o "$out" -b transparent --pdfFit
done
```

```powershell
# PowerShell
Get-ChildItem report/mermaid/*.mmd | ForEach-Object {
  $out = "report/figures/" + $_.BaseName + ".pdf"
  mmdc -i $_.FullName -o $out -b transparent --pdfFit
}
```

This produces:

```
report/figures/architecture.pdf
report/figures/hld.pdf
report/figures/worker_lld.pdf
report/figures/index_search_flow.pdf
report/figures/two_tier_frontier.pdf
report/figures/transactional_outbox.pdf
report/figures/processing_ledger.pdf
report/figures/pagerank_residual.pdf
```

If you only have `mmdc` rendering as PNG, use `-o report/figures/<name>.png`; the report's `\rendered` macro accepts either extension.

---

## Step 6 — Compile the PDF

```bash
pdflatex -interaction=nonstopmode -output-directory report report/main.tex
pdflatex -interaction=nonstopmode -output-directory report report/main.tex   # second pass for TOC
```

Output: `report/main.pdf`.

---

## Quick Sanity Check

After regeneration, the report should:

- Show all eight architecture diagrams (no `[Figure not yet rendered]` placeholders for `report/figures/architecture.pdf` etc.).
- Show all empirical figures with real curves (no placeholders for `throughput_vs_workers.pdf` etc.).
- Show fresh numbers in the empirical tables (no `Regenerate required` boxes).
- Compile to ~25-35 pages of PDF.

---

## Troubleshooting

**MongoDB transactions fail:** confirm `rs.status().ok` returns 1 and at least one `PRIMARY` member. The single-node replica set sometimes takes 10-30s to elect on first boot.

**MinIO blob writes fail:** `docker compose ps minio` should show `healthy`. The crawler's `S3HtmlBlobStore` will auto-create the `crawler-html` bucket on first use.

**Streaming indexer / PageRank worker have no events:** check `docker compose logs --tail=100 streaming-indexer pagerank-worker`. If the crawler hasn't run yet, the outboxes are empty — that's expected.

**Search API returns empty results:** the streaming indexer must have drained the outbox events. Run an explicit drain pass:

```bash
docker compose exec -T streaming-indexer python streaming_indexer.py --once --max-events 1000
docker compose exec -T pagerank-worker python pagerank_worker.py --once --max-events 1000 --max-pushes 2000
```

**Mermaid CLI prints errors:** Mermaid sometimes refuses to render syntactically valid `.mmd` files because of node-version mismatches. Try `npm install -g @mermaid-js/mermaid-cli@latest` and retry. If still broken, the report compiles fine without diagrams (it just shows placeholders).
