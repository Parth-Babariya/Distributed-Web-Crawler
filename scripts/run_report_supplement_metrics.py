#!/usr/bin/env python3
"""Generate supplemental report metrics from real local measurements.

This script fills the report metrics that are not covered by the main
distributed crawl/index/search experiment:

- Bloom filter memory calculation versus a measured Python exact-set baseline.
- MongoDB operation latency microbenchmark.
- Storage compression summary from the current crawler MongoDB collections.
- Sustained-throughput and resource-utilization summaries derived from the
  measured 1-to-20 worker scaling dataset.

The sustained-throughput file is derived from already measured scaling runs
rather than invented as a separate long crawl. The source column records that
provenance explicitly.
"""

import csv
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from pymongo import ASCENDING, MongoClient
from redis import Redis

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "results" / "processed"
RAW_DIR = PROJECT_ROOT / "results" / "raw"
SRC_DIR = PROJECT_ROOT / "src"
MONGO_URI = os.getenv(
    "EVAL_MONGO_URI",
    "mongodb://localhost:27017/web_crawler?directConnection=true",
)
DATABASE_NAME = os.getenv("EVAL_MONGO_DB", "web_crawler")


def write_csv(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def percentile(values, percent):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = math.ceil((percent / 100) * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def deep_size_url_set(count=100_000):
    urls = [f"http://benchmark.local/page/{i}?token={i % 997}" for i in range(count)]
    exact = set(urls)
    return sys.getsizeof(exact) + sum(sys.getsizeof(url) for url in exact)


def run_memory_experiment():
    capacity = 10_000_000
    error_rate = 0.001
    bloom_bits = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
    bloom_mb = bloom_bits / 8 / 1024 / 1024
    optimal_hashes = (bloom_bits / capacity) * math.log(2)

    sample_urls = 100_000
    sample_exact_bytes = deep_size_url_set(sample_urls)
    exact_bytes_per_url = sample_exact_bytes / sample_urls
    exact_10m_mb = exact_bytes_per_url * capacity / 1024 / 1024
    savings_percent = (1 - (bloom_mb / exact_10m_mb)) * 100

    rows = [
        {
            "method": "bloom_filter",
            "urls": capacity,
            "memory_mb": bloom_mb,
            "source": "formula_n_10000000_p_0.001",
        },
        {
            "method": "python_exact_set_extrapolated",
            "urls": capacity,
            "memory_mb": exact_10m_mb,
            "source": f"measured_{sample_urls}_urls_deep_size_then_extrapolated",
        },
    ]
    write_csv(PROCESSED_DIR / "memory_efficiency.csv", rows)

    summary = [
        {
            "capacity_urls": capacity,
            "error_rate": error_rate,
            "bloom_bits": bloom_bits,
            "bloom_mb": bloom_mb,
            "optimal_hash_count": optimal_hashes,
            "implementation_hash_count": 9,
            "sample_urls": sample_urls,
            "sample_exact_set_bytes": sample_exact_bytes,
            "exact_set_bytes_per_url": exact_bytes_per_url,
            "extrapolated_exact_set_10m_mb": exact_10m_mb,
            "savings_percent": savings_percent,
        }
    ]
    write_csv(PROCESSED_DIR / "bloom_memory_calculation.csv", summary)


def time_call(fn, runs):
    latencies = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        latencies.append((time.perf_counter() - started) * 1000)
    return {
        "runs": runs,
        "mean_ms": statistics.mean(latencies),
        "median_ms": statistics.median(latencies),
        "p95_ms": percentile(latencies, 95),
    }


def run_mongo_latency_experiment():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    txn_a = db["report_latency_txn_a"]
    txn_b = db["report_latency_txn_b"]
    outbox = db["report_latency_outbox"]
    postings = db["term_postings"]
    txn_a.drop()
    txn_b.drop()
    outbox.drop()
    outbox.create_index([("status", ASCENDING), ("_id", ASCENDING)])
    postings.create_index([("term", ASCENDING), ("url", ASCENDING)], unique=True)

    counter = {"value": 0}

    def page_transaction():
        counter["value"] += 1
        i = counter["value"]
        with client.start_session() as session:
            with session.start_transaction():
                txn_a.update_one(
                    {"url": f"http://latency.local/page/{i}"},
                    {
                        "$set": {
                            "url": f"http://latency.local/page/{i}",
                            "title": f"Latency page {i}",
                            "updated_at": datetime.utcnow(),
                        }
                    },
                    upsert=True,
                    session=session,
                )
                txn_b.insert_one(
                    {
                        "url": f"http://latency.local/page/{i}",
                        "status": "pending",
                        "crawl_version": f"latency-{i}",
                    },
                    session=session,
                )

    def outbox_claim():
        counter["value"] += 1
        outbox.insert_one({"status": "pending", "created_at": datetime.utcnow()})
        outbox.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing", "claimed_at": datetime.utcnow()}},
            sort=[("_id", ASCENDING)],
        )

    def posting_upsert():
        counter["value"] += 1
        postings.update_one(
            {"term": "reportlatency", "url": f"http://latency.local/doc/{counter['value']}"},
            {
                "$set": {
                    "term": "reportlatency",
                    "url": f"http://latency.local/doc/{counter['value']}",
                    "title": "Latency benchmark",
                    "term_frequency": counter["value"] % 11 + 1,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    # Ensure the search aggregation has a non-empty, indexed posting set.
    for i in range(1000):
        postings.update_one(
            {"term": "reportlatency", "url": f"http://latency.local/existing/{i}"},
            {
                "$set": {
                    "term": "reportlatency",
                    "url": f"http://latency.local/existing/{i}",
                    "title": "Latency benchmark",
                    "term_frequency": i % 9 + 1,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    search_pipeline = [
        {"$match": {"term": "reportlatency"}},
        {"$group": {"_id": "$url", "term_score": {"$sum": "$term_frequency"}}},
        {"$sort": {"term_score": -1, "_id": 1}},
        {"$limit": 10},
    ]

    rows = [
        {"operation": "page_transaction_2_writes", **time_call(page_transaction, 30), "notes": "same_commit_shape_as_page_plus_outbox"},
        {"operation": "outbox_findOneAndUpdate_claim", **time_call(outbox_claim, 50), "notes": "pending_to_processing_claim_pattern"},
        {"operation": "term_postings_upsert", **time_call(posting_upsert, 100), "notes": "streaming_indexer_posting_write"},
        {"operation": "search_aggregation_top10", **time_call(lambda: list(postings.aggregate(search_pipeline, allowDiskUse=True)), 50), "notes": "match_group_sort_limit_in_mongo"},
    ]
    write_csv(PROCESSED_DIR / "mongodb_latency.csv", rows)
    txn_a.drop()
    txn_b.drop()
    outbox.drop()
    client.close()


def run_storage_compression_summary():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    pipeline = [
        {
            "$group": {
                "_id": None,
                "pages": {"$sum": 1},
                "content_size_bytes": {"$sum": "$content_size"},
                "compressed_size_bytes": {"$sum": "$compressed_size"},
            }
        }
    ]
    rows = list(db.pages_metadata.aggregate(pipeline))
    if rows:
        row = rows[0]
        content_size = row.get("content_size_bytes", 0) or 0
        compressed_size = row.get("compressed_size_bytes", 0) or 0
        savings = (1 - compressed_size / content_size) * 100 if content_size else 0.0
        write_csv(
            PROCESSED_DIR / "storage_compression.csv",
            [
                {
                    "pages": row.get("pages", 0),
                    "content_size_bytes": content_size,
                    "compressed_size_bytes": compressed_size,
                    "compression_savings_percent": savings,
                }
            ],
        )
    client.close()


def run_redis_latency_experiment():
    redis_client = Redis(host="localhost", port=6379, decode_responses=True)
    for key in redis_client.scan_iter("report:redis:*"):
        redis_client.delete(key)

    frontier = "report:redis:frontier"
    bloom = "report:redis:bloom"
    lock = "report:redis:lock:example.local"
    robots = "report:redis:robots:example.local"

    # Preload a realistic frontier size for pop/cardinality measurements.
    for start in range(0, 10_000, 1_000):
        pipe = redis_client.pipeline()
        for i in range(start, start + 1_000):
            pipe.zadd(frontier, {f"http://redis.local/page/{i}": i % 100})
        pipe.execute()

    counter = {"value": 10_000}

    def zadd_one():
        counter["value"] += 1
        redis_client.zadd(frontier, {f"http://redis.local/new/{counter['value']}": counter["value"] % 100})

    def zpopmax_one():
        result = redis_client.zpopmax(frontier, 1)
        if result:
            url, score = result[0]
            redis_client.zadd(frontier, {url: score})

    def zcard_one():
        redis_client.zcard(frontier)

    def setbit_getbit():
        counter["value"] += 1
        pos = counter["value"] % 1_000_000
        redis_client.setbit(bloom, pos, 1)
        redis_client.getbit(bloom, pos)

    def lock_attempt():
        redis_client.set(lock, "1", nx=True, px=100)
        redis_client.delete(lock)

    def robots_cache_write():
        redis_client.hset(robots, mapping={"allowed": "1", "crawl_delay": "0.001", "last_check": str(time.time())})
        redis_client.expire(robots, 86400)

    rows = [
        {"operation": "ZADD_frontier", **time_call(zadd_one, 500), "notes": "priority_queue_insert"},
        {"operation": "ZPOPMAX_frontier", **time_call(zpopmax_one, 500), "notes": "priority_queue_pop"},
        {"operation": "ZCARD_frontier", **time_call(zcard_one, 500), "notes": "frontier_cardinality"},
        {"operation": "SETBIT_GETBIT_bloom", **time_call(setbit_getbit, 500), "notes": "bitmap_dedupe_bits"},
        {"operation": "SET_NX_PX_lock", **time_call(lock_attempt, 500), "notes": "domain_politeness_lock"},
        {"operation": "HSET_EXPIRE_robots", **time_call(robots_cache_write, 200), "notes": "robots_cache_write_ttl_86400"},
    ]
    write_csv(PROCESSED_DIR / "redis_latency.csv", rows)

    memory_rows = [
        {
            "key": frontier,
            "memory_bytes": redis_client.memory_usage(frontier) or 0,
            "items": redis_client.zcard(frontier),
            "notes": "10000_preloaded_plus_benchmark_insertions",
        },
        {
            "key": bloom,
            "memory_bytes": redis_client.memory_usage(bloom) or 0,
            "items": "",
            "notes": "sparse_bitmap_after_benchmark_bits",
        },
        {
            "key": robots,
            "memory_bytes": redis_client.memory_usage(robots) or 0,
            "items": redis_client.hlen(robots),
            "notes": "robots_hash_with_24h_ttl",
        },
    ]
    write_csv(PROCESSED_DIR / "redis_memory_usage.csv", memory_rows)

    for key in redis_client.scan_iter("report:redis:*"):
        redis_client.delete(key)
    redis_client.close()


def run_streaming_indexer_lag_experiment():
    """Measure pending index_outbox lag while the real streaming indexer drains."""
    sys.path.insert(0, str(SRC_DIR))
    from indexer.streaming_indexer import StreamingIndexer

    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    for name in ("pages_documents", "index_outbox", "term_postings", "indexed_documents"):
        db[name].delete_many({})

    total_docs = 300
    now = datetime.utcnow()
    docs = [
        {
            "page_id": f"lag-{i}",
            "url": f"http://lag.local/doc/{i}",
            "title": f"Lag document {i}",
            "text": ("streaming indexer latency crawl benchmark " * (1 + i % 5))
                    + f"unique_lag_{i}",
            "indexed_at": None,
            "crawl_version": f"lag-{i}-v1",
            "updated_at": now,
        }
        for i in range(total_docs)
    ]
    db.pages_documents.insert_many(docs)
    db.index_outbox.insert_many([
        {
            "url": doc["url"],
            "page_id": doc["page_id"],
            "crawl_version": doc["crawl_version"],
            "status": "pending",
            "timestamp": now,
        }
        for doc in docs
    ])

    indexer = StreamingIndexer(
        mongodb_uri=MONGO_URI,
        database=DATABASE_NAME,
        worker_id="report-lag-indexer",
        claim_timeout_seconds=5,
    )
    rows = []
    processed_total = 0
    started = time.perf_counter()
    try:
        step = 0
        while True:
            pending_before = db.index_outbox.count_documents({"status": "pending"})
            if pending_before == 0:
                rows.append({
                    "step": step,
                    "elapsed_seconds": time.perf_counter() - started,
                    "pending_events": 0,
                    "processed_total": processed_total,
                    "processed_this_step": 0,
                    "batch_size": 25,
                    "source": "real_streaming_indexer_once_batches",
                })
                break
            step_started = time.perf_counter()
            processed = indexer.process_pending_events(max_events=25)
            processed_total += processed
            rows.append({
                "step": step,
                "elapsed_seconds": time.perf_counter() - started,
                "step_time_ms": (time.perf_counter() - step_started) * 1000,
                "pending_events": pending_before,
                "processed_total": processed_total,
                "processed_this_step": processed,
                "batch_size": 25,
                "source": "real_streaming_indexer_once_batches",
            })
            if not processed:
                raise RuntimeError("streaming indexer made no progress during lag benchmark")
            step += 1
    finally:
        indexer.close()
        client.close()

    write_csv(PROCESSED_DIR / "streaming_indexer_lag.csv", rows)


def run_pagerank_convergence_experiment(max_iterations=50, epsilon=1e-8):
    """Run power-iteration math over the measured graph_edges snapshot."""
    sys.path.insert(0, str(SRC_DIR))
    from indexer.pagerank_worker import IncrementalPageRankWorker

    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    worker = IncrementalPageRankWorker(
        mongodb_uri=MONGO_URI,
        database=DATABASE_NAME,
        worker_id="report-convergence-pagerank",
        claim_timeout_seconds=5,
    )
    try:
        for _ in range(100):
            events = worker.process_pending_events(max_events=250)
            pushes = worker.propagate_residuals(max_pushes=250)
            if not events and not pushes:
                break
    finally:
        worker.close()

    edge_docs = list(db.graph_edges.find({}, {"source_url": 1, "outbound_urls": 1}))
    source = "measured_graph_edges_snapshot"

    if not edge_docs:
        source = "fallback_deterministic_fixture_graph"
        edge_docs = [
            {"source_url": "A", "outbound_urls": ["B", "C"]},
            {"source_url": "B", "outbound_urls": ["C"]},
            {"source_url": "C", "outbound_urls": ["A"]},
            {"source_url": "D", "outbound_urls": ["C", "A"]},
        ]

    outgoing = {}
    nodes = set()
    for edge in edge_docs:
        src = edge["source_url"]
        links = list(dict.fromkeys(edge.get("outbound_urls", [])))
        outgoing[src] = links
        nodes.add(src)
        nodes.update(links)

    damping = 0.85
    personalization = {url: 1.0 / len(nodes) for url in nodes}
    ranks = dict(personalization)
    rows = []

    for iteration in range(1, max_iterations + 1):
        new_ranks = {
            url: (1.0 - damping) * personalization[url]
            for url in nodes
        }
        dangling_mass = sum(rank for url, rank in ranks.items() if not outgoing.get(url))
        for url, weight in personalization.items():
            new_ranks[url] += damping * dangling_mass * weight

        for src, links in outgoing.items():
            if not links:
                continue
            contribution = damping * ranks[src] / len(links)
            for dst in links:
                new_ranks[dst] += contribution

        l1_delta = sum(abs(new_ranks[url] - ranks[url]) for url in nodes)
        rows.append({
            "iteration": iteration,
            "l1_delta": l1_delta,
            "nodes": len(nodes),
            "edges": sum(len(v) for v in outgoing.values()),
            "damping": damping,
            "epsilon": epsilon,
            "source": source,
        })
        ranks = new_ranks
        if l1_delta < epsilon:
            break

    write_csv(PROCESSED_DIR / "pagerank_convergence.csv", rows)
    client.close()


def run_derived_sustained_and_resource_metrics():
    scaling_path = PROCESSED_DIR / "crawl_scaling.csv"
    if not scaling_path.exists():
        return
    with scaling_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    # Use the 1..20 scaling run as a measured burst/stability dataset. The
    # windows group consecutive worker-count runs and summarize the measured
    # throughput inside each window.
    windows = [
        ("workers_1_to_5", rows[0:5]),
        ("workers_6_to_10", rows[5:10]),
        ("workers_11_to_15", rows[10:15]),
        ("workers_16_to_20", rows[15:20]),
    ]
    sustained_rows = []
    for label, subset in windows:
        pages = sum(int(float(row["crawled_pages"])) for row in subset)
        total_time = sum(float(row["total_time_seconds"]) for row in subset)
        rates = [float(row["pages_per_second"]) for row in subset]
        sustained_rows.append(
            {
                "window": label,
                "source": "derived_from_measured_worker_scaling_runs",
                "pages": pages,
                "total_time_seconds": total_time,
                "mean_pages_per_second": statistics.mean(rates),
                "median_pages_per_second": statistics.median(rates),
                "min_pages_per_second": min(rates),
                "max_pages_per_second": max(rates),
            }
        )
    write_csv(PROCESSED_DIR / "sustained_throughput_windows.csv", sustained_rows)

    # Detailed resource measurements are produced by
    # scripts/run_resource_scaling_1_20.py into resource_scaling_1_20.csv.


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    run_memory_experiment()
    run_mongo_latency_experiment()
    run_storage_compression_summary()
    run_redis_latency_experiment()
    run_streaming_indexer_lag_experiment()
    run_pagerank_convergence_experiment()
    run_derived_sustained_and_resource_metrics()
    print("Wrote supplemental report metrics under results/processed")


if __name__ == "__main__":
    main()
