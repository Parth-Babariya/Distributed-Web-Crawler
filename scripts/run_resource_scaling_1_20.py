#!/usr/bin/env python3
"""Measure CPU, memory, and Redis memory for 1..20 worker subprocess runs."""

import csv
import json
import multiprocessing
import subprocess
import sys
import time
from pathlib import Path

import psutil
from pymongo import MongoClient
from redis import Redis

from run_evaluation_experiments import (
    LOG_DIR,
    PROJECT_ROOT,
    TARGET_PAGES,
    WEB_BASE_PORT,
    WEB_PORT,
    DOMAIN_COUNT,
    DATABASE_NAME,
    MONGO_URI,
    clean_datastores,
    env,
    frontier_total,
    parse_worker_stats,
    run_command,
    serve_fixture,
    start_all_fixture_servers,
    stop_all_fixture_servers,
    wait_for_http,
)

PROCESSED_DIR = PROJECT_ROOT / "results" / "processed"
WORKER_COUNTS = list(range(1, 21))


def write_csv(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def run_one(worker_count, redis_client, db):
    clean_datastores(redis_client, db)
    seed_urls = [f'http://127.0.0.1:{port}/' for port in range(WEB_BASE_PORT, WEB_BASE_PORT + DOMAIN_COUNT)]
    run_command([sys.executable, "src/v3/master_v3.py", "seed"] + seed_urls)

    cpu_samples = []
    memory_samples = []
    redis_samples = []
    worker_cpu_samples = []
    stop_at = None
    processes = []

    started = time.perf_counter()
    for index in range(1, worker_count + 1):
        worker_id = f"resource-{worker_count}-{index}"
        log_path = LOG_DIR / f"resource_w{worker_count}_{worker_id}.log"
        log_file = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            [
                sys.executable,
                "src/v3/worker_v3.py",
                "--worker-id",
                worker_id,
                "--max-pages",
                str(TARGET_PAGES * 10),
                "--idle-timeout",
                "12",
                "--batch-size",
                "5",
                "--processing-timeout",
                "2",
            ],
            cwd=PROJECT_ROOT,
            env=env(),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        processes.append((process, log_file, log_path, worker_id))
        time.sleep(0.01)

    ps_processes = []
    for process, _, _, _ in processes:
        try:
            ps_process = psutil.Process(process.pid)
            ps_process.cpu_percent(interval=None)
            ps_processes.append(ps_process)
        except psutil.Error:
            pass

    while True:
        alive = [process for process, _, _, _ in processes if process.poll() is None]
        cpu_samples.append(psutil.cpu_percent(interval=0.2))
        memory_samples.append(psutil.virtual_memory().used / (1024 * 1024))
        try:
            redis_samples.append(redis_client.info("memory")["used_memory"] / (1024 * 1024))
        except Exception:
            pass
        process_cpu_total = 0.0
        for ps_process in list(ps_processes):
            try:
                process_cpu_total += ps_process.cpu_percent(interval=None)
            except psutil.Error:
                ps_processes.remove(ps_process)
        worker_cpu_samples.append(process_cpu_total)
        if not alive:
            break
        if db.pages_metadata.count_documents({}) >= TARGET_PAGES:
            redis_client.set("crawler:shutdown", "1", ex=30)
            stop_at = time.perf_counter()
        if stop_at is None and time.perf_counter() - started > 90:
            stop_at = time.perf_counter()
            redis_client.set("crawler:shutdown", "1", ex=30)
            for process in alive:
                process.terminate()

    for process, log_file, _, _ in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        log_file.close()

    elapsed = time.perf_counter() - started
    stats = [parse_worker_stats(log_path) for _, _, log_path, _ in processes]
    row = {
        "workers": worker_count,
        "target_pages": TARGET_PAGES,
        "pages_stored": db.pages_metadata.count_documents({}),
        "documents_stored": db.pages_documents.count_documents({}),
        "elapsed_seconds": elapsed,
        "throughput_pages_per_second": db.pages_metadata.count_documents({}) / elapsed if elapsed else 0,
        "avg_system_cpu_percent": sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
        "avg_worker_cpu_percent_total": sum(worker_cpu_samples) / len(worker_cpu_samples) if worker_cpu_samples else 0,
        "avg_system_memory_mb": sum(memory_samples) / len(memory_samples) if memory_samples else 0,
        "avg_redis_memory_mb": sum(redis_samples) / len(redis_samples) if redis_samples else 0,
        "max_redis_memory_mb": max(redis_samples) if redis_samples else 0,
        "frontier_size_after_run": frontier_total(redis_client),
        "processing_size_after_run": redis_client.hlen("crawler:processing"),
        "links_duplicate": sum(item.get("links_duplicate", 0) for item in stats),
        "errors": sum(item.get("errors", 0) for item in stats),
        "timeouts": sum(item.get("timeouts", 0) for item in stats),
        "source": "independent_worker_subprocesses",
    }
    print(json.dumps(row, indent=2))
    return row


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    redis_client = Redis(host="localhost", port=6379, decode_responses=False)
    mongo = MongoClient(MONGO_URI)
    db = mongo[DATABASE_NAME]

    server_processes = start_all_fixture_servers()
    rows = []
    try:
        for worker_count in WORKER_COUNTS:
            print(f"Measuring resource scaling with {worker_count} workers")
            rows.append(run_one(worker_count, redis_client, db))
        write_csv(PROCESSED_DIR / "resource_scaling_1_20.csv", rows)
    finally:
        stop_all_fixture_servers(server_processes)
        mongo.close()
        redis_client.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
