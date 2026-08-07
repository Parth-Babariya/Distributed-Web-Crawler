#!/usr/bin/env python3
"""Generate report-quality figures from measured evaluation CSV files."""

import csv
import json
import shutil
from pathlib import Path

import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / 'results' / 'processed'
FIGURES_DIR = PROJECT_ROOT / 'results' / 'figures'
REPORT_FIGURES_DIR = PROJECT_ROOT / 'report' / 'figures'


def read_csv(path):
    with path.open(newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def read_json(path):
    with path.open(encoding='utf-8') as handle:
        return json.load(handle)


def as_float(row, key):
    return float(row[key])


def style_axes(ax):
    ax.grid(True, alpha=0.25)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


def savefig(name):
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    pdf_path = FIGURES_DIR / f'{name}.pdf'
    png_path = FIGURES_DIR / f'{name}.png'
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.savefig(png_path, dpi=220)
    plt.close()
    shutil.copy2(pdf_path, REPORT_FIGURES_DIR / pdf_path.name)
    shutil.copy2(png_path, REPORT_FIGURES_DIR / png_path.name)


def plot_scaling(rows):
    workers = [int(row['workers']) for row in rows]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(workers, [as_float(row, 'total_time_seconds') for row in rows], marker='o', linewidth=2)
    ax.set_title('Crawl Completion Time vs Worker Count')
    ax.set_xlabel('Number of worker processes')
    ax.set_ylabel('Completion time (seconds)')
    ax.set_xticks(workers)
    style_axes(ax)
    savefig('crawl_time_vs_workers')

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(workers, [as_float(row, 'pages_per_second') for row in rows], marker='o', linewidth=2, color='#1f77b4')
    ax.set_title('Crawl Throughput vs Worker Count')
    ax.set_xlabel('Number of worker processes')
    ax.set_ylabel('Pages crawled per second')
    ax.set_xticks(workers)
    style_axes(ax)
    savefig('throughput_vs_workers')

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    speedup = [as_float(row, 'speedup') for row in rows]
    ax.plot(workers, speedup, marker='o', linewidth=2, label='Measured speedup')
    ax.plot(workers, workers, linestyle='--', color='gray', label='Ideal linear speedup')
    ax.set_title('Measured Speedup Relative to One Worker')
    ax.set_xlabel('Number of worker processes')
    ax.set_ylabel('Speedup (T1 / TN)')
    ax.set_xticks(workers)
    ax.legend()
    style_axes(ax)
    savefig('speedup_vs_workers')

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(workers, [as_float(row, 'parallel_efficiency') for row in rows], marker='o', linewidth=2, color='#2ca02c')
    ax.set_title('Parallel Efficiency vs Worker Count')
    ax.set_xlabel('Number of worker processes')
    ax.set_ylabel('Parallel efficiency')
    ax.set_ylim(bottom=0)
    ax.set_xticks(workers)
    style_axes(ax)
    savefig('parallel_efficiency_vs_workers')

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.bar(workers, [as_float(row, 'duplicate_urls_filtered') for row in rows], color='#9467bd')
    ax.set_title('Duplicate URLs Filtered During Crawl')
    ax.set_xlabel('Number of worker processes')
    ax.set_ylabel('Duplicate URLs filtered')
    ax.set_xticks(workers)
    style_axes(ax)
    savefig('duplicates_filtered_vs_workers')


def plot_worker_distribution(rows):
    if not rows:
        return
    max_workers = max(int(row['experiment_workers']) for row in rows)
    selected = [row for row in rows if int(row['experiment_workers']) == max_workers]
    labels = [row['worker_id'].replace(f'eval-{max_workers}-', 'w') for row in selected]
    pages = [int(row['pages_crawled']) for row in selected]

    fig, ax = plt.subplots(figsize=(8.2, 4.4))
    ax.bar(labels, pages, color='#17becf')
    ax.set_title(f'Worker Contribution Distribution ({max_workers} Workers)')
    ax.set_xlabel('Worker process')
    ax.set_ylabel('Pages crawled')
    ax.tick_params(axis='x', rotation=45)
    style_axes(ax)
    savefig('worker_contribution_distribution')


def plot_indexing(rows):
    docs = [int(row['documents']) for row in rows]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(docs, [as_float(row, 'indexing_time_seconds') for row in rows], marker='o', linewidth=2, color='#d62728')
    ax.set_title('Indexing Time vs Document Count')
    ax.set_xlabel('Documents indexed')
    ax.set_ylabel('Indexing time (seconds)')
    style_axes(ax)
    savefig('indexing_time_vs_documents')

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(docs, [int(row['unique_terms']) for row in rows], marker='o', linewidth=2, label='Unique terms')
    ax.plot(docs, [int(row['postings']) for row in rows], marker='s', linewidth=2, label='Postings')
    ax.set_title('Inverted Index Growth vs Document Count')
    ax.set_xlabel('Documents indexed')
    ax.set_ylabel('Index entries')
    ax.legend()
    style_axes(ax)
    savefig('index_growth_vs_documents')


def plot_query_latency(rows):
    labels = [row['query_type'].replace('_', ' ') for row in rows]
    means = [as_float(row, 'mean_latency_ms') for row in rows]
    medians = [as_float(row, 'median_latency_ms') for row in rows]
    p95s = [as_float(row, 'p95_latency_ms') for row in rows]
    x = list(range(len(rows)))
    width = 0.25

    fig, ax = plt.subplots(figsize=(7.8, 4.4))
    ax.bar([i - width for i in x], means, width=width, label='Mean')
    ax.bar(x, medians, width=width, label='Median')
    ax.bar([i + width for i in x], p95s, width=width, label='P95')
    ax.set_title('Search API Latency by Query Type')
    ax.set_xlabel('Query type')
    ax.set_ylabel('Latency (milliseconds)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha='right')
    ax.legend()
    style_axes(ax)
    savefig('query_latency_by_type')


def plot_streaming_indexer_lag(rows):
    if not rows:
        return
    elapsed = [as_float(row, 'elapsed_seconds') for row in rows]
    pending = [as_float(row, 'pending_events') for row in rows]
    processed = [as_float(row, 'processed_total') for row in rows]

    fig, ax1 = plt.subplots(figsize=(7.4, 4.4))
    ax1.plot(elapsed, pending, marker='o', linewidth=2, color='#1f77b4', label='Pending events')
    ax1.set_xlabel('Elapsed seconds')
    ax1.set_ylabel('Pending index_outbox events', color='#1f77b4')
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    ax2 = ax1.twinx()
    ax2.plot(elapsed, processed, marker='s', linewidth=2, color='#2ca02c', label='Processed total')
    ax2.set_ylabel('Processed events', color='#2ca02c')
    ax2.tick_params(axis='y', labelcolor='#2ca02c')
    ax1.set_title('Streaming Indexer Outbox Drain')
    ax1.grid(True, alpha=0.25)
    savefig('streaming_indexer_lag')


def plot_pagerank_convergence(rows):
    if not rows:
        return
    iterations = [int(row['iteration']) for row in rows]
    deltas = [as_float(row, 'l1_delta') for row in rows]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.semilogy(iterations, deltas, marker='o', linewidth=2, color='#9467bd')
    ax.set_title('PageRank Power-Iteration Convergence')
    ax.set_xlabel('Iteration')
    ax.set_ylabel('L1 rank-vector delta')
    style_axes(ax)
    savefig('pagerank_convergence')


def plot_fault_tolerance(rows, scaling_rows):
    if not rows:
        return
    fault = rows[0]
    baseline = next((row for row in scaling_rows if int(row['workers']) == 4), None)
    labels = ['4 workers\nnormal', '4 workers\none terminated']
    values = [
        as_float(baseline, 'crawled_pages') if baseline else 0,
        as_float(fault, 'completed_pages'),
    ]

    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    ax.bar(labels, values, color=['#1f77b4', '#ff7f0e'])
    ax.set_title('Fault-Tolerance Crawl Completion')
    ax.set_ylabel('Completed pages')
    style_axes(ax)
    savefig('fault_tolerance_completed_pages')


def plot_memory_efficiency(rows):
    if not rows:
        return
    labels = [row['method'].replace('_', '\n') for row in rows]
    values = [as_float(row, 'memory_mb') for row in rows]

    fig, ax = plt.subplots(figsize=(6.8, 4.4))
    bars = ax.bar(labels, values, color=['#2ca02c', '#7f7f7f'])
    ax.set_title('URL Deduplication Memory Requirement at 10M URLs')
    ax.set_ylabel('Memory (MB)')
    ax.set_yscale('log')
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value,
            f'{value:.1f} MB',
            ha='center',
            va='bottom',
            fontsize=9,
        )
    style_axes(ax)
    savefig('memory_efficiency_bloom_vs_exact')


def plot_mongo_latency(rows):
    if not rows:
        return
    labels = [row['operation'].replace('_', '\n') for row in rows]
    means = [as_float(row, 'mean_ms') for row in rows]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.bar(labels, means, color='#8c564b')
    ax.set_title('MongoDB Operation Latency Microbenchmark')
    ax.set_xlabel('Operation')
    ax.set_ylabel('Mean latency (milliseconds)')
    style_axes(ax)
    savefig('mongodb_latency')


def plot_sustained_windows(rows):
    if not rows:
        return
    labels = [row['window'].replace('workers_', 'w').replace('_to_', '-').replace('_', ' ') for row in rows]
    means = [as_float(row, 'mean_pages_per_second') for row in rows]
    mins = [as_float(row, 'min_pages_per_second') for row in rows]
    maxs = [as_float(row, 'max_pages_per_second') for row in rows]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(labels, means, marker='o', linewidth=2, label='Mean')
    ax.fill_between(labels, mins, maxs, alpha=0.18, label='Min-max range')
    ax.set_title('Measured Throughput Stability Across Scaling Windows')
    ax.set_xlabel('Measured worker-count window')
    ax.set_ylabel('Pages per second')
    ax.legend()
    style_axes(ax)
    savefig('sustained_throughput_windows')


def plot_resource_utilization(rows):
    if not rows:
        return
    workers = [int(row['workers']) for row in rows]
    cpu_key = 'avg_system_cpu_percent' if 'avg_system_cpu_percent' in rows[0] else 'avg_cpu_percent'
    redis_key = 'avg_redis_memory_mb' if 'avg_redis_memory_mb' in rows[0] else 'redis_memory_mb'
    cpu = [as_float(row, cpu_key) for row in rows]
    memory = [as_float(row, 'avg_system_memory_mb') / 1024 for row in rows]
    redis = [as_float(row, redis_key) for row in rows]

    fig, ax1 = plt.subplots(figsize=(7.2, 4.4))
    ax1.plot(workers, cpu, marker='o', linewidth=2, color='#1f77b4', label='Avg CPU %')
    ax1.set_xlabel('Worker processes')
    ax1.set_ylabel('Average CPU (%)', color='#1f77b4')
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    ax1.set_xticks(workers)
    ax2 = ax1.twinx()
    ax2.plot(workers, memory, marker='s', linewidth=2, color='#ff7f0e', label='System memory GB')
    ax2.set_ylabel('Average system memory (GB)', color='#ff7f0e')
    ax2.tick_params(axis='y', labelcolor='#ff7f0e')
    ax1.set_title('System Resource Utilization During Stress Runs')
    ax1.grid(True, alpha=0.25)
    savefig('resource_utilization')

    # Side-by-side: left panel anchored at 0 to prove stability vs the
    # configured Bloom capacity; right panel zoomed in to show the actual
    # per-worker variation that the left panel hides.
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11.5, 4.4))

    ax_left.plot(workers, redis, marker='o', linewidth=2, color='#d62728')
    ax_left.axhline(17.14, linestyle='--', color='gray', linewidth=1)
    ax_left.text(workers[-1], 17.14, ' Bloom bitmap = 17.14 MB',
                 va='bottom', ha='right', fontsize=8, color='gray')
    ax_left.set_title('Redis memory (anchored at 0)')
    ax_left.set_xlabel('Worker processes')
    ax_left.set_ylabel('Redis memory (MB)')
    ax_left.set_ylim(bottom=0, top=max(redis) * 1.6)
    ax_left.set_xticks(workers)
    style_axes(ax_left)

    ax_right.plot(workers, redis, marker='o', linewidth=2, color='#d62728')
    span = max(redis) - min(redis)
    pad = max(span * 0.2, 0.05)
    ax_right.set_ylim(min(redis) - pad, max(redis) + pad)
    ax_right.set_title(f'Zoomed view (range {span:.2f} MB across 1..20 workers)')
    ax_right.set_xlabel('Worker processes')
    ax_right.set_ylabel('Redis memory (MB)')
    ax_right.set_xticks(workers)
    for w, m in zip(workers, redis):
        if w in (1, 5, 10, 15, 20):
            ax_right.annotate(f'{m:.2f}', (w, m), textcoords='offset points',
                              xytext=(0, 6), ha='center', fontsize=7, color='#d62728')
    style_axes(ax_right)

    plt.suptitle('Redis Memory During Stress Runs', y=1.02)
    savefig('redis_memory_usage')


def plot_redis_latency(rows):
    if not rows:
        return
    labels = [row['operation'].replace('_', '\n') for row in rows]
    means = [as_float(row, 'mean_ms') for row in rows]
    p95s = [as_float(row, 'p95_ms') for row in rows]
    x = list(range(len(rows)))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8.0, 4.4))
    ax.bar([i - width / 2 for i in x], means, width=width, label='Mean')
    ax.bar([i + width / 2 for i in x], p95s, width=width, label='P95')
    ax.set_title('Redis Coordination Operation Latency')
    ax.set_xlabel('Redis operation')
    ax.set_ylabel('Latency (milliseconds)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha='right')
    ax.legend()
    style_axes(ax)
    savefig('redis_operation_latency')


def latex_escape(value):
    text = str(value)
    replacements = {
        '\\': r'\textbackslash{}',
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\textasciicircum{}',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def fnum(value, digits=2):
    try:
        return f'{float(value):.{digits}f}'
    except (TypeError, ValueError):
        return latex_escape(value)


def select_worker_rows(rows):
    wanted = {1, 2, 4, 8, 12, 16, 20}
    selected = [row for row in rows if int(float(row['workers'])) in wanted]
    return selected or rows


def table_or_note(rows, note):
    return bool(rows), note


def write_report_tables():
    """Write data-backed LaTeX tables consumed by report/main.tex."""
    report_path = PROJECT_ROOT / 'report' / 'generated_results.tex'
    scaling = read_csv(PROCESSED_DIR / 'crawl_scaling.csv') if (PROCESSED_DIR / 'crawl_scaling.csv').exists() else []
    indexing = read_csv(PROCESSED_DIR / 'indexing_scaling.csv') if (PROCESSED_DIR / 'indexing_scaling.csv').exists() else []
    queries = read_csv(PROCESSED_DIR / 'query_latency.csv') if (PROCESSED_DIR / 'query_latency.csv').exists() else []
    fault = read_csv(PROCESSED_DIR / 'fault_tolerance.csv') if (PROCESSED_DIR / 'fault_tolerance.csv').exists() else []
    streaming = read_csv(PROCESSED_DIR / 'streaming_indexer_lag.csv') if (PROCESSED_DIR / 'streaming_indexer_lag.csv').exists() else []
    pagerank = read_csv(PROCESSED_DIR / 'pagerank_convergence.csv') if (PROCESSED_DIR / 'pagerank_convergence.csv').exists() else []
    memory = read_csv(PROCESSED_DIR / 'memory_efficiency.csv') if (PROCESSED_DIR / 'memory_efficiency.csv').exists() else []
    mongo = read_csv(PROCESSED_DIR / 'mongodb_latency.csv') if (PROCESSED_DIR / 'mongodb_latency.csv').exists() else []
    redis = read_csv(PROCESSED_DIR / 'redis_latency.csv') if (PROCESSED_DIR / 'redis_latency.csv').exists() else []
    correctness_path = PROCESSED_DIR / 'search_correctness.json'
    if not correctness_path.exists():
        correctness_path = PROCESSED_DIR / 'inverted_index_correctness.json'
    correctness = read_json(correctness_path) if correctness_path.exists() else {}

    lines = [
        '% Auto-generated by scripts/generate_evaluation_graphs.py. Do not edit by hand.',
        r'\subsection{Measured Results}',
        'The tables in this subsection are generated directly from files under '
        r'\code{results/processed/}. No stale historical measurements are carried forward.',
    ]

    if scaling:
        baseline = scaling[0]
        example = next((row for row in scaling if int(float(row['workers'])) >= 4), scaling[-1])
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{Crawler worker scaling summary. Full data: \code{results/processed/crawl\_scaling.csv}.}',
            r'\begin{tabular}{rrrrrr}',
            r'\toprule',
            r'Workers & Pages & Time (s) & Pages/s & Speedup & Efficiency \\',
            r'\midrule',
        ])
        for row in select_worker_rows(scaling):
            lines.append(
                f"{int(float(row['workers']))} & "
                f"{int(float(row['crawled_pages']))} & "
                f"{fnum(row['total_time_seconds'])} & "
                f"{fnum(row['pages_per_second'])} & "
                f"{fnum(row['speedup'])} & "
                f"{fnum(row['parallel_efficiency'])} \\\\"
            )
        lines.extend([
            r'\bottomrule',
            r'\end{tabular}',
            r'\normalsize',
            r'\end{table}',
            (
                r'\noindent\textbf{Worked scaling calculation.} '
                f"For {int(float(example['workers']))} workers, throughput is "
                f"${int(float(example['crawled_pages']))}/{fnum(example['total_time_seconds'])}"
                f" = {fnum(example['pages_per_second'])}$ pages/s. "
                f"Using the one-worker baseline $T_1={fnum(baseline['total_time_seconds'])}$ s, "
                f"speedup is $T_1/T_N={fnum(example['speedup'])}$ and parallel efficiency is "
                f"$\\mathrm{{speedup}}/N={fnum(example['parallel_efficiency'])}$."
            ),
        ])

    if indexing:
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{Streaming indexer scaling on synthetic documents. Full data: \code{indexing\_scaling.csv}.}',
            r'\begin{tabular}{rrrrrr}',
            r'\toprule',
            r'Docs & Events & Tokens & Terms & Postings & Time (s) \\',
            r'\midrule',
        ])
        for row in indexing:
            lines.append(
                f"{int(float(row['documents']))} & "
                f"{int(float(row.get('events_processed', row['documents'])))} & "
                f"{int(float(row['total_tokens']))} & "
                f"{int(float(row['unique_terms']))} & "
                f"{int(float(row['postings']))} & "
                f"{fnum(row['indexing_time_seconds'])} \\\\"
            )
        lines.extend([r'\bottomrule', r'\end{tabular}', r'\normalsize', r'\end{table}'])

    if queries:
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{FastAPI search latency by query class. Full data: \code{query\_latency.csv}.}',
            r'\begin{tabular}{lrrrr}',
            r'\toprule',
            r'Query class & Runs & Mean ms & Median ms & P95 ms \\',
            r'\midrule',
        ])
        for row in queries:
            lines.append(
                f"{latex_escape(row['query_type'])} & "
                f"{int(float(row['runs']))} & "
                f"{fnum(row['mean_latency_ms'])} & "
                f"{fnum(row['median_latency_ms'])} & "
                f"{fnum(row['p95_latency_ms'])} \\\\"
            )
        lines.extend([r'\bottomrule', r'\end{tabular}', r'\normalsize', r'\end{table}'])

    if streaming:
        first, last = streaming[0], streaming[-1]
        max_pending = max(float(row['pending_events']) for row in streaming)
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{Streaming indexer drain summary. Full data: \code{streaming\_indexer\_lag.csv}.}',
            r'\begin{tabular}{lrrr}',
            r'\toprule',
            r'Metric & Value & Unit & Source \\',
            r'\midrule',
            f"Initial pending events & {int(float(first['pending_events']))} & events & outbox \\\\",
            f"Maximum pending events & {int(max_pending)} & events & outbox \\\\",
            f"Total processed & {int(float(last['processed_total']))} & events & streaming indexer \\\\",
            f"Drain time & {fnum(last['elapsed_seconds'])} & seconds & wall clock \\\\",
            r'\bottomrule',
            r'\end{tabular}',
            r'\normalsize',
            r'\end{table}',
        ])

    if pagerank:
        final = pagerank[-1]
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{PageRank recompute convergence summary. Full data: \code{pagerank\_convergence.csv}.}',
            r'\begin{tabular}{lll}',
            r'\toprule',
            r'Metric & Value & Notes \\',
            r'\midrule',
            f"Source & {latex_escape(final.get('source', 'unknown'))} & input graph \\\\",
            f"Nodes & {int(float(final['nodes']))} & graph snapshot \\\\",
            f"Edges & {int(float(final['edges']))} & graph snapshot \\\\",
            f"Iterations & {int(float(final['iteration']))} & stopped at epsilon/max \\\\",
            f"Final L1 delta & {fnum(final['l1_delta'], 8)} & epsilon {fnum(final['epsilon'], 8)} \\\\",
            r'\bottomrule',
            r'\end{tabular}',
            r'\normalsize',
            r'\end{table}',
        ])

    if fault:
        row = fault[0]
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{Fault-tolerance kill scenario. Full data: \code{fault\_tolerance.csv}.}',
            r'\begin{tabular}{lr}',
            r'\toprule',
            r'Metric & Value \\',
            r'\midrule',
            f"Workers started & {int(float(row['workers_started']))} \\\\",
            f"Completed pages & {int(float(row['completed_pages']))} \\\\",
            f"Processing leases remaining & {int(float(row['processing_size_after_run']))} \\\\",
            f"Index outbox pending & {int(float(row.get('index_outbox_pending', 0)))} \\\\",
            f"Graph outbox pending & {int(float(row.get('graph_outbox_pending', 0)))} \\\\",
            f"Recovered URLs & {int(float(row['recovered_urls']))} \\\\",
            r'\bottomrule',
            r'\end{tabular}',
            r'\normalsize',
            r'\end{table}',
        ])

    if correctness:
        kiwi = correctness.get('kiwi_top_result') or {}
        unique = correctness.get('unique_top_result') or {}
        lines.extend([
            r'\begin{table}[H]',
            r'\centering',
            r'\small',
            r'\caption{Deterministic search correctness fixture. Full data: \code{search\_correctness.json}.}',
            r'\begin{tabular}{p{0.26\textwidth}p{0.48\textwidth}p{0.14\textwidth}}',
            r'\toprule',
            r'Check & Observed & Passed \\',
            r'\midrule',
            f"kiwi top URL & {latex_escape(kiwi.get('url', 'missing'))} & {latex_escape(correctness.get('passed', False))} \\\\",
            f"nebulaunique top URL & {latex_escape(unique.get('url', 'missing'))} & {latex_escape(bool(unique))} \\\\",
            f"term postings & {latex_escape(correctness.get('term_postings_count', 'missing'))} & {latex_escape(correctness.get('term_postings_count', 0) > 0)} \\\\",
            r'\bottomrule',
            r'\end{tabular}',
            r'\normalsize',
            r'\end{table}',
        ])

    if memory or mongo or redis:
        lines.append(
            r'Memory, Redis, and MongoDB microbenchmarks are shown in the generated figures; '
            r'the exact measured rows are preserved in \code{memory\_efficiency.csv}, '
            r'\code{redis\_latency.csv}, and \code{mongodb\_latency.csv}.'
        )

    report_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_results_readme():
    text = """# Evaluation Results

These files are generated from real local experiments against the current Docker/Compose-backed crawler implementation. Do not copy old CSVs into this directory: the report intentionally reads these generated files so it cannot display stale numbers from the previous architecture.

## Data files

- `results/raw/evaluation_results.json`: combined raw JSON output from the experiment run.
- `results/processed/crawl_scaling.csv`: worker-count scaling measurements from independent worker subprocesses.
- `results/processed/worker_contribution.csv`: per-worker page counts from scaling runs.
- `results/processed/indexing_scaling.csv`: streaming-indexer drain time and index-size measurements.
- `results/processed/streaming_indexer_lag.csv`: index_outbox lag while the real streaming indexer drains events.
- `results/processed/pagerank_convergence.csv`: L1 delta per PageRank power-iteration pass.
- `results/processed/query_latency.csv`: FastAPI search latency over repeated HTTP requests.
- `results/processed/search_correctness.json`: deterministic streaming index/search correctness validation.
- `results/processed/fault_tolerance.csv`: one-worker-terminated experiment result.
- `results/processed/bloom_memory_calculation.csv`: Bloom filter formula output and exact-set memory baseline.
- `results/processed/memory_efficiency.csv`: Bloom versus exact-set memory comparison for graphing.
- `results/processed/redis_latency.csv`: Redis frontier, Bloom, lock, and robots-cache operation latency.
- `results/processed/redis_memory_usage.csv`: Redis benchmark memory usage for temporary keys.
- `results/processed/mongodb_latency.csv`: MongoDB transaction, outbox claim, posting upsert, and search aggregation latency.
- `results/processed/sustained_throughput_windows.csv`: throughput-window summaries derived from measured scaling runs.
- `results/processed/resource_scaling_1_20.csv`: CPU, memory, Redis memory, and throughput data from 1-to-20 worker resource runs.

## Figures

Figures in `results/figures/` and `report/figures/` are generated only from the processed data above.

## Reproduction

Start Redis and MongoDB first:

```powershell
docker compose up -d
```

Run experiments:

```powershell
python scripts/run_evaluation_experiments.py
python scripts/run_resource_scaling_1_20.py
python scripts/run_report_supplement_metrics.py
```

Generate figures:

```powershell
python scripts/generate_evaluation_graphs.py
```

Compile the report:

```powershell
pdflatex -interaction=nonstopmode -output-directory report report/main.tex
pdflatex -interaction=nonstopmode -output-directory report report/main.tex
```

## Limitations

The crawl experiments use a deterministic local website spread across multiple ports to simulate multiple domains. Workers still use the real Redis frontier, processing ledger, MongoDB transactions, MinIO blob store, streaming indexer, PageRank worker, and search aggregation path. The local fixture makes the results reproducible, but it does not capture real-world DNS variance, ISP/CDN latency, or anti-bot defences.
"""
    (PROJECT_ROOT / 'results' / 'README.md').write_text(text, encoding='utf-8')


def main():
    scaling = read_csv(PROCESSED_DIR / 'crawl_scaling.csv')
    workers = read_csv(PROCESSED_DIR / 'worker_contribution.csv')
    indexing = read_csv(PROCESSED_DIR / 'indexing_scaling.csv')
    queries = read_csv(PROCESSED_DIR / 'query_latency.csv')
    fault = read_csv(PROCESSED_DIR / 'fault_tolerance.csv')
    memory = read_csv(PROCESSED_DIR / 'memory_efficiency.csv') if (PROCESSED_DIR / 'memory_efficiency.csv').exists() else []
    mongo_latency = read_csv(PROCESSED_DIR / 'mongodb_latency.csv') if (PROCESSED_DIR / 'mongodb_latency.csv').exists() else []
    sustained = read_csv(PROCESSED_DIR / 'sustained_throughput_windows.csv') if (PROCESSED_DIR / 'sustained_throughput_windows.csv').exists() else []
    resources = read_csv(PROCESSED_DIR / 'resource_scaling_1_20.csv') if (PROCESSED_DIR / 'resource_scaling_1_20.csv').exists() else []
    redis_latency = read_csv(PROCESSED_DIR / 'redis_latency.csv') if (PROCESSED_DIR / 'redis_latency.csv').exists() else []
    streaming_lag = read_csv(PROCESSED_DIR / 'streaming_indexer_lag.csv') if (PROCESSED_DIR / 'streaming_indexer_lag.csv').exists() else []
    pagerank_convergence = read_csv(PROCESSED_DIR / 'pagerank_convergence.csv') if (PROCESSED_DIR / 'pagerank_convergence.csv').exists() else []

    plot_scaling(scaling)
    plot_worker_distribution(workers)
    plot_indexing(indexing)
    plot_query_latency(queries)
    plot_streaming_indexer_lag(streaming_lag)
    plot_pagerank_convergence(pagerank_convergence)
    plot_fault_tolerance(fault, scaling)
    plot_memory_efficiency(memory)
    plot_mongo_latency(mongo_latency)
    plot_sustained_windows(sustained)
    plot_resource_utilization(resources)
    plot_redis_latency(redis_latency)
    write_report_tables()
    write_results_readme()
    print(f'Wrote figures to {FIGURES_DIR} and {REPORT_FIGURES_DIR}')


if __name__ == '__main__':
    main()
