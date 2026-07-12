# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import logging
import os
import matplotlib.pyplot as plt
import numpy as np
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QUERY_KEY_RE = re.compile(r'^(\d+)([a-z]*)$')

def geomean(data):
    return np.prod(data) ** (1 / len(data))

def get_durations(result, query_key):
    """Extract durations from a query result, supporting both old (list) and new (dict) formats."""
    value = result[query_key]
    if isinstance(value, dict):
        return value["durations"]
    return value

def normalize_result(raw):
    """Return a Comet-style {query_str: {durations, row_count}} dict.

    Accepts either the Comet/Spark native format (already keyed by query
    string) or the Ballista ``tpch`` binary format
    ({"queries": [{"query", "iterations": [{"elapsed", "row_count"}]}]}).
    Comet/Spark input is returned unchanged.
    """
    if isinstance(raw, dict) and isinstance(raw.get("queries"), list):
        normalized = {}
        for q in raw["queries"]:
            key = str(q["query"])
            entry = {"durations": [it["elapsed"] for it in q["iterations"]]}
            row_counts = [it["row_count"] for it in q["iterations"] if "row_count" in it]
            if row_counts:
                entry["row_count"] = row_counts[0]
            normalized[key] = entry
        return normalized
    return raw

def query_sort_key(key):
    """Sort key for query labels like "14", "14a", "14b" so sub-queries sit between 14 and 15."""
    m = QUERY_KEY_RE.match(str(key))
    if m:
        return (int(m.group(1)), m.group(2))
    return (float('inf'), str(key))

def get_all_queries(results):
    """Return the sorted union of query keys across all result sets, as strings."""
    all_keys = set()
    for result in results:
        all_keys.update(result.keys())
    query_keys = [str(k) for k in all_keys if QUERY_KEY_RE.match(str(k))]
    return sorted(query_keys, key=query_sort_key)

def get_common_queries(results, labels):
    """Return queries present in ALL result sets, warning about queries missing from some files."""
    all_queries = get_all_queries(results)
    common = []
    for query in all_queries:
        key = str(query)
        present = [labels[i] for i, r in enumerate(results) if key in r]
        missing = [labels[i] for i, r in enumerate(results) if key not in r]
        if missing:
            logger.warning(f"Query {query}: present in [{', '.join(present)}] but missing from [{', '.join(missing)}]")
        if not missing:
            common.append(query)
    return common

def check_result_consistency(results, labels, benchmark):
    """Log warnings if row counts or result hashes differ across result sets."""
    all_queries = get_all_queries(results)
    for query in all_queries:
        key = str(query)
        row_counts = []
        hashes = []
        for i, result in enumerate(results):
            if key not in result:
                continue
            value = result[key]
            if not isinstance(value, dict):
                continue
            if "row_count" in value:
                row_counts.append((labels[i], value["row_count"]))
            if "result_hash" in value:
                hashes.append((labels[i], value["result_hash"]))

        if len(row_counts) > 1:
            counts = set(rc for _, rc in row_counts)
            if len(counts) > 1:
                details = ", ".join(f"{label}={rc}" for label, rc in row_counts)
                logger.warning(f"Query {query}: row count mismatch: {details}")

        if len(hashes) > 1:
            hash_values = set(h for _, h in hashes)
            if len(hash_values) > 1:
                details = ", ".join(f"{label}={h}" for label, h in hashes)
                logger.warning(f"Query {query}: result hash mismatch: {details}")

def generate_query_rel_speedup_chart(baseline, comparison, label1: str, label2: str, benchmark: str, title: str, common_queries=None, output_dir: str = '.'):
    if common_queries is None:
        common_queries = range(1, query_count(benchmark)+1)
    results = []
    for query in common_queries:
        a = np.median(np.array(get_durations(baseline, str(query))))
        b = np.median(np.array(get_durations(comparison, str(query))))
        if a > b:
            speedup = a/b-1
        else:
            speedup = -(1/(a/b)-1)
        results.append(("q" + str(query), round(speedup*100, 0)))

    results = sorted(results, key=lambda x: -x[1])

    queries, speedups = zip(*results)

    # Create figure and axis
    if benchmark == "tpch":
        fig, ax = plt.subplots(figsize=(10, 6))
    else:
        fig, ax = plt.subplots(figsize=(35, 10))

    # Create bar chart
    bars = ax.bar(queries, speedups, color='skyblue')

    # Add text annotations
    for bar, speedup in zip(bars, speedups):
        yval = bar.get_height()
        if yval >= 0:
            ax.text(bar.get_x() + bar.get_width() / 2.0, min(800, yval+5), f'{yval:.0f}%', va='bottom', ha='center', fontsize=8,
                    color='blue', rotation=90)
        else:
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval:.0f}%', va='top', ha='center', fontsize=8,
                    color='blue', rotation=90)

    # Add title and labels
    ax.set_title(label2 + " speedup over " + label1 + " (" + title + ")")
    ax.set_ylabel('Speedup Percentage (100% speedup = 2x faster)')
    ax.set_xlabel('Query')

    # Customize the y-axis to handle both positive and negative values better
    ax.axhline(0, color='black', linewidth=0.8)
    min_value = (min(speedups) // 100) * 100
    max_value = ((max(speedups) // 100) + 1) * 100 + 50
    ax.set_ylim(min_value, max_value)

    # Show grid for better readability
    ax.yaxis.grid(True)

    # Save the plot as an image file
    plt.savefig(os.path.join(output_dir, f'{benchmark}_queries_speedup_rel.png'), format='png')

def generate_query_abs_speedup_chart(baseline, comparison, label1: str, label2: str, benchmark: str, title: str, common_queries=None, output_dir: str = '.'):
    if common_queries is None:
        common_queries = range(1, query_count(benchmark)+1)
    results = []
    for query in common_queries:
        a = np.median(np.array(get_durations(baseline, str(query))))
        b = np.median(np.array(get_durations(comparison, str(query))))
        speedup = a-b
        results.append(("q" + str(query), round(speedup, 1)))

    results = sorted(results, key=lambda x: -x[1])

    queries, speedups = zip(*results)

    # Create figure and axis
    if benchmark == "tpch":
        fig, ax = plt.subplots(figsize=(10, 6))
    else:
        fig, ax = plt.subplots(figsize=(35, 10))

    # Create bar chart
    bars = ax.bar(queries, speedups, color='skyblue')

    # Add text annotations
    for bar, speedup in zip(bars, speedups):
        yval = bar.get_height()
        if yval >= 0:
            ax.text(bar.get_x() + bar.get_width() / 2.0, min(800, yval+5), f'{yval:.1f}', va='bottom', ha='center', fontsize=8,
                    color='blue', rotation=90)
        else:
            ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval:.1f}', va='top', ha='center', fontsize=8,
                    color='blue', rotation=90)

    # Add title and labels
    ax.set_title(label2 + " speedup over " + label1 + " (" + title + ")")
    ax.set_ylabel('Speedup (in seconds)')
    ax.set_xlabel('Query')

    # Customize the y-axis to handle both positive and negative values better
    ax.axhline(0, color='black', linewidth=0.8)
    min_value = min(speedups) * 2 - 20
    max_value = max(speedups) * 1.5
    ax.set_ylim(min_value, max_value)

    # Show grid for better readability
    ax.yaxis.grid(True)

    # Save the plot as an image file
    plt.savefig(os.path.join(output_dir, f'{benchmark}_queries_speedup_abs.png'), format='png')

def generate_query_comparison_chart(results, labels, benchmark: str, title: str, common_queries=None, output_dir: str = '.'):
    if common_queries is None:
        common_queries = range(1, query_count(benchmark)+1)
    queries = []
    benches = []
    for _ in results:
        benches.append([])
    for query in common_queries:
        queries.append("q" + str(query))
        for i in range(0, len(results)):
            benches[i].append(np.median(np.array(get_durations(results[i], str(query)))))

    # Define the width of the bars
    bar_width = 0.3

    # Define the positions of the bars on the x-axis
    index = np.arange(len(queries)) * 1.5

    # Create a bar chart
    if benchmark == "tpch":
        fig, ax = plt.subplots(figsize=(15, 6))
    else:
        fig, ax = plt.subplots(figsize=(35, 6))

    for i in range(0, len(results)):
        bar = ax.bar(index + i * bar_width, benches[i], bar_width, label=labels[i])

    # Add labels, title, and legend
    ax.set_title(title)
    ax.set_xlabel('Queries')
    ax.set_ylabel('Query Time (seconds)')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(queries)
    ax.legend()

    # Save the plot as an image file
    plt.savefig(os.path.join(output_dir, f'{benchmark}_queries_compare.png'), format='png')

def generate_summary(results, labels, benchmark: str, title: str, common_queries=None, output_dir: str = '.'):
    if common_queries is None:
        common_queries = range(1, query_count(benchmark)+1)
    timings = []
    for _ in results:
        timings.append(0)

    num_queries = len([q for q in common_queries])
    for query in common_queries:
        for i in range(0, len(results)):
            timings[i] += np.median(np.array(get_durations(results[i], str(query))))

    # Create figure and axis
    fig, ax = plt.subplots()
    fig.set_size_inches(10, 6)

    # Add title and labels
    ax.set_title(title)
    ax.set_ylabel(f'Time in seconds to run {num_queries} {benchmark} queries (lower is better)')

    times = [round(x,0) for x in timings]

    # Create bar chart
    bars = ax.bar(labels, times, color='skyblue', width=0.8)

    # Add text annotations
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, yval, f'{yval}', va='bottom')  # va: vertical alignment

    plt.savefig(os.path.join(output_dir, f'{benchmark}_allqueries.png'), format='png')

def query_count(benchmark: str):
    if benchmark == "tpch":
        return 22
    elif benchmark == "tpcds":
        return 99
    else:
        raise ValueError("invalid benchmark name")

def main(files, labels, benchmark: str, title: str, output_dir: str = '.'):
    results = []
    for filename in files:
        with open(filename) as f:
            results.append(normalize_result(json.load(f)))
    check_result_consistency(results, labels, benchmark)
    common_queries = get_common_queries(results, labels)
    if not common_queries:
        logger.error("No queries found in common across all result files")
        return
    generate_summary(results, labels, benchmark, title, common_queries, output_dir=output_dir)
    generate_query_comparison_chart(results, labels, benchmark, title, common_queries, output_dir=output_dir)
    if len(files) == 2:
        generate_query_abs_speedup_chart(results[0], results[1], labels[0], labels[1], benchmark, title, common_queries, output_dir=output_dir)
        generate_query_rel_speedup_chart(results[0], results[1], labels[0], labels[1], benchmark, title, common_queries, output_dir=output_dir)

if __name__ == '__main__':
    argparse = argparse.ArgumentParser(description='Generate comparison')
    argparse.add_argument('filenames', nargs='+', type=str, help='JSON result files')
    argparse.add_argument('--labels', nargs='+', type=str, help='Labels')
    argparse.add_argument('--benchmark', type=str, help='Benchmark name (tpch or tpcds)')
    argparse.add_argument('--title', type=str, help='Chart title')
    argparse.add_argument('--output-dir', type=str, default='.', help='Directory to write PNGs to (default: cwd)')
    args = argparse.parse_args()
    main(args.filenames, args.labels, args.benchmark, args.title, output_dir=args.output_dir)
