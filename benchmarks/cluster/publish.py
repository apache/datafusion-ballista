# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Regenerate the published benchmark results from the JSON in published/.

    python3 publish.py

Each file in published/tpch-sf1000/ is one column of the results table, in the
format run.py writes, plus a "label" for the column heading. A "total" is only
needed when the per-query means were rounded before they were recorded.

Rewrites the results table on the docs' benchmarking page, and the charts and
the speedups over Spark in the top-level README.

published/memory/tpch-q18-sf1000.json is a memory sweep: one query run at
smaller and smaller executor sizes for several memory configurations of each
engine. It becomes the tables and the two charts on the docs' memory page.
"""

import html
import json
import math
import os
import re
import sys

import compare

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PUBLISHED = os.path.join(HERE, "published", "tpch-sf1000")
GUIDE = os.path.join(
    REPO, "docs", "source", "contributors-guide", "benchmarking", "index.md"
)
README = os.path.join(REPO, "README.md")
MEMORY = os.path.join(HERE, "published", "memory", "tpch-q18-sf1000.json")
MEMORY_PAGE = os.path.join(
    REPO, "docs", "source", "contributors-guide", "benchmarking", "memory.md"
)
IMAGES = os.path.join(REPO, "docs", "source", "_static", "images")

# The result files, in the order of the table's columns.
TABLE_COLUMNS = [
    "ballista-staged",
    "ballista-pipelined",
    "datafusion-distributed",
    "spark",
    "comet",
    "trino",
]

BEGIN = "<!-- BEGIN GENERATED BENCHMARK RESULTS -->"
END = "<!-- END GENERATED BENCHMARK RESULTS -->"
SUMMARY = re.compile(
    r"\*\*[\d.]+x faster than Spark\*\* by default and \*\*[\d.]+x faster\*\*"
)

# The README compares Ballista with Spark. Each series is a result file, how the
# charts label it and its CSS class, with Spark first as the baseline.
CHART_SERIES = [
    ("spark", "Apache {label}", "spark"),
    ("ballista-staged", "{label} (default)", "staged"),
    ("ballista-pipelined", "{label} (experimental)", "pipelined"),
]

TOTAL_SUBTITLE = "Total time for the 22 queries of a 1 TB (SF1000) benchmark derived from TPC-H. Lower is better."
TOTAL_NOTE = "Derived from TPC-H, not an official TPC-H result. 32 executors with 8 vCPUs each. Mean of 2 runs including the cold first run."
PER_QUERY_SUBTITLE = "Time per query in seconds. Lower is better."
PER_QUERY_NOTE = "Derived from TPC-H, not an official TPC-H result. SF1000, 32 executors with 8 vCPUs each. Mean of 2 runs."

FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif'

PLOT_HEIGHT = 280
BASELINE = 384
# Bars run past the baseline and are clipped there, so only their tops are rounded.
BAR_OVERHANG = 3


def load_results():
    """The published results by file name, in the order of the table's columns."""
    return {
        name: compare.load(os.path.join(PUBLISHED, f"{name}.json"))
        for name in TABLE_COLUMNS
    }


def results_table(runs):
    """The results table for the docs, padded the way prettier pads it."""
    rows = compare.rows(runs, [run["label"] for run in runs])
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    rule = ["-" * (width - 1) + ":" for width in widths]
    return "\n".join(
        "| " + " | ".join(cell.rjust(w) for cell, w in zip(row, widths)) + " |"
        for row in [rows[0], rule, *rows[1:]]
    )


def with_table(page, table, markers=(BEGIN, END)):
    """The page with the text between a pair of generated-results markers replaced."""
    first, last = markers
    before, begin, rest = page.partition(first)
    _, end, after = rest.partition(last)
    if not begin or not end:
        sys.exit(f"the page has no {first} ... {last} block")
    return f"{before}{first}\n\n{table}\n\n{last}{after}"


def chart_series(results):
    """The charts' series, each with its label, CSS class, total and query times."""
    series = []
    for name, label, css in CHART_SERIES:
        run = results[name]
        if any(q.get("error") or "mean" not in q for q in run["queries"]):
            sys.exit(f"{name} has a failed query, so it cannot be charted")
        series.append(
            {
                "label": html.escape(label.format(label=run["label"])),
                "css": css,
                "total": compare.total(run),
                "means": {q["query"]: q["mean"] for q in run["queries"]},
            }
        )
    return series


def speedup(series, s):
    """How many times faster than the baseline a series is, such as 1.7x."""
    return f"{series[0]['total'] / s['total']:.1f}x"


def with_speedups(readme, series):
    """The README with its summary quoting the speedups over Spark."""
    updated, count = SUMMARY.subn(
        f"**{speedup(series, series[1])} faster than Spark** by default and "
        f"**{speedup(series, series[2])} faster**",
        readme,
    )
    if count != 1:
        sys.exit("the README does not quote the speedups over Spark where expected")
    return updated


def number(value):
    """An SVG coordinate: at most two decimals and no trailing zeros."""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def axis(slowest):
    """The top of the per-query chart's axis and its tick step, both round numbers."""
    top = math.ceil(slowest / 20) * 20
    steps = (5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000)
    return top, next(step for step in steps if top / step <= 6)


def stylesheet(rules, dark_rules):
    """A chart's style element, with the rules that chart adds to the shared ones."""
    return [
        "  <style>",
        f"    text {{ font-family: {FONT}; fill: #1f2328; }}",
        "    .bg { fill: #ffffff; }",
        "    .title { font-size: 20px; font-weight: 600; }",
        *[f"    {rule}" for rule in rules],
        "    .spark { fill: #898781; }",
        "    .staged { fill: #2a78d6; }",
        "    .pipelined { fill: #eb6834; }",
        "    @media (prefers-color-scheme: dark) {",
        "      text { fill: #e6edf3; }",
        "      .bg { fill: #0d1117; }",
        *[f"      {rule}" for rule in dark_rules],
        "      .staged { fill: #3987e5; }",
        "      .pipelined { fill: #d95926; }",
        "    }",
        "  </style>",
    ]


def total_chart(series):
    """The bar chart of each series' total time."""
    scale = 540 / (math.ceil(max(s["total"] for s in series) / 100) * 100)
    height = 116 + 40 * len(series)
    title = (
        f"{speedup(series, series[1])} faster than Apache Spark by default, "
        f"{speedup(series, series[2])} with pipelined execution"
    )
    described = [
        f"{s['label']} takes {s['total']:.0f} seconds"
        + ("" if s is series[0] else f", {speedup(series, s)} faster")
        for s in series
    ]
    aria = f"Bar chart of total time for the 22 queries. {'. '.join(described)}."

    bars, labels, values = [], [], []
    for i, s in enumerate(series):
        top = 84 + 40 * i
        end = number(264 + s["total"] * scale)
        corner = number(264 + s["total"] * scale - 4)
        bars.append(
            f'  <path class="{s["css"]}" d="M264 {top}H{corner}Q{end} {top} {end} '
            f'{top + 4}V{top + 20}Q{end} {top + 24} {corner} {top + 24}H264Z"/>'
        )
        labels.append(
            f'  <text class="label" x="254" y="{top + 17}" text-anchor="end">'
            f"{s['label']}</text>"
        )
        detail = (
            ""
            if s is series[0]
            else f'<tspan class="delta"> · {speedup(series, s)} faster</tspan>'
        )
        values.append(
            f'  <text class="value" x="{number(264 + s["total"] * scale + 10)}" '
            f'y="{top + 17}">{s["total"]:.0f} s{detail}</text>'
        )

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 {height}" '
        f'width="880" height="{height}" role="img" aria-label="{aria}">',
        *stylesheet(
            [
                ".sub { font-size: 14px; fill: #59636e; }",
                ".label { font-size: 15px; }",
                ".value { font-size: 15px; font-weight: 600; }",
                ".delta { font-weight: 400; }",
                ".note { font-size: 12.5px; fill: #59636e; }",
                ".axis { stroke: #c3c2b7; stroke-width: 1; }",
            ],
            [
                ".sub, .note { fill: #9198a1; }",
                ".axis { stroke: #3d444d; }",
            ],
        ),
        f'  <rect class="bg" width="880" height="{height}" rx="6"/>',
        f'  <text class="title" x="24" y="36">{title}</text>',
        f'  <text class="sub" x="24" y="60">{TOTAL_SUBTITLE}</text>',
        "",
        *bars,
        f'  <line class="axis" x1="264" y1="76" x2="264" y2="{76 + 40 * len(series)}"/>',
        "",
        *labels,
        "",
        *values,
        "",
        f'  <text class="note" x="24" y="{height - 14}">{TOTAL_NOTE}</text>',
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


def per_query_chart(series):
    """The column chart of each series' time per query."""
    baseline = series[0]["means"]
    queries = sorted(baseline)
    top, step = axis(max(mean for s in series for mean in s["means"].values()))
    scale = PLOT_HEIGHT / top
    ticks = range(0, top + 1, step)
    bar_width = (31 - 2 * (len(series) - 1)) // len(series)

    def wins(s):
        won = sum(s["means"][q] < baseline[q] for q in queries)
        return (
            f"all {len(queries)}" if won == len(queries) else f"{won} of {len(queries)}"
        )

    title = (
        f"Faster than Spark on {wins(series[1])} queries by default, "
        f"on {wins(series[2])} pipelined"
    )
    names = f"{', '.join(s['label'] for s in series[:-1])} and {series[-1]['label']}"
    aria = (
        f"Column chart of time per query, comparing {names}. {title}. "
        "The numbers are in the table in the benchmarking guide."
    )

    legend = []
    x = 24
    for s in series:
        legend.append(
            f'  <rect class="{s["css"]}" x="{x}" y="76" width="12" height="12" rx="2"/>'
        )
        legend.append(f'  <text class="legend" x="{x + 18}" y="87">{s["label"]}</text>')
        x += 38 + 7 * len(s["label"])

    columns = []
    for offset, s in enumerate(series):
        columns.append(f'  <g class="{s["css"]}" clip-path="url(#above-baseline)">')
        for i, query in enumerate(queries):
            height = s["means"][query] * scale
            columns.append(
                f'    <rect x="{55 + i * 37 + offset * (bar_width + 2)}" '
                f'y="{number(BASELINE - height)}" width="{bar_width}" '
                f'height="{number(height + BAR_OVERHANG)}" rx="2"/>'
            )
        columns += ["  </g>", ""]

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 444" '
        f'width="880" height="444" role="img" aria-label="{aria}">',
        *stylesheet(
            [
                ".sub { font-size: 14px; fill: #59636e; }",
                ".legend { font-size: 14px; }",
                ".tick { font-size: 12.5px; fill: #59636e; }",
                ".note { font-size: 12.5px; fill: #59636e; }",
                ".grid { stroke: #e1e0d9; stroke-width: 1; }",
                ".axis { stroke: #c3c2b7; stroke-width: 1; }",
            ],
            [
                ".sub, .note, .tick { fill: #9198a1; }",
                ".grid { stroke: #21262d; }",
                ".axis { stroke: #3d444d; }",
            ],
        ),
        "  <defs>",
        f'    <clipPath id="above-baseline"><rect x="0" y="0" width="880" height="{BASELINE}"/></clipPath>',
        "  </defs>",
        '  <rect class="bg" width="880" height="444" rx="6"/>',
        f'  <text class="title" x="24" y="36">{title}</text>',
        f'  <text class="sub" x="24" y="60">{PER_QUERY_SUBTITLE}</text>',
        "",
        *legend,
        "",
        *[
            f'  <line class="grid" x1="52" y1="{number(BASELINE - tick * scale)}" '
            f'x2="866" y2="{number(BASELINE - tick * scale)}"/>'
            for tick in ticks[1:]
        ],
        "",
        *columns,
        f'  <line class="axis" x1="52" y1="{BASELINE}" x2="866" y2="{BASELINE}"/>',
        "",
        '  <g class="tick" text-anchor="end">',
        *[
            f'    <text class="tick" x="44" y="{number(BASELINE + 4 - tick * scale)}">{tick}</text>'
            for tick in ticks
        ],
        "  </g>",
        "",
        '  <g text-anchor="middle">',
        *[
            f'    <text class="tick" x="{number(70.5 + i * 37)}" y="402">Q{query}</text>'
            for i, query in enumerate(queries)
        ],
        "  </g>",
        "",
        f'  <text class="note" x="24" y="430">{PER_QUERY_NOTE}</text>',
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Memory sweep
# ---------------------------------------------------------------------------

MEMORY_SUMMARY = (
    "<!-- BEGIN GENERATED MEMORY SUMMARY -->",
    "<!-- END GENERATED MEMORY SUMMARY -->",
)
MEMORY_RUNS = (
    "<!-- BEGIN GENERATED MEMORY RUNS -->",
    "<!-- END GENERATED MEMORY RUNS -->",
)
# Each engine's CSS class and name in the charts: Spark grey and Ballista blue
# as in the README charts, Comet the next colour.
MEMORY_ENGINES = {
    "ballista": ("staged", "Ballista"),
    "spark": ("spark", "Spark"),
    "comet": ("comet", "Spark + Comet"),
}
MEMORY_NOTE = "Derived from TPC-H, not an official TPC-H result. 2 executors of 16 CPUs. Each size is a fresh cluster; mean of 2 runs."
RESULTS = {
    "ok": "Passed",
    "oom-killed": "Failed: an executor ran out of memory",
    "timeout": "Did not finish within the time limit",
    "jvm-oom": "Failed: Java heap exhausted",
    "pool-exhausted": "Failed: memory pool exhausted",
}


def load_memory():
    with open(MEMORY) as f:
        return json.load(f)


def is_clean(run):
    """Passed with no executor killed for running out of memory."""
    return run["status"] == "ok" and not run.get("executor_oom_kills")


def memory_summary(config):
    """A configuration's smallest clean size and its time there and at its largest size.

    "failed" is the largest size below the smallest clean one, which failed or
    needed an executor restarted.
    """
    runs = {r["executor_memory_gib"]: r for r in config["runs"]}
    clean = sorted(size for size, r in runs.items() if is_clean(r))
    if not clean:
        return None
    smallest, largest = clean[0], max(runs)
    below = [size for size in runs if size < smallest]
    return {
        "smallest": smallest,
        "smallest_mean": runs[smallest]["mean"],
        "largest": largest,
        "largest_mean": runs[largest].get("mean"),
        "failed": max(below) if below else None,
    }


def best_configs(sweep):
    """Each engine's configuration with the smallest clean size, ties to the faster."""
    best = {}
    for config in sweep["configs"]:
        summary = memory_summary(config)
        if summary is None:
            continue
        key = (summary["smallest"], summary["smallest_mean"])
        engine = config["engine"]
        if engine not in best or key < best[engine][0]:
            best[engine] = (key, config, summary)
    return [best[e][1:] for e in MEMORY_ENGINES if e in best]


def markdown_table(rows, right):
    """A table padded the way prettier pads it; right[i] right-aligns column i."""
    widths = [max(3, *(len(row[i]) for row in rows)) for i in range(len(rows[0]))]
    rule = [("-" * (w - 1) + ":") if r else "-" * w for w, r in zip(widths, right)]

    def line(cells):
        return (
            "| "
            + " | ".join(
                c.rjust(w) if r else c.ljust(w) for c, w, r in zip(cells, widths, right)
            )
            + " |"
        )

    return "\n".join(
        [line(rows[0]), "| " + " | ".join(rule) + " |"] + [line(r) for r in rows[1:]]
    )


def gib(value):
    return f"{value:g}"


def memory_summary_table(sweep):
    rows = [
        [
            "Configuration",
            "Memory split",
            "Smallest size (GiB)",
            "Mean there (s)",
            "Largest size (GiB)",
            "Mean there (s)",
        ]
    ]
    for config in sweep["configs"]:
        s = memory_summary(config)
        if s is None:
            rows.append([config["label"], config["split"], "-", "", "", ""])
            continue
        rows.append(
            [
                config["label"],
                config["split"],
                gib(s["smallest"]),
                f"{s['smallest_mean']:.2f}",
                gib(s["largest"]),
                f"{s['largest_mean']:.2f}",
            ]
        )
    return markdown_table(rows, [False, False, True, True, True, True])


def run_result(run):
    if run["status"] == "ok" and run.get("executor_oom_kills"):
        kills = run["executor_oom_kills"]
        return (
            f"Passed after {kills} executor{'s' if kills > 1 else ''} ran out of memory"
        )
    return RESULTS.get(run["status"], f"Failed: {run['status']}")


def memory_runs_table(sweep):
    rows = [
        [
            "Configuration",
            "Executor (GiB)",
            "Heap / off-heap / overhead (GiB)",
            "Result",
            "Mean (s)",
            "Iterations (s)",
        ]
    ]
    for config in sweep["configs"]:
        for i, run in enumerate(config["runs"]):
            if "memory_pool_gib" in run:
                split = f"memory pool {run['memory_pool_gib']:.1f}"
            else:
                split = " / ".join(
                    f"{run[k]:.1f}" for k in ("heap_gib", "offheap_gib", "overhead_gib")
                )
            ok = run["status"] == "ok"
            rows.append(
                [
                    config["label"] if i == 0 else "",
                    gib(run["executor_memory_gib"]),
                    split,
                    run_result(run),
                    f"{run['mean']:.2f}" if ok else "",
                    ", ".join(f"{e:.2f}" for e in run["elapsed"]) if ok else "",
                ]
            )
    return markdown_table(rows, [False, True, True, False, True, True])


def memory_stylesheet():
    return stylesheet(
        [
            ".sub { font-size: 14px; fill: #59636e; }",
            ".label { font-size: 14px; }",
            ".value { font-size: 14px; font-weight: 600; }",
            ".detail { font-weight: 400; fill: #59636e; }",
            ".legend { font-size: 13px; }",
            ".tick { font-size: 12.5px; fill: #59636e; }",
            ".note { font-size: 12.5px; fill: #59636e; }",
            ".grid { stroke: #e1e0d9; stroke-width: 1; }",
            ".axis { stroke: #c3c2b7; stroke-width: 1; }",
            ".comet { fill: #1baf7a; }",
            ".line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }",
            ".line.spark { stroke: #898781; }",
            ".line.staged { stroke: #2a78d6; }",
            ".line.comet { stroke: #1baf7a; }",
            ".ring { stroke: #ffffff; stroke-width: 2; }",
            ".cross { fill: none; stroke-width: 2.5; stroke-linecap: round; }",
            ".cross.spark { stroke: #898781; }",
            ".cross.staged { stroke: #2a78d6; }",
            ".cross.comet { stroke: #1baf7a; }",
            ".cross.key { stroke: #59636e; }",
            # The engines' fill classes come later in the stylesheet; lines are never filled.
            ".line.spark, .line.staged, .line.comet, .cross.spark, .cross.staged, .cross.comet { fill: none; }",
        ],
        [
            ".sub, .note, .tick, .detail { fill: #9198a1; }",
            ".grid { stroke: #21262d; }",
            ".axis { stroke: #3d444d; }",
            ".comet { fill: #199e70; }",
            ".line.staged, .cross.staged { stroke: #3987e5; }",
            ".line.comet, .cross.comet { stroke: #199e70; }",
            ".ring { stroke: #0d1117; }",
            ".cross.key { stroke: #9198a1; }",
        ],
    )


def headline(best):
    """The memory charts' title: each engine's smallest size."""
    names = {c["engine"]: s["smallest"] for c, s in best}
    if not {"ballista", "spark", "comet"} <= set(names):
        return "Smallest executor memory for each engine"
    return (
        f"Ballista finished in {gib(names['ballista'])} GiB per executor, "
        f"Spark needed {gib(names['spark'])} GiB and Spark + Comet {gib(names['comet'])} GiB"
    )


def smallest_chart(sweep):
    """Horizontal bars: every configuration's smallest clean size."""
    rows = [(c, memory_summary(c)) for c in sweep["configs"]]
    rows = [(c, s) for c, s in rows if s]
    left = 330
    scale = 380 / (math.ceil(max(s["smallest"] for _, s in rows) / 8) * 8)
    height = 120 + 36 * len(rows)
    q = sweep["query"]
    described = [f"{c['label']}: {gib(s['smallest'])} GiB" for c, s in rows]
    aria = f"Bar chart of the smallest executor memory for TPC-H q{q}. {'. '.join(described)}."
    bars, labels, values = [], [], []
    for i, (c, s) in enumerate(rows):
        top = 88 + 36 * i
        css = MEMORY_ENGINES[c["engine"]][0]
        end = number(left + s["smallest"] * scale)
        corner = number(left + s["smallest"] * scale - 4)
        bars.append(
            f'  <path class="{css}" d="M{left} {top}H{corner}Q{end} {top} {end} '
            f'{top + 4}V{top + 16}Q{end} {top + 20} {corner} {top + 20}H{left}Z"/>'
        )
        labels.append(
            f'  <text class="label" x="{left - 10}" y="{top + 15}" text-anchor="end">'
            f"{html.escape(c['label'])}</text>"
        )
        values.append(
            f'  <text class="value" x="{number(left + s["smallest"] * scale + 10)}" '
            f'y="{top + 15}">{gib(s["smallest"])} GiB'
            f'<tspan class="detail"> · {s["smallest_mean"]:.0f} s</tspan></text>'
        )
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 {height}" '
        f'width="880" height="{height}" role="img" aria-label="{aria}">',
        *memory_stylesheet(),
        f'  <rect class="bg" width="880" height="{height}" rx="6"/>',
        f'  <text class="title" x="24" y="36">{headline(best_configs(sweep))}</text>',
        f'  <text class="sub" x="24" y="60">Smallest executor memory at which TPC-H q{q} (SF{sweep["scale_factor"]}) '
        "finished with no executor out of memory, and its time there.</text>",
        "",
        *bars,
        f'  <line class="axis" x1="{left}" y1="80" x2="{left}" y2="{84 + 36 * len(rows)}"/>',
        "",
        *labels,
        "",
        *values,
        "",
        f'  <text class="note" x="24" y="{height - 14}">{MEMORY_NOTE}</text>',
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


def spread(ys, gap):
    """Label positions at least gap apart, each group centred on what it labels."""
    groups = [[y] for y in ys]
    while True:
        positions = []
        for g in groups:
            mid = sum(g) / len(g)
            positions.append(
                [mid - gap * (len(g) - 1) / 2 + gap * i for i in range(len(g))]
            )
        for i in range(len(groups) - 1):
            if positions[i + 1][0] - positions[i][-1] < gap:
                groups[i : i + 2] = [groups[i] + groups[i + 1]]
                break
        else:
            return [p for ps in positions for p in ps]


def time_chart(sweep):
    """Query time against executor memory for each engine's best configuration."""
    best = best_configs(sweep)
    series = []
    for c, s in best:
        points = sorted(
            (r["executor_memory_gib"], r["mean"]) for r in c["runs"] if is_clean(r)
        )
        series.append((c, s, MEMORY_ENGINES[c["engine"]], points))
    xmax = max(p[0] for *_, pts in series for p in pts)
    top, step = axis(max(p[1] for *_, pts in series for p in pts))
    left, right, plot_top, base = 60, 700, 112, 392

    def x(gib_):
        return left + gib_ / xmax * (right - left)

    def y(seconds):
        return base - seconds / top * (base - plot_top)

    q = sweep["query"]
    names = ", ".join(
        f"{name} {gib(s['smallest'])} GiB" for _, s, (_, name), _ in series
    )
    aria = (
        f"Line chart of TPC-H q{q} time against executor memory. Smallest size that "
        f"finished: {names}. The numbers are in the tables on this page."
    )
    legend, x0 = [], 24
    for _, _, (css, name), _ in series:
        legend.append(
            f'  <rect class="{css}" x="{x0}" y="78" width="12" height="12" rx="2"/>'
        )
        legend.append(f'  <text class="legend" x="{x0 + 18}" y="88">{name}</text>')
        x0 += 38 + 7 * len(name)
    legend.append(
        f'  <path class="cross key" d="M{x0 + 1} 79L{x0 + 11} 89M{x0 + 1} 89L{x0 + 11} 79"/>'
    )
    legend.append(
        f'  <text class="legend" x="{x0 + 18}" y="88">largest size that failed or needed '
        "an executor restarted</text>"
    )
    lines_, marks = [], []
    for c, s, (css, name), pts in series:
        d = " ".join(
            f"{'M' if i == 0 else 'L'}{number(x(g))} {number(y(t))}"
            for i, (g, t) in enumerate(pts)
        )
        lines_.append(f'  <path class="line {css}" d="{d}"/>')
        for g, t in pts:
            marks.append(
                f'  <circle class="{css} ring" cx="{number(x(g))}" cy="{number(y(t))}" r="5"/>'
            )
        if s["failed"] is not None:
            cx, cy = x(s["failed"]), base
            marks.append(
                f'  <path class="cross {css}" d="M{number(cx - 5)} {number(cy - 5)}'
                f"L{number(cx + 5)} {number(cy + 5)}M{number(cx - 5)} {number(cy + 5)}"
                f'L{number(cx + 5)} {number(cy - 5)}"/>'
            )
    ends = sorted(
        (y(pts[-1][1]), c, s, css_name, pts) for c, s, css_name, pts in series
    )
    placed = spread([e[0] for e in ends], 34)
    end_labels = []
    for (ey, c, s, (css, name), pts), ly in zip(ends, placed):
        g, t = pts[-1]
        if abs(ly - ey) > 2:
            end_labels.append(
                f'  <path class="axis" fill="none" d="M{number(x(g) + 8)} {number(ey)}'
                f'H{number(x(g) + 14)}L{number(x(g) + 20)} {number(ly - 4)}"/>'
            )
        end_labels.append(
            f'  <text class="value" x="{number(x(g) + 24)}" y="{number(ly)}">{name}</text>'
        )
        end_labels.append(
            f'  <text class="tick" x="{number(x(g) + 24)}" y="{number(ly + 15)}">'
            f"{t:.0f} s at {gib(g)} GiB</text>"
        )
    xticks = range(0, int(xmax) + 1, 8)
    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 470" '
        f'width="880" height="470" role="img" aria-label="{aria}">',
        *memory_stylesheet(),
        '  <rect class="bg" width="880" height="470" rx="6"/>',
        f'  <text class="title" x="24" y="36">Time for TPC-H q{q} as executor memory shrinks</text>',
        '  <text class="sub" x="24" y="60">Query time in seconds for each engine in its configuration '
        "with the smallest size. Lower is better; lines stop at that size.</text>",
        "",
        *legend,
        "",
        *[
            f'  <line class="grid" x1="{left}" y1="{number(y(t))}" x2="{right}" y2="{number(y(t))}"/>'
            for t in range(step, top + 1, step)
        ],
        f'  <line class="axis" x1="{left}" y1="{base}" x2="{right}" y2="{base}"/>',
        "",
        '  <g text-anchor="end">',
        *[
            f'    <text class="tick" x="{left - 8}" y="{number(y(t) + 4)}">{t}</text>'
            for t in range(0, top + 1, step)
        ],
        "  </g>",
        '  <g text-anchor="middle">',
        *[
            f'    <text class="tick" x="{number(x(g))}" y="{base + 20}">{g}</text>'
            for g in xticks
        ],
        f'    <text class="tick" x="{(left + right) // 2}" y="{base + 40}">Executor memory (GiB)</text>',
        "  </g>",
        "",
        *lines_,
        *marks,
        "",
        *end_labels,
        "",
        f'  <text class="note" x="24" y="456">{MEMORY_NOTE}</text>',
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


def memory_outputs():
    sweep = load_memory()
    page = with_table(read(MEMORY_PAGE), memory_summary_table(sweep), MEMORY_SUMMARY)
    page = with_table(page, memory_runs_table(sweep), MEMORY_RUNS)
    return {
        MEMORY_PAGE: page,
        os.path.join(IMAGES, "tpch_q18_memory_smallest.svg"): smallest_chart(sweep),
        os.path.join(IMAGES, "tpch_q18_memory_time.svg"): time_chart(sweep),
    }


def read(path):
    with open(path) as f:
        return f.read()


def outputs():
    """The content of each generated file, by path."""
    results = load_results()
    series = chart_series(results)
    return {
        GUIDE: with_table(read(GUIDE), results_table(list(results.values()))),
        README: with_speedups(read(README), series),
        os.path.join(IMAGES, "tpch_sf1000_total.svg"): total_chart(series),
        os.path.join(IMAGES, "tpch_sf1000_per_query.svg"): per_query_chart(series),
        **memory_outputs(),
    }


def main():
    for path, content in outputs().items():
        with open(path, "w") as f:
            f.write(content)


if __name__ == "__main__":
    main()
