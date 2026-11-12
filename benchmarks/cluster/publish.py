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

Rewrites the results table on the docs' benchmarking page, the results and
headline numbers on the comparison with DataFusion Distributed, and the charts
and the speedups over Spark in the top-level README.
"""

import html
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
COMPARISON = os.path.join(
    REPO, "docs", "source", "about", "datafusion-distributed-comparison.md"
)
README = os.path.join(REPO, "README.md")
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

# The result files the comparison with DataFusion Distributed shows, in the order
# of its table's columns.
COMPARISON_COLUMNS = [
    "ballista-staged",
    "ballista-pipelined",
    "datafusion-distributed",
]

BEGIN = "<!-- BEGIN GENERATED BENCHMARK RESULTS -->"
END = "<!-- END GENERATED BENCHMARK RESULTS -->"
FINDINGS_BEGIN = "<!-- BEGIN GENERATED COMPARISON FINDINGS -->"
FINDINGS_END = "<!-- END GENERATED COMPARISON FINDINGS -->"
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


def with_table(page, table, begin=BEGIN, end=END):
    """The page with the text between a pair of generated-text markers replaced."""
    before, found_begin, rest = page.partition(begin)
    _, found_end, after = rest.partition(end)
    if not found_begin or not found_end:
        sys.exit(f"the page has no {begin} ... {end} block")
    return f"{before}{begin}\n\n{table}\n\n{end}{after}"


def protocol(run):
    """How a run's times were taken, such as "mean of 3 iterations after an untimed warm-up"."""
    warmup = (
        "after an untimed warm-up" if run["warmup"] else "including the cold first one"
    )
    return f"mean of {run['iterations']} iterations {warmup}"


def commit(sha):
    return f"`{sha[:8]}`"


def comparison_results(results):
    """The comparison page's results: where they came from, then the table."""
    staged, pipelined, dfd = (results[name] for name in COMPARISON_COLUMNS)
    if staged["ballista_commit"] != pipelined["ballista_commit"]:
        sys.exit("the staged and pipelined results come from different commits")
    provenance = (
        f"Ballista at {commit(staged['ballista_commit'])} "
        f"(DataFusion {staged['datafusion_version']}), {protocol(staged)}; "
        f"DataFusion Distributed at {commit(dfd['datafusion_distributed_commit'])} "
        f"(DataFusion {dfd['datafusion_version']}), {protocol(dfd)}. "
        "Times in seconds; lower is better."
    )
    table = results_table([staged, pipelined, dfd])
    return f"{provenance}\n\n{table}"


def geometric_mean(ratios):
    return math.exp(sum(math.log(r) for r in ratios) / len(ratios))


def query_list(queries):
    """Queries as prose, such as "Q1, Q10, and Q13"."""
    names = [f"Q{q}" for q in queries]
    if len(names) <= 2:
        return " and ".join(names)
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def comparison_findings(results):
    """The headline numbers comparing pipelined Ballista with DataFusion Distributed."""
    staged, pipelined, dfd = (results[name] for name in COMPARISON_COLUMNS)
    for name in COMPARISON_COLUMNS:
        if any(q.get("error") or "mean" not in q for q in results[name]["queries"]):
            sys.exit(f"{name} has a failed query, so it cannot be compared")
    ballista = {q["query"]: q["mean"] for q in pipelined["queries"]}
    other = {q["query"]: q["mean"] for q in dfd["queries"]}
    queries = sorted(other)
    ballista_wins = [q for q in queries if ballista[q] < other[q]]
    other_wins = [q for q in queries if other[q] < ballista[q]]
    ratio = geometric_mean([other[q] / ballista[q] for q in queries])
    widest = max(queries, key=lambda q: abs(other[q] - ballista[q]))
    without_widest = geometric_mean(
        [other[q] / ballista[q] for q in queries if q != widest]
    )
    gap = compare.total(dfd) - compare.total(pipelined)
    favours = "pipelined Ballista" if ratio >= 1 else "DataFusion Distributed"
    favours_without = (
        "pipelined Ballista" if without_widest >= 1 else "DataFusion Distributed"
    )
    return "\n".join(
        [
            f"- Pipelined Ballista was faster on {len(ballista_wins)} of the "
            f"{len(queries)} queries and DataFusion Distributed on {len(other_wins)}"
            + (f" ({query_list(other_wins)})" if other_wins else "")
            + ". The geometric mean of the per-query ratios favours "
            f"{favours} by about {max(ratio, 1 / ratio):.1f}x.",
            f"- The totals differ by {abs(gap):.1f} s, of which Q{widest} accounts for "
            f"{abs(other[widest] - ballista[widest]):.1f} s. Excluding Q{widest}, the "
            f"geometric mean favours {favours_without} by about "
            f"{max(without_widest, 1 / without_widest):.1f}x.",
            f"- Staged Ballista took {compare.total(staged) / compare.total(pipelined):.2f} "
            "times as long as pipelined Ballista in total.",
        ]
    )


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


def read(path):
    with open(path) as f:
        return f.read()


def outputs():
    """The content of each generated file, by path."""
    results = load_results()
    series = chart_series(results)
    comparison = with_table(read(COMPARISON), comparison_results(results))
    comparison = with_table(
        comparison, comparison_findings(results), FINDINGS_BEGIN, FINDINGS_END
    )
    return {
        GUIDE: with_table(read(GUIDE), results_table(list(results.values()))),
        COMPARISON: comparison,
        README: with_speedups(read(README), series),
        os.path.join(IMAGES, "tpch_sf1000_total.svg"): total_chart(series),
        os.path.join(IMAGES, "tpch_sf1000_per_query.svg"): per_query_chart(series),
    }


def main():
    for path, content in outputs().items():
        with open(path, "w") as f:
            f.write(content)


if __name__ == "__main__":
    main()
