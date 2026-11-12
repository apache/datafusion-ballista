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

"""Tests for publish.py and the published results it reads.

Run from benchmarks/cluster:  python3 -m unittest -v test_publish
"""

import re
import unittest

import compare
import publish


def result(label, means, **fields):
    queries = [{"query": number, "mean": mean} for number, mean in enumerate(means, 1)]
    return {"label": label, "queries": queries, **fields}


def results(spark, staged, pipelined):
    return {
        "spark": result("Spark 4.1.3", spark),
        "ballista-staged": result("Ballista staged", staged),
        "ballista-pipelined": result("Ballista pipelined", pipelined),
    }


def chart_series(spark, staged, pipelined):
    return publish.chart_series(results(spark, staged, pipelined))


def bar_ends(svg):
    """Where the bars of a total chart end, in the order they are drawn."""
    return [float(end) for end in re.findall(r'd="M264 \d+H[\d.]+Q([\d.]+) ', svg)]


def bar_heights(svg, css):
    """The heights above the baseline of one series in a per-query chart."""
    block = re.search(rf'<g class="{css}"[^>]*>(.*?)</g>', svg, re.S).group(1)
    return [
        float(height) - publish.BAR_OVERHANG
        for height in re.findall(r'height="([\d.]+)"', block)
    ]


class ResultsTableTest(unittest.TestCase):
    def test_cells_are_padded_the_way_prettier_aligns_them(self):
        self.assertEqual(
            publish.results_table([result("A", [1.5, 20.25])]),
            "\n".join(
                [
                    "|     Query |     A (s) |",
                    "| --------: | --------: |",
                    "|         1 |      1.50 |",
                    "|         2 |     20.25 |",
                    "| **Total** | **21.75** |",
                ]
            ),
        )

    def test_a_recorded_total_replaces_the_sum_of_rounded_means(self):
        table = publish.results_table([result("A", [1.0, 2.0], total=3.04)])
        self.assertIn("**3.04**", table)

    def test_a_failed_query_is_marked_and_leaves_no_total(self):
        run = result("A", [1.0])
        run["queries"].append({"query": 2, "error": "connection reset"})
        lines = publish.results_table([run]).split("\n")
        self.assertRegex(lines[3], r"\| +FAIL \|$")
        self.assertRegex(lines[4], r"\| +- \|$")

    def test_a_query_out_of_memory_is_marked_oom(self):
        run = result("A", [1.0])
        run["queries"].append({"query": 2, "error": "OOM"})
        self.assertRegex(publish.results_table([run]).split("\n")[3], r"\| +OOM \|$")


class WithTableTest(unittest.TestCase):
    def test_only_the_text_between_the_markers_changes(self):
        page = f"before\n\n{publish.BEGIN}\n\nold\n\n{publish.END}\n\nafter\n"
        self.assertEqual(
            publish.with_table(page, "new"),
            f"before\n\n{publish.BEGIN}\n\nnew\n\n{publish.END}\n\nafter\n",
        )

    def test_a_page_without_the_markers_is_an_error(self):
        with self.assertRaises(SystemExit):
            publish.with_table("no markers here\n", "new")


class ChartSeriesTest(unittest.TestCase):
    def test_labels_are_built_from_the_labels_in_the_results(self):
        self.assertEqual(
            [s["label"] for s in chart_series([1.0], [1.0], [1.0])],
            [
                "Apache Spark 4.1.3",
                "Ballista staged (default)",
                "Ballista pipelined (experimental)",
            ],
        )

    def test_a_result_with_a_failed_query_cannot_be_charted(self):
        inputs = results([1.0], [1.0], [1.0])
        inputs["ballista-staged"]["queries"][0] = {"query": 1, "error": "boom"}
        with self.assertRaises(SystemExit):
            publish.chart_series(inputs)

    def test_labels_are_escaped_for_svg(self):
        inputs = results([1.0], [1.0], [1.0])
        inputs["spark"]["label"] = "Spark & friends"
        self.assertIn("&amp;", publish.total_chart(publish.chart_series(inputs)))


class AxisTest(unittest.TestCase):
    def test_top_and_step_are_round_and_cover_the_slowest_query(self):
        self.assertEqual(publish.axis(134.11), (140, 25))
        self.assertEqual(publish.axis(3.0), (20, 5))
        self.assertEqual(publish.axis(4321.0), (4340, 1000))


class TotalChartTest(unittest.TestCase):
    def test_title_states_each_modes_speedup_over_spark(self):
        svg = publish.total_chart(chart_series([100.0], [50.0], [25.0]))
        self.assertIn(
            "2.0x faster than Apache Spark by default, 4.0x with pipelined execution",
            svg,
        )

    def test_bar_lengths_are_proportional_to_the_totals(self):
        svg = publish.total_chart(chart_series([1500.0], [750.0], [300.0]))
        spark, staged, pipelined = [end - 264 for end in bar_ends(svg)]
        self.assertAlmostEqual(staged / spark, 0.5, places=3)
        self.assertAlmostEqual(pipelined / spark, 0.2, places=3)

    def test_the_longest_bar_leaves_room_for_its_value_at_any_scale(self):
        for slowest in (1.0, 898.17, 100000.0):
            svg = publish.total_chart(chart_series([slowest], [1.0], [1.0]))
            self.assertLessEqual(max(bar_ends(svg)), 804)


class PerQueryChartTest(unittest.TestCase):
    def test_title_counts_the_queries_each_mode_wins(self):
        svg = publish.per_query_chart(
            chart_series([10.0, 10.0, 10.0], [5.0, 5.0, 20.0], [1.0, 1.0, 1.0])
        )
        self.assertIn(
            "Faster than Spark on 2 of 3 queries by default, on all 3 pipelined", svg
        )

    def test_all_bars_share_one_scale(self):
        svg = publish.per_query_chart(
            chart_series([100.0, 50.0], [50.0, 25.0], [10.0, 5.0])
        )
        spark = bar_heights(svg, "spark")
        staged = bar_heights(svg, "staged")
        self.assertAlmostEqual(spark[1] / spark[0], 0.5, places=2)
        self.assertAlmostEqual(staged[0] / spark[0], 0.5, places=2)

    def test_the_tallest_bar_fits_the_plot_at_any_scale(self):
        for slowest in (3.0, 134.11, 4321.0):
            svg = publish.per_query_chart(chart_series([slowest], [1.0], [1.0]))
            self.assertLessEqual(max(bar_heights(svg, "spark")), publish.PLOT_HEIGHT)

    def test_a_query_missing_from_one_series_is_an_error(self):
        series = chart_series([1.0, 2.0], [1.0], [1.0, 2.0])
        with self.assertRaises(KeyError):
            publish.per_query_chart(series)


class WithSpeedupsTest(unittest.TestCase):
    def test_the_summary_quotes_the_speedups_over_spark(self):
        readme = "Is **9.9x faster than Spark** by default and **9.9x faster** piped.\n"
        self.assertEqual(
            publish.with_speedups(readme, chart_series([100.0], [50.0], [25.0])),
            "Is **2.0x faster than Spark** by default and **4.0x faster** piped.\n",
        )

    def test_a_readme_without_the_summary_is_an_error(self):
        with self.assertRaises(SystemExit):
            publish.with_speedups("no summary\n", chart_series([1.0], [1.0], [1.0]))


def sweep_config(engine, label, runs):
    """A memory-sweep configuration; runs are (size, status, mean, executor OOM kills)."""
    return {
        "name": label,
        "engine": engine,
        "label": label,
        "split": "",
        "runs": [
            {
                "executor_memory_gib": size,
                "status": status,
                "executor_oom_kills": kills,
                **({"mean": mean, "elapsed": [mean]} if mean is not None else {}),
                "memory_pool_gib": size * 0.7,
            }
            for size, status, mean, kills in runs
        ],
    }


SWEEP = {
    "query": 18,
    "scale_factor": 1000,
    "configs": [
        sweep_config(
            "ballista",
            "Ballista",
            [
                (16, "ok", 130.0, 0),
                (8, "ok", 131.0, 0),
                (6, "ok", 360.0, 2),
                (5, "timeout", None, 2),
            ],
        ),
        sweep_config(
            "spark",
            "Spark, on-heap",
            [(32, "ok", 330.0, 0), (16, "oom-killed", None, 4)],
        ),
        sweep_config(
            "spark",
            "Spark, 50% off-heap",
            [(32, "ok", 330.0, 0), (12, "ok", 390.0, 0), (8, "oom-killed", None, 6)],
        ),
        sweep_config(
            "comet", "Spark + Comet", [(32, "ok", 150.0, 0), (20, "ok", 150.0, 0)]
        ),
    ],
}


class MemorySweepTest(unittest.TestCase):
    def test_a_pass_that_needed_restarts_is_not_clean(self):
        summary = publish.memory_summary(SWEEP["configs"][0])
        self.assertEqual((summary["smallest"], summary["failed"]), (8, 6))
        self.assertEqual((summary["largest"], summary["largest_mean"]), (16, 130.0))

    def test_each_engine_is_shown_by_its_smallest_configuration(self):
        best = [c["label"] for c, _ in publish.best_configs(SWEEP)]
        self.assertEqual(best, ["Ballista", "Spark, 50% off-heap", "Spark + Comet"])

    def test_the_headline_names_each_engines_smallest_size(self):
        self.assertEqual(
            publish.headline(publish.best_configs(SWEEP)),
            "Ballista finished in 8 GiB per executor, Spark needed 12 GiB and "
            "Spark + Comet 20 GiB",
        )

    def test_tables_are_padded_the_way_prettier_aligns_them(self):
        table = publish.markdown_table([["Name", "N"], ["ab", "10"]], [False, True])
        self.assertEqual(table, "| Name |   N |\n| ---- | --: |\n| ab   |  10 |")

    def test_the_runs_table_explains_failures_and_restarts(self):
        table = publish.memory_runs_table(SWEEP)
        self.assertIn("Passed after 2 executors ran out of memory", table)
        self.assertIn("Did not finish within the time limit", table)
        self.assertIn("Failed: an executor ran out of memory", table)

    def test_bar_lengths_are_proportional_to_the_smallest_sizes(self):
        svg = publish.smallest_chart(SWEEP)
        ends = [
            float(e) - 330 for e in re.findall(r'd="M330 \d+H[\d.]+Q([\d.]+) ', svg)
        ]
        self.assertEqual(len(ends), 4)
        for end, size in zip(ends, [8, 32, 12, 20]):
            self.assertAlmostEqual(end / ends[0], size / 8, places=2)

    def test_end_labels_that_would_collide_are_spread_around_their_points(self):
        self.assertEqual(publish.spread([100, 300], 30), [100, 300])
        self.assertEqual(publish.spread([100, 110], 30), [90, 120])

    def test_the_time_chart_draws_one_line_per_engine(self):
        svg = publish.time_chart(SWEEP)
        self.assertEqual(len(re.findall(r'<path class="line ', svg)), 3)
        # Comet never failed, so only Ballista and Spark have a failure marked.
        self.assertEqual(len(re.findall(r'<path class="cross (?!key)', svg)), 2)


class PublishedResultsTest(unittest.TestCase):
    def test_every_generated_file_is_up_to_date(self):
        for path, expected in publish.outputs().items():
            with open(path) as f:
                self.assertEqual(f.read(), expected, f"{path}: run publish.py")

    def test_recorded_totals_agree_with_the_query_times(self):
        for name, run in publish.load_results().items():
            rounding = 0.005 * len(run["queries"])
            sum_of_means = sum(q["mean"] for q in run["queries"])
            self.assertLessEqual(abs(compare.total(run) - sum_of_means), rounding, name)


if __name__ == "__main__":
    unittest.main()
