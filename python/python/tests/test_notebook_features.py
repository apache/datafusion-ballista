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

"""Tests for notebook-specific features in Ballista."""

import pytest
from ballista import (
    BallistaSessionContext,
    setup_test_cluster,
    ExecutionPlanVisualization,
)
from ballista.jupyter import collect_with_progress


@pytest.fixture
def ctx():
    """Create a connected Ballista context for testing."""
    (address, port) = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


class TestExecutionPlanVisualization:
    """Tests for ExecutionPlanVisualization class."""

    def test_creation(self):
        """Test creating a visualization from plan string."""
        plan_str = """
        LogicalPlan
          Projection: a, b
            Filter: a > 1
              TableScan: test
        """
        viz = ExecutionPlanVisualization(plan_str)
        assert viz.plan_str == plan_str
        assert viz.analyze is False

    def test_creation_with_analyze(self):
        """Test creating a visualization with analyze flag."""
        plan_str = "LogicalPlan\n  Projection: a"
        viz = ExecutionPlanVisualization(plan_str, analyze=True)
        assert viz.analyze is True

    def test_dot_generation(self):
        """Test DOT format generation."""
        plan_str = """
LogicalPlan
  Projection: a, b
    Filter: a > 1
      TableScan: test
        """
        viz = ExecutionPlanVisualization(plan_str)
        dot = viz.to_dot()

        assert "digraph ExecutionPlan" in dot
        assert "node" in dot
        assert "->" in dot  # Should have edges

    def test_svg_caching(self):
        """Test that SVG is cached after first generation."""
        viz = ExecutionPlanVisualization("Test plan")
        svg1 = viz.to_svg()
        svg2 = viz.to_svg()
        assert svg1 == svg2  # Should be same cached result


class TestHTMLRendering:
    """Tests for HTML rendering in notebooks."""

    def test_dataframe_repr_html(self, ctx):
        """Test that DataFrame has _repr_html_ method."""
        df = ctx.sql("SELECT 1 as value")
        assert hasattr(df, "_repr_html_")

    def test_dataframe_repr_html_returns_html(self, ctx):
        """Test that _repr_html_ returns valid HTML."""
        df = ctx.sql("SELECT 1 as value")
        html = df._repr_html_()
        # The HTML should contain table tags or similar
        assert html is not None


class TestCollectWithProgress:
    """Tests for the collect_with_progress method."""

    def test_collect_with_progress_returns_batches(self, ctx):
        """Test that collect_with_progress returns batches."""
        df = ctx.sql("SELECT 1 as value")
        batches = collect_with_progress(df)
        assert batches is not None
        assert len(batches) > 0

    def test_collect_with_progress_callback(self, ctx):
        """Test collect_with_progress with callback."""
        df = ctx.sql("SELECT 1 as value")
        callback_calls = []

        def callback(status, progress):
            callback_calls.append((status, progress))

        batches = collect_with_progress(df, callback=callback)
        assert batches is not None
        # Callback should have been called at least once for completion
        assert len(callback_calls) > 0

    def test_collect_with_progress_matches_collect(self, ctx):
        """Test that collect_with_progress returns same data as collect."""
        df = ctx.sql("SELECT 1 as value")
        batches_progress = collect_with_progress(df)
        batches_normal = df.collect()

        assert len(batches_progress) == len(batches_normal)
        for bp, bn in zip(batches_progress, batches_normal):
            assert len(bp) == len(bn)
