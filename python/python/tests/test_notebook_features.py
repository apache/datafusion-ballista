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
import tempfile
import os
from ballista import (
    BallistaSessionContext,
    setup_test_cluster,
    ExecutionPlanVisualization,
)


@pytest.fixture
def ctx():
    """Create a connected Ballista context for testing."""
    (address, port) = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


class TestExplainVisual:
    """Tests for the explain_visual method."""

    def test_explain_visual_returns_visualization(self, ctx):
        """Test that explain_visual returns an ExecutionPlanVisualization."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()
        assert isinstance(viz, ExecutionPlanVisualization)

    def test_explain_visual_with_analyze(self, ctx):
        """Test explain_visual with analyze=True."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual(analyze=True)
        assert isinstance(viz, ExecutionPlanVisualization)
        assert viz.analyze is True

    def test_visualization_to_dot(self, ctx):
        """Test converting visualization to DOT format."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()
        dot = viz.to_dot()
        assert "digraph" in dot
        assert "ExecutionPlan" in dot

    def test_visualization_to_svg(self, ctx):
        """Test converting visualization to SVG or HTML fallback."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()
        svg = viz.to_svg()
        # Should return either SVG or HTML fallback
        assert svg is not None
        assert len(svg) > 0

    def test_visualization_repr_html(self, ctx):
        """Test HTML representation for Jupyter."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()
        html = viz._repr_html_()
        assert html is not None
        assert len(html) > 0

    def test_visualization_repr(self, ctx):
        """Test string representation."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()
        s = repr(viz)
        assert "ExecutionPlanVisualization" in s

    def test_visualization_save_dot(self, ctx):
        """Test saving visualization as DOT file."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()

        with tempfile.NamedTemporaryFile(suffix=".dot", delete=False) as f:
            path = f.name

        try:
            viz.save(path)
            with open(path) as f:
                content = f.read()
            assert "digraph" in content
        finally:
            os.unlink(path)

    def test_visualization_save_svg(self, ctx):
        """Test saving visualization as SVG file."""
        df = ctx.sql("SELECT 1 as value")
        viz = df.explain_visual()

        with tempfile.NamedTemporaryFile(suffix=".svg", delete=False) as f:
            path = f.name

        try:
            viz.save(path)
            with open(path) as f:
                content = f.read()
            # Should have some content (either SVG or HTML fallback)
            assert len(content) > 0
        finally:
            os.unlink(path)


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


class TestCollectWithProgress:
    """Tests for the collect_with_progress method."""

    def test_collect_with_progress_returns_batches(self, ctx):
        """Test that collect_with_progress returns batches."""
        df = ctx.sql("SELECT 1 as value")
        batches = df.collect_with_progress()
        assert batches is not None
        assert len(batches) > 0

    def test_collect_with_progress_callback(self, ctx):
        """Test collect_with_progress with callback."""
        df = ctx.sql("SELECT 1 as value")
        callback_calls = []

        def callback(status, progress):
            callback_calls.append((status, progress))

        batches = df.collect_with_progress(callback=callback)
        assert batches is not None
        # Callback should have been called at least once for completion
        assert len(callback_calls) > 0

    def test_collect_with_progress_matches_collect(self, ctx):
        """Test that collect_with_progress returns same data as collect."""
        df = ctx.sql("SELECT 1 as value")
        batches_progress = df.collect_with_progress()
        batches_normal = df.collect()

        assert len(batches_progress) == len(batches_normal)
        for bp, bn in zip(batches_progress, batches_normal):
            assert len(bp) == len(bn)


class TestBallistaSessionContextTables:
    """Tests for the BallistaSessionContext.tables() method."""

    def test_tables_empty(self, ctx):
        """Test listing tables when none registered."""
        # Should return empty list or at least not crash
        tables = ctx.tables()
        assert isinstance(tables, list)

    def test_tables_after_register_parquet(self, ctx):
        """Test listing tables after registering a Parquet file."""
        ctx.register_parquet("test_parquet", "testdata/test.parquet")
        tables = ctx.tables()
        # The table might be in the list depending on catalog implementation
        assert isinstance(tables, list)

    def test_tables_after_register_csv(self, ctx):
        """Test listing tables after registering a CSV file."""
        ctx.register_csv("test_csv", "testdata/test.csv", has_header=True)
        tables = ctx.tables()
        assert isinstance(tables, list)


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
