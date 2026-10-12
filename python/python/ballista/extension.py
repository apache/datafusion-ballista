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

from datafusion import (
    SessionConfig,
    SessionContext,
)
from typing import Dict, Optional
import warnings

from ._internal_ballista import ballista_datafusion_config_defaults
from ._internal_ballista import with_ballista_query_planner


class ExecutionPlanVisualization:
    """
    A wrapper for execution plan visualizations that can render as SVG in Jupyter.

    This class takes the text representation of an execution plan and converts
    it to a Graphviz DOT format, which is then rendered as SVG.
    """

    def __init__(self, plan_str: str, analyze: bool = False):
        self.plan_str = plan_str
        self.analyze = analyze
        self._svg_cache: Optional[str] = None

    def _parse_plan_to_dot(self) -> str:
        """Convert the plan string to DOT format for Graphviz."""
        lines = self.plan_str.strip().split("\n")

        dot_lines = [
            "digraph ExecutionPlan {",
            "    rankdir=TB;",
            '    node [shape=box, style="rounded,filled", fontname="Helvetica"];',
            '    edge [fontname="Helvetica"];',
            "",
        ]

        nodes = []
        edges = []
        node_id = 0
        stack = []  # (indent_level, node_id)

        for line in lines:
            if not line.strip():
                continue

            # Calculate indent level
            indent = len(line) - len(line.lstrip())
            content = line.strip()

            # Skip non-plan lines
            if content.startswith("physical_plan") or content.startswith(
                "logical_plan"
            ):
                continue

            # Create a node for this plan element
            current_id = node_id
            node_id += 1

            # Determine node color based on operation type
            color = "#E3F2FD"  # Default light blue
            if "Scan" in content or "TableScan" in content:
                color = "#E8F5E9"  # Light green for scans
            elif "Filter" in content:
                color = "#FFF3E0"  # Light orange for filters
            elif "Aggregate" in content or "HashAggregate" in content:
                color = "#F3E5F5"  # Light purple for aggregations
            elif "Join" in content:
                color = "#FFEBEE"  # Light red for joins
            elif "Sort" in content:
                color = "#E0F7FA"  # Light cyan for sorts
            elif "Projection" in content:
                color = "#FFF8E1"  # Light amber for projections

            # Escape special characters for DOT format
            label = content.replace('"', '\\"').replace("\n", "\\n")
            if len(label) > 60:
                # Wrap long labels
                label = label[:57] + "..."

            nodes.append(
                f'    node{current_id} [label="{label}", fillcolor="{color}"];'
            )

            # Connect to parent based on indentation
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent_id = stack[-1][1]
                edges.append(f"    node{parent_id} -> node{current_id};")

            stack.append((indent, current_id))

        dot_lines.extend(nodes)
        dot_lines.append("")
        dot_lines.extend(edges)
        dot_lines.append("}")

        return "\n".join(dot_lines)

    def to_dot(self) -> str:
        """Get the DOT representation of the execution plan."""
        return self._parse_plan_to_dot()

    def to_svg(self) -> str:
        """
        Convert the execution plan to SVG format.

        Requires graphviz to be installed. If graphviz is not available,
        returns a simple HTML representation instead.
        """
        if self._svg_cache is not None:
            return self._svg_cache

        dot_source = self._parse_plan_to_dot()

        try:
            import subprocess

            # Try to use graphviz's dot command
            process = subprocess.run(
                ["dot", "-Tsvg"],
                input=dot_source.encode(),
                capture_output=True,
                timeout=30,
            )

            if process.returncode == 0:
                self._svg_cache = process.stdout.decode()
                return self._svg_cache
        except (
            subprocess.SubprocessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ) as e:
            warnings.warn(f"Could not convert the execution plan to SVG format: {e}")
            pass

        # Fallback: return a pre-formatted HTML representation
        escaped_plan = (
            self.plan_str.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        self._svg_cache = f"""
        <div style="font-family: monospace; background: #f5f5f5; padding: 10px; 
                    border-radius: 5px; overflow-x: auto;">
            <div style="color: #666; margin-bottom: 5px;">
                Execution Plan {"(with statistics)" if self.analyze else ""}
                <br><small>Install graphviz for visual diagram: brew install graphviz</small>
            </div>
            <pre style="margin: 0;">{escaped_plan}</pre>
        </div>
        """
        return self._svg_cache

    def save(self, path: str) -> None:
        """Save the visualization to a file (SVG or DOT format)."""
        if path.endswith(".dot"):
            content = self.to_dot()
        else:
            content = self.to_svg()

        with open(path, "w") as f:
            f.write(content)

    def _repr_html_(self) -> str:
        """HTML representation for Jupyter notebooks."""
        return self.to_svg()

    def _repr_svg_(self) -> str:
        """SVG representation for Jupyter notebooks."""
        svg = self.to_svg()
        # Only return if it's actual SVG content
        if svg.strip().startswith("<svg") or svg.strip().startswith("<?xml"):
            return svg
        return ""

    def __repr__(self) -> str:
        """String representation."""
        return f"ExecutionPlanVisualization(analyze={self.analyze})\n{self.plan_str}"


def BallistaSessionContext(
    address: str,
    config=None,
    runtime=None,
    cluster_config: Optional[Dict[str, str]] = None,
):
    cluster_config = (
        {str(k): str(v) for k, v in cluster_config.items()}
        if cluster_config is not None
        else None
    )
    # Apply overrides to the local SessionContext too: some settings
    # (e.g. datafusion.execution.listing_table_factory_infer_partitions)
    # are consulted during local table registration / planning, before
    # the plan is shipped to the scheduler. Ballista-namespaced keys
    # are not understood by the local SessionConfig and are forwarded
    # to the scheduler only.
    if config is None:
        config = SessionConfig()
        for key, value in ballista_datafusion_config_defaults().items():
            config = config.set(key, value)

    source_ctx = SessionContext(config, runtime)
    configured_ctx = with_ballista_query_planner(
        source_ctx,
        address,
        cluster_config,
    )
    return configured_ctx
