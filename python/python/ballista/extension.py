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

from datafusion import SessionContext, DataFrame, ParquetWriterOptions
from datafusion.dataframe import Compression

from typing import (
    Union,
    Optional,
    List,
)


from ._internal_ballista import create_ballista_data_frame
from ._internal_ballista import ParquetColumnOptions as ParquetColumnOptionsInternal
from ._internal_ballista import ParquetWriterOptions as ParquetWriterOptionsInternal
import pathlib

# DataFrame execution methods which should be automatically
# overridden.

OVERRIDDEN_EXECUTION_METHODS = [
    "show",
    "count",
    "collect",
    "collect_partitioned",
    "write_json",
    "to_arrow_table",
    "to_pandas",
    "to_pydict",
    "to_polars",
    "to_pylist",
    "_repr_html_",
    "execute_stream",
    "execute_stream_partitioned",
]


# class used to redefine DataFrame object
# intercepting execution methods and methods
# which returns `DataFrame`
class RedefiningDataFrameMeta(type):
    def __new__(cls, name, bases, attrs):
        # wrapper function intercept all execution functions
        # replacing ordinary DataFrame with Ballista data frame
        def __wrap_dataframe_execution(func):
            def method_wrapper(*args, **kwargs):
                slf, *argz = args
                df = slf._to_internal_df()

                return getattr(df, func)(*argz, **kwargs)

            return method_wrapper

        # wrapper function intercepts all methods which
        # return DataFrame and wraps DataFrame with DistributedDataFrame
        def __wrap_dataframe_result(func):
            def method_wrapper(*args, **kwargs):
                address = args[0].address
                session_id = args[0].session_id
                df = func(*args, **kwargs)
                return DistributedDataFrame(df, session_id, address)

            return method_wrapper

        for base_name, base_value in bases[0].__dict__.items():
            #
            # TODO: could we not use 'DataFrame' as a string here?
            #
            if (
                callable(base_value)
                and not base_name.startswith("__")
                and base_value.__annotations__.get("return") == "DataFrame"
            ):
                #
                # functions which return DataFrame are redefined
                # to return DistributedDataFrame
                #
                attrs[base_name] = __wrap_dataframe_result(base_value)

        # TODO: we could do better here
        for function in OVERRIDDEN_EXECUTION_METHODS:
            attrs[function] = __wrap_dataframe_execution(function)

        return super().__new__(cls, name, bases, attrs)


class RedefiningSessionContextMeta(type):
    def __new__(cls, name, bases, attrs):
        def __wrap_dataframe_result(func):
            def method_wrapper(*args, **kwargs):
                address = args[0].address
                session_id = args[0].session_id
                df = func(*args, **kwargs)
                return DistributedDataFrame(df, session_id, address)

            return method_wrapper

        for base_name, base_value in bases[0].__dict__.items():
            #
            # could we not use 'DataFrame' as a string here?
            #
            if (
                callable(base_value)
                and not base_name.startswith("__")
                and base_value.__annotations__.get("return") == "DataFrame"
            ):
                #
                # functions which return DataFrame are redefined
                # to return DistributedDataFrame
                #
                attrs[base_name] = __wrap_dataframe_result(base_value)

        return super().__new__(cls, name, bases, attrs)


# main difference between this class and DataFrame is that
# operations which would execute logical plan will
# serialize it and invoke ballista client to execute it
#
# this class keeps reference to remote ballista


class DistributedDataFrame(DataFrame, metaclass=RedefiningDataFrameMeta):
    def __init__(self, df: DataFrame, session_id: str, address: str):
        super().__init__(df.df)
        self.address = address
        self.session_id = session_id

    #
    # this will create a ballista dataframe, which has ballista
    # session context, and ballista planner.
    #
    def _to_internal_df(self):
        blob_plan = self.logical_plan().to_proto()
        df = create_ballista_data_frame(blob_plan, self.address, self.session_id)
        return df

    def write_csv(self, path, with_header=False):
        df = self._to_internal_df()
        df.write_csv(path, with_header)

    def write_parquet_with_options(
        self,
        path: str,
        options: ParquetWriterOptions,
    ):
        options_internal = ParquetWriterOptionsInternal(
            options.data_pagesize_limit,
            options.write_batch_size,
            options.writer_version,
            options.skip_arrow_metadata,
            options.compression,
            options.dictionary_enabled,
            options.dictionary_page_size_limit,
            options.statistics_enabled,
            options.max_row_group_size,
            options.created_by,
            options.column_index_truncate_length,
            options.statistics_truncate_length,
            options.data_page_row_count_limit,
            options.encoding,
            options.bloom_filter_on_write,
            options.bloom_filter_fpp,
            options.bloom_filter_ndv,
            options.allow_single_file_parallelism,
            options.maximum_parallel_row_group_writers,
            options.maximum_buffered_record_batches_per_stream,
        )

        column_specific_options_internal = {}
        for column, opts in (options.column_specific_options or {}).items():
            column_specific_options_internal[column] = ParquetColumnOptionsInternal(
                bloom_filter_enabled=opts.bloom_filter_enabled,
                encoding=opts.encoding,
                dictionary_enabled=opts.dictionary_enabled,
                compression=opts.compression,
                statistics_enabled=opts.statistics_enabled,
                bloom_filter_fpp=opts.bloom_filter_fpp,
                bloom_filter_ndv=opts.bloom_filter_ndv,
            )

        # raw_write_options = (
        #     write_options._raw_write_options if write_options is not None else None
        # )

        df = self._to_internal_df()

        df.write_parquet_with_options(
            str(path),
            options_internal,
            column_specific_options_internal,
            # raw_write_options,
        )

    def write_parquet(
        self,
        path: Union[str, pathlib.Path],
        compression: Union[str, Compression, ParquetWriterOptions] = Compression.ZSTD,
        compression_level: Union[int, None] = None,
    ) -> None:
        if isinstance(compression, ParquetWriterOptions):
            if compression_level is not None:
                msg = "compression_level should be None when using ParquetWriterOptions"
                raise ValueError(msg)
            self.write_parquet_with_options(path, compression)
            return

        if isinstance(compression, str):
            compression = Compression.from_str(compression)

        if (
            compression in {Compression.GZIP, Compression.BROTLI, Compression.ZSTD}
            and compression_level is None
        ):
            compression_level = compression.get_default_level()
        df = self._to_internal_df()
        df.write_parquet(str(path), compression.value, compression_level)

    def explain_visual(self, analyze: bool = False) -> "ExecutionPlanVisualization":
        """
        Generate a visual representation of the execution plan.

        This method creates an SVG visualization of the query execution plan,
        which can be displayed directly in Jupyter notebooks.

        Args:
            analyze: If True, includes runtime statistics from actual execution.

        Returns:
            ExecutionPlanVisualization: An object that renders as SVG in Jupyter.

        Example:
            >>> df = ctx.sql("SELECT * FROM orders WHERE amount > 100")
            >>> df.explain_visual()  # Displays SVG in notebook
            >>> viz = df.explain_visual(analyze=True)
            >>> viz.save("plan.svg")  # Save to file
        """
        # Get the execution plan as a string representation
        # Note: explain() prints but doesn't return a string, so we use logical_plan()
        try:
            plan = self.logical_plan()
            plan_str = plan.display_indent()
        except Exception:
            # Fallback if logical_plan() fails
            plan_str = "Unable to retrieve execution plan"
        return ExecutionPlanVisualization(plan_str, analyze=analyze)

    def collect_with_progress(
        self,
        callback: Optional[callable] = None,
        poll_interval: float = 0.5,
    ):
        """
        Collect results with progress indication.

        For long-running queries, this method provides progress updates
        through a callback function or displays a progress bar in Jupyter.

        Args:
            callback: Optional function to call with progress updates.
                     Signature: callback(status: str, progress: float)
            poll_interval: How often to check progress (seconds).

        Returns:
            The collected result batches.

        Example:
            >>> def my_callback(status, progress):
            ...     print(f"{status}: {progress:.1%}")
            >>> batches = df.collect_with_progress(callback=my_callback)
        """
        import threading
        import time

        result = [None]
        error = [None]
        done = threading.Event()

        def execute():
            try:
                result[0] = self.collect()
            except Exception as e:
                error[0] = e
            finally:
                done.set()

        thread = threading.Thread(target=execute)
        thread.start()

        # Check if we're in a Jupyter environment
        try:
            from IPython.display import display, clear_output
            from IPython import get_ipython

            in_jupyter = get_ipython() is not None
        except (ImportError, AttributeError):
            in_jupyter = False

        start_time = time.time()

        if in_jupyter and callback is None:
            # Display a simple progress indicator
            try:
                while not done.wait(timeout=poll_interval):
                    elapsed = time.time() - start_time
                    clear_output(wait=True)
                    print(f"⏳ Query executing... ({elapsed:.1f}s elapsed)")

                clear_output(wait=True)
                elapsed = time.time() - start_time
                print(f"✓ Query completed in {elapsed:.1f}s")
            except Exception:
                pass  # Ignore display errors
        elif callback is not None:
            while not done.wait(timeout=poll_interval):
                elapsed = time.time() - start_time
                callback(f"Executing ({elapsed:.1f}s)", -1.0)  # -1 means indeterminate

            elapsed = time.time() - start_time
            callback(f"Completed in {elapsed:.1f}s", 1.0)
        else:
            done.wait()

        thread.join()

        if error[0] is not None:
            raise error[0]

        return result[0]


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
            '    rankdir=TB;',
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
            if content.startswith("physical_plan") or content.startswith("logical_plan"):
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

            nodes.append(f'    node{current_id} [label="{label}", fillcolor="{color}"];')

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
        except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
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
                Execution Plan {'(with statistics)' if self.analyze else ''}
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


class BallistaSessionContext(SessionContext, metaclass=RedefiningSessionContextMeta):
    """
    A session context for connecting to and querying a Ballista cluster.

    This class extends DataFusion's SessionContext to work with distributed
    Ballista clusters, automatically routing query execution to the cluster
    while maintaining API compatibility with local DataFusion usage.

    Example:
        >>> from ballista import BallistaSessionContext
        >>> ctx = BallistaSessionContext("df://localhost:50050")
        >>> df = ctx.sql("SELECT * FROM my_table LIMIT 10")
        >>> df.show()

    For Jupyter notebook users:
        >>> %load_ext ballista.jupyter
        >>> %ballista connect df://localhost:50050
        >>> %sql SELECT * FROM my_table
    """

    def __init__(self, address: str, config=None, runtime=None):
        super().__init__(config, runtime)
        self.address = address
        self.session_id = self.session_id()

    def tables(self) -> List[str]:
        """
        List all registered tables in this session.

        Returns:
            List[str]: Names of all registered tables.

        Example:
            >>> ctx.register_parquet("orders", "data/orders.parquet")
            >>> ctx.tables()
            ['orders']
        """
        # Get catalog information
        try:
            # Try to use the catalog API if available
            catalog = self.catalog()
            if catalog is not None:
                return list(catalog.names())
        except (AttributeError, NotImplementedError):
            pass

        # Fallback: query information schema
        try:
            df = super().sql("SELECT table_name FROM information_schema.tables")
            result = df.collect()
            if result:
                return [row["table_name"] for batch in result for row in batch.to_pydict()["table_name"]]
        except Exception:
            pass

        return []
