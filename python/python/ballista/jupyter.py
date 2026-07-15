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

"""
Ballista Jupyter Magic Commands.

This module provides IPython magic commands for interacting with Ballista
clusters directly from Jupyter notebooks.

Usage:
    %load_ext ballista.jupyter

    # Connect to a Ballista cluster
    %ballista connect df://localhost:50050

    # Check connection status
    %ballista status

    # List registered tables
    %ballista tables

    # Show schema for a table
    %ballista schema my_table

    # Execute a simple SQL query (line magic)
    %sql SELECT COUNT(*) FROM my_table

    # Execute a complex SQL query (cell magic)
    %%sql
    SELECT
        customer_id,
        SUM(amount) as total
    FROM orders
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 10
"""

from typing import Optional, List, Dict, Any
import warnings
import time

try:
    from IPython.core.magic import Magics, magics_class, line_magic, line_cell_magic
    from IPython.display import display, HTML

    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False

    # Provide stub classes for when IPython is not available
    class Magics:
        def __init__(self, shell=None):
            self.shell = shell

    def magics_class(cls):
        return cls

    def line_magic(name_or_func):
        """Stub line_magic decorator for when IPython is not available."""
        # Handle both @line_magic and @line_magic("name") usage
        if callable(name_or_func):
            # Used as @line_magic without arguments
            return name_or_func
        else:
            # Used as @line_magic("name") with arguments
            def decorator(func):
                return func

            return decorator

    def cell_magic(name_or_func):
        """Stub cell_magic decorator for when IPython is not available."""
        # Handle both @cell_magic and @cell_magic("name") usage
        if callable(name_or_func):
            return name_or_func
        else:

            def decorator(func):
                return func

            return decorator

    def line_cell_magic(name_or_func):
        """Stub line_cell_magic decorator for when IPython is not available"""
        # Handle both @line_cell_magic and @line_cell_magic("name") usage
        if callable(name_or_func):
            return name_or_func
        else:

            def decorator(func):
                return func

            return decorator


from datafusion.dataframe_formatter import (
    configure_formatter,
    get_formatter,
    set_formatter,
)

from .extension import BallistaSessionContext, DistributedDataFrame

# Flags accepted by the ``%%sql`` cell magic, kept as constants so the parser
# and its error messages share a single source of truth.
LIMIT_FLAG = "--limit"
NO_DISPLAY_FLAG = "--no-display"

# Default number of rows rendered for a ``%%sql`` cell when ``--limit`` is not
# given. This caps only the display (via datafusion's HTML formatter); the
# underlying result keeps all of its rows.
DEFAULT_DISPLAY_LIMIT = 50


class BallistaConnectionError(Exception):
    """Raised when not connected to a Ballista cluster."""

    pass


@magics_class
class BallistaMagics(Magics):
    """
    IPython magic commands for Ballista.

    Provides convenient commands for connecting to Ballista clusters,
    executing SQL queries, and exploring table schemas.
    """

    def __init__(self, shell=None):
        super().__init__(shell)
        self._ctx: Optional[BallistaSessionContext] = None
        self._address: Optional[str] = None
        self._last_result: Optional[DistributedDataFrame] = None
        self._query_history: List[Dict[str, Any]] = []

    @property
    def ctx(self) -> BallistaSessionContext:
        """Get the current context, raising an error if not connected."""
        if self._ctx is None:
            raise BallistaConnectionError(
                "Not connected to a Ballista cluster. "
                "Use: %ballista connect df://host:port"
            )
        return self._ctx

    @property
    def is_connected(self) -> bool:
        """Check if connected to a Ballista cluster."""
        return self._ctx is not None

    @line_magic
    def ballista(self, line: str) -> Optional[str]:
        """
        Ballista management commands.

        Usage:
            %ballista connect df://localhost:50050  - Connect to cluster
            %ballista status                        - Show connection status
            %ballista tables                        - List registered tables
            %ballista schema <table>                - Show table schema
            %ballista disconnect                    - Disconnect from cluster
            %ballista history                       - Show query history

        Examples:
            %ballista connect df://localhost:50050
            %ballista tables
            %ballista schema orders
        """
        parts = line.strip().split(maxsplit=1)
        if not parts:
            return self._show_help()

        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        if cmd == "connect":
            return self._connect(args)
        elif cmd == "status":
            return self._status()
        elif cmd == "tables":
            return self._tables()
        elif cmd == "schema":
            return self._schema(args)
        elif cmd == "disconnect":
            return self._disconnect()
        elif cmd == "history":
            return self._show_history()
        elif cmd == "help":
            return self._show_help()
        else:
            return (
                f"Unknown command: {cmd}. Use '%ballista help' for available commands."
            )

    @line_magic
    def register(self, line: str) -> Optional[str]:
        """Register a new table"""
        if not line:
            return "You should provide file extension and table name to register"
        elif self._ctx is None:
            raise BallistaConnectionError(
                "Not connected to a Ballista cluster. "
                "Use: %ballista connect df://host:port"
            )
        else:
            args = line.strip().split()
            file_type = args[0]
            if len(args) < 2:
                return f"You should provide table name for this .{file_type} file"
            table_name = args[1]
            if len(args) < 3:
                return "You should provide path to your file"
            file_name = args[2]

            if file_type == "parquet":
                self._ctx.register_parquet(table_name, file_name)
            elif file_type == "csv":
                self._ctx.register_csv(table_name, file_name)
            else:
                raise NotImplementedError(
                    "Currently not supporting the inserted file format"
                )

    @staticmethod
    def _parse_cell_magic_args(line: str):
        """Parse the argument line of a ``%%sql`` cell magic.

        Recognises the ``--no-display`` and ``--limit N`` flags (space form,
        e.g. ``--limit 5``, consistent with the other magics in this module).
        The first non-flag token, if any, is treated as the variable name to
        store the result in.

        Returns a ``(var_name, no_display, limit)`` tuple where ``limit`` is
        ``None`` when ``--limit`` was not supplied. Raises ``ValueError`` for a
        missing or invalid ``--limit`` value.
        """
        tokens = line.strip().split()
        var_name = None
        no_display = False
        limit = None

        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token == NO_DISPLAY_FLAG:
                no_display = True
            elif token == LIMIT_FLAG:
                i += 1
                if i >= len(tokens):
                    raise ValueError(
                        f"{LIMIT_FLAG} requires a number, e.g. {LIMIT_FLAG} 5"
                    )
                try:
                    limit = int(tokens[i])
                except ValueError:
                    raise ValueError(
                        f"{LIMIT_FLAG} expects an integer, got '{tokens[i]}'"
                    )
                if limit < 1:
                    raise ValueError(f"{LIMIT_FLAG} must be a positive integer")
            elif not token.startswith("--") and var_name is None:
                var_name = token
            i += 1

        return var_name, no_display, limit

    @line_cell_magic
    def sql(self, line: str, cell=None) -> Optional[DistributedDataFrame]:
        """
        Execute a SQL query (both line and cell magic).

        Two cases possible: with cell or without cell

        Examples:
        1. Without a cell (line_magic):
            %sql SELECT * FROM test_table
        2. With a cell (cell_magic) -- we can also use a variable to store the result of the query like this:
            %%sql my_result
            SELECT
                id,
                bool_col,
                tinyint_col
            FROM test_data_v1
            WHERE id > 2
            ORDER BY id
            LIMIT 5

        `my_result` will store the result of the SQL-query

        The cell magic accepts two optional flags before the variable name:
            --limit N      Render at most N rows in the cell output (default
                           50). This caps the display only; the stored result
                           keeps every row.
            --no-display   Run the query and store the result without
                           displaying it.
        """
        if not cell:
            return self._execute_sql(line.strip()) if line.strip() else None
        else:
            query = cell.strip()
            if not query:
                return None

            try:
                var_name, no_display, limit = self._parse_cell_magic_args(line)
            except ValueError as e:
                return str(e)

            result = self._execute_sql(query)

            # The stored variable always holds the full, untruncated result.
            if var_name and self.shell is not None:
                self.shell.user_ns[var_name] = result

            if no_display:
                return None

            # Outside IPython there is no auto-render to cap or suppress, so
            # just return the result rather than swallowing it.
            if not IPYTHON_AVAILABLE:
                return result

            # Display-only cap: limits the rows rendered for THIS cell, never
            # the underlying data, so an in-query LIMIT always takes effect.
            # datafusion's formatter is a process-global singleton, so we
            # render eagerly with the cap applied and then restore the previous
            # formatter, keeping the effect scoped to this cell instead of
            # leaking into the rest of the session. Both min_rows and max_rows
            # are set because the formatter requires min_rows <= max_rows.
            rows = limit if limit is not None else DEFAULT_DISPLAY_LIMIT
            previous_formatter = get_formatter()
            try:
                configure_formatter(max_rows=rows, min_rows=rows)
                display(result)
            finally:
                set_formatter(previous_formatter)

            # Returning None avoids a second, un-capped auto-render by IPython;
            # the result is still available via the variable and _last_result.
            return None

    def _connect(self, address: str) -> Optional[str]:
        """Connect to a Ballista cluster."""
        if not address:
            return "Usage: %ballista connect df://host:port"

        # Normalize address
        if not address.startswith("df://"):
            address = f"df://{address}"

        try:
            self._ctx = BallistaSessionContext(address)
            self._address = address
            if IPYTHON_AVAILABLE:
                display(HTML(f"✓ Connected to Ballista cluster at {address}"))
            else:
                print(f"✓ Connected to Ballista cluster at {address}")
        except Exception as e:
            self._ctx = None
            self._address = None
            if IPYTHON_AVAILABLE:
                display(HTML(f"✗ Failed to connect to {address}: {e}"))
            else:
                print(f"✗ Failed to connect to {address}: {e}")

    def _disconnect(self) -> Optional[str]:
        """Disconnect from the Ballista cluster."""
        if not self.is_connected:
            return "Not connected to any cluster."

        address = self._address
        self._ctx = None
        self._address = None
        self._last_result = None
        if IPYTHON_AVAILABLE:
            display(HTML(f"✓ Disconnected from {address}"))
        else:
            print(f"✓ Disconnected from {address}")

    def _status(self) -> Optional[str]:
        """Show connection status."""
        if not self.is_connected:
            return "Status: Not connected\n\nUse '%ballista connect df://host:port' to connect."

        status_lines = [
            "Status: Connected",
            f"Address: {self._address}",
            f"Session ID: {self._ctx.session_id}",
            f"Queries executed: {len(self._query_history)}",
        ]

        if self._last_result is not None:
            status_lines.append(
                "Last result: Available (access via '_' or '_last_result')"
            )

        def _format_html_status_output(line: str) -> str:
            name, value = line.split(":", 1)
            return f"<pre><strong>{name}:</strong> {value.strip()}</pre>"

        html = "".join(_format_html_status_output(line) for line in status_lines)
        if IPYTHON_AVAILABLE:
            display(HTML(html))
        else:
            print("\n".join(status_lines))

    def _tables(self) -> Optional[str]:
        """List all registered tables."""
        try:
            # Get table names from the catalog and their respective schemas
            tables = self.ctx.get_tables()
            if not tables:
                return "No tables registered.\n\nUse ctx.register_parquet() or ctx.register_csv() to register tables."
            schema_count = len(tables.keys())
            table_count = sum(len(v) for v in tables.values())
            # Build a nice table display (HTML-formatted if applicable)
            lines = [
                {
                    "content": f"Total: {table_count} table(s) in {schema_count} schema(s)",
                    "is_info": True,
                },
                {"content": "Registered tables:", "is_info": True},
                *[
                    {
                        "content": f"Schema: {schema_name}. Tables: {', '.join(table_names)}",
                        "is_info": False,
                    }
                    for schema_name, table_names in tables.items()
                ],
            ]

            def _format_html_tables_output(line: str, is_info: bool = False) -> str:
                if is_info:
                    return f"<pre><strong>{line}</strong></pre>"
                else:
                    return f"<p><pre><i>{line}</i></pre></p>"

            if IPYTHON_AVAILABLE:
                display(
                    HTML(
                        "".join(
                            _format_html_tables_output(val["content"], val["is_info"])
                            for val in lines
                        )
                    )
                )
            else:
                print("".join(val["content"] for val in lines))
        except Exception as e:
            warnings.warn(f"Error listing tables: {e}")

    def _schema(self, table_name: str) -> Optional[str]:
        """Show schema for a table."""
        if not table_name:
            return "Usage: %ballista schema <table_name>"

        try:
            # Query the table with LIMIT 0 to get schema without data
            df = self.ctx.sql(f"SELECT * FROM {table_name} LIMIT 0")
            schema = df.schema()

            lines = [f"Schema for '{table_name}':", "-" * 50]
            for field in schema:
                nullable = "NULL" if field.nullable else "NOT NULL"
                lines.append(f"  {field.name:20} {str(field.type):15} {nullable}")
            lines.append("-" * 50)
            lines.append(f"Total: {len(schema)} column(s)")

            print("\n".join(lines))
        except Exception as e:
            warnings.warn(f"Error getting schema for '{table_name}': {e}")

    def _execute_sql(
        self,
        query: str,
    ) -> Optional[DistributedDataFrame]:
        """Execute a SQL query and return the result."""
        start_time = time.time()

        try:
            result = self.ctx.sql(query)
            elapsed = time.time() - start_time

            # Store result
            self._last_result = result
            if self.shell is not None and hasattr(self.shell, "user_ns"):
                self.shell.user_ns["_last_result"] = result

            # Record in history
            self._query_history.append(
                {
                    "query": query,
                    "elapsed_seconds": elapsed,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

            # Display if requested and in notebook environment
            return result

        except Exception as e:
            error_msg = f"Query failed: {e}"
            if IPYTHON_AVAILABLE:
                try:
                    display(
                        HTML(
                            f'<div style="color: red; font-weight: bold;">{error_msg}</div>'
                        )
                    )
                except Exception:
                    print(error_msg)
            else:
                print(error_msg)
            return None

    def _show_history(self) -> Optional[str]:
        """Show query history."""
        if not self._query_history:
            return "No queries executed yet."

        lines = ["Query History:", "-" * 60]
        for i, entry in enumerate(self._query_history[-10:], 1):  # Last 10 queries
            query_preview = (
                entry["query"][:50] + "..."
                if len(entry["query"]) > 50
                else entry["query"]
            )
            query_preview = query_preview.replace("\n", " ")
            lines.append(
                f"{i}. [{entry['timestamp']}] ({entry['elapsed_seconds']:.2f}s)"
            )
            lines.append(f"   {query_preview}")
        lines.append("-" * 60)
        output = "\n".join(lines)
        if IPYTHON_AVAILABLE:
            display(HTML(f"<pre>{output}</pre>"))
        else:
            print(output)

    def _show_help(self) -> Optional[str]:
        """Show help for Ballista magic commands."""
        help_info = """
Ballista Jupyter Magic Commands
================================

Connection:
    %ballista connect <url>   - Connect to Ballista cluster
    %ballista disconnect      - Disconnect from cluster
    %ballista status          - Show connection status

Exploration:
    %ballista tables          - List registered tables
    %ballista schema <table>  - Show table schema

Table-register:
    %register [format] [schema.table_name] [file_path] - Register a new table in the current Ballista Context

Query:
    %sql <query>              - Execute single-line SQL query

    %%sql [options] [var]     - Execute multi-line SQL query
        Options:
            --no-display      - Run the query and store the result without displaying it
            --limit N         - Limit displayed rows (default: 50)
        var                   - Store result in variable

History:
    %ballista history         - Show recent query history

Examples:
    %ballista connect df://localhost:50050
    %ballista tables
    %ballista schema orders

    %sql SELECT COUNT(*) FROM orders

    %%sql my_result
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 10
        """
        if IPYTHON_AVAILABLE:
            display(HTML(f"<pre>{help_info}</pre>"))
        else:
            print(help_info)


def load_ipython_extension(ipython):
    """
    Load the Ballista IPython extension.

    Usage in Jupyter notebook:
        %load_ext ballista.jupyter
    """
    ipython.register_magics(BallistaMagics)


def unload_ipython_extension(ipython):
    """Unload the Ballista IPython extension."""
    pass  # IPython handles magic cleanup automatically
