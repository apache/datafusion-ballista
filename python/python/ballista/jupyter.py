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
import time
import threading

try:
    from IPython.core.magic import Magics, magics_class, line_magic, cell_magic
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


from .extension import BallistaSessionContext, DistributedDataFrame


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
            return f"Unknown command: {cmd}. Use '%ballista help' for available commands."

    @line_magic
    def sql(self, line: str) -> Optional[DistributedDataFrame]:
        """
        Execute a SQL query (line magic).

        Usage:
            %sql SELECT COUNT(*) FROM my_table

        The result is returned as a DataFrame and also stored in the
        variable '_' for further use.
        """
        if not line.strip():
            return None

        return self._execute_sql(line.strip())

    @cell_magic
    def sql(self, line: str, cell: str) -> Optional[DistributedDataFrame]:
        """
        Execute a SQL query (cell magic).

        Usage:
            %%sql
            SELECT
                customer_id,
                SUM(amount) as total
            FROM orders
            GROUP BY customer_id
            ORDER BY total DESC
            LIMIT 10

        Optional line arguments:
            %%sql --no-display      - Execute without displaying results
            %%sql --limit 100       - Limit displayed rows (default: 50)
            %%sql result_var        - Store result in specified variable

        Examples:
            %%sql my_result
            SELECT * FROM orders WHERE status = 'pending'
        """
        # Parse line arguments
        args = line.strip().split()
        display_results = True
        limit = 50
        var_name = None

        i = 0
        while i < len(args):
            if args[i] == "--no-display":
                display_results = False
            elif args[i] == "--limit" and i + 1 < len(args):
                try:
                    limit = int(args[i + 1])
                    i += 1
                except ValueError:
                    pass
            elif not args[i].startswith("--"):
                var_name = args[i]
            i += 1

        query = cell.strip()
        if not query:
            return None

        result = self._execute_sql(query, display_results=display_results, limit=limit)

        # Store in user namespace if variable name provided
        if var_name and self.shell is not None:
            self.shell.user_ns[var_name] = result

        return result

    def _connect(self, address: str) -> str:
        """Connect to a Ballista cluster."""
        if not address:
            return "Usage: %ballista connect df://host:port"

        # Normalize address
        if not address.startswith("df://"):
            address = f"df://{address}"

        try:
            self._ctx = BallistaSessionContext(address)
            self._address = address
            return f"✓ Connected to Ballista cluster at {address}"
        except Exception as e:
            self._ctx = None
            self._address = None
            return f"✗ Failed to connect to {address}: {e}"

    def _disconnect(self) -> str:
        """Disconnect from the Ballista cluster."""
        if not self.is_connected:
            return "Not connected to any cluster."

        address = self._address
        self._ctx = None
        self._address = None
        self._last_result = None
        return f"✓ Disconnected from {address}"

    def _status(self) -> str:
        """Show connection status."""
        if not self.is_connected:
            return "Status: Not connected\n\nUse '%ballista connect df://host:port' to connect."

        status_lines = [
            f"Status: Connected",
            f"Address: {self._address}",
            f"Session ID: {self._ctx.session_id}",
            f"Queries executed: {len(self._query_history)}",
        ]

        if self._last_result is not None:
            status_lines.append("Last result: Available (access via '_' or '_last_result')")

        return "\n".join(status_lines)

    def _tables(self) -> Optional[str]:
        """List all registered tables."""
        try:
            # Get table names from the catalog
            tables = self.ctx.tables()

            if not tables:
                return "No tables registered.\n\nUse ctx.register_parquet() or ctx.register_csv() to register tables."

            # Build a nice table display
            lines = ["Registered Tables:", "-" * 40]
            for table in sorted(tables):
                lines.append(f"  • {table}")
            lines.append("-" * 40)
            lines.append(f"Total: {len(tables)} table(s)")

            return "\n".join(lines)
        except Exception as e:
            return f"Error listing tables: {e}"

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

            return "\n".join(lines)
        except Exception as e:
            return f"Error getting schema for '{table_name}': {e}"

    def _execute_sql(
        self,
        query: str,
        display_results: bool = True,
        limit: int = 50,
    ) -> Optional[DistributedDataFrame]:
        """Execute a SQL query and return the result."""
        start_time = time.time()

        try:
            result = self.ctx.sql(query)
            elapsed = time.time() - start_time

            # Store result
            self._last_result = result
            if self.shell is not None and hasattr(self.shell, 'user_ns'):
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
            if display_results and IPYTHON_AVAILABLE:
                try:
                    # The result has _repr_html_ so it will render nicely
                    display(result)
                except Exception:
                    # Fallback to show()
                    result.show(limit)

            return result

        except Exception as e:
            error_msg = f"Query failed: {e}"
            if IPYTHON_AVAILABLE:
                try:
                    display(HTML(f'<div style="color: red; font-weight: bold;">{error_msg}</div>'))
                except Exception:
                    print(error_msg)
            else:
                print(error_msg)
            return None

    def _show_history(self) -> str:
        """Show query history."""
        if not self._query_history:
            return "No queries executed yet."

        lines = ["Query History:", "-" * 60]
        for i, entry in enumerate(self._query_history[-10:], 1):  # Last 10 queries
            query_preview = entry["query"][:50] + "..." if len(entry["query"]) > 50 else entry["query"]
            query_preview = query_preview.replace("\n", " ")
            lines.append(f"{i}. [{entry['timestamp']}] ({entry['elapsed_seconds']:.2f}s)")
            lines.append(f"   {query_preview}")
        lines.append("-" * 60)

        return "\n".join(lines)

    def _show_help(self) -> str:
        """Show help for Ballista magic commands."""
        return """
Ballista Jupyter Magic Commands
================================

Connection:
    %ballista connect <url>   - Connect to Ballista cluster
    %ballista disconnect      - Disconnect from cluster
    %ballista status          - Show connection status

Exploration:
    %ballista tables          - List registered tables
    %ballista schema <table>  - Show table schema

Query:
    %sql <query>              - Execute single-line SQL query

    %%sql [options] [var]     - Execute multi-line SQL query
        Options:
            --no-display      - Don't display results
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
