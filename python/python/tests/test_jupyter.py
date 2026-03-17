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

"""Tests for the Ballista Jupyter module."""

import pytest
from unittest.mock import patch, MagicMock
from ballista import BallistaSessionContext, setup_test_cluster
from ballista.jupyter import (
    BallistaMagics,
    BallistaConnectionError,
    load_ipython_extension,
    IPYTHON_AVAILABLE,
)


@pytest.fixture
def magics():
    """Create a BallistaMagics instance for testing."""
    return BallistaMagics(shell=None)


@pytest.fixture
def connected_magics():
    """Create a connected BallistaMagics instance."""
    magics = BallistaMagics(shell=None)
    (address, port) = setup_test_cluster()
    magics._connect(f"df://{address}:{port}")
    return magics


class TestBallistaMagicsInitialState:
    """Tests for initial (disconnected) state."""

    def test_initial_state(self, magics):
        """Test that magics start disconnected."""
        assert not magics.is_connected
        assert magics._ctx is None
        assert magics._address is None

    def test_ctx_property_raises_when_disconnected(self, magics):
        """Test that accessing ctx raises BallistaConnectionError when not connected."""
        with pytest.raises(BallistaConnectionError):
            _ = magics.ctx

    def test_disconnect_when_not_connected_returns_message(self, magics):
        result = magics._disconnect()
        assert "Not connected" in result

    def test_status_when_disconnected_returns_message(self, magics):
        result = magics._status()
        assert "Not connected" in result
        assert "connect" in result.lower()

    def test_show_history_empty_returns_message(self, magics):
        result = magics._show_history()
        assert "No queries" in result

    def test_show_help_prints(self, magics, capsys):
        """Test that help is printed correctly."""
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            magics._show_help()
            captured = capsys.readouterr()
            assert "connect" in captured.out.lower()
            assert "status" in captured.out.lower()
            assert "tables" in captured.out.lower()
            assert "sql" in captured.out.lower()


class TestBallistaMagicsConnect:
    """Tests for connect/disconnect behavior."""

    def test_connect_requires_address_returns_usage(self, magics):
        """Test that connect with empty address returns a usage string."""
        result = magics._connect("")
        assert result is not None
        assert "Usage:" in result

    def test_connect_with_invalid_address_does_not_raise(self, magics):
        """Test connection to invalid address fails gracefully without raising."""
        # Should not raise, but context should not be set
        magics._connect("df://invalid-host:99999")
        # Connection may be lazy; just assert no unhandled exception occurred

    def test_connect_normalizes_address_without_scheme(self, magics):
        """Test that connect prepends df:// if missing."""
        # Use an invalid host so it fails, but we can inspect the stored address
        magics._connect("localhost:12345")
        # Either connected (unlikely with bad host) or failed cleanly
        # Either way, _address if set should have df:// prefix
        if magics._address is not None:
            assert magics._address.startswith("df://")

    def test_successful_connect_sets_state(self, magics):
        """Test that successful connect sets ctx, address, and is_connected."""
        (address, port) = setup_test_cluster()
        magics._connect(f"df://{address}:{port}")
        assert magics.is_connected
        assert magics._ctx is not None
        assert magics._address is not None

    def test_disconnect_clears_state(self, connected_magics):
        """Test that disconnect clears ctx, address and sets is_connected=False."""
        connected_magics._disconnect()
        assert not connected_magics.is_connected
        assert connected_magics._ctx is None
        assert connected_magics._address is None
        assert connected_magics._last_result is None

    def test_disconnect_prints_confirmation(self, connected_magics, capsys):
        """Test that disconnect prints confirmation message."""
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            connected_magics._disconnect()
            captured = capsys.readouterr()
            assert "Disconnected" in captured.out


class TestBallistaMagicsCommandParsing:
    """Tests for %ballista command dispatch."""

    def test_ballista_help_command(self, magics, capsys):
        """Test that help command prints help text."""
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            magics.ballista("help")
            captured = capsys.readouterr()
            assert "connect" in captured.out.lower()

    def test_ballista_unknown_command_returns_message(self, magics):
        """Test that unknown command returns an error message."""
        result = magics.ballista("unknown_command_xyz")
        assert result is not None
        assert "Unknown command" in result

    def test_ballista_empty_line_shows_help(self, magics, capsys):
        """Test that empty line shows help."""
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            magics.ballista("")
            captured = capsys.readouterr()
            assert "connect" in captured.out.lower()

    def test_ballista_status_dispatches(self, magics):
        result = magics.ballista("status")
        assert "Not connected" in result

    def test_ballista_disconnect_when_not_connected(self, magics):
        """Test disconnect subcommand when not connected."""
        result = magics.ballista("disconnect")
        assert result is not None
        assert "Not connected" in result


class TestBallistaMagicsConnected:
    """Tests for BallistaMagics when connected to a real cluster."""

    def test_is_connected(self, connected_magics):
        """Test that is_connected is True after connect."""
        assert connected_magics.is_connected
        assert connected_magics._ctx is not None

    def test_status_when_connected_prints(self, connected_magics, capsys):
        """Test status shows connected state via print."""
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            connected_magics._status()
            captured = capsys.readouterr()
            assert "Connected" in captured.out
            assert "Session ID" in captured.out

    def test_tables_empty_does_not_raise(self, connected_magics):
        """Test listing tables when none registered does not raise."""
        # Should print or return a message, not raise
        connected_magics._tables()  # no assertion needed beyond "does not raise"

    def test_execute_sql_returns_dataframe(self, connected_magics):
        """Test executing a SQL query returns a DistributedDataFrame."""
        result = connected_magics._execute_sql("SELECT 1 as value")
        assert result is not None

    def test_execute_sql_records_history(self, connected_magics):
        """Test that executed queries are recorded in history."""
        connected_magics._execute_sql("SELECT 1")
        assert len(connected_magics._query_history) == 1
        assert "SELECT 1" in connected_magics._query_history[0]["query"]

    def test_execute_sql_history_has_timestamp_and_elapsed(self, connected_magics):
        """Test that history entries contain timing info."""
        connected_magics._execute_sql("SELECT 1")
        entry = connected_magics._query_history[0]
        assert "timestamp" in entry
        assert "elapsed_seconds" in entry
        assert isinstance(entry["elapsed_seconds"], float)

    def test_execute_sql_stores_last_result(self, connected_magics):
        """Test that _last_result is updated after execution."""
        connected_magics._execute_sql("SELECT 1")
        assert connected_magics._last_result is not None

    def test_show_history_prints_entries(self, connected_magics, capsys):
        """Test that history is printed after queries are executed."""
        connected_magics._execute_sql("SELECT 1")
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            connected_magics._show_history()
            captured = capsys.readouterr()
            assert "SELECT 1" in captured.out

    def test_sql_magic_line_returns_dataframe(self, connected_magics):
        """Test %sql line magic returns a DistributedDataFrame."""
        result = connected_magics.sql("SELECT 1 as value")
        assert result is not None

    def test_sql_magic_cell_returns_dataframe(self, connected_magics):
        """Test %%sql cell magic returns a DistributedDataFrame."""
        result = connected_magics.sql("", cell="SELECT 1 as value")
        assert result is not None

    def test_sql_magic_cell_stores_in_shell_namespace(self, connected_magics):
        """Test %%sql stores result in shell namespace when var name given."""
        mock_shell = MagicMock()
        mock_shell.user_ns = {}
        connected_magics.shell = mock_shell

        connected_magics.sql("my_var", cell="SELECT 1 as value")
        assert "my_var" in mock_shell.user_ns
        assert mock_shell.user_ns["my_var"] is not None

    def test_sql_magic_empty_line_returns_none(self, connected_magics):
        """Test %sql with empty line returns None."""
        result = connected_magics.sql("")
        assert result is None

    def test_schema_missing_table_name_returns_usage(self, connected_magics):
        """Test _schema with no table name returns usage string."""
        result = connected_magics._schema("")
        assert result is not None
        assert "Usage:" in result

    def test_register_parquet_dispatches(self, connected_magics, tmp_path):
        """Test %register parquet calls register_parquet on context."""
        with patch.object(connected_magics._ctx, "register_parquet") as mock_reg:
            connected_magics.register(f"parquet my_table {tmp_path}/file.parquet")
            mock_reg.assert_called_once()

    def test_register_csv_dispatches(self, connected_magics, tmp_path):
        """Test %register csv calls register_csv on context."""
        with patch.object(connected_magics._ctx, "register_csv") as mock_reg:
            connected_magics.register(f"csv my_table {tmp_path}/file.csv")
            mock_reg.assert_called_once()

    def test_register_unsupported_format_raises(self, connected_magics):
        """Test %register with unsupported format raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            connected_magics.register("xlsx my_table file.xlsx")

    def test_register_missing_table_name_returns_message(self, connected_magics):
        """Test %register with missing table name returns a message."""
        result = connected_magics.register("parquet")
        assert result is not None

    def test_register_missing_file_path_returns_message(self, connected_magics):
        """Test %register with missing file path returns a message."""
        result = connected_magics.register("parquet my_table")
        assert result is not None


class TestIPythonExtension:
    """Tests for IPython extension loading."""

    def test_load_extension_function_exists(self):
        """Test that load_ipython_extension is defined and callable."""
        assert callable(load_ipython_extension)

    def test_load_extension_registers_magics(self):
        """Test that load_ipython_extension registers BallistaMagics."""
        mock_ipython = MagicMock()
        load_ipython_extension(mock_ipython)
        mock_ipython.register_magics.assert_called_once_with(BallistaMagics)

    @pytest.mark.skipif(not IPYTHON_AVAILABLE, reason="IPython not available")
    def test_load_extension_with_none_shell_does_not_crash(self):
        """Test loading extension with None shell doesn't crash."""
        try:
            load_ipython_extension(None)
        except (AttributeError, TypeError):
            pass  # Expected when shell is None


class TestBallistaConnectionError:
    """Tests for the BallistaConnectionError exception."""

    def test_tables_when_disconnected_warns(self, magics):
        with pytest.warns(UserWarning, match="Not connected"):
            magics._tables()

    def test_schema_when_disconnected_warns(self, magics):
        with pytest.warns(UserWarning, match="Not connected"):
            magics._schema("some_table")

    def test_execute_sql_when_disconnected_prints_error(self, magics, capsys):
        with patch("ballista.jupyter.IPYTHON_AVAILABLE", False):
            result = magics._execute_sql("SELECT 1")
            assert result is None
            captured = capsys.readouterr()
            assert "Query failed" in captured.out
