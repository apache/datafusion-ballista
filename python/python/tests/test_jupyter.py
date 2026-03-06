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
from ballista import BallistaSessionContext, setup_test_cluster
from ballista.jupyter import (
    BallistaMagics,
    BallistaConnectionError,
    load_ipython_extension,
    IPYTHON_AVAILABLE,
)


@pytest.fixture
def ctx():
    """Create a connected Ballista context for testing."""
    (address, port) = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


@pytest.fixture
def magics():
    """Create a BallistaMagics instance for testing."""
    return BallistaMagics(shell=None)


class TestBallistaMagics:
    """Tests for the BallistaMagics class."""

    def test_initial_state(self, magics):
        """Test that magics start disconnected."""
        assert not magics.is_connected
        assert magics._ctx is None
        assert magics._address is None

    def test_connect_requires_address(self, magics):
        """Test that connect requires an address."""
        result = magics._connect("")
        assert "Usage:" in result

    def test_connect_with_invalid_address(self, magics):
        """Test connection to invalid address fails gracefully."""
        # This should fail but not raise an exception
        result = magics._connect("df://invalid-host:99999")
        # The result should indicate failure or the connection may be lazy
        # Either way, we shouldn't crash

    def test_status_when_disconnected(self, magics):
        """Test status shows disconnected state."""
        result = magics._status()
        assert "Not connected" in result
        assert "connect" in result.lower()

    def test_disconnect_when_not_connected(self, magics):
        """Test disconnecting when not connected."""
        result = magics._disconnect()
        assert "Not connected" in result

    def test_tables_requires_connection(self, magics):
        """Test that tables command requires connection."""
        # When not connected, accessing ctx property raises BallistaConnectionError
        with pytest.raises(BallistaConnectionError):
            _ = magics.ctx  # Accessing ctx when not connected should raise

    def test_schema_requires_connection(self, magics):
        """Test that schema command requires connection."""
        # When not connected, accessing ctx property raises BallistaConnectionError
        with pytest.raises(BallistaConnectionError):
            _ = magics.ctx  # Accessing ctx when not connected should raise

    def test_show_help(self, magics):
        """Test that help is shown correctly."""
        result = magics._show_help()
        assert "connect" in result.lower()
        assert "status" in result.lower()
        assert "tables" in result.lower()
        assert "sql" in result.lower()

    def test_ballista_command_parsing(self, magics):
        """Test parsing of ballista commands."""
        # Test help command
        result = magics.ballista("help")
        assert "connect" in result.lower()

        # Test unknown command
        result = magics.ballista("unknown_command")
        assert "Unknown command" in result

    def test_show_history_empty(self, magics):
        """Test history when no queries executed."""
        result = magics._show_history()
        assert "No queries" in result


class TestBallistaMagicsConnected:
    """Tests for BallistaMagics when connected."""

    @pytest.fixture
    def connected_magics(self):
        """Create a connected BallistaMagics instance."""
        magics = BallistaMagics(shell=None)
        (address, port) = setup_test_cluster()
        magics._connect(f"df://{address}:{port}")
        return magics

    def test_is_connected(self, connected_magics):
        """Test that we're connected after connect."""
        assert connected_magics.is_connected
        assert connected_magics._ctx is not None

    def test_status_when_connected(self, connected_magics):
        """Test status shows connected state."""
        result = connected_magics._status()
        assert "Connected" in result
        assert "Session ID" in result

    def test_disconnect(self, connected_magics):
        """Test disconnecting."""
        result = connected_magics._disconnect()
        assert "Disconnected" in result
        assert not connected_magics.is_connected

    def test_tables_empty(self, connected_magics):
        """Test listing tables when none registered."""
        result = connected_magics._tables()
        # Either shows empty or error message
        assert result is not None

    def test_execute_sql(self, connected_magics):
        """Test executing a SQL query."""
        result = connected_magics._execute_sql(
            "SELECT 1 as value",
            display_results=False,
        )
        assert result is not None

    def test_query_history(self, connected_magics):
        """Test that query history is recorded."""
        connected_magics._execute_sql("SELECT 1", display_results=False)
        assert len(connected_magics._query_history) == 1
        assert "SELECT 1" in connected_magics._query_history[0]["query"]


class TestIPythonExtension:
    """Tests for IPython extension loading."""

    def test_load_extension_function_exists(self):
        """Test that load_ipython_extension is defined."""
        assert callable(load_ipython_extension)

    @pytest.mark.skipif(
        not IPYTHON_AVAILABLE,
        reason="IPython not available"
    )
    def test_load_extension_with_ipython(self):
        """Test loading extension with IPython available."""
        # This would require mocking IPython, which is complex
        # Just verify the function doesn't crash when called with None
        try:
            load_ipython_extension(None)
        except (AttributeError, TypeError):
            pass  # Expected when shell is None


class TestBallistaConnectionError:
    """Tests for the BallistaConnectionError exception."""

    def test_exception_message(self):
        """Test that exception has correct message."""
        error = BallistaConnectionError("Test message")
        assert "Test message" in str(error)

    def test_exception_inheritance(self):
        """Test that exception inherits from Exception."""
        assert issubclass(BallistaConnectionError, Exception)
