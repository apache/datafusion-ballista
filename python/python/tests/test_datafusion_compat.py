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

"""Compatibility tests against the underlying ``datafusion`` Python package.

Ballista's ``DistributedDataFrame`` and ``BallistaSessionContext`` rely on
metaclass introspection of ``datafusion.DataFrame`` / ``datafusion.SessionContext``
(see ``ballista/extension.py``):

1. Methods on the parent class whose return annotation is the literal string
   ``"DataFrame"`` are re-wrapped to return ``DistributedDataFrame``.
2. A hardcoded list ``EXECUTION_METHODS`` is re-wrapped to route execution
   through the Ballista cluster instead of running locally.

Both mechanisms can break **silently** if datafusion changes the annotation
style, renames methods, or alters signatures. These tests exercise each
mechanism so that drift surfaces as a test failure rather than as queries
that quietly fall back to local execution.
"""

import os
import pathlib

import pyarrow as pa
import pytest
from datafusion import DataFrame, SessionContext

from ballista import BallistaSessionContext, setup_test_cluster
from ballista.extension import EXECUTION_METHODS, DistributedDataFrame


@pytest.fixture
def ctx():
    address, port = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


# ---------------------------------------------------------------------------
# Metaclass smoke tests: confirm wrapping actually happened.
# ---------------------------------------------------------------------------


def test_distributed_dataframe_wraps_dataframe_returning_methods():
    """Methods on the parent ``DataFrame`` whose return annotation is the
    string ``"DataFrame"`` must be wrapped on ``DistributedDataFrame``. If
    datafusion-python changes its annotation style (e.g. real class objects
    instead of forward-reference strings), this test fails before queries
    silently start executing locally.
    """
    annotated = [
        name
        for name, m in DataFrame.__dict__.items()
        if callable(m)
        and not name.startswith("_")
        and getattr(m, "__annotations__", {}).get("return") == "DataFrame"
    ]
    # Sanity: datafusion still uses the string-annotation pattern at all.
    assert annotated, (
        "No DataFrame methods carry a string 'DataFrame' return annotation. "
        "datafusion-python likely changed its annotation style; "
        "ballista's metaclass wrapping in extension.py needs updating."
    )

    # Spot-check a representative subset that we expect to keep returning
    # DataFrame across releases. If any of these aren't wrapped, the
    # introspection no longer matches.
    for method in ("select", "filter", "with_column", "aggregate"):
        assert method in annotated, (
            f"datafusion DataFrame.{method} is no longer annotated as "
            f"returning 'DataFrame'; metaclass wrapping will skip it."
        )
        wrapped = DistributedDataFrame.__dict__.get(method)
        original = DataFrame.__dict__.get(method)
        assert wrapped is not None and wrapped is not original, (
            f"DistributedDataFrame.{method} was not re-wrapped by "
            f"RedefiningDataFrameMeta."
        )


def test_ballista_session_context_wraps_dataframe_returning_methods():
    """Same check on the ``SessionContext`` side: ``sql``, ``read_csv`` and
    friends must be wrapped on ``BallistaSessionContext`` so that they return
    a ``DistributedDataFrame``.
    """
    for method in ("sql", "read_csv", "read_parquet"):
        original = SessionContext.__dict__.get(method)
        assert original is not None, (
            f"datafusion SessionContext.{method} is missing; ballista's "
            f"BallistaSessionContext can no longer rely on it."
        )
        ann = getattr(original, "__annotations__", {}).get("return")
        assert ann == "DataFrame", (
            f"SessionContext.{method} return annotation is {ann!r}, not "
            f"'DataFrame'. Metaclass wrapping in extension.py won't catch it."
        )
        wrapped = BallistaSessionContext.__dict__.get(method)
        assert wrapped is not None and wrapped is not original, (
            f"BallistaSessionContext.{method} was not re-wrapped."
        )


def test_execution_methods_are_present_on_dataframe():
    """Every name in ``EXECUTION_METHODS`` must exist on the parent
    ``DataFrame`` class. Otherwise the wrapper at ``extension.py:55-62`` calls
    a method that doesn't exist and only fails at runtime.
    """
    missing = [m for m in EXECUTION_METHODS if not hasattr(DataFrame, m)]
    assert not missing, (
        f"EXECUTION_METHODS no longer present on datafusion DataFrame: "
        f"{missing}. extension.py:39 needs updating."
    )


# ---------------------------------------------------------------------------
# Round-trip per execution method: ensure each wrapped method routes through
# the Ballista cluster and returns a sensible value.
# ---------------------------------------------------------------------------


def _df(ctx):
    """A small DistributedDataFrame the execution methods can act on."""
    ctx.register_csv("test", "testdata/test.csv", has_header=True)
    return ctx.sql("SELECT a, b FROM test")


def test_execution_method_collect(ctx):
    batches = _df(ctx).collect()
    assert batches and all(isinstance(b, pa.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == 5


def test_execution_method_collect_partitioned(ctx):
    partitions = _df(ctx).collect_partitioned()
    assert partitions
    flat = [batch for part in partitions for batch in part]
    assert sum(b.num_rows for b in flat) == 5


def test_execution_method_show(ctx, capsys):
    result = _df(ctx).show()
    captured = capsys.readouterr()
    assert "a" in captured.out and "b" in captured.out
    assert result is None


def test_execution_method_count(ctx):
    assert _df(ctx).count() == 5


def test_execution_method_to_arrow_table(ctx):
    table = _df(ctx).to_arrow_table()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 5


def test_execution_method_to_pandas(ctx):
    pytest.importorskip("pandas")
    pdf = _df(ctx).to_pandas()
    assert pdf.shape == (5, 2)


def test_execution_method_to_polars(ctx):
    pytest.importorskip("polars")
    pldf = _df(ctx).to_polars()
    assert pldf.shape == (5, 2)


def test_execution_method_write_json(ctx, tmp_path):
    out = tmp_path / "out"
    # write_options is declared with a default of None in datafusion 51 but
    # the PyO3 binding still requires the argument to be passed explicitly.
    _df(ctx).write_json(str(out), None)
    written = list(pathlib.Path(out).glob("*.json"))
    assert written, f"write_json produced no files in {out}"
    assert sum(os.path.getsize(p) for p in written) > 0
