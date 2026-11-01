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

"""TPC-H table definitions for the Trino Hive connector.

Trino does not infer a schema from Parquet files, so the benchmark tables are
declared here. The types are the ones `tpchgen-cli` writes: 64-bit keys, 32-bit
integers, `decimal(15, 2)` money columns and dates. Data written by other tools
may store the money columns with another precision; `money_type` declares it.

Data can be laid out in one of two ways, chosen with `partitioned`:

- One directory of Parquet files per table, as `tpchgen-cli --parts N` writes.
- Hive-style partitioned: `part` by `p_brand`, `customer` by `c_mktsegment`,
  `orders` by `o_orderdate` and `lineitem` by `l_shipdate`, one directory per
  value (`lineitem/l_shipdate=1994-01-01/`). The Hive connector takes a
  partition column's value from the directory name, even when the files store
  the column too.

Used by run.py (to key the metastore directory on the DDL) and by
run_queries.py, which runs in the cluster.
"""

import hashlib

BIGINT = "bigint"
INTEGER = "integer"
VARCHAR = "varchar"
DATE = "date"
# Replaced by table_ddl()'s money_type.
MONEY = "money"
DEFAULT_MONEY_TYPE = "decimal(15, 2)"

# (table name, partition column in the partitioned layout or None, [(column, type), ...]).
# The partition column is listed in its natural position here; table_ddl()
# moves it last, as the Hive connector requires.
TPCH_TABLES = [
    ("part", "p_brand", [
        ("p_partkey", BIGINT),
        ("p_name", VARCHAR),
        ("p_mfgr", VARCHAR),
        ("p_brand", VARCHAR),
        ("p_type", VARCHAR),
        ("p_size", INTEGER),
        ("p_container", VARCHAR),
        ("p_retailprice", MONEY),
        ("p_comment", VARCHAR),
    ]),
    ("supplier", None, [
        ("s_suppkey", BIGINT),
        ("s_name", VARCHAR),
        ("s_address", VARCHAR),
        ("s_nationkey", BIGINT),
        ("s_phone", VARCHAR),
        ("s_acctbal", MONEY),
        ("s_comment", VARCHAR),
    ]),
    ("partsupp", None, [
        ("ps_partkey", BIGINT),
        ("ps_suppkey", BIGINT),
        ("ps_availqty", INTEGER),
        ("ps_supplycost", MONEY),
        ("ps_comment", VARCHAR),
    ]),
    ("customer", "c_mktsegment", [
        ("c_custkey", BIGINT),
        ("c_name", VARCHAR),
        ("c_address", VARCHAR),
        ("c_nationkey", BIGINT),
        ("c_phone", VARCHAR),
        ("c_acctbal", MONEY),
        ("c_mktsegment", VARCHAR),
        ("c_comment", VARCHAR),
    ]),
    ("orders", "o_orderdate", [
        ("o_orderkey", BIGINT),
        ("o_custkey", BIGINT),
        ("o_orderstatus", VARCHAR),
        ("o_totalprice", MONEY),
        ("o_orderdate", DATE),
        ("o_orderpriority", VARCHAR),
        ("o_clerk", VARCHAR),
        ("o_shippriority", INTEGER),
        ("o_comment", VARCHAR),
    ]),
    ("lineitem", "l_shipdate", [
        ("l_orderkey", BIGINT),
        ("l_partkey", BIGINT),
        ("l_suppkey", BIGINT),
        ("l_linenumber", INTEGER),
        ("l_quantity", MONEY),
        ("l_extendedprice", MONEY),
        ("l_discount", MONEY),
        ("l_tax", MONEY),
        ("l_returnflag", VARCHAR),
        ("l_linestatus", VARCHAR),
        ("l_shipdate", DATE),
        ("l_commitdate", DATE),
        ("l_receiptdate", DATE),
        ("l_shipinstruct", VARCHAR),
        ("l_shipmode", VARCHAR),
        ("l_comment", VARCHAR),
    ]),
    ("nation", None, [
        ("n_nationkey", BIGINT),
        ("n_name", VARCHAR),
        ("n_regionkey", BIGINT),
        ("n_comment", VARCHAR),
    ]),
    ("region", None, [
        ("r_regionkey", BIGINT),
        ("r_name", VARCHAR),
        ("r_comment", VARCHAR),
    ]),
]


def partition_column(table, partitioned):
    """The table's partition column in this layout, or None."""
    _, partition_col, _ = next(t for t in TPCH_TABLES if t[0] == table)
    return partition_col if partitioned else None


def table_ddl(table, catalog, schema, data_path, partitioned, money_type=DEFAULT_MONEY_TYPE):
    """CREATE TABLE IF NOT EXISTS for one external table over existing Parquet.

    `data_path` is the directory that holds one subdirectory per table, as an
    s3:// URI. The table is EXTERNAL: dropping it only drops metadata, and
    Trino never writes to the location.
    """
    _, _, columns = next(t for t in TPCH_TABLES if t[0] == table)
    partition_col = partition_column(table, partitioned)
    # Hive requires the partition columns to be the last columns.
    ordered = [c for c in columns if c[0] != partition_col]
    ordered += [c for c in columns if c[0] == partition_col]
    column_sql = ",\n  ".join(f"{col} {money_type if typ == MONEY else typ}" for col, typ in ordered)
    properties = [
        "format = 'PARQUET'",
        f"external_location = '{data_path.rstrip('/')}/{table}'",
    ]
    if partition_col:
        properties.append(f"partitioned_by = ARRAY['{partition_col}']")
    properties_sql = ",\n  ".join(properties)
    return (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (\n  {column_sql}\n)\n"
        f"WITH (\n  {properties_sql}\n)"
    )


def ddl_fingerprint(partitioned, money_type=DEFAULT_MONEY_TYPE):
    """Short hash of the table definitions.

    Part of the metastore directory name, so changing a definition here starts a
    fresh metastore instead of reusing tables created from the old one.
    """
    text = repr((TPCH_TABLES, partitioned, money_type))
    return hashlib.sha256(text.encode()).hexdigest()[:8]
