// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Driver-facing metadata: server capabilities, type info, and catalog
//! introspection.
//!
//! Catalog answers come from the session's real DataFusion catalog rather than
//! hand-built record batches, so what a driver sees in its schema browser is
//! what a query in the same session can actually reference.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::sql::metadata::{
    GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder, SqlInfoData,
    SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
    Nullable, Searchable, SqlInfo, SqlSupportedTransaction, XdbcDataType,
};
use ballista_core::BALLISTA_VERSION;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableType;
use datafusion::prelude::SessionContext;
use tonic::Status;

/// Table type strings reported to clients, matching the JDBC vocabulary that
/// Flight SQL drivers expect.
const TABLE: &str = "TABLE";
const VIEW: &str = "VIEW";
const LOCAL_TEMPORARY: &str = "LOCAL TEMPORARY";

/// Describes the server to drivers deciding what SQL they may emit.
///
/// The old implementation left `CommandGetSqlInfo` unimplemented with a
/// `// TODO: implement for FlightSQL JDBC to work` comment, which is precisely
/// why the JDBC driver could not connect.
pub(crate) fn sql_info() -> SqlInfoData {
    let mut builder = SqlInfoDataBuilder::new();

    builder.append(SqlInfo::FlightSqlServerName, "Apache DataFusion Ballista");
    builder.append(SqlInfo::FlightSqlServerVersion, BALLISTA_VERSION);
    // Arrow IPC format version, per format/Schema.fbs.
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.append(SqlInfo::FlightSqlServerCancel, true);
    builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);

    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, true);
    builder.append(SqlInfo::SqlDdlTable, true);
    // DataFusion folds unquoted identifiers to lower case and preserves the
    // case of quoted ones: SQL_CASE_SENSITIVITY_LOWERCASE / _CASE_INSENSITIVE.
    builder.append(SqlInfo::SqlIdentifierCase, 2i32);
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(SqlInfo::SqlQuotedIdentifierCase, 3i32);
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(SqlInfo::SqlSearchStringEscape, "\\");
    builder.append(SqlInfo::SqlCatalogTerm, "catalog");
    builder.append(SqlInfo::SqlSchemaTerm, "schema");
    builder.append(SqlInfo::SqlCatalogAtStart, true);
    builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
    builder.append(SqlInfo::SqlNullPlusNullIsNull, true);
    builder.append(SqlInfo::SqlSupportsLikeEscapeClause, true);
    builder.append(SqlInfo::SqlSupportsNonNullableColumns, true);
    builder.append(SqlInfo::SqlSupportsExpressionsInOrderBy, true);
    builder.append(SqlInfo::SqlSupportsOrderByUnrelated, true);

    builder
        .build()
        .expect("static SqlInfo data is well-formed by construction")
}

/// A deliberately small type list: the types a Ballista result set can
/// actually contain, so a driver's type mapping does not promise more than
/// the engine delivers.
pub(crate) fn xdbc_type_info() -> XdbcTypeInfoData {
    let mut builder = XdbcTypeInfoDataBuilder::new();

    let types = [
        ("BOOLEAN", XdbcDataType::XdbcBit, Some(1)),
        ("TINYINT", XdbcDataType::XdbcTinyint, Some(3)),
        ("SMALLINT", XdbcDataType::XdbcSmallint, Some(5)),
        ("INTEGER", XdbcDataType::XdbcInteger, Some(10)),
        ("BIGINT", XdbcDataType::XdbcBigint, Some(19)),
        ("FLOAT", XdbcDataType::XdbcFloat, Some(7)),
        ("DOUBLE", XdbcDataType::XdbcDouble, Some(15)),
        ("DECIMAL", XdbcDataType::XdbcDecimal, Some(38)),
        ("VARCHAR", XdbcDataType::XdbcVarchar, None),
        ("VARBINARY", XdbcDataType::XdbcVarbinary, None),
        ("DATE", XdbcDataType::XdbcDate, Some(10)),
        ("TIME", XdbcDataType::XdbcTime, Some(8)),
        ("TIMESTAMP", XdbcDataType::XdbcTimestamp, Some(29)),
    ];

    for (type_name, data_type, column_size) in types {
        builder.append(XdbcTypeInfo {
            type_name: type_name.into(),
            data_type,
            column_size,
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some(type_name.into()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: data_type,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });
    }

    builder
        .build()
        .expect("static XdbcTypeInfo data is well-formed by construction")
}

/// Lists the catalogs registered in the session.
pub(crate) fn catalogs(
    ctx: &SessionContext,
    query: CommandGetCatalogs,
) -> Result<RecordBatch, Status> {
    let mut builder: GetCatalogsBuilder = query.into_builder();
    for catalog in ctx.catalog_names() {
        builder.append(catalog);
    }
    builder.build().map_err(flight_to_status)
}

/// Lists the schemas of every registered catalog. Filtering is applied by the
/// builder from the filters carried on the command.
pub(crate) fn db_schemas(
    ctx: &SessionContext,
    query: CommandGetDbSchemas,
) -> Result<RecordBatch, Status> {
    let mut builder: GetDbSchemasBuilder = query.into_builder();
    for catalog_name in ctx.catalog_names() {
        let Some(catalog) = ctx.catalog(&catalog_name) else {
            continue;
        };
        for schema_name in catalog.schema_names() {
            builder.append(&catalog_name, &schema_name);
        }
    }
    builder.build().map_err(flight_to_status)
}

/// Lists the tables of every registered catalog, optionally including each
/// table's Arrow schema.
pub(crate) async fn tables(
    ctx: &SessionContext,
    query: CommandGetTables,
) -> Result<RecordBatch, Status> {
    let mut builder: GetTablesBuilder = query.into_builder();

    for catalog_name in ctx.catalog_names() {
        let Some(catalog) = ctx.catalog(&catalog_name) else {
            continue;
        };
        for schema_name in catalog.schema_names() {
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                // A table can disappear between listing and lookup; skip it
                // rather than failing the whole introspection request.
                let Ok(Some(table)) = schema.table(&table_name).await else {
                    continue;
                };
                builder
                    .append(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        table_type(table.as_ref()),
                        table.schema().as_ref(),
                    )
                    .map_err(flight_to_status)?;
            }
        }
    }

    builder.build().map_err(flight_to_status)
}

/// Lists the table types this server can report.
pub(crate) fn table_types(query: CommandGetTableTypes) -> Result<RecordBatch, Status> {
    let mut builder = query.into_builder();
    for table_type in [TABLE, VIEW, LOCAL_TEMPORARY] {
        builder.append(table_type);
    }
    builder.build().map_err(flight_to_status)
}

fn table_type(table: &dyn TableProvider) -> &'static str {
    match table.table_type() {
        TableType::Base => TABLE,
        TableType::View => VIEW,
        TableType::Temporary => LOCAL_TEMPORARY,
    }
}

fn flight_to_status(e: arrow_flight::error::FlightError) -> Status {
    Status::internal(format!("failed to build metadata result: {e}"))
}

/// Convenience for the callers above, which all need the batch as a
/// single-element vector.
pub(crate) fn one(
    batch: RecordBatch,
) -> (Arc<arrow::datatypes::Schema>, Vec<RecordBatch>) {
    (batch.schema(), vec![batch])
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    fn ctx_with_table() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        ctx.register_table("t", Arc::new(EmptyTable::new(schema)))
            .unwrap();
        ctx
    }

    #[test]
    fn sql_info_is_buildable() {
        // `sql_info` panics on malformed input, so building it is the assertion.
        let data = sql_info();
        assert!(
            data.record_batch([SqlInfo::FlightSqlServerName as u32])
                .unwrap()
                .num_rows()
                > 0
        );
    }

    #[test]
    fn xdbc_type_info_is_buildable() {
        let data = xdbc_type_info();
        assert!(data.record_batch(None).unwrap().num_rows() > 0);
    }

    #[test]
    fn catalogs_come_from_the_session() {
        let batch = catalogs(&ctx_with_table(), CommandGetCatalogs {}).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn tables_come_from_the_session_catalog() {
        let batch = tables(
            &ctx_with_table(),
            CommandGetTables {
                catalog: None,
                db_schema_filter_pattern: None,
                table_name_filter_pattern: None,
                table_types: vec![],
                include_schema: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn table_types_are_reported() {
        let batch = table_types(CommandGetTableTypes {}).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }
}
