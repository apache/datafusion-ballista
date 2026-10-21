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

//! Encodes and decodes plans with the Iceberg codecs, as Ballista does between
//! client, scheduler and executors.

mod common;

#[cfg(test)]
mod iceberg_nodes {
    use std::sync::Arc;

    use datafusion::assert_batches_eq;
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Result, TableReference};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionContext, col, lit};
    use datafusion_proto::logical_plan::LogicalExtensionCodec;
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    };
    use iceberg_ballista::{IcebergLogicalCodec, IcebergPhysicalCodec};
    use iceberg_datafusion::IcebergStaticTableProvider;
    use iceberg_datafusion::read::IcebergDataSource;

    use crate::common::{catalog_table, insert, memory_catalog};

    #[tokio::test]
    async fn should_keep_scan_snapshot_projection_filter_and_fetch() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let catalog = memory_catalog(dir.path()).await;
        let first = insert(&catalog, "(1, 'a'), (2, 'b'), (3, 'c')").await;
        let first = first.metadata().current_snapshot_id().unwrap();
        let table = insert(&catalog, "(4, 'd')").await;
        let provider =
            IcebergStaticTableProvider::try_new_from_table_snapshot(table, first).await?;
        let ctx = SessionContext::new();
        let filters = [col("id").gt(lit(1))];
        let scan = provider
            .scan(&ctx.state(), Some(&vec![1]), &filters, Some(1))
            .await?;

        let codec = IcebergPhysicalCodec::default();
        let mut buf = Vec::new();
        codec.try_encode(scan, &mut buf, &DefaultPhysicalProtoConverter {})?;
        let decoded = codec.try_decode(
            &buf,
            &[],
            &ctx.task_ctx(),
            &DefaultPhysicalProtoConverter {},
        )?;

        let source = decoded
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<IcebergDataSource>())
            .unwrap();
        assert_eq!(source.snapshot_id(), Some(first));
        assert_eq!(
            displayable(decoded.as_ref()).one_line().to_string(),
            "DataSourceExec: format=iceberg, projection=[name], predicate=id > 1, fetch=1\n"
        );
        let result = collect(decoded, ctx.task_ctx()).await?;
        let expected = ["+------+", "| name |", "+------+", "| b    |", "+------+"];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    // Multi-threaded: encoding a catalog-backed table loads it.
    #[tokio::test(flavor = "multi_thread")]
    async fn should_pin_catalog_table_when_encoded() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let catalog = memory_catalog(dir.path()).await;
        let provider = catalog_table(&catalog).await;
        let schema = provider.schema();
        let codec = IcebergLogicalCodec::default();
        let table = TableReference::bare("t");

        let pinned = insert(&catalog, "(1, 'a')").await;
        let mut buf = Vec::new();
        codec.try_encode_table_provider(&table, Arc::new(provider), &mut buf)?;
        insert(&catalog, "(2, 'b')").await;
        // Decoding uses the metadata the client sent, not the file.
        std::fs::remove_file(pinned.metadata_location().unwrap())?;
        let decoded = codec.try_decode_table_provider(
            &buf,
            &table,
            schema.clone(),
            &TaskContext::default(),
        )?;

        let ctx = SessionContext::new();
        let input = Arc::new(EmptyExec::new(schema));
        let err = decoded
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "This feature is not implemented: INSERT INTO an Iceberg table isn't \
             supported on Ballista yet"
        );

        ctx.register_table("t", decoded)?;
        let result = ctx.sql("SELECT * FROM t").await?.collect().await?;
        let expected = [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "+----+------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_refuse_catalog_table_on_single_threaded_runtime() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let provider = catalog_table(&memory_catalog(dir.path()).await).await;

        let err = IcebergLogicalCodec::default()
            .try_encode_table_provider(
                &TableReference::bare("t"),
                Arc::new(provider),
                &mut Vec::new(),
            )
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Execution error: Iceberg table t is catalog-backed, so sending the query \
             loads it, which can hang on a single-threaded tokio runtime; run the \
             client on a multi-threaded runtime, or register the table as an \
             IcebergStaticTableProvider"
        );

        Ok(())
    }
}

#[cfg(test)]
mod other_nodes {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Result, TableReference};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::{Extension, LogicalPlan};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_proto::logical_plan::LogicalExtensionCodec;
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
        PhysicalProtoConverterExtension,
    };
    use iceberg_ballista::{IcebergLogicalCodec, IcebergPhysicalCodec};

    /// An inner codec that encodes anything as `inner`, and decodes only that.
    #[derive(Debug)]
    struct Inner;

    impl PhysicalExtensionCodec for Inner {
        fn try_decode(
            &self,
            buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(buf, b"inner");
            Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            buf.extend_from_slice(b"inner");
            Ok(())
        }
    }

    impl LogicalExtensionCodec for Inner {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> Result<Extension> {
            unreachable!()
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
            unreachable!()
        }

        fn try_decode_table_provider(
            &self,
            buf: &[u8],
            _table_ref: &TableReference,
            schema: Arc<Schema>,
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn TableProvider>> {
            assert_eq!(buf, b"inner");
            Ok(Arc::new(EmptyTable::new(schema)))
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            _node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            buf.extend_from_slice(b"inner");
            Ok(())
        }
    }

    #[test]
    fn should_delegate_non_iceberg_nodes_to_inner_codec() -> Result<()> {
        let ctx = TaskContext::default();
        let schema = Arc::new(Schema::empty());

        let physical = IcebergPhysicalCodec::new(Arc::new(Inner));
        let mut buf = Vec::new();
        let plan = Arc::new(EmptyExec::new(schema.clone()));
        physical.try_encode(plan, &mut buf, &DefaultPhysicalProtoConverter {})?;
        assert_eq!(buf, b"\0inner");
        let decoded =
            physical.try_decode(&buf, &[], &ctx, &DefaultPhysicalProtoConverter {})?;
        assert_eq!(
            displayable(decoded.as_ref()).one_line().to_string(),
            "EmptyExec\n"
        );

        let logical = IcebergLogicalCodec::new(Arc::new(Inner));
        let table = TableReference::bare("t");
        let mut buf = Vec::new();
        let provider = Arc::new(EmptyTable::new(schema.clone()));
        logical.try_encode_table_provider(&table, provider, &mut buf)?;
        assert_eq!(buf, b"\0inner");
        let decoded =
            logical.try_decode_table_provider(&buf, &table, schema.clone(), &ctx)?;
        assert_eq!(
            format!("{decoded:?}"),
            format!("{:?}", EmptyTable::new(schema))
        );

        Ok(())
    }

    #[test]
    fn should_reject_malformed_buffers() {
        let codec = IcebergPhysicalCodec::new(Arc::new(Inner));
        let decode = |buf: &[u8]| {
            codec
                .try_decode(
                    buf,
                    &[],
                    &TaskContext::default(),
                    &DefaultPhysicalProtoConverter {},
                )
                .unwrap_err()
                .to_string()
        };

        assert_eq!(
            decode(b""),
            "Execution error: empty iceberg-ballista codec buffer"
        );
        assert_eq!(
            decode(b"\x07inner"),
            "Execution error: unknown iceberg-ballista codec tag 7: was the plan \
             encoded by another version of iceberg-ballista?"
        );
        assert_eq!(
            decode(b"\x01inner"),
            "Execution error: invalid iceberg-ballista payload: expected value at line 1 \
             column 1"
        );
    }
}
