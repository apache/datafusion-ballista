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

pub mod from_proto;

#[macro_export]
macro_rules! into_logical_plan {
    ($PB:expr, $CTX:expr, $CODEC:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into_logical_plan($CTX, $CODEC)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[cfg(test)]
mod roundtrip_tests {

    use super::super::{super::error::Result, protobuf};
    use crate::serde::{AsLogicalPlan, BallistaCodec};
    use async_trait::async_trait;
    use core::panic;
    use datafusion::common::DFSchemaRef;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::logical_plan::source_as_provider;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datafusion_data_access::{
            self,
            object_store::{FileMetaStream, ListEntryStream, ObjectReader, ObjectStore},
            SizedFile,
        },
        datasource::listing::ListingTable,
        logical_plan::{
            binary_expr, col, CreateExternalTable, Expr, FileType, LogicalPlan,
            LogicalPlanBuilder, Operator, Repartition, ToDFSchema,
        },
        prelude::*,
    };
    use std::io;
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestObjectStore {}

    #[async_trait]
    impl ObjectStore for TestObjectStore {
        async fn list_file(
            &self,
            _prefix: &str,
        ) -> datafusion_data_access::Result<FileMetaStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }

        async fn list_dir(
            &self,
            _prefix: &str,
            _delimiter: Option<String>,
        ) -> datafusion_data_access::Result<ListEntryStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }

        fn file_reader(
            &self,
            _file: SizedFile,
        ) -> datafusion_data_access::Result<Arc<dyn ObjectReader>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }
    }

    // Given a identity of a LogicalPlan converts it to protobuf and back, using debug formatting to test equality.
    macro_rules! roundtrip_test {
        ($initial_struct:ident, $proto_type:ty, $struct_type:ty) => {
            let proto: $proto_type = (&$initial_struct).try_into()?;

            let round_trip: $struct_type = (&proto).try_into()?;

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
        ($initial_struct:ident, $struct_type:ty) => {
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, $struct_type);
        };
        ($initial_struct:ident) => {
            let ctx = SessionContext::new();
            let codec: BallistaCodec<
                datafusion_proto::protobuf::LogicalPlanNode,
                protobuf::PhysicalPlanNode,
            > = BallistaCodec::default();
            let proto: datafusion_proto::protobuf::LogicalPlanNode =
                datafusion_proto::protobuf::LogicalPlanNode::try_from_logical_plan(
                    &$initial_struct,
                    codec.logical_extension_codec(),
                )
                .expect("from logical plan");
            let round_trip: LogicalPlan = proto
                .try_into_logical_plan(&ctx, codec.logical_extension_codec())
                .expect("to logical plan");

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
        ($initial_struct:ident, $ctx:ident) => {
            let codec: BallistaCodec<
                protobuf::LogicalPlanNode,
                protobuf::PhysicalPlanNode,
            > = BallistaCodec::default();
            let proto: datafusion_proto::protobuf::LogicalPlanNode =
                protobuf::LogicalPlanNode::try_from_logical_plan(&$initial_struct)
                    .expect("from logical plan");
            let round_trip: LogicalPlan = proto
                .try_into_logical_plan(&$ctx, codec.logical_extension_codec())
                .expect("to logical plan");

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
    }

    #[tokio::test]
    async fn roundtrip_repartition() -> Result<()> {
        use datafusion::logical_plan::Partitioning;

        let test_partition_counts = [usize::MIN, usize::MAX, 43256];

        let test_expr: Vec<Expr> =
            vec![col("c1") + col("c2"), Expr::Literal((4.0).into())];

        let plan = std::sync::Arc::new(
            test_scan_csv("employee", Some(vec![3, 4]))
                .await?
                .sort(vec![col("salary")])?
                .build()?,
        );

        for partition_count in test_partition_counts.iter() {
            let rr_repartition = Partitioning::RoundRobinBatch(*partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: rr_repartition,
            });

            roundtrip_test!(roundtrip_plan);

            let h_repartition = Partitioning::Hash(test_expr.clone(), *partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: h_repartition,
            });

            roundtrip_test!(roundtrip_plan);

            let no_expr_hrepartition = Partitioning::Hash(Vec::new(), *partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: no_expr_hrepartition,
            });

            roundtrip_test!(roundtrip_plan);
        }

        Ok(())
    }

    #[test]
    fn roundtrip_create_external_table() -> Result<()> {
        let schema = test_schema();

        let df_schema_ref = schema.to_dfschema_ref()?;

        let filetypes: [FileType; 4] = [
            FileType::NdJson,
            FileType::Parquet,
            FileType::CSV,
            FileType::Avro,
        ];

        for file in filetypes.iter() {
            let create_table_node =
                LogicalPlan::CreateExternalTable(CreateExternalTable {
                    schema: df_schema_ref.clone(),
                    name: String::from("TestName"),
                    location: String::from("employee.csv"),
                    file_type: *file,
                    has_header: true,
                    delimiter: ',',
                    table_partition_cols: vec![],
                    if_not_exists: false,
                });

            roundtrip_test!(create_table_node);
        }

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_analyze() -> Result<()> {
        let verbose_plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .sort(vec![col("salary")])?
            .explain(true, true)?
            .build()?;

        let plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .sort(vec![col("salary")])?
            .explain(false, true)?
            .build()?;

        roundtrip_test!(plan);

        roundtrip_test!(verbose_plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_explain() -> Result<()> {
        let verbose_plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .sort(vec![col("salary")])?
            .explain(true, false)?
            .build()?;

        let plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .sort(vec![col("salary")])?
            .explain(false, false)?
            .build()?;

        roundtrip_test!(plan);

        roundtrip_test!(verbose_plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_join() -> Result<()> {
        let scan_plan = test_scan_csv("employee1", Some(vec![0, 3, 4]))
            .await?
            .build()?;
        let filter = binary_expr(col("employee1.x"), Operator::Gt, col("employee2.y"));

        let plan = test_scan_csv("employee2", Some(vec![0, 3, 4]))
            .await?
            .join(
                &scan_plan,
                JoinType::Inner,
                (vec!["id"], vec!["id"]),
                Some(filter),
            )?
            .build()?;

        roundtrip_test!(plan);
        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_sort() -> Result<()> {
        let plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .sort(vec![col("salary")])?
            .build()?;
        roundtrip_test!(plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_empty_relation() -> Result<()> {
        let plan_false = LogicalPlanBuilder::empty(false).build()?;

        roundtrip_test!(plan_false);

        let plan_true = LogicalPlanBuilder::empty(true).build()?;

        roundtrip_test!(plan_true);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan() -> Result<()> {
        let plan = test_scan_csv("employee", Some(vec![3, 4]))
            .await?
            .aggregate(vec![col("state")], vec![max(col("salary"))])?
            .build()?;

        roundtrip_test!(plan);

        Ok(())
    }

    #[ignore] // see https://github.com/apache/arrow-datafusion/issues/2546
    #[tokio::test]
    async fn roundtrip_logical_plan_custom_ctx() -> Result<()> {
        let ctx = SessionContext::new();
        let codec: BallistaCodec<
            datafusion_proto::protobuf::LogicalPlanNode,
            protobuf::PhysicalPlanNode,
        > = BallistaCodec::default();
        let custom_object_store = Arc::new(TestObjectStore {});
        ctx.runtime_env()
            .register_object_store("test", custom_object_store.clone());

        let table_path = "test:///employee.csv";
        let url = ListingTableUrl::parse(table_path).unwrap();
        let os = ctx.runtime_env().object_store(&url)?;
        assert_eq!("TestObjectStore", &format!("{:?}", os));
        assert_eq!(table_path, &url.to_string());

        let schema = test_schema();
        let plan = ctx
            .read_csv(
                table_path,
                CsvReadOptions::new().schema(&schema).has_header(true),
            )
            .await?
            .to_logical_plan()?;

        let proto: datafusion_proto::protobuf::LogicalPlanNode =
            datafusion_proto::protobuf::LogicalPlanNode::try_from_logical_plan(
                &plan,
                codec.logical_extension_codec(),
            )
            .expect("from logical plan");
        let round_trip: LogicalPlan = proto
            .try_into_logical_plan(&ctx, codec.logical_extension_codec())
            .expect("to logical plan");

        assert_eq!(format!("{:?}", plan), format!("{:?}", round_trip));

        let table_path = match round_trip {
            LogicalPlan::TableScan(scan) => {
                let source = source_as_provider(&scan.source)?;
                match source.as_ref().as_any().downcast_ref::<ListingTable>() {
                    Some(listing_table) => listing_table.table_path().clone(),
                    _ => panic!("expected a ListingTable"),
                }
            }
            _ => panic!("expected a TableScan"),
        };

        assert_eq!(table_path.as_str(), url.as_str());

        Ok(())
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    async fn test_scan_csv(
        table_name: &str,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let options = CsvReadOptions::new().schema(&schema);

        let uri = format!("file:///{}.csv", table_name);
        ctx.register_csv(table_name, &uri, options).await?;

        let df = ctx.table(table_name)?;
        let plan = match df.to_logical_plan()? {
            LogicalPlan::TableScan(ref scan) => {
                let mut scan = scan.clone();
                scan.projection = projection;
                let mut projected_schema = scan.projected_schema.as_ref().clone();
                projected_schema = projected_schema.replace_qualifier(table_name);
                scan.projected_schema = DFSchemaRef::new(projected_schema);
                LogicalPlan::TableScan(scan)
            }
            _ => unimplemented!(),
        };
        Ok(LogicalPlanBuilder::from(plan))
    }
}
