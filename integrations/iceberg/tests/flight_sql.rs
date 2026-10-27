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

//! Queries Iceberg tables through a scheduler's Flight SQL frontend, as a SQL
//! client with no catalog of its own does.

mod common;

#[cfg(test)]
mod flight_sql {
    use std::error::Error;
    use std::sync::Arc;

    use arrow_flight::sql::client::FlightSqlServiceClient;
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
    use ballista_core::utils::default_config_producer;
    use ballista_iceberg::{
        IcebergLogicalCodec, IcebergPhysicalCodec, iceberg_session_builder,
    };
    use ballista_scheduler::cluster::BallistaCluster;
    use ballista_scheduler::config::SchedulerConfig;
    use ballista_scheduler::metrics::default_metrics_collector;
    use ballista_scheduler::scheduler_process::start_grpc_service_with_listener;
    use ballista_scheduler::scheduler_server::SchedulerServer;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use futures::TryStreamExt;
    use iceberg_datafusion::IcebergCatalogProvider;
    use tonic::transport::Channel;

    use crate::common::{insert, memory_catalog};

    type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

    #[tokio::test]
    async fn should_query_scheduler_catalogs_from_a_sql_client() -> Result {
        let dir = tempfile::tempdir()?;
        let catalog = memory_catalog(dir.path()).await;
        insert(&catalog, "(1, 'a'), (2, NULL)").await;
        let mount = IcebergCatalogProvider::try_new(catalog.clone()).await?;
        let mut client =
            start_cluster(vec![("iceberg".to_string(), Arc::new(mount) as _)]).await?;

        let query = "SELECT * FROM iceberg.ns.t ORDER BY id";
        let expected = [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "| 2  |      |",
            "+----+------+",
        ];
        assert_batches_eq!(expected, &run(&mut client, query).await?);

        insert(&catalog, "(3, 'c')").await;
        let expected = [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "| 2  |      |",
            "| 3  | c    |",
            "+----+------+",
        ];
        assert_batches_eq!(expected, &run(&mut client, query).await?);

        Ok(())
    }

    /// Starts a scheduler serving Flight SQL with `catalogs` mounted, plus one
    /// executor, and returns a Flight SQL client connected to it.
    async fn start_cluster(
        catalogs: Vec<(String, Arc<dyn datafusion::catalog::CatalogProvider>)>,
    ) -> Result<FlightSqlServiceClient<Channel>> {
        let codec = || {
            BallistaCodec::new(
                Arc::new(IcebergLogicalCodec::default()),
                Arc::new(IcebergPhysicalCodec::default()),
            )
        };
        let cluster = BallistaCluster::new_memory(
            "localhost:50050",
            Arc::new(iceberg_session_builder(catalogs)),
            Arc::new(default_config_producer),
        );
        let config = SchedulerConfig::default()
            .with_scheduler_policy(TaskSchedulingPolicy::PullStaged)
            .with_flight_sql(true);
        let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
            SchedulerServer::new(
                "localhost:50050".to_owned(),
                cluster,
                codec(),
                Arc::new(config),
                default_metrics_collector()?,
            );
        scheduler.init().await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        tokio::spawn(start_grpc_service_with_listener(listener, scheduler));

        let channel = Channel::from_shared(url)?.connect().await?;
        ballista_executor::new_standalone_executor(
            SchedulerGrpcClient::new(channel.clone()),
            2,
            codec(),
        )
        .await?;
        let mut client = FlightSqlServiceClient::new(channel);
        client.handshake("anonymous", "").await?;
        Ok(client)
    }

    async fn run(
        client: &mut FlightSqlServiceClient<Channel>,
        query: &str,
    ) -> Result<Vec<RecordBatch>> {
        let info = client.execute(query.to_string(), None).await?;
        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint.ticket.ok_or("endpoint has no ticket")?;
            batches.extend(client.do_get(ticket).await?.try_collect::<Vec<_>>().await?);
        }
        Ok(batches)
    }
}
