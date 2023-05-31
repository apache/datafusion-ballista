#[cfg(test)]
mod proto;
#[cfg(test)]
mod test_logical_codec;
#[cfg(test)]
mod test_physical_codec;
#[cfg(test)]
mod test_table;
#[cfg(test)]
mod test_table_exec;

#[cfg(test)]
mod tests {

    use std::{convert::TryInto, net::SocketAddr, path::PathBuf, sync::Arc, vec};

    use anyhow::Context;
    use ballista_core::{
        serde::{
            protobuf::{
                execute_query_params::Query, executor_registration::OptionalHost,
                executor_resource::Resource, scheduler_grpc_client::SchedulerGrpcClient,
                ExecuteQueryParams, ExecutorRegistration, ExecutorResource,
                ExecutorSpecification, GetJobStatusParams, JobStatus, SuccessfulJob,
            },
            BallistaCodec,
        },
        utils::create_grpc_client_connection,
    };
    use ballista_executor::{
        execution_loop,
        executor::Executor,
        executor_server::{self, ServerHandle},
        metrics::LoggingMetricsCollector,
        shutdown::ShutdownNotifier,
    };
    use ballista_scheduler::standalone::new_standalone_scheduler_with_codec;
    use datafusion::{
        common::DFSchema,
        config::Extensions,
        datasource::{provider_as_source, TableProvider},
        execution::runtime_env::{RuntimeConfig, RuntimeEnv},
        logical_expr::{LogicalPlan, TableScan},
        sql::TableReference,
    };
    use datafusion_proto::{
        logical_plan::{AsLogicalPlan, LogicalExtensionCodec},
        physical_plan::PhysicalExtensionCodec,
        protobuf::LogicalPlanNode,
    };
    use tokio::{
        sync::mpsc,
        time::{sleep, Duration},
    };
    use tonic::transport::Channel;

    use crate::{
        test_logical_codec::TestLogicalCodec, test_physical_codec::TestPhysicalCodec,
        test_table::TestTable,
    };

    use ballista_core::serde::protobuf::job_status::Status;

    lazy_static::lazy_static! {
        pub static ref WORKSPACE_DIR: PathBuf =
                PathBuf::from(std::env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    }

    #[tokio::test]
    async fn test_global_limit() {
        env_logger::init();

        let row_limit = 100;

        let logical_codec = Arc::new(TestLogicalCodec::new());
        let physical_codec = Arc::new(TestPhysicalCodec::new());

        let scheduler_socket =
            start_scheduler_local(logical_codec.clone(), physical_codec.clone()).await;

        let scheduler_url = format!("http://localhost:{}", scheduler_socket.port());

        let connection = create_grpc_client_connection(scheduler_url.clone())
            .await
            .context("Connecting to scheduler")
            .unwrap();

        let mut scheduler_client = SchedulerGrpcClient::new(connection);

        let codec = BallistaCodec::new(logical_codec.clone(), physical_codec.clone());

        let shutdown_not = Arc::new(ShutdownNotifier::new());

        let executors = start_executors_local(
            2,
            scheduler_client.clone(),
            codec,
            shutdown_not.clone(),
        )
        .await;

        let test_table = Arc::new(TestTable::new(2, row_limit));

        let reference: TableReference = "test_table".into();
        let schema = DFSchema::try_from_qualified_schema(
            reference.clone(),
            test_table.schema().as_ref(),
        )
        .context("Creating schema")
        .unwrap();

        let test_data = TableScan {
            table_name: reference,
            source: provider_as_source(test_table),
            fetch: None,
            filters: vec![],
            projection: None,
            projected_schema: Arc::new(schema),
        };

        let node = LogicalPlan::TableScan(test_data);

        let mut buf = vec![];

        LogicalPlanNode::try_from_logical_plan(&node, logical_codec.as_ref())
            .context("Converting to protobuf")
            .unwrap()
            .try_encode(&mut buf)
            .context("Encoding protobuf")
            .unwrap();

        let request = ExecuteQueryParams {
            settings: vec![],
            optional_session_id: None,
            query: Some(Query::LogicalPlan(buf)),
        };

        let result = scheduler_client
            .execute_query(request)
            .await
            .context("Executing query")
            .unwrap();

        let job_id = result.into_inner().job_id;

        let successful_job = await_job_completion(&mut scheduler_client, &job_id)
            .await
            .unwrap();

        assert!(successful_job.circuit_breaker_tripped);

        let partitions = successful_job.partition_location;

        assert_eq!(partitions.len(), 2);

        let partition1 = &partitions[0];
        let partition2 = &partitions[1];

        let num_rows1 = partition1
            .partition_stats
            .clone()
            .context("Get partition stats")
            .unwrap()
            .num_rows;

        let num_rows2 = partition2
            .partition_stats
            .clone()
            .context("Get partition stats")
            .unwrap()
            .num_rows;

        println!("num_rows1: {}", num_rows1);
        println!("num_rows2: {}", num_rows2);

        assert!(num_rows1 + num_rows2 > row_limit.try_into().unwrap());
        assert!(num_rows1 + num_rows2 < 1000);

        shutdown_not.notify_shutdown.send(()).unwrap();

        for (_, handle) in executors {
            handle
                .await
                .context("Waiting for executor to shutdown")
                .unwrap()
                .context("Waiting for executor to shutdown 2")
                .unwrap();
        }
    }

    async fn await_job_completion(
        scheduler_client: &mut SchedulerGrpcClient<Channel>,
        job_id: &str,
    ) -> Result<SuccessfulJob, String> {
        let mut job_status = scheduler_client
            .get_job_status(GetJobStatusParams {
                job_id: job_id.to_owned(),
            })
            .await
            .map_err(|e| format!("Job status request for job {} failed: {}", job_id, e))?
            .into_inner();

        let mut attempts = 60;

        while attempts > 0 {
            sleep(Duration::from_millis(1000)).await;

            if let Some(JobStatus {
                job_id: _,
                job_name: _,
                status: Some(status),
            }) = &job_status.status
            {
                match status {
                    Status::Failed(failed_job) => {
                        return Err(format!(
                            "Job {} failed: {:?}",
                            job_id, failed_job.error
                        ));
                    }
                    Status::Successful(s) => {
                        return Ok(s.clone());
                    }
                    _ => {}
                }
            }

            attempts -= 1;

            job_status = scheduler_client
                .get_job_status(GetJobStatusParams {
                    job_id: job_id.to_owned(),
                })
                .await
                .map_err(|e| {
                    format!("Job status request for job {} failed: {}", job_id, e)
                })?
                .into_inner();
        }

        Err(format!(
            "Job {} did not complete, last status: {:?}",
            job_id, job_status.status
        ))
    }

    async fn start_scheduler_local(
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        physical_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SocketAddr {
        new_standalone_scheduler_with_codec(physical_codec, logical_codec)
            .await
            .context("Starting scheduler process")
            .unwrap()
    }

    async fn start_executors_local(
        n: usize,
        scheduler: SchedulerGrpcClient<Channel>,
        codec: BallistaCodec,
        shutdown_not: Arc<ShutdownNotifier>,
    ) -> Vec<(String, ServerHandle)> {
        let work_dir = "/tmp";
        let cfg = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(cfg).unwrap());

        let mut handles = Vec::with_capacity(n);

        for i in 0..n {
            let specification = ExecutorSpecification {
                resources: vec![ExecutorResource {
                    resource: Some(Resource::TaskSlots(1)),
                }],
            };

            let port = 50051 + i as u32;
            let grpc_port = (50052 + n + i) as u32;

            let metadata = ExecutorRegistration {
                id: format!("executor-{}", i),
                port,
                grpc_port,
                specification: Some(specification),
                optional_host: Some(OptionalHost::Host("localhost".to_owned())),
            };

            let metrics_collector = Arc::new(LoggingMetricsCollector {});

            let concurrent_tasks = 1;

            let execution_engine = None;

            let executor = Executor::new(
                metadata,
                work_dir,
                runtime.clone(),
                metrics_collector,
                concurrent_tasks,
                execution_engine,
            );

            let stop_send = mpsc::channel(1).0;

            let executor = Arc::new(executor);

            let handle = executor_server::startup(
                scheduler.clone(),
                "0.0.0.0".to_owned(),
                executor.clone(),
                codec.clone(),
                stop_send,
                shutdown_not.as_ref(),
                Extensions::new(),
            )
            .await
            .context(format!("Starting executor {}", i))
            .unwrap();

            handles.push((format!("executor {} process", i), handle));

            // Don't save the handle as this one cannot be interrupted and just has to be dropped
            tokio::spawn(execution_loop::poll_loop(
                scheduler.clone(),
                executor,
                codec.clone(),
                Extensions::default(),
            ));
        }

        handles
    }
}
