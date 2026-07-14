# ./ballista/client/testdata/alltypes_plain.parquet



# let config = SessionConfig::new_with_ballista()
# .with_target_partitions(1)
# .with_ballista_standalone_parallelism(2);
# 
# let state = SessionStateBuilder::new()
# .with_config(config)
# .with_default_features()
# .build();
# 
# let ctx = SessionContext::standalone_with_state(state).await?;
# 
# let test_data = test_util::examples_test_data();
# 
# // register parquet file with the execution context
# ctx.register_parquet(
#     "test",
# &format!("{test_data}/alltypes_plain.parquet"),
# ParquetReadOptions::default(),
# )
# .await?;
# 
# let df = ctx.sql("select count(1) from test").await?;
# 
# df.show().await?;