# Ballista Tuning Guide

The goal of any distributed compute engine is to parallelize work as much as possible, allowing the work to scale by adding more compute resource.

The basic unit of concurrency and parallelism in Ballista is the concept of a partition. The leaf nodes of a query 
are typically table scans that read from files on disk and Ballista currently treats each file within a table as a 
single partition (in the future, Ballista will support splitting files into partitions but this is not implemented yet). 
For example, if there is a table "customer" that consists of 200 Parquet files, that table scan will naturally have 
200 partitions and the table scan and certain subsequent operations will also have 200 partitions. Conversely, if the 
table only has a single Parquet file then there will be a single partition and the work will not be able to scale even 
if the cluster has resource available. Ballista supports repartitioning within a query to improve parallelism. 
The configuration setting `ballista.shuffle.partitions`can be set to the desired number of partitions. This is 
currently a global setting for the entire context. The default value for this setting is 2.

Example: Setting the desired number of shuffle partitions when creating a context.

```rust
let config = BallistaConfig::builder()
    .set("ballista.shuffle.partitions", "200")
    .build()?;

let ctx = BallistaContext::remote("localhost", 50050, &config).await?;
```







