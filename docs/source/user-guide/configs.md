<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Configuration

Ballista has a number of configuration settings that can be specified when creating a BallistaContext.

_Example: Specifying configuration options when creating a context_

```rust
let config = BallistaConfig::builder()
    .set("ballista.shuffle.partitions", "200")
    .set("ballista.batch.size", "16384")
    .build()?;

let ctx = BallistaContext::remote("localhost", 50050, &config).await?;
```

## Ballista Configuration Settings

| key                               | type    | default | description                                                                                                                                                               |
| --------------------------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ballista.shuffle.partitions       | UInt16  | 2       | Sets the default number of partitions to create when repartitioning query stages.                                                                                         |
| ballista.batch.size               | UInt16  | 8192    | Sets the default batch size.                                                                                                                                              |
| ballista.repartition.joins        | Boolean | true    | When set to true, Ballista will repartition data using the join keys to execute joins in parallel using the provided `ballista.shuffle.partitions` level.                 |
| ballista.repartition.aggregations | Boolean | true    | When set to true, Ballista will repartition data using the aggregate keys to execute aggregates in parallel using the provided `ballista.shuffle.partitions` level.       |
| ballista.repartition.windows      | Boolean | true    | When set to true, Ballista will repartition data using the partition keys to execute window functions in parallel using the provided `ballista.shuffle.partitions` level. |
| ballista.parquet.pruning          | Boolean | true    | Determines whether Parquet pruning should be enabled or not.                                                                                                              |
| ballista.with_information_schema  | Boolean | true    | Determines whether the `information_schema` should be created in the context. This is necessary for supporting DDL commands such as `SHOW TABLES`.                        |
| ballista.plugin_dir               | Boolean | true    | Specified a path for plugin files. Dynamic library files in this directory will be loaded when scheduler state initializes.                                               |

## DataFusion Configuration Settings

In addition to Ballista-specific configuration settings, the following DataFusion settings can also be specified.

| key                                             | type    | default | description                                                                                                                                                                                                                                                                                                                                                   |
| ----------------------------------------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| datafusion.execution.coalesce_batches           | Boolean | true    | When set to true, record batches will be examined between each operator and small batches will be coalesced into larger batches. This is helpful when there are highly selective filters or joins that could produce tiny output batches. The target batch size is determined by the configuration setting 'datafusion.execution.coalesce_target_batch_size'. |
| datafusion.execution.coalesce_target_batch_size | UInt64  | 4096    | Target batch size when coalescing batches. Uses in conjunction with the configuration setting 'datafusion.execution.coalesce_batches'.                                                                                                                                                                                                                        |
| datafusion.explain.logical_plan_only            | Boolean | false   | When set to true, the explain statement will only print logical plans.                                                                                                                                                                                                                                                                                        |
| datafusion.explain.physical_plan_only           | Boolean | false   | When set to true, the explain statement will only print physical plans.                                                                                                                                                                                                                                                                                       |
| datafusion.optimizer.filter_null_join_keys      | Boolean | false   | When set to true, the optimizer will insert filters before a join between a nullable and non-nullable column to filter out nulls on the nullable side. This filter can add additional overhead when the file format does not fully support predicate push down.                                                                                               |
| datafusion.optimizer.skip_failed_rules          | Boolean | true    | When set to true, the logical plan optimizer will produce warning messages if any optimization rules produce errors and then proceed to the next rule. When set to false, any rules that produce errors will cause the query to fail.                                                                                                                         |
