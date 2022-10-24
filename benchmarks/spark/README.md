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

# Spark Benchmarks

These benchmarks exist so that we can measure relative performance between Ballista and
Apache Spark

## Pre-requisites

- Download Apache Maven from https://maven.apache.org/download.cgi
- Download Apache Spark 3.3.0 from https://spark.apache.org/downloads.html

Untar these downloads and set `MAVEN_HOME` and `SPARK_HOME` environment variables to point to the
install location.

## Build the benchmark JAR file

```bash
$MAVEN_HOME/bin/mvn package
```

## Generating TPC-H data

Use https://crates.io/crates/tpctools

## Converting TPC-H data from CSV to Parquet

```bash
$SPARK_HOME/bin/spark-submit --master spark://localhost:7077 \
    --class org.apache.arrow.SparkTpch \
    --conf spark.driver.memory=8G \
    --num-executors=1 \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    --conf spark.cores.max=24 \
    target/spark-tpch-0.5.0-SNAPSHOT-jar-with-dependencies.jar \
    convert-tpch \
    --input-path /mnt/bigdata/tpch/sf10-csv/ \
    --input-format tbl \
    --output-path /mnt/bigdata/tpch/sf10-parquet/ \
    --output-format parquet \
    --partitions 24
```

## Submit the benchmark application to the cluster

```bash
$SPARK_HOME/bin/spark-submit --master spark://localhost:7077 \
    --class org.apache.arrow.SparkTpch \
    --conf spark.driver.memory=8G \
    --num-executors=1 \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    --conf spark.cores.max=24 \
    target/spark-tpch-0.5.0-SNAPSHOT-jar-with-dependencies.jar \
    tpch \
    --input-path /mnt/bigdata/tpch/sf10-parquet-float/ \
    --input-format parquet \
    --query-path /path/to/arrow-ballista/benchmarks/queries \
    --query 1
```

# Standalone Mode

## Start a local Spark cluster in standalone mode

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://localhost:7077
```

Monitor progress via the Spark UI at http://localhost:8080

## Shut down the cluster

```bash
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```
