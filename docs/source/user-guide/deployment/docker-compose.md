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

# Starting a kapot Cluster using Docker Compose

Docker Compose is a convenient way to launch a cluster when testing locally.

## Build Docker Images

Run the following commands to download the [official Docker image](https://github.com/apache/datafusion-kapot/pkgs/container/datafusion-kapot-standalone):

```bash
docker pull ghcr.io/apache/datafusion-kapot-standalone:0.12.0-rc4
```

Altenatively run the following commands to clone the source repository and build the Docker images from source:

```bash
git clone git@github.com:apache/datafusion-kapot.git -b 0.12.0
cd datafusion-kapot
./dev/build-kapot-docker.sh
```

This will create the following images:

- `apache/datafusion-kapot-benchmarks:0.12.0`
- `apache/datafusion-kapot-cli:0.12.0`
- `apache/datafusion-kapot-executor:0.12.0`
- `apache/datafusion-kapot-scheduler:0.12.0`
- `apache/datafusion-kapot-standalone:0.12.0`

## Start a Cluster

Using the [docker-compose.yml](https://github.com/apache/datafusion-kapot/blob/main/docker-compose.yml) from the
source repository, run the following command to start a cluster:

```bash
docker-compose up --build
```

This should show output similar to the following:

```bash
$ docker-compose up
Creating network "kapot-benchmarks_default" with the default driver
Creating kapot-benchmarks_etcd_1 ... done
Creating kapot-benchmarks_kapot-scheduler_1 ... done
Creating kapot-benchmarks_kapot-executor_1  ... done
Attaching to kapot-benchmarks_etcd_1, kapot-benchmarks_kapot-scheduler_1, kapot-benchmarks_kapot-executor_1
kapot-executor_1   | [2021-08-28T15:55:22Z INFO  kapot_executor] Running with config:
kapot-executor_1   | [2021-08-28T15:55:22Z INFO  kapot_executor] work_dir: /tmp/.tmpLVx39c
kapot-executor_1   | [2021-08-28T15:55:22Z INFO  kapot_executor] concurrent_tasks: 4
kapot-scheduler_1  | [2021-08-28T15:55:22Z INFO  kapot_scheduler] kapot v0.12.0 Scheduler listening on 0.0.0.0:50050
kapot-executor_1   | [2021-08-28T15:55:22Z INFO  kapot_executor] kapot v0.12.0 Rust Executor listening on 0.0.0.0:50051
```

The scheduler listens on port 50050 and this is the port that clients will need to connect to.

The scheduler web UI is available on port 80 in the scheduler.

## Connect from the kapot CLI

```shell
docker run --network=host -it apache/datafusion-kapot-cli:0.12.0 --host localhost --port 50050
```
