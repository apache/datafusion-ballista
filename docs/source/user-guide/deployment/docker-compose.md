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

# Starting a Ballista Cluster using Docker Compose

Docker Compose is a convenient way to launch a cluster when testing locally.

## Build Docker Images

Run the following commands to download the [official Docker image](https://github.com/apache/datafusion-ballista/pkgs/container/datafusion-ballista-standalone):

```bash
docker pull ghcr.io/apache/datafusion-ballista-standalone:latest
```

Altenatively run the following commands to clone the source repository and build the Docker images from source:

```bash
git clone git@github.com:apache/datafusion-ballista.git -b latest
cd datafusion-ballista
./dev/build-ballista-docker.sh
```

This will create the following images:

- `apache/datafusion-ballista-benchmarks:latest`
- `apache/datafusion-ballista-cli:latest`
- `apache/datafusion-ballista-executor:latest`
- `apache/datafusion-ballista-scheduler:latest`
- `apache/datafusion-ballista-standalone:latest`

## Start a Cluster

Using the [docker-compose.yml](https://github.com/apache/datafusion-ballista/blob/main/docker-compose.yml) from the
source repository, run the following command to start a cluster:

```bash
docker-compose up --build
```

This should show output similar to the following:

```bash
$ docker-compose up
Creating network "ballista-benchmarks_default" with the default driver
Creating ballista-benchmarks_etcd_1 ... done
Creating ballista-benchmarks_ballista-scheduler_1 ... done
Creating ballista-benchmarks_ballista-executor_1  ... done
Attaching to ballista-benchmarks_etcd_1, ballista-benchmarks_ballista-scheduler_1, ballista-benchmarks_ballista-executor_1
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] Running with config:
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] work_dir: /tmp/.tmpLVx39c
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] concurrent_tasks: 4
ballista-scheduler_1  | [2021-08-28T15:55:22Z INFO  ballista_scheduler] Ballista v0.12.0 Scheduler listening on 0.0.0.0:50050
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] Ballista v0.12.0 Rust Executor listening on 0.0.0.0:50051
```

The scheduler listens on port 50050 and this is the port that clients will need to connect to.

## Connect from the Ballista CLI

```shell
docker run --network=host -it apache/datafusion-ballista-cli:latest --host localhost --port 50050
```
