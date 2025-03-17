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

# Starting a Ballista Cluster using Docker

## Build Docker Images

Run the following commands to download the [official Docker image](https://github.com/apache/datafusion-ballista/pkgs/container/datafusion-ballista-standalone):

```bash
docker pull ghcr.io/apache/datafusion-ballista-standalone:latest
```

Altenatively run the following commands to clone the source repository and build the Docker images from source:

```bash
git clone git@github.com:apache/datafusion-ballista.git
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

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
 -d apache/datafusion-ballista-scheduler:latest \
 --bind-port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
a756055576f3   apache/datafusion-ballista-scheduler:latest   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs a756055576f3
2024-02-03T14:49:47.904571Z  INFO main ThreadId(01) ballista_scheduler::cluster: Initializing Sled database in temp directory

2024-02-03T14:49:47.924679Z  INFO main ThreadId(01) ballista_scheduler::scheduler_process: Ballista v0.12.0 Scheduler listening on 0.0.0.0:50050
2024-02-03T14:49:47.924709Z  INFO main ThreadId(01) ballista_scheduler::scheduler_process: Starting Scheduler grpc server with task scheduling policy of PullStaged
2024-02-03T14:49:47.925261Z  INFO main ThreadId(01) ballista_scheduler::cluster::kv: Initializing heartbeat listener
2024-02-03T14:49:47.925476Z  INFO main ThreadId(01) ballista_scheduler::scheduler_server::query_stage_scheduler: Starting QueryStageScheduler
2024-02-03T14:49:47.925587Z  INFO tokio-runtime-worker ThreadId(47) ballista_core::event_loop: Starting the event loop query_stage
```

### Start Executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d apache/datafusion-ballista-executor:latest \
  --external-host localhost --bind-port 50051
```

Use `docker ps` to check that both the scheduler and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
fb8b530cee6d   apache/datafusion-ballista-executor:latest    "/root/executor-entr…"   2 seconds ago   Up 1 second              gallant_galois
a756055576f3   apache/datafusion-ballista-scheduler:latest   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs fb8b530cee6d
2024-02-03T14:50:24.061607Z  INFO main ThreadId(01) ballista_executor::executor_process: Running with config:
2024-02-03T14:50:24.061649Z  INFO main ThreadId(01) ballista_executor::executor_process: work_dir: /tmp/.tmpAkP3pZ
2024-02-03T14:50:24.061655Z  INFO main ThreadId(01) ballista_executor::executor_process: concurrent_tasks: 48
2024-02-03T14:50:24.063256Z  INFO tokio-runtime-worker ThreadId(44) ballista_executor::executor_process: Ballista v0.12.0 Rust Executor Flight Server listening on 0.0.0.0:50051
2024-02-03T14:50:24.063281Z  INFO tokio-runtime-worker ThreadId(47) ballista_executor::execution_loop: Starting poll work loop with scheduler
```

## Connect from the CLI

```shell
docker run --network=host -it apache/datafusion-ballista-cli:latest --host localhost --port 50050
```
