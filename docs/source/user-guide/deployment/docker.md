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

The official Docker images are published to the GitHub Container Registry:

- [`ghcr.io/apache/datafusion-ballista-standalone`](https://github.com/apache/datafusion-ballista/pkgs/container/datafusion-ballista-standalone)
- [`ghcr.io/apache/datafusion-ballista-scheduler`](https://github.com/apache/datafusion-ballista/pkgs/container/datafusion-ballista-scheduler)
- [`ghcr.io/apache/datafusion-ballista-executor`](https://github.com/apache/datafusion-ballista/pkgs/container/datafusion-ballista-executor)

Pull the images needed for your deployment with the following commands:

```bash
docker pull ghcr.io/apache/datafusion-ballista-standalone:latest
docker pull ghcr.io/apache/datafusion-ballista-scheduler:latest
docker pull ghcr.io/apache/datafusion-ballista-executor:latest
```

Alternatively run the following commands to clone the source repository and build the Docker images from source:

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

The CLI and benchmarks images are built locally only, so the
`apache/datafusion-ballista-cli:latest` command below requires a local build
first. Use the `ghcr.io/apache/` names shown above for the published images.

## Start a Cluster

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
 -d ghcr.io/apache/datafusion-ballista-scheduler:latest \
 --bind-port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
a756055576f3   ghcr.io/apache/datafusion-ballista-scheduler:latest   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs a756055576f3
INFO ballista_scheduler::scheduler_process: Ballista Scheduler v54.0.0 (DataFusion v55.1.0) listening on 0.0.0.0:50050
INFO ballista_scheduler::scheduler_process: Starting Scheduler grpc server with task scheduling policy of PushStaged
INFO ballista_scheduler::scheduler_server::query_stage_scheduler: Starting QueryStageScheduler
INFO ballista_core::event_loop: Starting the event loop query_stage
```

### Start Executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d ghcr.io/apache/datafusion-ballista-executor:latest \
  --external-host localhost --bind-port 50051
```

Use `docker ps` to check that both the scheduler and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
fb8b530cee6d   ghcr.io/apache/datafusion-ballista-executor:latest    "/root/executor-entr…"   2 seconds ago   Up 1 second              gallant_galois
a756055576f3   ghcr.io/apache/datafusion-ballista-scheduler:latest   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs fb8b530cee6d
INFO ballista_executor::executor_process: Ballista Executor v54.0.0 (DataFusion v55.1.0) starting ...
INFO ballista_executor::executor_process: Executor working directory: /tmp/.tmpAkP3pZ
INFO ballista_executor::executor_process: Executor vcores (default: available CPU cores): 48
INFO ballista_executor::executor_process: Executor scheduling policy: PushStaged
INFO ballista_executor::executor_server: Ballista v54.0.0 Rust Executor Grpc Server listening on 0.0.0.0:50052
INFO ballista_executor::executor_server: Executor registration succeed
INFO ballista_executor::executor_process: Built-in arrow flight server listening on: 0.0.0.0:50051 max_encoding_size: 16777216 max_decoding_size: 16777216
```

## Connect from the CLI

```shell
docker run --network=host -it apache/datafusion-ballista-cli:latest --host localhost --port 50050
```
