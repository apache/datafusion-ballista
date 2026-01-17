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
INFO ballista_scheduler::scheduler_process: Ballista v51.0.0 Scheduler listening on 0.0.0.0:50050
INFO ballista_scheduler::scheduler_process: Starting Scheduler grpc server with task scheduling policy of PullStaged
INFO ballista_scheduler::scheduler_server::query_stage_scheduler: Starting QueryStageScheduler
INFO ballista_core::event_loop: Starting the event loop query_stage
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
INFO ballista_executor::executor_process: Running with config:
INFO ballista_executor::executor_process: work_dir: /tmp/.tmpAkP3pZ
INFO ballista_executor::executor_process: concurrent_tasks: 48
INFO ballista_executor::executor_process: Ballista v51.0.0 Rust Executor Flight Server listening on 0.0.0.0:50051
INFO ballista_executor::execution_loop: Starting poll work loop with scheduler
```

## Connect from the CLI

```shell
docker run --network=host -it apache/datafusion-ballista-cli:latest --host localhost --port 50050
```
