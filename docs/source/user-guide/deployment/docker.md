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

# Starting a kapot Cluster using Docker

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

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
 -d apache/datafusion-kapot-scheduler:0.12.0 \
 --bind-port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
a756055576f3   apache/datafusion-kapot-scheduler:0.12.0   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs a756055576f3
Starting nginx to serve kapot Scheduler web UI on port 80
2024-02-03T14:49:47.904571Z  INFO main ThreadId(01) kapot_scheduler::cluster: Initializing Sled database in temp directory
nginx: [warn] duplicate value "error" in /etc/nginx/sites-enabled/default:49
nginx: [warn] duplicate value "non_idempotent" in /etc/nginx/sites-enabled/default:49
2024-02-03T14:49:47.924679Z  INFO main ThreadId(01) kapot_scheduler::scheduler_process: kapot v0.12.0 Scheduler listening on 0.0.0.0:50050
2024-02-03T14:49:47.924709Z  INFO main ThreadId(01) kapot_scheduler::scheduler_process: Starting Scheduler grpc server with task scheduling policy of PullStaged
2024-02-03T14:49:47.925261Z  INFO main ThreadId(01) kapot_scheduler::cluster::kv: Initializing heartbeat listener
2024-02-03T14:49:47.925476Z  INFO main ThreadId(01) kapot_scheduler::scheduler_server::query_stage_scheduler: Starting QueryStageScheduler
2024-02-03T14:49:47.925587Z  INFO tokio-runtime-worker ThreadId(47) kapot_core::event_loop: Starting the event loop query_stage
```

### Start Executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d apache/datafusion-kapot-executor:0.12.0 \
  --external-host localhost --bind-port 50051
```

Use `docker ps` to check that both the scheduler and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS     NAMES
fb8b530cee6d   apache/datafusion-kapot-executor:0.12.0    "/root/executor-entr…"   2 seconds ago   Up 1 second              gallant_galois
a756055576f3   apache/datafusion-kapot-scheduler:0.12.0   "/root/scheduler-ent…"   8 seconds ago   Up 8 seconds             xenodochial_carson
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs fb8b530cee6d
2024-02-03T14:50:24.061607Z  INFO main ThreadId(01) kapot_executor::executor_process: Running with config:
2024-02-03T14:50:24.061649Z  INFO main ThreadId(01) kapot_executor::executor_process: work_dir: /tmp/.tmpAkP3pZ
2024-02-03T14:50:24.061655Z  INFO main ThreadId(01) kapot_executor::executor_process: concurrent_tasks: 48
2024-02-03T14:50:24.063256Z  INFO tokio-runtime-worker ThreadId(44) kapot_executor::executor_process: kapot v0.12.0 Rust Executor Flight Server listening on 0.0.0.0:50051
2024-02-03T14:50:24.063281Z  INFO tokio-runtime-worker ThreadId(47) kapot_executor::execution_loop: Starting poll work loop with scheduler
```

### Using etcd as a Backing Store

_NOTE: This functionality is currently experimental_

kapot can optionally use [etcd](https://etcd.io/) as a backing store for the scheduler. Use the following commands
to launch the scheduler with this option enabled.

```bash
docker run --network=host \
  -d apache/datafusion-kapot-scheduler:0.12.0 \
  --bind-port 50050 \
  --config-backend etcd \
  --etcd-urls etcd:2379
```

Please refer to the [etcd](https://etcd.io/) website for installation instructions. Etcd version 3.4.9 or later is
recommended.

## Connect from the CLI

```shell
docker run --network=host -it apache/datafusion-kapot-cli:0.12.0 --host localhost --port 50050
```
