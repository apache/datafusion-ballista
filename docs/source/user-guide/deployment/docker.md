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

# Starting a Ballista cluster using Docker

## Build Docker image

There is no officially published Docker image so it is currently necessary to build the image from source instead.

Run the following commands to clone the source repository and build the Docker image.

```bash
git clone git@github.com:apache/arrow-ballista.git -b 8.0.0
cd arrow-ballista
./dev/build-ballista-docker.sh
```

This will create the following images:

- `apache/arrow-ballista-scheduler:0.8.0`
- `apache/arrow-ballista-executor:0.8.0`

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
 -d apache/arrow-ballista-scheduler:0.8.0 \
 --bind-port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE                                   COMMAND                  CREATED         STATUS        PORTS     NAMES
8cdea4956c97   apache/arrow-ballista-scheduler:0.8.0   "/scheduler-entrypoi…"   2 seconds ago   Up 1 second             nervous_swirles
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs 8cdea4956c97
Starting nginx to serve Ballista Scheduler web UI on port 80
2022-09-19T13:51:34.792363Z  INFO main ThreadId(01) ballista_scheduler: Ballista v0.8.0 Scheduler listening on 0.0.0.0:50050
2022-09-19T13:51:34.792395Z  INFO main ThreadId(01) ballista_scheduler: Starting Scheduler grpc server with task scheduling policy of PullStaged
2022-09-19T13:51:34.792494Z  INFO main ThreadId(01) ballista_scheduler::scheduler_server::query_stage_scheduler: Starting QueryStageScheduler
2022-09-19T13:51:34.792581Z  INFO tokio-runtime-worker ThreadId(45) ballista_core::event_loop: Starting the event loop query_stage
```

### Start executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d apache/arrow-ballista-executor:0.8.0 \
  --external-host localhost --bind-port 50051
```

Use `docker ps` to check that both the scheduler and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE                                   COMMAND                  CREATED         STATUS         PORTS     NAMES
f0b21f6b5050   apache/arrow-ballista-executor:0.8.0    "/executor-entrypoin…"   2 seconds ago   Up 1 second              relaxed_goldberg
8cdea4956c97   apache/arrow-ballista-scheduler:0.8.0   "/scheduler-entrypoi…"   2 minutes ago   Up 2 minutes             nervous_swirles
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs f0b21f6b5050
2022-09-19T13:54:10.806231Z  INFO main ThreadId(01) ballista_executor: Running with config:
2022-09-19T13:54:10.806261Z  INFO main ThreadId(01) ballista_executor: work_dir: /tmp/.tmp5BdxT2
2022-09-19T13:54:10.806265Z  INFO main ThreadId(01) ballista_executor: concurrent_tasks: 48
2022-09-19T13:54:10.807454Z  INFO tokio-runtime-worker ThreadId(49) ballista_executor: Ballista v0.8.0 Rust Executor Flight Server listening on 0.0.0.0:50051
2022-09-19T13:54:10.807467Z  INFO tokio-runtime-worker ThreadId(46) ballista_executor::execution_loop: Starting poll work loop with scheduler
```

### Using etcd as backing store

_NOTE: This functionality is currently experimental_

Ballista can optionally use [etcd](https://etcd.io/) as a backing store for the scheduler. Use the following commands
to launch the scheduler with this option enabled.

```bash
docker run --network=host \
  -d apache/arrow-ballista-scheduler:0.8.0 \
  --bind-port 50050 \
  --config-backend etcd \
  --etcd-urls etcd:2379
```

Please refer to the [etcd](https://etcd.io/) website for installation instructions. Etcd version 3.4.9 or later is
recommended.
