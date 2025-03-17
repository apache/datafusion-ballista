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

# Ballista Scheduler

## REST API

The scheduler also provides a REST API that allows jobs to be monitored.

> This is optional scheduler feature which should be enabled with `rest-api` feature

| API                                  | Method | Description                                                       |
| ------------------------------------ | ------ | ----------------------------------------------------------------- |
| /api/jobs                            | GET    | Get a list of jobs that have been submitted to the cluster.       |
| /api/job/{job_id}                    | GET    | Get a summary of a submitted job.                                 |
| /api/job/{job_id}/dot                | GET    | Produce a query plan in DOT (graphviz) format.                    |
| /api/job/:job_id/dot_svg             | GET    | Produce a query plan in SVG format. (`graphviz-support` required) |
| /api/job/{job_id}                    | PATCH  | Cancel a currently running job                                    |
| /api/job/:job_id/stage/:stage_id/dot | GET    | Produces stage plan in DOT (graphviz) format                      |
| /api/metrics                         | GET    | Return current scheduler metric set                               |
