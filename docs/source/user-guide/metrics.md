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

# Ballista Scheduler Metrics

## Prometheus 

Built with default features, the ballista scheduler will automatically collect and expose a standard set of prometheus metrics.
The metrics currently collected automatically include:
- *job_exec_time_seconds* - Histogram of successful job execution time in seconds
- *planning_time_ms* - Histogram of job planning time in milliseconds
- *failed* - Counter of failed jobs
- *job_failed_total* - Counter of failed jobs
- *job_cancelled_total* - Counter of cancelled jobs 
- *job_completed_total* - Counter of completed jobs
- *job_submitted_total* - Counter of submitted jobs 
- *pending_task_queue_size* - Number of pending tasks

**NOTE** Currently the histogram buckets for the above metrics are set to reasonable defaults. If the defaults are not 
appropriate for a given use case, the only workaround is to implement a customer `SchedulerMetricsCollector`. In the future
the buckets should be made configurable. 

The metrics are then exported through the scheduler REST API at `GET /api/metrics`. It should be sufficient to ingest metrics
into an existing metrics system by point your chosen prometheus exporter at that endpoint. 
