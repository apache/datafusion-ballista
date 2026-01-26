### Summary
Add an interactive terminal-based user interface (TUI) for monitoring Ballista clusters, viewing query plans, and inspecting job statistics. This would provide a lightweight alternative to a web UI that can be used directly from the command line.

### Motivation
Currently, monitoring a Ballista cluster requires either:

* Manually calling REST API endpoints with curl/httpie
* Building custom tooling to consume the API
* No built-in visualization of query plans or job progress
A TUI would make Ballista more accessible for operators and developers who prefer terminal workflows, and would provide immediate visibility into cluster health and job execution.

### Proposed Features

#### Dashboard View

* Cluster overview: scheduler uptime, version, connected executors
* Real-time executor list with host, port, and last-seen status
* Job summary: running, queued, completed, failed counts

#### Jobs View
* List all jobs with status, progress percentage, and stage completion
* Filter/search jobs by ID or name
* Sort by status, start time, or progress
* Cancel jobs interactively

#### Job Detail View
* Stage breakdown with input/output rows and elapsed compute time
* Query plan visualization (render DOT graph as ASCII art or tree view)
* Per-stage drill-down

####Metrics View
* Display Prometheus metrics in a formatted table
* Highlight key metrics: job execution times, planning times, failure counts

####Available REST Endpoints
The scheduler already exposes all necessary data via REST API (requires rest-api feature):

### Endpoint	Data Available
|-------------------------------|----------------------------------|
|GET /api/state |	Scheduler start time, version |
| GET /api/executors |	Executor id, host, port, last_seen |
| GET /api/jobs	| Job id, name, status, num_stages, completed_stages, percent_complete |
| PATCH /api/job/{job_id}	| Cancel a job |
| GET /api/job/{job_id}/stages	| Stage id, status, input_rows, output_rows, elapsed_compute |
| GET /api/job/{job_id}/dot |	Graphviz DOT representation of execution graph |
| GET /api/job/{job_id}/stage/{stage_id}/dot |	DOT for specific stage |
| GET /api/metrics |	Prometheus metrics (requires prometheus-metrics feature) |
|-------------------------------|----------------------------------|
