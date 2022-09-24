# Ballista Scheduler

## Web User Interface

The scheduler provides a web user interface that allows queries to be monitored.

![Ballista Scheduler Web UI](./images/ballista-web-ui.png)

## REST API

| API                   | Description                                                 |
|-----------------------|-------------------------------------------------------------|
| /api/jobs             | Get a list of jobs that have been submitted to the cluster. |
| /api/job/{job_id}     | Get a summary of a submitted job.                           |
| /api/job/{job_id}/dot | Produce a query plan in DOT (graphviz) formmat.             |
 