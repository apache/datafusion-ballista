## Roadmap

There is an excellent discussion in https://github.com/apache/arrow-ballista/issues/30 about the future of the project,
and we encourage you to participate and add your feedback there if you are interested in using or contributing to
Ballista.

The current focus is on the following items:

- Make production ready
  - Shuffle file cleanup
    - Periodically ([#185](https://github.com/apache/arrow-ballista/issues/185))
    - Add gRPC & REST interfaces for clients/UI to actively call the cleanup for a job or the whole system
  - Fill functional gaps between DataFusion and Ballista
  - Improve task scheduling and data exchange efficiency
  - Better error handling
    - Scheduler restart
  - Improve monitoring, logging, and metrics
  - Auto scaling support
  - Better configuration management
  - Support for multi-scheduler deployments. Initially for resiliency and fault tolerance but ultimately to support
    sharding for scalability and more efficient caching.
- Shuffle improvement
  - Shuffle memory control ([#320](https://github.com/apache/arrow-ballista/issues/320))
  - Improve shuffle IO to avoid producing too many files
  - Support sort-based shuffle
  - Support range partition
  - Support broadcast shuffle ([#342](https://github.com/apache/arrow-ballista/issues/342))
- Scheduler Improvements
  - All-at-once job task scheduling
  - Executor deployment grouping based on resource allocation
- Cloud Support
  - Support Azure Blob Storage ([#294](https://github.com/apache/arrow-ballista/issues/294))
  - Support Google Cloud Storage ([#293](https://github.com/apache/arrow-ballista/issues/293))
- Performance and scalability
  - Implement Adaptive Query Execution ([#387](https://github.com/apache/arrow-ballista/issues/387))
  - Implement bubble execution ([#408](https://github.com/apache/arrow-ballista/issues/408))
  - Improve benchmark results ([#339](https://github.com/apache/arrow-ballista/issues/339))
- Python Support
  - Support Python UDFs ([#173](https://github.com/apache/arrow-ballista/issues/173))
