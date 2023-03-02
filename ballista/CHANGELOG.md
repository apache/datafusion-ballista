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

# Changelog

## [0.11.0](https://github.com/apache/arrow-ballista/tree/0.11.0) (2023-02-19)

[Full Changelog](https://github.com/apache/arrow-ballista/compare/0.10.0...0.11.0)

**Implemented enhancements:**

- Remove `python` since it has been moved to its own repo, `arrow-ballista-python` [\#653](https://github.com/apache/arrow-ballista/issues/653)
-  Add executor self-registration mechanism in the heartbeat service [\#648](https://github.com/apache/arrow-ballista/issues/648)
- Upgrade to DataFusion 17 [\#638](https://github.com/apache/arrow-ballista/issues/638)
- Move Python bindings to separate repo? [\#635](https://github.com/apache/arrow-ballista/issues/635)
- Implement new release process [\#622](https://github.com/apache/arrow-ballista/issues/622)
- Change default branch name from master to main [\#618](https://github.com/apache/arrow-ballista/issues/618)
- Update latest datafusion dependency [\#610](https://github.com/apache/arrow-ballista/issues/610)
- Implement optimizer rule to remove redundant repartitioning [\#608](https://github.com/apache/arrow-ballista/issues/608)
- ballista-cli as \(docker\) images [\#600](https://github.com/apache/arrow-ballista/issues/600)
- Update contributor guide [\#598](https://github.com/apache/arrow-ballista/issues/598)
- Fix cargo clippy [\#570](https://github.com/apache/arrow-ballista/issues/570)
- Support Alibaba Cloud OSS with ObjectStore [\#566](https://github.com/apache/arrow-ballista/issues/566)
- Refactor `StateBackendClient` to be a higher-level interface [\#554](https://github.com/apache/arrow-ballista/issues/554)
- Make it concurrently to launch tasks to executors [\#544](https://github.com/apache/arrow-ballista/issues/544)
- Simplify docs [\#531](https://github.com/apache/arrow-ballista/issues/531)
- Provide an in-memory StateBackend [\#505](https://github.com/apache/arrow-ballista/issues/505)
- Add support for Azure blob storage [\#294](https://github.com/apache/arrow-ballista/issues/294)
- Add a workflow to build the image and publish it to the package [\#71](https://github.com/apache/arrow-ballista/issues/71)

**Fixed bugs:**

- Rust / Check Cargo.toml formatting \(amd64, stable\) \(pull\_request\) Failing [\#662](https://github.com/apache/arrow-ballista/issues/662)
- Protobuf parsing error [\#646](https://github.com/apache/arrow-ballista/issues/646)
- jobs from python client not showing up in Scheduler UI [\#625](https://github.com/apache/arrow-ballista/issues/625)
- ballista ui fails to build [\#594](https://github.com/apache/arrow-ballista/issues/594)
- cargo build --release fails for ballista-scheduler [\#590](https://github.com/apache/arrow-ballista/issues/590)
- docker build fails [\#589](https://github.com/apache/arrow-ballista/issues/589)
- Multi-scheduler Job Starvation [\#585](https://github.com/apache/arrow-ballista/issues/585)
- Cannot query file from S3  [\#559](https://github.com/apache/arrow-ballista/issues/559)
- Benchmark q16 fails [\#373](https://github.com/apache/arrow-ballista/issues/373)

**Documentation updates:**

- Check in benchmark image [\#647](https://github.com/apache/arrow-ballista/pull/647) ([andygrove](https://github.com/andygrove))
- MINOR: Fix benchmark image link [\#596](https://github.com/apache/arrow-ballista/pull/596) ([andygrove](https://github.com/andygrove))

**Merged pull requests:**

- Upgrade to DataFusion 18 [\#668](https://github.com/apache/arrow-ballista/pull/668) ([andygrove](https://github.com/andygrove))
- Enable physical plan round-trip tests [\#666](https://github.com/apache/arrow-ballista/pull/666) ([andygrove](https://github.com/andygrove))
- Upgrade to DataFusion 18.0.0-rc1 [\#664](https://github.com/apache/arrow-ballista/pull/664) ([andygrove](https://github.com/andygrove))
- add test\_util to make examples work well [\#661](https://github.com/apache/arrow-ballista/pull/661) ([jiangzhx](https://github.com/jiangzhx))
- Minor refactor to reduce duplicate code [\#659](https://github.com/apache/arrow-ballista/pull/659) ([andygrove](https://github.com/andygrove))
- Cluster state refactor Part 2 [\#658](https://github.com/apache/arrow-ballista/pull/658) ([thinkharderdev](https://github.com/thinkharderdev))
- Remove `python` dir & python-related workflows [\#654](https://github.com/apache/arrow-ballista/pull/654) ([iajoiner](https://github.com/iajoiner))
- Add executor self-registration mechanism in the heartbeat service [\#649](https://github.com/apache/arrow-ballista/pull/649) ([yahoNanJing](https://github.com/yahoNanJing))
- Upgrade to DataFusion 17 [\#639](https://github.com/apache/arrow-ballista/pull/639) ([avantgardnerio](https://github.com/avantgardnerio))
- Upgrade to DataFusion 16 \(again\) [\#636](https://github.com/apache/arrow-ballista/pull/636) ([avantgardnerio](https://github.com/avantgardnerio))
- Update release process documentation [\#632](https://github.com/apache/arrow-ballista/pull/632) ([andygrove](https://github.com/andygrove))
- Implement new release process [\#623](https://github.com/apache/arrow-ballista/pull/623) ([andygrove](https://github.com/andygrove))
- Update contributor guide [\#617](https://github.com/apache/arrow-ballista/pull/617) ([andygrove](https://github.com/andygrove))
- Fix Cargo.toml format issue [\#616](https://github.com/apache/arrow-ballista/pull/616) ([andygrove](https://github.com/andygrove))
- Refactor scheduler main [\#615](https://github.com/apache/arrow-ballista/pull/615) ([andygrove](https://github.com/andygrove))
- Refactor executor main [\#614](https://github.com/apache/arrow-ballista/pull/614) ([andygrove](https://github.com/andygrove))
- Update datafusion dependency to the latest version [\#612](https://github.com/apache/arrow-ballista/pull/612) ([yahoNanJing](https://github.com/yahoNanJing))
- Add support for Azure Blob Storage [\#599](https://github.com/apache/arrow-ballista/pull/599) ([aidankovacic-8451](https://github.com/aidankovacic-8451))
- Python: add method to get explain output as a string [\#593](https://github.com/apache/arrow-ballista/pull/593) ([andygrove](https://github.com/andygrove))
- Handle job resubmission [\#586](https://github.com/apache/arrow-ballista/pull/586) ([thinkharderdev](https://github.com/thinkharderdev))
- updated readme to contain correct versions of dependencies. [\#580](https://github.com/apache/arrow-ballista/pull/580) ([saikrishna1-bidgely](https://github.com/saikrishna1-bidgely))
- Update graphviz-rust requirement from 0.4.0 to 0.5.0 [\#574](https://github.com/apache/arrow-ballista/pull/574) ([dependabot[bot]](https://github.com/apps/dependabot))
- Super minor spelling error [\#573](https://github.com/apache/arrow-ballista/pull/573) ([jdye64](https://github.com/jdye64))
- Fix cargo clippy [\#571](https://github.com/apache/arrow-ballista/pull/571) ([yahoNanJing](https://github.com/yahoNanJing))
- Support Alibaba Cloud OSS with ObjectStore [\#567](https://github.com/apache/arrow-ballista/pull/567) ([r4ntix](https://github.com/r4ntix))
- fix\(ui\): fix last seen [\#562](https://github.com/apache/arrow-ballista/pull/562) ([duyet](https://github.com/duyet))
- Cluster state refactor part 1 [\#560](https://github.com/apache/arrow-ballista/pull/560) ([thinkharderdev](https://github.com/thinkharderdev))
- Make it concurrently to launch tasks to executors [\#557](https://github.com/apache/arrow-ballista/pull/557) ([yahoNanJing](https://github.com/yahoNanJing))
- Update datafusion requirement from 14.0.0 to 15.0.0 [\#552](https://github.com/apache/arrow-ballista/pull/552) ([yahoNanJing](https://github.com/yahoNanJing))
- docs: fix style in the Helm readme [\#551](https://github.com/apache/arrow-ballista/pull/551) ([haoxins](https://github.com/haoxins))
- Fix Helm chart's image format [\#550](https://github.com/apache/arrow-ballista/pull/550) ([haoxins](https://github.com/haoxins))
- Update env\_logger requirement from 0.9 to 0.10 [\#539](https://github.com/apache/arrow-ballista/pull/539) ([dependabot[bot]](https://github.com/apps/dependabot))
- only build docker images on rc tags [\#535](https://github.com/apache/arrow-ballista/pull/535) ([andygrove](https://github.com/andygrove))
- Remove `--locked` when building Python wheels [\#533](https://github.com/apache/arrow-ballista/pull/533) ([andygrove](https://github.com/andygrove))
- Bump actions/labeler from 4.0.2 to 4.1.0 [\#525](https://github.com/apache/arrow-ballista/pull/525) ([dependabot[bot]](https://github.com/apps/dependabot))
- Provide a memory StateBackendClient [\#523](https://github.com/apache/arrow-ballista/pull/523) ([yahoNanJing](https://github.com/yahoNanJing))


## [0.10.0](https://github.com/apache/arrow-ballista/tree/0.10.0) (2022-11-18)

[Full Changelog](https://github.com/apache/arrow-ballista/compare/0.9.0...0.10.0)

**Implemented enhancements:**

- Add user guide section on prometheus metrics [\#507](https://github.com/apache/arrow-ballista/issues/507)
- Don't throw error when job path not exist in remove\_job\_data [\#502](https://github.com/apache/arrow-ballista/issues/502)
- Fix clippy warning [\#494](https://github.com/apache/arrow-ballista/issues/494)
- Use job\_data\_clean\_up\_interval\_seconds == 0 to indicate executor\_cleanup\_enable [\#488](https://github.com/apache/arrow-ballista/issues/488)
- Add a config for tracing log rolling policy for both scheduler and executor [\#486](https://github.com/apache/arrow-ballista/issues/486)
- Set up repo where we can push benchmark results [\#473](https://github.com/apache/arrow-ballista/issues/473)
- Make the delayed time interval for cleanup job data in both scheduler and executor configurable [\#469](https://github.com/apache/arrow-ballista/issues/469)
- Add some validation for the remove\_job\_data grpc service [\#467](https://github.com/apache/arrow-ballista/issues/467)
- Add ability to build docker images using `release-lto` profile [\#463](https://github.com/apache/arrow-ballista/issues/463)
- Suggest users download \(rather than build\) the FlightSQL JDBC Driver [\#460](https://github.com/apache/arrow-ballista/issues/460)
- Clean up legacy job shuffle data [\#459](https://github.com/apache/arrow-ballista/issues/459)
- Add grpc service for the scheduler to make it able to be triggered by client explicitly [\#458](https://github.com/apache/arrow-ballista/issues/458)
- Replace Mutex\<HashMap\> by using DashMap [\#448](https://github.com/apache/arrow-ballista/issues/448)
- Refine log level  [\#446](https://github.com/apache/arrow-ballista/issues/446)
- Upgrade to DataFusion 14.0.0 [\#445](https://github.com/apache/arrow-ballista/issues/445)
- Add a feature for hdfs3 [\#419](https://github.com/apache/arrow-ballista/issues/419)
- Add optional flag which advertises host for Arrow Flight SQL [\#418](https://github.com/apache/arrow-ballista/issues/418)
- Partitioning reasoning in DataFusion and Ballista [\#284](https://github.com/apache/arrow-ballista/issues/284)
- Stop wasting time in CI on MIRI runs  [\#283](https://github.com/apache/arrow-ballista/issues/283)
- Publish Docker images as part of each release [\#236](https://github.com/apache/arrow-ballista/issues/236)
- Cleanup job/stage status from TaskManager and clean up shuffle data after a period after JobFinished [\#185](https://github.com/apache/arrow-ballista/issues/185)

**Fixed bugs:**

- build broken: configure\_me\_codegen retroactively reserved `bind_host` [\#519](https://github.com/apache/arrow-ballista/issues/519)
- Return empty results for SQLs with order by [\#451](https://github.com/apache/arrow-ballista/issues/451)
- ballista scheduler is not taken inline parameters into account [\#443](https://github.com/apache/arrow-ballista/issues/443)
- \[FlightSQL\] Cannot connect with Tableau Desktop [\#428](https://github.com/apache/arrow-ballista/issues/428)
- Benchmark q15 fails [\#372](https://github.com/apache/arrow-ballista/issues/372)
- Incorrect documentation for building Ballista on Linux when using docker-compose [\#362](https://github.com/apache/arrow-ballista/issues/362)
- Scheduler silently replaces `ParquetExec` with `EmptyExec` if data path is not correctly mounted in container [\#353](https://github.com/apache/arrow-ballista/issues/353)
- SQL with order by limit returns nothing [\#334](https://github.com/apache/arrow-ballista/issues/334)

**Documentation updates:**

- README updates [\#433](https://github.com/apache/arrow-ballista/pull/433) ([andygrove](https://github.com/andygrove))

**Merged pull requests:**

- configure\_me\_codegen retroactively reserved on our `bind_host` parame… [\#520](https://github.com/apache/arrow-ballista/pull/520) ([avantgardnerio](https://github.com/avantgardnerio))
- Bump actions/cache from 2 to 3 [\#517](https://github.com/apache/arrow-ballista/pull/517) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update graphviz-rust requirement from 0.3.0 to 0.4.0 [\#515](https://github.com/apache/arrow-ballista/pull/515) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add Prometheus metrics endpoint [\#511](https://github.com/apache/arrow-ballista/pull/511) ([thinkharderdev](https://github.com/thinkharderdev))
- Enable tests that work since upgrading to DataFusion 14 [\#510](https://github.com/apache/arrow-ballista/pull/510) ([andygrove](https://github.com/andygrove))
- Update hashbrown requirement from 0.12 to 0.13 [\#506](https://github.com/apache/arrow-ballista/pull/506) ([dependabot[bot]](https://github.com/apps/dependabot))
- Don't throw error when job shuffle data path not exist in executor [\#503](https://github.com/apache/arrow-ballista/pull/503) ([yahoNanJing](https://github.com/yahoNanJing))
- Upgrade to DataFusion 14.0.0 and Arrow 26.0.0 [\#499](https://github.com/apache/arrow-ballista/pull/499) ([andygrove](https://github.com/andygrove))
- Fix clippy warning [\#495](https://github.com/apache/arrow-ballista/pull/495) ([yahoNanJing](https://github.com/yahoNanJing))
- Stop wasting time in CI on MIRI runs [\#491](https://github.com/apache/arrow-ballista/pull/491) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Remove executor config executor\_cleanup\_enable and make the configuation name for executor cleanup more intuitive [\#489](https://github.com/apache/arrow-ballista/pull/489) ([yahoNanJing](https://github.com/yahoNanJing))
- Add a config for tracing log rolling policy for both scheduler and executor [\#487](https://github.com/apache/arrow-ballista/pull/487) ([yahoNanJing](https://github.com/yahoNanJing))
- Add grpc service of cleaning up job shuffle data for the scheduler to make it able to be triggered by client explicitly [\#485](https://github.com/apache/arrow-ballista/pull/485) ([yahoNanJing](https://github.com/yahoNanJing))
- \[Minor\] Bump DataFusion [\#480](https://github.com/apache/arrow-ballista/pull/480) ([Dandandan](https://github.com/Dandandan))
- Remove benchmark results from README [\#478](https://github.com/apache/arrow-ballista/pull/478) ([andygrove](https://github.com/andygrove))
- Update `flightsql.md` to provide correct instruction [\#476](https://github.com/apache/arrow-ballista/pull/476) ([iajoiner](https://github.com/iajoiner))
- Add support for Tableau [\#475](https://github.com/apache/arrow-ballista/pull/475) ([avantgardnerio](https://github.com/avantgardnerio))
-  Add SchedulerConfig for the scheduler configurations, like event\_loop\_buffer\_size, finished\_job\_data\_clean\_up\_interval\_seconds, finished\_job\_state\_clean\_up\_interval\_seconds [\#472](https://github.com/apache/arrow-ballista/pull/472) ([yahoNanJing](https://github.com/yahoNanJing))
- Bump DataFusion [\#471](https://github.com/apache/arrow-ballista/pull/471) ([Dandandan](https://github.com/Dandandan))
- Add some validation for remove\_job\_data in the executor server [\#468](https://github.com/apache/arrow-ballista/pull/468) ([yahoNanJing](https://github.com/yahoNanJing))
- Update documentation to reflect the release of the FlightSQL JDBC Driver [\#461](https://github.com/apache/arrow-ballista/pull/461) ([avantgardnerio](https://github.com/avantgardnerio))
- Bump DataFusion version [\#453](https://github.com/apache/arrow-ballista/pull/453) ([andygrove](https://github.com/andygrove))
- Add shuffle for SortPreservingMergeExec physical operator [\#452](https://github.com/apache/arrow-ballista/pull/452) ([yahoNanJing](https://github.com/yahoNanJing))
- Replace Mutex\<HashMap\> by using DashMap [\#449](https://github.com/apache/arrow-ballista/pull/449) ([yahoNanJing](https://github.com/yahoNanJing))
- Refine log level for trial info and periodically invoked places [\#447](https://github.com/apache/arrow-ballista/pull/447) ([yahoNanJing](https://github.com/yahoNanJing))
- MINOR: Add `set -e` to scripts, fix a typo [\#444](https://github.com/apache/arrow-ballista/pull/444) ([andygrove](https://github.com/andygrove))
- Add optional flag which advertises host for Arrow Flight SQL \#418 [\#442](https://github.com/apache/arrow-ballista/pull/442) ([DaltonModlin](https://github.com/DaltonModlin))
- Reorder joins after resolving stage inputs [\#441](https://github.com/apache/arrow-ballista/pull/441) ([Dandandan](https://github.com/Dandandan))
- Add a feature for hdfs3 [\#439](https://github.com/apache/arrow-ballista/pull/439) ([yahoNanJing](https://github.com/yahoNanJing))
- Add Spark benchmarks [\#438](https://github.com/apache/arrow-ballista/pull/438) ([andygrove](https://github.com/andygrove))
- scheduler now verifies that `file://` ListingTable URLs are accessible [\#414](https://github.com/apache/arrow-ballista/pull/414) ([andygrove](https://github.com/andygrove))


## [0.9.0](https://github.com/apache/arrow-ballista/tree/0.9.0) (2022-10-22)

[Full Changelog](https://github.com/apache/arrow-ballista/compare/0.8.0...0.9.0)

**Implemented enhancements:**

- Support count distinct aggregation function [\#411](https://github.com/apache/arrow-ballista/issues/411)
- Use multi-task definition in pull-based execution loop [\#400](https://github.com/apache/arrow-ballista/issues/400)
- Make the scheduler event loop buffer size configurable [\#397](https://github.com/apache/arrow-ballista/issues/397)
- Remove active execution graph when the related job is successful or failed. [\#391](https://github.com/apache/arrow-ballista/issues/391)
- Improve launch task efficiency by calling LaunchMultiTask [\#389](https://github.com/apache/arrow-ballista/issues/389)
- Use `tokio::sync::Semaphore` to wait for available task slots [\#388](https://github.com/apache/arrow-ballista/issues/388)
- stdout and file log level settings are inconsistent [\#385](https://github.com/apache/arrow-ballista/issues/385)
- Use dedicated executor in pull based loop [\#383](https://github.com/apache/arrow-ballista/issues/383)
- Avoid calling scheduler when the executor cannot accept new tasks [\#377](https://github.com/apache/arrow-ballista/issues/377)
- Add round robin executor slots reservation policy for the scheduler to evenly assign tasks to executors [\#371](https://github.com/apache/arrow-ballista/issues/371)
- Switch to mimalloc and enable by default [\#369](https://github.com/apache/arrow-ballista/issues/369)
- Integration test script should use docker-compose [\#364](https://github.com/apache/arrow-ballista/issues/364)
- Use local shuffle reader in containerized environments [\#356](https://github.com/apache/arrow-ballista/issues/356)
- Add `--ext` option to benchmark [\#352](https://github.com/apache/arrow-ballista/issues/352)
- Add job cancel in the UI [\#350](https://github.com/apache/arrow-ballista/issues/350)
- Using local shuffle reader avoid flight rpc call. [\#346](https://github.com/apache/arrow-ballista/issues/346)
- Add a Helm Chart [\#321](https://github.com/apache/arrow-ballista/issues/321)
- \[UI\] Show list of query stages with metrics [\#306](https://github.com/apache/arrow-ballista/issues/306)
- \[UI\] Add ability to specify job name and have it show in the job listing page in the UI [\#277](https://github.com/apache/arrow-ballista/issues/277)
- \[UI\] Add ability to download query plans in dot format [\#276](https://github.com/apache/arrow-ballista/issues/276)
- \[UI\] Add ability to render query plans [\#275](https://github.com/apache/arrow-ballista/issues/275)
- Add REST API documentation to User Guide [\#272](https://github.com/apache/arrow-ballista/issues/272)
- Graceful shutdown: Handle `SIGTERM` [\#266](https://github.com/apache/arrow-ballista/issues/266)
- \[EPIC\] Scheduler UI [\#265](https://github.com/apache/arrow-ballista/issues/265)
- Introduce the datafusion-objectstore-hdfs in datafusion-contrib as an object store feature [\#259](https://github.com/apache/arrow-ballista/issues/259)
- Add a feature based object store provider [\#257](https://github.com/apache/arrow-ballista/issues/257)
- Add docker build files [\#248](https://github.com/apache/arrow-ballista/issues/248)
- Allow IDEs to recognize generated code  [\#246](https://github.com/apache/arrow-ballista/issues/246)
- Add user guide section on Flight SQL support [\#230](https://github.com/apache/arrow-ballista/issues/230)
- `dev/release/README.md` is outdated [\#228](https://github.com/apache/arrow-ballista/issues/228)
- Make ShuffleReaderExec output less verbose [\#211](https://github.com/apache/arrow-ballista/issues/211)
- Add LaunchMultiTask rpc interface for executor [\#209](https://github.com/apache/arrow-ballista/issues/209)
- Make executor fetch shuffle partition data in parallel [\#208](https://github.com/apache/arrow-ballista/issues/208)
- Concurrency control and rate limit during shuffle reader [\#195](https://github.com/apache/arrow-ballista/issues/195)
- Update User Guide [\#160](https://github.com/apache/arrow-ballista/issues/160)
- Ballista 0.8.0 Release [\#159](https://github.com/apache/arrow-ballista/issues/159)
- Save encoded execution plan in the ExecutionStage to reduce cost of task serialization and deserialization [\#142](https://github.com/apache/arrow-ballista/issues/142)
- Failed task retry [\#140](https://github.com/apache/arrow-ballista/issues/140)
- Redefine the executor task slots [\#132](https://github.com/apache/arrow-ballista/issues/132)
- Use ArrowFlight bearer token auth to create a session key for FlightSql clients [\#112](https://github.com/apache/arrow-ballista/issues/112)
- Leverage Atomic for the in-memory states in Scheduler [\#101](https://github.com/apache/arrow-ballista/issues/101)
- Introduce the object stores in datafusion-contrib as optional features [\#87](https://github.com/apache/arrow-ballista/issues/87)
- Support multiple paths for ListingTableScanNode [\#75](https://github.com/apache/arrow-ballista/issues/75)
- Need clean up intermediate data in Ballista [\#9](https://github.com/apache/arrow-ballista/issues/9)
- Ballista does not support external file systems  [\#10](https://github.com/apache/arrow-ballista/issues/10)

**Fixed bugs:**

- Build errors in ./dev/build-ballista-rust.sh [\#407](https://github.com/apache/arrow-ballista/issues/407)
- The Ballista Scheduler Dockerfile copies a file that no longer exists [\#402](https://github.com/apache/arrow-ballista/issues/402)
- Benchmark q20 fails [\#374](https://github.com/apache/arrow-ballista/issues/374)
- Integration tests fail [\#360](https://github.com/apache/arrow-ballista/issues/360)
- Helm deploy fails [\#344](https://github.com/apache/arrow-ballista/issues/344)
- Executor get stopped unexpected [\#333](https://github.com/apache/arrow-ballista/issues/333)
- Executor poll work loop failure [\#311](https://github.com/apache/arrow-ballista/issues/311)
- Queries with `LIMIT` are failing with "PhysicalExtensionCodec is not provided" [\#300](https://github.com/apache/arrow-ballista/issues/300)
- Schema inference does not work in Ballista-cli with a remote context [\#287](https://github.com/apache/arrow-ballista/issues/287)
- There are bugs in the yarn build github misses but break our internal build [\#270](https://github.com/apache/arrow-ballista/issues/270)
- Race condition running docker-compose [\#267](https://github.com/apache/arrow-ballista/issues/267)
- Scheduler UI not working in Docker image [\#250](https://github.com/apache/arrow-ballista/issues/250)
- Use bind host rather than the external host for starting a local executor service [\#244](https://github.com/apache/arrow-ballista/issues/244)
- Initial query stages read parquet files and repartition them needlessly [\#243](https://github.com/apache/arrow-ballista/issues/243)
- Cannot build Docker images on macOS 12.5.1 with M1 chip [\#234](https://github.com/apache/arrow-ballista/issues/234)
- CLI uses DataFusion context if no host or port are provided [\#219](https://github.com/apache/arrow-ballista/issues/219)
- Unsupported binary operator `StringConcat` [\#201](https://github.com/apache/arrow-ballista/issues/201)
- Ballista assumes all aggregate expressions are not DISTINCT [\#5](https://github.com/apache/arrow-ballista/issues/5)
- Start ballista ui with docker, but it can not found ballista scheduler [\#11](https://github.com/apache/arrow-ballista/issues/11)
- Cannot build Ballista docker images on Apple silicon [\#17](https://github.com/apache/arrow-ballista/issues/17)

**Documentation updates:**

- Fixup links in README.md [\#366](https://github.com/apache/arrow-ballista/pull/366) ([romanz](https://github.com/romanz))
- Update README in preparation for 0.9.0 release [\#318](https://github.com/apache/arrow-ballista/pull/318) ([andygrove](https://github.com/andygrove))
- User Guide improvements [\#274](https://github.com/apache/arrow-ballista/pull/274) ([andygrove](https://github.com/andygrove))

**Closed issues:**

- Automatic version updates for github actions with dependabot [\#127](https://github.com/apache/arrow-ballista/issues/127)

**Merged pull requests:**

- Return multiple tasks in poll\_work based on free slots [\#429](https://github.com/apache/arrow-ballista/pull/429) ([Dandandan](https://github.com/Dandandan))
- Run integration tests as part of release verification script [\#426](https://github.com/apache/arrow-ballista/pull/426) ([andygrove](https://github.com/andygrove))
- Bump actions/setup-node from 2 to 3 [\#424](https://github.com/apache/arrow-ballista/pull/424) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-python from 2 to 4 [\#423](https://github.com/apache/arrow-ballista/pull/423) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/checkout from 2 to 3 [\#422](https://github.com/apache/arrow-ballista/pull/422) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/download-artifact from 2 to 3 [\#421](https://github.com/apache/arrow-ballista/pull/421) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 2 to 3 [\#420](https://github.com/apache/arrow-ballista/pull/420) ([dependabot[bot]](https://github.com/apps/dependabot))
- MINOR: Fix yarn warnings [\#415](https://github.com/apache/arrow-ballista/pull/415) ([andygrove](https://github.com/andygrove))
- Fix q20 sql typo in benchmarks [\#409](https://github.com/apache/arrow-ballista/pull/409) ([r4ntix](https://github.com/r4ntix))
- MINOR: Add notes on Apache Reporter [\#401](https://github.com/apache/arrow-ballista/pull/401) ([andygrove](https://github.com/andygrove))
- Use local shuffle reader in containerized environments and some impro… [\#399](https://github.com/apache/arrow-ballista/pull/399) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Make the scheduler event loop buffer size configurable [\#398](https://github.com/apache/arrow-ballista/pull/398) ([yahoNanJing](https://github.com/yahoNanJing))
- Add RoundRobinLocal slots policy for caching executor data to avoid seld persistency [\#396](https://github.com/apache/arrow-ballista/pull/396) ([yahoNanJing](https://github.com/yahoNanJing))
- Add round robin executor slots reservation policy for the scheduler to evenly assign tasks to executors [\#395](https://github.com/apache/arrow-ballista/pull/395) ([yahoNanJing](https://github.com/yahoNanJing))
- Improve launch task efficiency by calling LaunchMultiTask [\#394](https://github.com/apache/arrow-ballista/pull/394) ([yahoNanJing](https://github.com/yahoNanJing))
- Cache encoded stage plan [\#393](https://github.com/apache/arrow-ballista/pull/393) ([yahoNanJing](https://github.com/yahoNanJing))
- Remove active execution graph when the related job is successful or failed [\#392](https://github.com/apache/arrow-ballista/pull/392) ([yahoNanJing](https://github.com/yahoNanJing))
- Update flatbuffers requirement from 2.1.2 to 22.9.29 [\#390](https://github.com/apache/arrow-ballista/pull/390) ([dependabot[bot]](https://github.com/apps/dependabot))
- Unified the log level configuration behavior [\#386](https://github.com/apache/arrow-ballista/pull/386) ([r4ntix](https://github.com/r4ntix))
- Add DistinctCount support [\#384](https://github.com/apache/arrow-ballista/pull/384) ([r4ntix](https://github.com/r4ntix))
- Pull-based execution loop improvements [\#380](https://github.com/apache/arrow-ballista/pull/380) ([Dandandan](https://github.com/Dandandan))
- Fix latest commit [\#379](https://github.com/apache/arrow-ballista/pull/379) ([Dandandan](https://github.com/Dandandan))
- Avoid calling scheduler when the executor cannot accept new tasks [\#378](https://github.com/apache/arrow-ballista/pull/378) ([Dandandan](https://github.com/Dandandan))
- Switch to mimalloc and enable by default in executor [\#370](https://github.com/apache/arrow-ballista/pull/370) ([Dandandan](https://github.com/Dandandan))
- Benchmark looks for path with and without extension [\#354](https://github.com/apache/arrow-ballista/pull/354) ([andygrove](https://github.com/andygrove))
- Implement job cancellation in UI [\#349](https://github.com/apache/arrow-ballista/pull/349) ([Dandandan](https://github.com/Dandandan))
- Using local shuffle reader avoid flight rpc call. [\#347](https://github.com/apache/arrow-ballista/pull/347) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Make helm deployable [\#345](https://github.com/apache/arrow-ballista/pull/345) ([avantgardnerio](https://github.com/avantgardnerio))
- Benchmark & UI improvements [\#343](https://github.com/apache/arrow-ballista/pull/343) ([andygrove](https://github.com/andygrove))
- Add `cancel_job` REST API [\#340](https://github.com/apache/arrow-ballista/pull/340) ([tfeda](https://github.com/tfeda))
- Fix labeler [\#337](https://github.com/apache/arrow-ballista/pull/337) ([andygrove](https://github.com/andygrove))
- Upgrade to DataFusion 13.0.0 [\#336](https://github.com/apache/arrow-ballista/pull/336) ([andygrove](https://github.com/andygrove))
- Check executor id consistency when receive stop executor request [\#335](https://github.com/apache/arrow-ballista/pull/335) ([yahoNanJing](https://github.com/yahoNanJing))
- Enable more benchmark serde tests [\#331](https://github.com/apache/arrow-ballista/pull/331) ([andygrove](https://github.com/andygrove))
- Downgrade `docker-compose.yaml` to version 3.3 so that we can support Ubuntu 20.04.4 LTS [\#329](https://github.com/apache/arrow-ballista/pull/329) ([andygrove](https://github.com/andygrove))
- update labeler [\#326](https://github.com/apache/arrow-ballista/pull/326) ([andygrove](https://github.com/andygrove))
- Upgrade to DataFusion 13.0.0-rc1 [\#325](https://github.com/apache/arrow-ballista/pull/325) ([andygrove](https://github.com/andygrove))
- Dependabot stop suggesting arrow and datafusion updates [\#324](https://github.com/apache/arrow-ballista/pull/324) ([andygrove](https://github.com/andygrove))
- Show job stages metrics [\#323](https://github.com/apache/arrow-ballista/pull/323) ([onthebridgetonowhere](https://github.com/onthebridgetonowhere))
- Add helm chart [\#322](https://github.com/apache/arrow-ballista/pull/322) ([avantgardnerio](https://github.com/avantgardnerio))
- Atomic support for enhancement [\#319](https://github.com/apache/arrow-ballista/pull/319) ([metesynnada](https://github.com/metesynnada))
- Allow automatic schema inference when registering csv [\#313](https://github.com/apache/arrow-ballista/pull/313) ([r4ntix](https://github.com/r4ntix))
- Add ability to specify job name and have it show in the job listing page in the UI [\#312](https://github.com/apache/arrow-ballista/pull/312) ([andygrove](https://github.com/andygrove))
- Add REST API to generate DOT graph for individual query stage [\#310](https://github.com/apache/arrow-ballista/pull/310) ([andygrove](https://github.com/andygrove))
- \[UI\] Use tabbed pane with Queries and Executors tabs [\#309](https://github.com/apache/arrow-ballista/pull/309) ([andygrove](https://github.com/andygrove))
- REST API to get query stages [\#305](https://github.com/apache/arrow-ballista/pull/305) ([andygrove](https://github.com/andygrove))
- Add support for SortPreservingMergeExec; fix LIMIT bug [\#304](https://github.com/apache/arrow-ballista/pull/304) ([andygrove](https://github.com/andygrove))
- Add Python script to run benchmarks [\#302](https://github.com/apache/arrow-ballista/pull/302) ([andygrove](https://github.com/andygrove))
- \[UI\] Add ability to view query plans directly in the UI [\#301](https://github.com/apache/arrow-ballista/pull/301) ([onthebridgetonowhere](https://github.com/onthebridgetonowhere))
- Update datafusion.proto [\#299](https://github.com/apache/arrow-ballista/pull/299) ([andygrove](https://github.com/andygrove))
- Replace function `from_proto_binary_op` from upstream [\#298](https://github.com/apache/arrow-ballista/pull/298) ([askoa](https://github.com/askoa))
- Fix dead link in contribution guideline readme file [\#297](https://github.com/apache/arrow-ballista/pull/297) ([onthebridgetonowhere](https://github.com/onthebridgetonowhere))
- UI code cleanup [\#291](https://github.com/apache/arrow-ballista/pull/291) ([KenSuenobu](https://github.com/KenSuenobu))
- Add support for S3 data sources [\#290](https://github.com/apache/arrow-ballista/pull/290) ([andygrove](https://github.com/andygrove))
- Use latest datafusion [\#289](https://github.com/apache/arrow-ballista/pull/289) ([andygrove](https://github.com/andygrove))
- Fix documentation example [\#288](https://github.com/apache/arrow-ballista/pull/288) ([onthebridgetonowhere](https://github.com/onthebridgetonowhere))
- Improve formatting of job status in UI [\#286](https://github.com/apache/arrow-ballista/pull/286) ([andygrove](https://github.com/andygrove))
- Enabled download of dot files from Download icon [\#279](https://github.com/apache/arrow-ballista/pull/279) ([KenSuenobu](https://github.com/KenSuenobu))
- Executor graceful shutdown: Handle SIGTERM [\#278](https://github.com/apache/arrow-ballista/pull/278) ([mingmwang](https://github.com/mingmwang))
- Also run yarn build to catch JavaScript errors in CI [\#271](https://github.com/apache/arrow-ballista/pull/271) ([avantgardnerio](https://github.com/avantgardnerio))
- Store sessions so users can register tables and query them through flight [\#269](https://github.com/apache/arrow-ballista/pull/269) ([avantgardnerio](https://github.com/avantgardnerio))
- Fix compose for Ian [\#268](https://github.com/apache/arrow-ballista/pull/268) ([avantgardnerio](https://github.com/avantgardnerio))
- Task level retry and Stage level retry [\#261](https://github.com/apache/arrow-ballista/pull/261) ([mingmwang](https://github.com/mingmwang))
- Introduce the datafusion-objectstore-hdfs in datafusion-contrib as an object store feature [\#260](https://github.com/apache/arrow-ballista/pull/260) ([yahoNanJing](https://github.com/yahoNanJing))
- Add a feature based object store provider [\#258](https://github.com/apache/arrow-ballista/pull/258) ([yahoNanJing](https://github.com/yahoNanJing))
- Make fetch shuffle partition data in parallel [\#256](https://github.com/apache/arrow-ballista/pull/256) ([yahoNanJing](https://github.com/yahoNanJing))
- Add LaunchMultiTask rpc interface for executor [\#255](https://github.com/apache/arrow-ballista/pull/255) ([yahoNanJing](https://github.com/yahoNanJing))
- CLI uses ballista context instead of datafusion context in local mode [\#252](https://github.com/apache/arrow-ballista/pull/252) ([r4ntix](https://github.com/r4ntix))
- Fix Scheduler UI in Docker image [\#251](https://github.com/apache/arrow-ballista/pull/251) ([andygrove](https://github.com/andygrove))
- Generate into source folder to make IDEs happy [\#247](https://github.com/apache/arrow-ballista/pull/247) ([avantgardnerio](https://github.com/avantgardnerio))
- Use bind host rather than the external host for starting a local executor service [\#245](https://github.com/apache/arrow-ballista/pull/245) ([yahoNanJing](https://github.com/yahoNanJing))
- Add REST endpoint to get DOT graph of a job [\#242](https://github.com/apache/arrow-ballista/pull/242) ([andygrove](https://github.com/andygrove))
- Add list of jobs to scheduler UI [\#241](https://github.com/apache/arrow-ballista/pull/241) ([andygrove](https://github.com/andygrove))
- Clean up job data on both Scheduler and Executor [\#188](https://github.com/apache/arrow-ballista/pull/188) ([mingmwang](https://github.com/mingmwang))
- Update etcd-client requirement from 0.9 to 0.10 [\#111](https://github.com/apache/arrow-ballista/pull/111) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump terser from 4.8.0 to 4.8.1 in /ballista/ui/scheduler [\#91](https://github.com/apache/arrow-ballista/pull/91) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump jsdom from 16.4.0 to 16.7.0 in /ballista/ui/scheduler [\#74](https://github.com/apache/arrow-ballista/pull/74) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump numpy from 1.21.3 to 1.22.0 in /python [\#72](https://github.com/apache/arrow-ballista/pull/72) ([dependabot[bot]](https://github.com/apps/dependabot))


## [0.8.0](https://github.com/apache/arrow-ballista/tree/0.8.0) (2022-09-16)

[Full Changelog](https://github.com/apache/arrow-ballista/compare/0.7.0...0.8.0)

**Implemented enhancements:**

- Executor should use all available cores by default [\#218](https://github.com/apache/arrow-ballista/issues/218)
- Update task status to the task job curator scheduler [\#179](https://github.com/apache/arrow-ballista/issues/179)
- update datafusion and arrow  to 20.0.0 [\#176](https://github.com/apache/arrow-ballista/issues/176)
- No scheduler logs when deployed to k8s [\#165](https://github.com/apache/arrow-ballista/issues/165)
- Upgrade to DataFusion 11.0.0 [\#163](https://github.com/apache/arrow-ballista/issues/163)
- Better encapsulation for ExecutionGraph [\#149](https://github.com/apache/arrow-ballista/issues/149)
- A stage may act as the input of multiple stages [\#144](https://github.com/apache/arrow-ballista/issues/144)
- Executor Lost handling [\#143](https://github.com/apache/arrow-ballista/issues/143)
- Cancel a running query. [\#139](https://github.com/apache/arrow-ballista/issues/139)
- Ignore the previous job\_id inside fill\_reservations\(\) [\#138](https://github.com/apache/arrow-ballista/issues/138)
- Normalize the serialization and deserialization places of protobuf structs [\#137](https://github.com/apache/arrow-ballista/issues/137)
- Remove revive offer event loop [\#136](https://github.com/apache/arrow-ballista/issues/136)
- Remove Keyspace::QueuedJobs [\#133](https://github.com/apache/arrow-ballista/issues/133)
- Spawn a thread for execution plan generation [\#131](https://github.com/apache/arrow-ballista/issues/131)
- Introduce CuratorTaskManager for make an active job be curated by only one scheduler [\#130](https://github.com/apache/arrow-ballista/issues/130)
- Using tokio tracing for log file [\#122](https://github.com/apache/arrow-ballista/issues/122)
- Ballista Executor report plan/operators metrics to Ballista Scheduler when task finish   [\#116](https://github.com/apache/arrow-ballista/issues/116)
- Add timeout settings for Grpc Client [\#114](https://github.com/apache/arrow-ballista/issues/114)
- Add log level config in ballista [\#102](https://github.com/apache/arrow-ballista/issues/102)
- Use another channel to update the status of a task set for executor [\#96](https://github.com/apache/arrow-ballista/issues/96)
- Add config for concurrent\_task in executor [\#94](https://github.com/apache/arrow-ballista/issues/94)
- Ballista should support Arrow FlightSQL  [\#92](https://github.com/apache/arrow-ballista/issues/92)
- Why not include the `ballista-cli` in the member of workspace [\#88](https://github.com/apache/arrow-ballista/issues/88)
- Upgrade dependency of arrow-datafusion to commit d0d5564b8f689a01e542b8c1df829d74d0fab2b0 [\#84](https://github.com/apache/arrow-ballista/issues/84)
- Support  sled path in config file. [\#79](https://github.com/apache/arrow-ballista/issues/79)
- Support for multi-scheduler deployments [\#39](https://github.com/apache/arrow-ballista/issues/39)
- Ballista 0.7.0 Release [\#126](https://github.com/apache/arrow-ballista/issues/126)
- Improvements to Ballista extensibility [\#8](https://github.com/apache/arrow-ballista/issues/8)
- Implement Python bindings for BallistaContext [\#15](https://github.com/apache/arrow-ballista/issues/15)

**Fixed bugs:**

- Run example fails via PushStaged mode [\#214](https://github.com/apache/arrow-ballista/issues/214)
- Config settings in BallistaContext do not get passed to DataFusion context [\#213](https://github.com/apache/arrow-ballista/issues/213)
- Start scheduler fails with arguments "-s PushStaged" [\#207](https://github.com/apache/arrow-ballista/issues/207)
- FlightSQL is broken and CI isn't catching it [\#190](https://github.com/apache/arrow-ballista/issues/190)
- Query fails with "NULL is invalid as a DataFusion scalar value" [\#180](https://github.com/apache/arrow-ballista/issues/180)
- Executor doesn't compile, missing `tokio::signal` [\#171](https://github.com/apache/arrow-ballista/issues/171)
- Unable to build master [\#76](https://github.com/apache/arrow-ballista/issues/76)


## [ballista-0.7.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.7.0) (2022-05-12)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.1.0-rc1...ballista-0.7.0)

**Breaking changes:**

- Make `ExecutionPlan::execute` Sync [\#2434](https://github.com/apache/arrow-datafusion/pull/2434) ([tustvold](https://github.com/tustvold))
- Add `Expr::Exists` to represent EXISTS subquery expression [\#2339](https://github.com/apache/arrow-datafusion/pull/2339) ([andygrove](https://github.com/andygrove))
- Remove dependency from `LogicalPlan::TableScan` to `ExecutionPlan` [\#2284](https://github.com/apache/arrow-datafusion/pull/2284) ([andygrove](https://github.com/andygrove))
- Move logical expression type-coercion code from `physical-expr` crate to `expr` crate [\#2257](https://github.com/apache/arrow-datafusion/pull/2257) ([andygrove](https://github.com/andygrove))
- feat: 2061 create external table ddl table partition cols [\#2099](https://github.com/apache/arrow-datafusion/pull/2099) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([jychen7](https://github.com/jychen7))
- Reorganize the project folders [\#2081](https://github.com/apache/arrow-datafusion/pull/2081) ([yahoNanJing](https://github.com/yahoNanJing))
- Support more ScalarFunction in Ballista [\#2008](https://github.com/apache/arrow-datafusion/pull/2008) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Merge dataframe and dataframe imp [\#1998](https://github.com/apache/arrow-datafusion/pull/1998) ([vchag](https://github.com/vchag))
- Rename `ExecutionContext` to `SessionContext`, `ExecutionContextState` to `SessionState`, add `TaskContext` to support multi-tenancy configurations - Part 1 [\#1987](https://github.com/apache/arrow-datafusion/pull/1987) ([mingmwang](https://github.com/mingmwang))
- Add Coalesce function [\#1969](https://github.com/apache/arrow-datafusion/pull/1969) ([msathis](https://github.com/msathis))
- Add Create Schema functionality in SQL [\#1959](https://github.com/apache/arrow-datafusion/pull/1959) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- remove sync constraint of SendableRecordBatchStream [\#1884](https://github.com/apache/arrow-datafusion/pull/1884) ([doki23](https://github.com/doki23))

**Implemented enhancements:**

- Add `CREATE VIEW` [\#2279](https://github.com/apache/arrow-datafusion/pull/2279) ([matthewmturner](https://github.com/matthewmturner))
- \[Ballista\] Support Union in ballista. [\#2098](https://github.com/apache/arrow-datafusion/pull/2098) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add missing aggr\_expr to PhysicalExprNode for Ballista. [\#1989](https://github.com/apache/arrow-datafusion/pull/1989) ([Ted-Jiang](https://github.com/Ted-Jiang))

**Fixed bugs:**

- Ballista integration tests no longer work [\#2440](https://github.com/apache/arrow-datafusion/issues/2440)
- Ballista crates cannot be released from DafaFusion 7.0.0 source release [\#1980](https://github.com/apache/arrow-datafusion/issues/1980)
- protobuf OctetLength should be deserialized as octet\_length, not length [\#1834](https://github.com/apache/arrow-datafusion/pull/1834) ([carols10cents](https://github.com/carols10cents))

**Documentation updates:**

- MINOR: Make crate READMEs consistent [\#2437](https://github.com/apache/arrow-datafusion/pull/2437) ([andygrove](https://github.com/andygrove))
- docs: Update the Ballista dev env instructions [\#2419](https://github.com/apache/arrow-datafusion/pull/2419) ([haoxins](https://github.com/haoxins))
- Revise document of installing ballista pinned to specified version [\#2034](https://github.com/apache/arrow-datafusion/pull/2034) ([WinkerDu](https://github.com/WinkerDu))
- Fix typos \(Datafusion -\> DataFusion\) [\#1993](https://github.com/apache/arrow-datafusion/pull/1993) ([andygrove](https://github.com/andygrove))

**Performance improvements:**

- Introduce StageManager for managing tasks stage by stage [\#1983](https://github.com/apache/arrow-datafusion/pull/1983) ([yahoNanJing](https://github.com/yahoNanJing))

**Closed issues:**

- Make expected result string in unit tests more readable [\#2412](https://github.com/apache/arrow-datafusion/issues/2412)
- remove duplicated `fn aggregate()` in aggregate expression tests [\#2399](https://github.com/apache/arrow-datafusion/issues/2399)
- split `distinct_expression.rs` into `count_distinct.rs` and `array_agg_distinct.rs` [\#2385](https://github.com/apache/arrow-datafusion/issues/2385)
- move sql tests in `context.rs` to corresponding test files in `datafustion/core/tests/sql` [\#2328](https://github.com/apache/arrow-datafusion/issues/2328)
- Date32/Date64 as join keys for merge join [\#2314](https://github.com/apache/arrow-datafusion/issues/2314)
- Error precision and scale for decimal coercion in logic comparison [\#2232](https://github.com/apache/arrow-datafusion/issues/2232)
- Support Multiple row layout [\#2188](https://github.com/apache/arrow-datafusion/issues/2188)
- Discussion: Is Ballista a standalone system or framework [\#1916](https://github.com/apache/arrow-datafusion/issues/1916)

**Merged pull requests:**

- MINOR: Enable multi-statement benchmark queries [\#2507](https://github.com/apache/arrow-datafusion/pull/2507) ([andygrove](https://github.com/andygrove))
- Persist session configs in scheduler [\#2501](https://github.com/apache/arrow-datafusion/pull/2501) ([thinkharderdev](https://github.com/thinkharderdev))
- Update to `sqlparser` `0.17.0` [\#2500](https://github.com/apache/arrow-datafusion/pull/2500) ([alamb](https://github.com/alamb))
- Limit cpu cores used when generating changelog [\#2494](https://github.com/apache/arrow-datafusion/pull/2494) ([andygrove](https://github.com/andygrove))
- MINOR: Parameterize changelog script [\#2484](https://github.com/apache/arrow-datafusion/pull/2484) ([jychen7](https://github.com/jychen7))
- Fix stage key extraction [\#2472](https://github.com/apache/arrow-datafusion/pull/2472) ([thinkharderdev](https://github.com/thinkharderdev))
- Add support for list\_dir\(\) on local fs [\#2467](https://github.com/apache/arrow-datafusion/pull/2467) ([wjones127](https://github.com/wjones127))
- minor: update versions and paths in changelog scripts [\#2429](https://github.com/apache/arrow-datafusion/pull/2429) ([andygrove](https://github.com/andygrove))
- Fix Ballista executing during plan [\#2428](https://github.com/apache/arrow-datafusion/pull/2428) ([tustvold](https://github.com/tustvold))
- Re-organize and rename aggregates physical plan [\#2388](https://github.com/apache/arrow-datafusion/pull/2388) ([yjshen](https://github.com/yjshen))
- Upgrade to arrow 13 [\#2382](https://github.com/apache/arrow-datafusion/pull/2382) ([alamb](https://github.com/alamb))
- Grouped Aggregate in row format [\#2375](https://github.com/apache/arrow-datafusion/pull/2375) ([yjshen](https://github.com/yjshen))
- Stop optimizing queries twice [\#2369](https://github.com/apache/arrow-datafusion/pull/2369) ([andygrove](https://github.com/andygrove))
- Bump follow-redirects from 1.13.2 to 1.14.9 in /ballista/ui/scheduler [\#2325](https://github.com/apache/arrow-datafusion/pull/2325) ([dependabot[bot]](https://github.com/apps/dependabot))
- Move FileType enum from sql module to logical\_plan module [\#2290](https://github.com/apache/arrow-datafusion/pull/2290) ([andygrove](https://github.com/andygrove))
- Add BatchPartitioner \(\#2285\) [\#2287](https://github.com/apache/arrow-datafusion/pull/2287) ([tustvold](https://github.com/tustvold))
- Update uuid requirement from 0.8 to 1.0 [\#2280](https://github.com/apache/arrow-datafusion/pull/2280) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump async from 2.6.3 to 2.6.4 in /ballista/ui/scheduler [\#2277](https://github.com/apache/arrow-datafusion/pull/2277) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump minimist from 1.2.5 to 1.2.6 in /ballista/ui/scheduler [\#2276](https://github.com/apache/arrow-datafusion/pull/2276) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump url-parse from 1.5.1 to 1.5.10 in /ballista/ui/scheduler [\#2275](https://github.com/apache/arrow-datafusion/pull/2275) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump nanoid from 3.1.20 to 3.3.3 in /ballista/ui/scheduler [\#2274](https://github.com/apache/arrow-datafusion/pull/2274) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update to Arrow 12.0.0, update tonic and prost [\#2253](https://github.com/apache/arrow-datafusion/pull/2253) ([alamb](https://github.com/alamb))
- Add ExecutorMetricsCollector interface [\#2234](https://github.com/apache/arrow-datafusion/pull/2234) ([thinkharderdev](https://github.com/thinkharderdev))
- minor: add editor config file [\#2224](https://github.com/apache/arrow-datafusion/pull/2224) ([jackwener](https://github.com/jackwener))
- \[Ballista\] Enable ApproxPercentileWithWeight in Ballista and fill UT  [\#2192](https://github.com/apache/arrow-datafusion/pull/2192) ([Ted-Jiang](https://github.com/Ted-Jiang))
- make nightly clippy happy [\#2186](https://github.com/apache/arrow-datafusion/pull/2186) ([xudong963](https://github.com/xudong963))
- \[Ballista\]Make PhysicalAggregateExprNode has repeated PhysicalExprNode [\#2184](https://github.com/apache/arrow-datafusion/pull/2184) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add LogicalPlan::SubqueryAlias [\#2172](https://github.com/apache/arrow-datafusion/pull/2172) ([andygrove](https://github.com/andygrove))
- Implement fast path of with\_new\_children\(\) in ExecutionPlan [\#2168](https://github.com/apache/arrow-datafusion/pull/2168) ([mingmwang](https://github.com/mingmwang))
- \[MINOR\] ignore suspicious slow test in Ballista [\#2167](https://github.com/apache/arrow-datafusion/pull/2167) ([Ted-Jiang](https://github.com/Ted-Jiang))
- enable explain for ballista [\#2163](https://github.com/apache/arrow-datafusion/pull/2163) ([doki23](https://github.com/doki23))
- Add delimiter for create external table [\#2162](https://github.com/apache/arrow-datafusion/pull/2162) ([matthewmturner](https://github.com/matthewmturner))
- Update sqlparser requirement from 0.15 to 0.16 [\#2152](https://github.com/apache/arrow-datafusion/pull/2152) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add IF NOT EXISTS to `CREATE TABLE` and `CREATE EXTERNAL TABLE` [\#2143](https://github.com/apache/arrow-datafusion/pull/2143) ([matthewmturner](https://github.com/matthewmturner))
- Update quarterly roadmap for Q2 [\#2133](https://github.com/apache/arrow-datafusion/pull/2133) ([matthewmturner](https://github.com/matthewmturner))
- \[Ballista\] Add ballista plugin manager and UDF plugin [\#2131](https://github.com/apache/arrow-datafusion/pull/2131) ([gaojun2048](https://github.com/gaojun2048))
- Serialize scalar UDFs in physical plan [\#2130](https://github.com/apache/arrow-datafusion/pull/2130) ([thinkharderdev](https://github.com/thinkharderdev))
- doc: update release schedule [\#2110](https://github.com/apache/arrow-datafusion/pull/2110) ([jychen7](https://github.com/jychen7))
- Reduce repetition in Decimal binary kernels, upgrade to arrow 11.1 [\#2107](https://github.com/apache/arrow-datafusion/pull/2107) ([alamb](https://github.com/alamb))
- update zlib version to 1.2.12 [\#2106](https://github.com/apache/arrow-datafusion/pull/2106) ([waitingkuo](https://github.com/waitingkuo))
- Add CREATE DATABASE command to SQL [\#2094](https://github.com/apache/arrow-datafusion/pull/2094) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- Refactor SessionContext, BallistaContext to support multi-tenancy configurations - Part 3 [\#2091](https://github.com/apache/arrow-datafusion/pull/2091) ([mingmwang](https://github.com/mingmwang))
- Remove dependency of common for the storage crate [\#2076](https://github.com/apache/arrow-datafusion/pull/2076) ([yahoNanJing](https://github.com/yahoNanJing))
- [MINOR] fix doc in `EXTRACT\(field FROM source\) [\#2074](https://github.com/apache/arrow-datafusion/pull/2074) ([Ted-Jiang](https://github.com/Ted-Jiang))
- \[Bug\]\[Datafusion\] fix TaskContext session\_config bug [\#2070](https://github.com/apache/arrow-datafusion/pull/2070) ([gaojun2048](https://github.com/gaojun2048))
- Short-circuit evaluation for `CaseWhen` [\#2068](https://github.com/apache/arrow-datafusion/pull/2068) ([yjshen](https://github.com/yjshen))
- split datafusion-object-store module [\#2065](https://github.com/apache/arrow-datafusion/pull/2065) ([yahoNanJing](https://github.com/yahoNanJing))
- Change log level for noisy logs [\#2060](https://github.com/apache/arrow-datafusion/pull/2060) ([thinkharderdev](https://github.com/thinkharderdev))
- Update to arrow/parquet 11.0 [\#2048](https://github.com/apache/arrow-datafusion/pull/2048) ([alamb](https://github.com/alamb))
- minor: format comments \(`//` to `// `\) [\#2047](https://github.com/apache/arrow-datafusion/pull/2047) ([jackwener](https://github.com/jackwener))
- use cargo-tomlfmt to check Cargo.toml formatting in CI [\#2033](https://github.com/apache/arrow-datafusion/pull/2033) ([WinkerDu](https://github.com/WinkerDu))
- Refactor SessionContext, SessionState and SessionConfig to support multi-tenancy configurations - Part 2 [\#2029](https://github.com/apache/arrow-datafusion/pull/2029) ([mingmwang](https://github.com/mingmwang))
- Simplify prerequisites for running examples [\#2028](https://github.com/apache/arrow-datafusion/pull/2028) ([doki23](https://github.com/doki23))
- Use SessionContext to parse Expr protobuf [\#2024](https://github.com/apache/arrow-datafusion/pull/2024) ([thinkharderdev](https://github.com/thinkharderdev))
- Fix stuck issue for the load testing of Push-based task scheduling [\#2006](https://github.com/apache/arrow-datafusion/pull/2006) ([yahoNanJing](https://github.com/yahoNanJing))
- Fixing a typo in documentation [\#1997](https://github.com/apache/arrow-datafusion/pull/1997) ([psvri](https://github.com/psvri))
- Fix minor clippy issue [\#1995](https://github.com/apache/arrow-datafusion/pull/1995) ([alamb](https://github.com/alamb))
- Make it possible to only scan part of a parquet file in a partition [\#1990](https://github.com/apache/arrow-datafusion/pull/1990) ([yjshen](https://github.com/yjshen))
- Update Dockerfile to fix integration tests [\#1982](https://github.com/apache/arrow-datafusion/pull/1982) ([andygrove](https://github.com/andygrove))
- Update sqlparser requirement from 0.14 to 0.15 [\#1966](https://github.com/apache/arrow-datafusion/pull/1966) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix logical conflict with protobuf [\#1958](https://github.com/apache/arrow-datafusion/pull/1958) ([alamb](https://github.com/alamb))
- Update to arrow 10.0.0, pyo3 0.16 [\#1957](https://github.com/apache/arrow-datafusion/pull/1957) ([alamb](https://github.com/alamb))
- update jit-related dependencies [\#1953](https://github.com/apache/arrow-datafusion/pull/1953) ([xudong963](https://github.com/xudong963))
- Allow different types of query variables \(`@@var`\) rather than just string [\#1943](https://github.com/apache/arrow-datafusion/pull/1943) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([maxburke](https://github.com/maxburke))
- Pruning serialization [\#1941](https://github.com/apache/arrow-datafusion/pull/1941) ([thinkharderdev](https://github.com/thinkharderdev))
- Fix select from EmptyExec always return 0 row after optimizer passes [\#1938](https://github.com/apache/arrow-datafusion/pull/1938) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Introduce Ballista query stage scheduler [\#1935](https://github.com/apache/arrow-datafusion/pull/1935) ([yahoNanJing](https://github.com/yahoNanJing))
- Add db benchmark script [\#1928](https://github.com/apache/arrow-datafusion/pull/1928) ([matthewmturner](https://github.com/matthewmturner))
- fix a typo [\#1919](https://github.com/apache/arrow-datafusion/pull/1919) ([vchag](https://github.com/vchag))
- \[MINOR\] Update copyright year in Docs [\#1918](https://github.com/apache/arrow-datafusion/pull/1918) ([alamb](https://github.com/alamb))
- add metadata to DFSchema, close \#1806. [\#1914](https://github.com/apache/arrow-datafusion/pull/1914) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([jiacai2050](https://github.com/jiacai2050))
- Refactor scheduler state mod [\#1913](https://github.com/apache/arrow-datafusion/pull/1913) ([yahoNanJing](https://github.com/yahoNanJing))
- Refactor the event channel [\#1912](https://github.com/apache/arrow-datafusion/pull/1912) ([yahoNanJing](https://github.com/yahoNanJing))
- Refactor scheduler server [\#1911](https://github.com/apache/arrow-datafusion/pull/1911) ([yahoNanJing](https://github.com/yahoNanJing))
- Clippy fix on nightly [\#1907](https://github.com/apache/arrow-datafusion/pull/1907) ([yjshen](https://github.com/yjshen))
- Updated Rust version to 1.59 in all the files [\#1903](https://github.com/apache/arrow-datafusion/pull/1903) ([NaincyKumariKnoldus](https://github.com/NaincyKumariKnoldus))
- Remove uneeded Mutex in Ballista Client [\#1898](https://github.com/apache/arrow-datafusion/pull/1898) ([alamb](https://github.com/alamb))
- Create a `datafusion-proto` crate for datafusion protobuf serialization [\#1887](https://github.com/apache/arrow-datafusion/pull/1887) ([carols10cents](https://github.com/carols10cents))
- Fix clippy lints [\#1885](https://github.com/apache/arrow-datafusion/pull/1885) ([HaoYang670](https://github.com/HaoYang670))
- Separate cpu-bound \(query-execution\) and IO-bound\(heartbeat\) to  … [\#1883](https://github.com/apache/arrow-datafusion/pull/1883) ([Ted-Jiang](https://github.com/Ted-Jiang))
- \[Minor\] Clean up DecimalArray API Usage [\#1869](https://github.com/apache/arrow-datafusion/pull/1869) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Changes after went through "Datafusion as a library section" [\#1868](https://github.com/apache/arrow-datafusion/pull/1868) ([nonontb](https://github.com/nonontb))
- Remove allow unused imports from ballista-core, then fix all warnings [\#1853](https://github.com/apache/arrow-datafusion/pull/1853) ([carols10cents](https://github.com/carols10cents))
- Update to arrow 9.1.0 [\#1851](https://github.com/apache/arrow-datafusion/pull/1851) ([alamb](https://github.com/alamb))
- move some tests out of context and into sql [\#1846](https://github.com/apache/arrow-datafusion/pull/1846) ([alamb](https://github.com/alamb))
- Fix compiling ballista  in standalone mode, add build to CI [\#1839](https://github.com/apache/arrow-datafusion/pull/1839) ([alamb](https://github.com/alamb))
- Update documentation example for change in API [\#1812](https://github.com/apache/arrow-datafusion/pull/1812) ([alamb](https://github.com/alamb))
- Refactor scheduler state with different management policy for volatile and stable states [\#1810](https://github.com/apache/arrow-datafusion/pull/1810) ([yahoNanJing](https://github.com/yahoNanJing))
- DataFusion + Conbench Integration [\#1791](https://github.com/apache/arrow-datafusion/pull/1791) ([dianaclarke](https://github.com/dianaclarke))
- Enable periodic cleanup of work\_dir directories in ballista executor [\#1783](https://github.com/apache/arrow-datafusion/pull/1783) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Use`eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` kernels from arrow [\#1475](https://github.com/apache/arrow-datafusion/pull/1475) ([alamb](https://github.com/alamb))

## [7.1.0-rc1](https://github.com/apache/arrow-datafusion/tree/7.1.0-rc1) (2022-04-10)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.0.0-rc2...7.1.0-rc1)

**Implemented enhancements:**

- Support substring with three arguments: \(str, from, for\) for DataFrame API and Ballista [\#2092](https://github.com/apache/arrow-datafusion/issues/2092)
- UnionAll support for Ballista [\#2032](https://github.com/apache/arrow-datafusion/issues/2032)
- Separate cpu-bound and IO-bound work in ballista-executor by using diff tokio runtime. [\#1770](https://github.com/apache/arrow-datafusion/issues/1770)
- \[Ballista\] Introduce DAGScheduler for better managing the stage-based task scheduling [\#1704](https://github.com/apache/arrow-datafusion/issues/1704)
- \[Ballista\] Support to better manage cluster state, like alive executors, executor available task slots, etc [\#1703](https://github.com/apache/arrow-datafusion/issues/1703)

**Closed issues:**

- Optimize memory usage pattern to avoid "double memory" behavior [\#2149](https://github.com/apache/arrow-datafusion/issues/2149)
- Document approx\_percentile\_cont\_with\_weight in users guide [\#2078](https://github.com/apache/arrow-datafusion/issues/2078)
- \[follow up\]cleaning up statements.remove\(0\) [\#1986](https://github.com/apache/arrow-datafusion/issues/1986)
- Formatting error on documentation for Python [\#1873](https://github.com/apache/arrow-datafusion/issues/1873)
- Remove duplicate tests from `test_const_evaluator_scalar_functions` [\#1727](https://github.com/apache/arrow-datafusion/issues/1727)
- Question: Is the Ballista project providing value to the overall DataFusion project? [\#1273](https://github.com/apache/arrow-datafusion/issues/1273)

## [7.0.0-rc2](https://github.com/apache/arrow-datafusion/tree/7.0.0-rc2) (2022-02-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.0.0...7.0.0-rc2)

## [7.0.0](https://github.com/apache/arrow-datafusion/tree/7.0.0) (2022-02-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/6.0.0-rc0...7.0.0)

**Breaking changes:**

- Update `ExecutionPlan` to know about sortedness and repartitioning optimizer pass respect the invariants [\#1776](https://github.com/apache/arrow-datafusion/pull/1776) ([alamb](https://github.com/alamb))
- Update to `arrow 8.0.0` [\#1673](https://github.com/apache/arrow-datafusion/pull/1673) ([alamb](https://github.com/alamb))

**Implemented enhancements:**

- Task assignment between Scheduler and Executors [\#1221](https://github.com/apache/arrow-datafusion/issues/1221)
- Add `approx_median()` aggregate function [\#1729](https://github.com/apache/arrow-datafusion/pull/1729) ([realno](https://github.com/realno))
- \[Ballista\] Add Decimal128, Date64, TimestampSecond, TimestampMillisecond, Interv… [\#1659](https://github.com/apache/arrow-datafusion/pull/1659) ([gaojun2048](https://github.com/gaojun2048))
- Add `corr` aggregate function [\#1561](https://github.com/apache/arrow-datafusion/pull/1561) ([realno](https://github.com/realno))
- Add `covar`, `covar_pop` and `covar_samp` aggregate functions [\#1551](https://github.com/apache/arrow-datafusion/pull/1551) ([realno](https://github.com/realno))
- Add `approx_quantile()` aggregation function [\#1539](https://github.com/apache/arrow-datafusion/pull/1539) ([domodwyer](https://github.com/domodwyer))
- Initial MemoryManager and DiskManager APIs  for query execution + External Sort implementation [\#1526](https://github.com/apache/arrow-datafusion/pull/1526) ([yjshen](https://github.com/yjshen))
- Add `stddev` and `variance` [\#1525](https://github.com/apache/arrow-datafusion/pull/1525) ([realno](https://github.com/realno))
- Add `rem` operation for Expr [\#1467](https://github.com/apache/arrow-datafusion/pull/1467) ([liukun4515](https://github.com/liukun4515))
- Implement `array_agg` aggregate function [\#1300](https://github.com/apache/arrow-datafusion/pull/1300) ([viirya](https://github.com/viirya))

**Fixed bugs:**

- Ballista context::tests::test\_standalone\_mode test fails [\#1020](https://github.com/apache/arrow-datafusion/issues/1020)
- \[Ballista\] Fix scheduler state mod bug [\#1655](https://github.com/apache/arrow-datafusion/pull/1655) ([gaojun2048](https://github.com/gaojun2048))
- Pass local address host so we do not get mismatch between IPv4 and IP… [\#1466](https://github.com/apache/arrow-datafusion/pull/1466) ([thinkharderdev](https://github.com/thinkharderdev))
- Add Timezone to Scalar::Time\* types,   and better timezone awareness to Datafusion's time types [\#1455](https://github.com/apache/arrow-datafusion/pull/1455) ([maxburke](https://github.com/maxburke))

**Documentation updates:**

- Add dependencies to ballista example documentation [\#1346](https://github.com/apache/arrow-datafusion/pull/1346) ([jgoday](https://github.com/jgoday))
- \[MINOR\] Fix some typos. [\#1310](https://github.com/apache/arrow-datafusion/pull/1310) ([Ted-Jiang](https://github.com/Ted-Jiang))
- fix some clippy warnings from nightly channel [\#1277](https://github.com/apache/arrow-datafusion/pull/1277) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Jimexist](https://github.com/Jimexist))

**Performance improvements:**

- Introduce push-based task scheduling for Ballista [\#1560](https://github.com/apache/arrow-datafusion/pull/1560) ([yahoNanJing](https://github.com/yahoNanJing))

**Closed issues:**

- Track memory usage in Non Limited Operators [\#1569](https://github.com/apache/arrow-datafusion/issues/1569)
- \[Question\] Why does ballista store tables in the client instead of in the SchedulerServer [\#1473](https://github.com/apache/arrow-datafusion/issues/1473)
- Why use the expr types before coercion to get the result type? [\#1358](https://github.com/apache/arrow-datafusion/issues/1358)
- A problem about the projection\_push\_down optimizer gathers valid columns  [\#1312](https://github.com/apache/arrow-datafusion/issues/1312)
- apply constant folding to `LogicalPlan::Values` [\#1170](https://github.com/apache/arrow-datafusion/issues/1170)
- reduce usage of `IntoIterator<Item = Expr>` in logical plan builder window fn [\#372](https://github.com/apache/arrow-datafusion/issues/372)

**Merged pull requests:**

- Fix verification scripts for 7.0.0 release [\#1830](https://github.com/apache/arrow-datafusion/pull/1830) ([alamb](https://github.com/alamb))
- update README for ballista [\#1817](https://github.com/apache/arrow-datafusion/pull/1817) ([liukun4515](https://github.com/liukun4515))
- Fix logical conflict [\#1801](https://github.com/apache/arrow-datafusion/pull/1801) ([alamb](https://github.com/alamb))
- Improve the error message and UX of tpch benchmark program [\#1800](https://github.com/apache/arrow-datafusion/pull/1800) ([alamb](https://github.com/alamb))
- Update to sqlparser 0.14 [\#1796](https://github.com/apache/arrow-datafusion/pull/1796) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Update datafusion versions [\#1793](https://github.com/apache/arrow-datafusion/pull/1793) ([matthewmturner](https://github.com/matthewmturner))
- Update datafusion to use arrow 9.0.0 [\#1775](https://github.com/apache/arrow-datafusion/pull/1775) ([alamb](https://github.com/alamb))
- Update parking\_lot requirement from 0.11 to 0.12 [\#1735](https://github.com/apache/arrow-datafusion/pull/1735) ([dependabot[bot]](https://github.com/apps/dependabot))
- substitute `parking_lot::Mutex` for `std::sync::Mutex` [\#1720](https://github.com/apache/arrow-datafusion/pull/1720) ([xudong963](https://github.com/xudong963))
- Create ListingTableConfig which includes file format and schema inference [\#1715](https://github.com/apache/arrow-datafusion/pull/1715) ([matthewmturner](https://github.com/matthewmturner))
- Support `create_physical_expr` and `ExecutionContextState` or `DefaultPhysicalPlanner` for faster speed [\#1700](https://github.com/apache/arrow-datafusion/pull/1700) ([alamb](https://github.com/alamb))
- Use NamedTempFile rather than `String` in DiskManager [\#1680](https://github.com/apache/arrow-datafusion/pull/1680) ([alamb](https://github.com/alamb))
- Abstract over logical and physical plan representations in Ballista [\#1677](https://github.com/apache/arrow-datafusion/pull/1677) ([thinkharderdev](https://github.com/thinkharderdev))
- upgrade clap to version 3 [\#1672](https://github.com/apache/arrow-datafusion/pull/1672) ([Jimexist](https://github.com/Jimexist))
- Improve configuration and resource use of `MemoryManager` and `DiskManager` [\#1668](https://github.com/apache/arrow-datafusion/pull/1668) ([alamb](https://github.com/alamb))
- Make `MemoryManager` and `MemoryStream` public [\#1664](https://github.com/apache/arrow-datafusion/pull/1664) ([yjshen](https://github.com/yjshen))
- Consolidate Schema and RecordBatch projection [\#1638](https://github.com/apache/arrow-datafusion/pull/1638) ([alamb](https://github.com/alamb))
- Update hashbrown requirement from 0.11 to 0.12 [\#1631](https://github.com/apache/arrow-datafusion/pull/1631) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update etcd-client requirement from 0.7 to 0.8 [\#1626](https://github.com/apache/arrow-datafusion/pull/1626) ([dependabot[bot]](https://github.com/apps/dependabot))
- update nightly version [\#1597](https://github.com/apache/arrow-datafusion/pull/1597) ([Jimexist](https://github.com/Jimexist))
- Add support show tables and show columns for ballista [\#1593](https://github.com/apache/arrow-datafusion/pull/1593) ([gaojun2048](https://github.com/gaojun2048))
- minor: improve the benchmark readme [\#1567](https://github.com/apache/arrow-datafusion/pull/1567) ([xudong963](https://github.com/xudong963))
- Consolidate `batch_size` configuration in `ExecutionConfig`, `RuntimeConfig` and `PhysicalPlanConfig` [\#1562](https://github.com/apache/arrow-datafusion/pull/1562) ([yjshen](https://github.com/yjshen))
- Update to rust 1.58 [\#1557](https://github.com/apache/arrow-datafusion/pull/1557) ([xudong963](https://github.com/xudong963))
- support mathematics operation for decimal data type [\#1554](https://github.com/apache/arrow-datafusion/pull/1554) ([liukun4515](https://github.com/liukun4515))
- Make call SchedulerServer::new once in ballista-scheduler process [\#1537](https://github.com/apache/arrow-datafusion/pull/1537) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add load test command in tpch.rs. [\#1530](https://github.com/apache/arrow-datafusion/pull/1530) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Remove one copy of ballista datatype serialization code [\#1524](https://github.com/apache/arrow-datafusion/pull/1524) ([alamb](https://github.com/alamb))
- Update to arrow-7.0.0 [\#1523](https://github.com/apache/arrow-datafusion/pull/1523) ([alamb](https://github.com/alamb))
- Workaround build failure: Pin quote to 1.0.10 [\#1499](https://github.com/apache/arrow-datafusion/pull/1499) ([alamb](https://github.com/alamb))
- add rfcs for datafusion [\#1490](https://github.com/apache/arrow-datafusion/pull/1490) ([xudong963](https://github.com/xudong963))
- support comparison for decimal data type and refactor the binary coercion rule [\#1483](https://github.com/apache/arrow-datafusion/pull/1483) ([liukun4515](https://github.com/liukun4515))
- Update arrow-rs to 6.4.0 and replace boolean comparison in datafusion with arrow compute kernel [\#1446](https://github.com/apache/arrow-datafusion/pull/1446) ([xudong963](https://github.com/xudong963))
- support cast/try\_cast for decimal: signed numeric to decimal [\#1442](https://github.com/apache/arrow-datafusion/pull/1442) ([liukun4515](https://github.com/liukun4515))
- use 0.13 sql parser [\#1435](https://github.com/apache/arrow-datafusion/pull/1435) ([Jimexist](https://github.com/Jimexist))
- Clarify communication on bi-weekly sync [\#1427](https://github.com/apache/arrow-datafusion/pull/1427) ([alamb](https://github.com/alamb))
- Minimize features [\#1399](https://github.com/apache/arrow-datafusion/pull/1399) ([carols10cents](https://github.com/carols10cents))
- Update rust vesion to 1.57 [\#1395](https://github.com/apache/arrow-datafusion/pull/1395) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Add coercion rules for AggregateFunctions [\#1387](https://github.com/apache/arrow-datafusion/pull/1387) ([liukun4515](https://github.com/liukun4515))
- upgrade the arrow-rs version [\#1385](https://github.com/apache/arrow-datafusion/pull/1385) ([liukun4515](https://github.com/liukun4515))
- Extract logical plan: rename the plan name \(follow up\) [\#1354](https://github.com/apache/arrow-datafusion/pull/1354) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([liukun4515](https://github.com/liukun4515))
- upgrade arrow-rs to 6.2.0 [\#1334](https://github.com/apache/arrow-datafusion/pull/1334) ([liukun4515](https://github.com/liukun4515))
- Update release instructions [\#1331](https://github.com/apache/arrow-datafusion/pull/1331) ([alamb](https://github.com/alamb))
- Extract Aggregate, Sort, and Join to struct from AggregatePlan [\#1326](https://github.com/apache/arrow-datafusion/pull/1326) ([matthewmturner](https://github.com/matthewmturner))
- Extract `EmptyRelation`, `Limit`, `Values` from `LogicalPlan` [\#1325](https://github.com/apache/arrow-datafusion/pull/1325) ([liukun4515](https://github.com/liukun4515))
- Extract CrossJoin, Repartition, Union in LogicalPlan [\#1322](https://github.com/apache/arrow-datafusion/pull/1322) ([liukun4515](https://github.com/liukun4515))
- Extract Explain, Analyze, Extension in LogicalPlan as independent struct [\#1317](https://github.com/apache/arrow-datafusion/pull/1317) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Extract CreateMemoryTable, DropTable, CreateExternalTable in LogicalPlan as independent struct [\#1311](https://github.com/apache/arrow-datafusion/pull/1311) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([liukun4515](https://github.com/liukun4515))
- Extract Projection, Filter, Window in LogicalPlan as independent struct [\#1309](https://github.com/apache/arrow-datafusion/pull/1309) ([ic4y](https://github.com/ic4y))
- Add PSQL comparison tests for except, intersect [\#1292](https://github.com/apache/arrow-datafusion/pull/1292) ([mrob95](https://github.com/mrob95))
- Extract logical plans in LogicalPlan as independent struct: TableScan [\#1290](https://github.com/apache/arrow-datafusion/pull/1290) ([xudong963](https://github.com/xudong963))

## [6.0.0-rc0](https://github.com/apache/arrow-datafusion/tree/6.0.0-rc0) (2021-11-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/6.0.0...6.0.0-rc0)

## [6.0.0](https://github.com/apache/arrow-datafusion/tree/6.0.0) (2021-11-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/ballista-0.6.0...6.0.0)


## [ballista-0.6.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.6.0) (2021-11-13)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/ballista-0.5.0...ballista-0.6.0)

**Breaking changes:**

- File partitioning for ListingTable [\#1141](https://github.com/apache/arrow-datafusion/pull/1141) ([rdettai](https://github.com/rdettai))
- Register tables in BallistaContext using TableProviders instead of Dataframe [\#1028](https://github.com/apache/arrow-datafusion/pull/1028) ([rdettai](https://github.com/rdettai))
- Make TableProvider.scan\(\) and PhysicalPlanner::create\_physical\_plan\(\) async [\#1013](https://github.com/apache/arrow-datafusion/pull/1013) ([rdettai](https://github.com/rdettai))
- Reorganize table providers by table format [\#1010](https://github.com/apache/arrow-datafusion/pull/1010) ([rdettai](https://github.com/rdettai))
- Move CBOs and Statistics to physical plan [\#965](https://github.com/apache/arrow-datafusion/pull/965) ([rdettai](https://github.com/rdettai))
- Update to sqlparser v 0.10.0 [\#934](https://github.com/apache/arrow-datafusion/pull/934) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- FilePartition and PartitionedFile for scanning flexibility [\#932](https://github.com/apache/arrow-datafusion/pull/932) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([yjshen](https://github.com/yjshen))
- Improve SQLMetric APIs, port existing metrics [\#908](https://github.com/apache/arrow-datafusion/pull/908) ([alamb](https://github.com/alamb))
- Add support for EXPLAIN ANALYZE [\#858](https://github.com/apache/arrow-datafusion/pull/858) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Rename concurrency to target\_partitions [\#706](https://github.com/apache/arrow-datafusion/pull/706) ([andygrove](https://github.com/andygrove))

**Implemented enhancements:**

- Update datafusion-cli to support Ballista, or implement new ballista-cli [\#886](https://github.com/apache/arrow-datafusion/issues/886)
- Prepare Ballista crates for publishing [\#509](https://github.com/apache/arrow-datafusion/issues/509)
- Add drop table support [\#1266](https://github.com/apache/arrow-datafusion/pull/1266) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([viirya](https://github.com/viirya))
- use arrow 6.1.0 [\#1255](https://github.com/apache/arrow-datafusion/pull/1255) ([Jimexist](https://github.com/Jimexist))
- Add support for `create table as` via MemTable [\#1243](https://github.com/apache/arrow-datafusion/pull/1243) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Dandandan](https://github.com/Dandandan))
- add values list expression [\#1165](https://github.com/apache/arrow-datafusion/pull/1165) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Jimexist](https://github.com/Jimexist))
- Multiple files per partitions for CSV Avro Json [\#1138](https://github.com/apache/arrow-datafusion/pull/1138) ([rdettai](https://github.com/rdettai))
- Implement INTERSECT & INTERSECT DISTINCT [\#1135](https://github.com/apache/arrow-datafusion/pull/1135) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Simplify file struct abstractions [\#1120](https://github.com/apache/arrow-datafusion/pull/1120) ([rdettai](https://github.com/rdettai))
- Implement `is [not] distinct from` [\#1117](https://github.com/apache/arrow-datafusion/pull/1117) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Dandandan](https://github.com/Dandandan))
- add digest\(utf8, method\) function and refactor all current hash digest functions [\#1090](https://github.com/apache/arrow-datafusion/pull/1090) ([Jimexist](https://github.com/Jimexist))
- \[crypto\] add `blake3` algorithm to `digest` function [\#1086](https://github.com/apache/arrow-datafusion/pull/1086) ([Jimexist](https://github.com/Jimexist))
- \[crypto\] add blake2b and blake2s functions [\#1081](https://github.com/apache/arrow-datafusion/pull/1081) ([Jimexist](https://github.com/Jimexist))
-  Update sqlparser-rs to 0.11 [\#1052](https://github.com/apache/arrow-datafusion/pull/1052) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- remove hard coded partition count in ballista logicalplan deserialization [\#1044](https://github.com/apache/arrow-datafusion/pull/1044) ([xudong963](https://github.com/xudong963))
- Indexed field access for List [\#1006](https://github.com/apache/arrow-datafusion/pull/1006) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Igosuki](https://github.com/Igosuki))
- Update DataFusion to arrow 6.0 [\#984](https://github.com/apache/arrow-datafusion/pull/984) ([alamb](https://github.com/alamb))
- Implement Display for Expr, improve operator display [\#971](https://github.com/apache/arrow-datafusion/pull/971) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- ObjectStore API to read from remote storage systems [\#950](https://github.com/apache/arrow-datafusion/pull/950) ([yjshen](https://github.com/yjshen))
- fixes \#933 replace placeholder fmt\_as fr ExecutionPlan impls [\#939](https://github.com/apache/arrow-datafusion/pull/939) ([tiphaineruy](https://github.com/tiphaineruy))
- Support `NotLike` in Ballista [\#916](https://github.com/apache/arrow-datafusion/pull/916) ([Dandandan](https://github.com/Dandandan))
- Avro Table Provider [\#910](https://github.com/apache/arrow-datafusion/pull/910) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Igosuki](https://github.com/Igosuki))
- Add BaselineMetrics, Timestamp metrics, add for `CoalescePartitionsExec`, rename output\_time -\> elapsed\_compute [\#909](https://github.com/apache/arrow-datafusion/pull/909) ([alamb](https://github.com/alamb))
- \[Ballista\] Add executor last seen info to the ui [\#895](https://github.com/apache/arrow-datafusion/pull/895) ([msathis](https://github.com/msathis))
- add cross join support to ballista [\#891](https://github.com/apache/arrow-datafusion/pull/891) ([houqp](https://github.com/houqp))
- Add Ballista support to DataFusion CLI [\#889](https://github.com/apache/arrow-datafusion/pull/889) ([andygrove](https://github.com/andygrove))
- Add support for PostgreSQL regex match [\#870](https://github.com/apache/arrow-datafusion/pull/870) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([b41sh](https://github.com/b41sh))

**Fixed bugs:**

- Test execution\_plans::shuffle\_writer::tests::test Fail [\#1040](https://github.com/apache/arrow-datafusion/issues/1040)
- Integration test fails to build docker images [\#918](https://github.com/apache/arrow-datafusion/issues/918)
- Ballista: Remove hard-coded concurrency from logical plan serde code [\#708](https://github.com/apache/arrow-datafusion/issues/708)
- How can I make ballista distributed compute work? [\#327](https://github.com/apache/arrow-datafusion/issues/327)
- fix subquery alias [\#1067](https://github.com/apache/arrow-datafusion/pull/1067) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Fix compilation for ballista in stand-alone mode [\#1008](https://github.com/apache/arrow-datafusion/pull/1008) ([Igosuki](https://github.com/Igosuki))

**Documentation updates:**

- Add Ballista roadmap [\#1166](https://github.com/apache/arrow-datafusion/pull/1166) ([andygrove](https://github.com/andygrove))
- Adds note on compatible rust version [\#1097](https://github.com/apache/arrow-datafusion/pull/1097) ([1nF0rmed](https://github.com/1nF0rmed))
- implement `approx_distinct` function using HyperLogLog [\#1087](https://github.com/apache/arrow-datafusion/pull/1087) ([Jimexist](https://github.com/Jimexist))
- Improve User Guide [\#954](https://github.com/apache/arrow-datafusion/pull/954) ([andygrove](https://github.com/andygrove))
- Update plan\_query\_stages doc [\#951](https://github.com/apache/arrow-datafusion/pull/951) ([rdettai](https://github.com/rdettai))
- \[DataFusion\] -  Add show and show\_limit function for DataFrame [\#923](https://github.com/apache/arrow-datafusion/pull/923) ([francis-du](https://github.com/francis-du))
- update docs related to protoc and optional syntax [\#902](https://github.com/apache/arrow-datafusion/pull/902) ([Jimexist](https://github.com/Jimexist))
- Improve Ballista crate README content [\#878](https://github.com/apache/arrow-datafusion/pull/878) ([andygrove](https://github.com/andygrove))

**Performance improvements:**

- optimize build profile for datafusion python binding, cli and ballista [\#1137](https://github.com/apache/arrow-datafusion/pull/1137) ([houqp](https://github.com/houqp))

**Closed issues:**

- InList expr with NULL literals do not work [\#1190](https://github.com/apache/arrow-datafusion/issues/1190)
- update the homepage README to include values, `approx_distinct`, etc. [\#1171](https://github.com/apache/arrow-datafusion/issues/1171)
- \[Python\]: Inconsistencies with Python package name  [\#1011](https://github.com/apache/arrow-datafusion/issues/1011)
- Wanting to contribute to project where to start? [\#983](https://github.com/apache/arrow-datafusion/issues/983)
- delete redundant code [\#973](https://github.com/apache/arrow-datafusion/issues/973)
- How to build DataFusion python wheel  [\#853](https://github.com/apache/arrow-datafusion/issues/853)
- Produce a design for a metrics framework [\#21](https://github.com/apache/arrow-datafusion/issues/21)

**Merged pull requests:**

- \[nit\] simplify ballista executor `CollectExec` impl codes [\#1140](https://github.com/apache/arrow-datafusion/pull/1140) ([panarch](https://github.com/panarch))


For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

## [ballista-0.5.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.5.0) (2021-08-10)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/4.0.0...ballista-0.5.0)

**Breaking changes:**

- \[ballista\] support date\_part and date\_turnc ser/de, pass tpch 7 [\#840](https://github.com/apache/arrow-datafusion/pull/840) ([houqp](https://github.com/houqp))
- Box ScalarValue:Lists, reduce size by half size [\#788](https://github.com/apache/arrow-datafusion/pull/788) ([alamb](https://github.com/alamb))
- Support DataFrame.collect for Ballista DataFrames [\#785](https://github.com/apache/arrow-datafusion/pull/785) ([andygrove](https://github.com/andygrove))
- JOIN conditions are order dependent [\#778](https://github.com/apache/arrow-datafusion/pull/778) ([seddonm1](https://github.com/seddonm1))
- UnresolvedShuffleExec should represent a single shuffle [\#727](https://github.com/apache/arrow-datafusion/pull/727) ([andygrove](https://github.com/andygrove))
- Ballista: Make shuffle partitions configurable in benchmarks [\#702](https://github.com/apache/arrow-datafusion/pull/702) ([andygrove](https://github.com/andygrove))
- Rename MergeExec to CoalescePartitionsExec [\#635](https://github.com/apache/arrow-datafusion/pull/635) ([andygrove](https://github.com/andygrove))
- Ballista: Rename QueryStageExec to ShuffleWriterExec [\#633](https://github.com/apache/arrow-datafusion/pull/633) ([andygrove](https://github.com/andygrove))
- fix 593, reduce cloning by taking ownership in logical planner's `from` fn [\#610](https://github.com/apache/arrow-datafusion/pull/610) ([Jimexist](https://github.com/Jimexist))
- fix join column handling logic for `On` and `Using` constraints [\#605](https://github.com/apache/arrow-datafusion/pull/605) ([houqp](https://github.com/houqp))
- Move ballista standalone mode to client [\#589](https://github.com/apache/arrow-datafusion/pull/589) ([edrevo](https://github.com/edrevo))
- Ballista: Implement map-side shuffle [\#543](https://github.com/apache/arrow-datafusion/pull/543) ([andygrove](https://github.com/andygrove))
- ShuffleReaderExec now supports multiple locations per partition [\#541](https://github.com/apache/arrow-datafusion/pull/541) ([andygrove](https://github.com/andygrove))
- Make external hostname in executor optional [\#232](https://github.com/apache/arrow-datafusion/pull/232) ([edrevo](https://github.com/edrevo))
- Remove namespace from executors [\#75](https://github.com/apache/arrow-datafusion/pull/75) ([edrevo](https://github.com/edrevo))
- Support qualified columns in queries [\#55](https://github.com/apache/arrow-datafusion/pull/55) ([houqp](https://github.com/houqp))
- Read CSV format text from stdin or memory [\#54](https://github.com/apache/arrow-datafusion/pull/54) ([heymind](https://github.com/heymind))
- Remove Ballista DataFrame [\#48](https://github.com/apache/arrow-datafusion/pull/48) ([andygrove](https://github.com/andygrove))
- Use atomics for SQLMetric implementation, remove unused name field [\#25](https://github.com/apache/arrow-datafusion/pull/25) ([returnString](https://github.com/returnString))

**Implemented enhancements:**

- Add crate documentation for Ballista crates [\#830](https://github.com/apache/arrow-datafusion/issues/830)
- Support DataFrame.collect for Ballista DataFrames [\#787](https://github.com/apache/arrow-datafusion/issues/787)
- Ballista: Prep for supporting shuffle correctly, part one [\#736](https://github.com/apache/arrow-datafusion/issues/736)
- Ballista: Implement physical plan serde for ShuffleWriterExec [\#710](https://github.com/apache/arrow-datafusion/issues/710)
- Ballista: Finish implementing shuffle mechanism [\#707](https://github.com/apache/arrow-datafusion/issues/707)
- Rename QueryStageExec to ShuffleWriterExec [\#542](https://github.com/apache/arrow-datafusion/issues/542)
- Ballista ShuffleReaderExec should be able to read from multiple locations per partition [\#540](https://github.com/apache/arrow-datafusion/issues/540)
- \[Ballista\] Use deployments in k8s user guide [\#473](https://github.com/apache/arrow-datafusion/issues/473)
- Ballista refactor QueryStageExec in preparation for map-side shuffle [\#458](https://github.com/apache/arrow-datafusion/issues/458)
- Ballista: Implement map-side of shuffle [\#456](https://github.com/apache/arrow-datafusion/issues/456)
- Refactor Ballista to separate Flight logic from execution logic [\#449](https://github.com/apache/arrow-datafusion/issues/449)
- Use published versions of arrow rather than github shas [\#393](https://github.com/apache/arrow-datafusion/issues/393)
- BallistaContext::collect\(\) logging is too noisy [\#352](https://github.com/apache/arrow-datafusion/issues/352)
- Update Ballista to use new physical plan formatter utility [\#343](https://github.com/apache/arrow-datafusion/issues/343)
- Add Ballista Getting Started documentation [\#329](https://github.com/apache/arrow-datafusion/issues/329)
- Remove references to ballistacompute Docker Hub repo [\#325](https://github.com/apache/arrow-datafusion/issues/325)
- Implement scalable distributed joins [\#63](https://github.com/apache/arrow-datafusion/issues/63)
- Remove hard-coded Ballista version from scripts [\#32](https://github.com/apache/arrow-datafusion/issues/32)
- Implement streaming versions of Dataframe.collect methods [\#789](https://github.com/apache/arrow-datafusion/pull/789) ([andygrove](https://github.com/andygrove))
- Ballista shuffle is finally working as intended, providing scalable distributed joins [\#750](https://github.com/apache/arrow-datafusion/pull/750) ([andygrove](https://github.com/andygrove))
- Update to use arrow 5.0 [\#721](https://github.com/apache/arrow-datafusion/pull/721) ([alamb](https://github.com/alamb))
- Implement serde for ShuffleWriterExec [\#712](https://github.com/apache/arrow-datafusion/pull/712) ([andygrove](https://github.com/andygrove))
- dedup using join column in wildcard expansion [\#678](https://github.com/apache/arrow-datafusion/pull/678) ([houqp](https://github.com/houqp))
- Implement metrics for shuffle read and write [\#676](https://github.com/apache/arrow-datafusion/pull/676) ([andygrove](https://github.com/andygrove))
- Remove hard-coded PartitionMode from Ballista serde [\#637](https://github.com/apache/arrow-datafusion/pull/637) ([andygrove](https://github.com/andygrove))
- Ballista: Implement scalable distributed joins [\#634](https://github.com/apache/arrow-datafusion/pull/634) ([andygrove](https://github.com/andygrove))
- Add Keda autoscaling for ballista in k8s [\#586](https://github.com/apache/arrow-datafusion/pull/586) ([edrevo](https://github.com/edrevo))
- Add some resiliency to lost executors [\#568](https://github.com/apache/arrow-datafusion/pull/568) ([edrevo](https://github.com/edrevo))
- Add `partition by` constructs in window functions and modify logical planning [\#501](https://github.com/apache/arrow-datafusion/pull/501) ([Jimexist](https://github.com/Jimexist))
- Support anti join [\#482](https://github.com/apache/arrow-datafusion/pull/482) ([Dandandan](https://github.com/Dandandan))
- add `order by` construct in window function and logical plans [\#463](https://github.com/apache/arrow-datafusion/pull/463) ([Jimexist](https://github.com/Jimexist))
- Refactor Ballista executor so that FlightService delegates to an Executor struct [\#450](https://github.com/apache/arrow-datafusion/pull/450) ([andygrove](https://github.com/andygrove))
- implement lead and lag built-in window function [\#429](https://github.com/apache/arrow-datafusion/pull/429) ([Jimexist](https://github.com/Jimexist))
- Implement fmt\_as for ShuffleReaderExec [\#400](https://github.com/apache/arrow-datafusion/pull/400) ([andygrove](https://github.com/andygrove))
- Add window expression part 1 - logical and physical planning, structure, to/from proto, and explain, for empty over clause only [\#334](https://github.com/apache/arrow-datafusion/pull/334) ([Jimexist](https://github.com/Jimexist))
- \[breaking change\] fix 265, log should be log10, and add ln [\#271](https://github.com/apache/arrow-datafusion/pull/271) ([Jimexist](https://github.com/Jimexist))
- Allow table providers to indicate their type for catalog metadata [\#205](https://github.com/apache/arrow-datafusion/pull/205) ([returnString](https://github.com/returnString))
- Add query 19 to TPC-H regression tests [\#59](https://github.com/apache/arrow-datafusion/pull/59) ([Dandandan](https://github.com/Dandandan))
- Use arrow eq kernels in CaseWhen expression evaluation [\#52](https://github.com/apache/arrow-datafusion/pull/52) ([Dandandan](https://github.com/Dandandan))
- Add option param for standalone mode [\#42](https://github.com/apache/arrow-datafusion/pull/42) ([djKooks](https://github.com/djKooks))
- \[DataFusion\] Optimize hash join inner workings, null handling fix [\#24](https://github.com/apache/arrow-datafusion/pull/24) ([Dandandan](https://github.com/Dandandan))
- \[Ballista\] Docker files for ui [\#22](https://github.com/apache/arrow-datafusion/pull/22) ([msathis](https://github.com/msathis))

**Fixed bugs:**

- Ballista: TPC-H q3 @ SF=1000 never completes [\#835](https://github.com/apache/arrow-datafusion/issues/835)
- Ballista does not support MIN/MAX aggregate functions [\#832](https://github.com/apache/arrow-datafusion/issues/832)
- Ballista docker images fail to build [\#828](https://github.com/apache/arrow-datafusion/issues/828)
- Ballista: UnresolvedShuffleExec should only have a single stage\_id [\#726](https://github.com/apache/arrow-datafusion/issues/726)
- Ballista integration tests are failing [\#623](https://github.com/apache/arrow-datafusion/issues/623)
- Integration test build failure due to arrow-rs using unstable feature [\#596](https://github.com/apache/arrow-datafusion/issues/596)
- `cargo build` cannot build the project [\#531](https://github.com/apache/arrow-datafusion/issues/531)
- ShuffleReaderExec does not get formatted correctly in displayable physical plan [\#399](https://github.com/apache/arrow-datafusion/issues/399)
- Implement serde for MIN and MAX [\#833](https://github.com/apache/arrow-datafusion/pull/833) ([andygrove](https://github.com/andygrove))
- Ballista: Prep for fixing shuffle mechansim, part 1 [\#738](https://github.com/apache/arrow-datafusion/pull/738) ([andygrove](https://github.com/andygrove))
- Ballista: Shuffle write bug fix [\#714](https://github.com/apache/arrow-datafusion/pull/714) ([andygrove](https://github.com/andygrove))
- honor table name for csv/parquet scan in ballista plan serde [\#629](https://github.com/apache/arrow-datafusion/pull/629) ([houqp](https://github.com/houqp))
- MINOR: Fix integration tests by adding datafusion-cli module to docker image [\#322](https://github.com/apache/arrow-datafusion/pull/322) ([andygrove](https://github.com/andygrove))

**Documentation updates:**

- Add minimal crate documentation for Ballista crates [\#831](https://github.com/apache/arrow-datafusion/pull/831) ([andygrove](https://github.com/andygrove))
- Add Ballista examples [\#775](https://github.com/apache/arrow-datafusion/pull/775) ([andygrove](https://github.com/andygrove))
- Update ballista.proto link in architecture doc [\#502](https://github.com/apache/arrow-datafusion/pull/502) ([terrycorley](https://github.com/terrycorley))
- Update k8s user guide to use deployments [\#474](https://github.com/apache/arrow-datafusion/pull/474) ([edrevo](https://github.com/edrevo))
- use prettier to format md files [\#367](https://github.com/apache/arrow-datafusion/pull/367) ([Jimexist](https://github.com/Jimexist))
- Make it easier for developers to find Ballista documentation [\#330](https://github.com/apache/arrow-datafusion/pull/330) ([andygrove](https://github.com/andygrove))
- Instructions for cross-compiling Ballista to the Raspberry Pi [\#263](https://github.com/apache/arrow-datafusion/pull/263) ([andygrove](https://github.com/andygrove))
- Add install guide in README [\#236](https://github.com/apache/arrow-datafusion/pull/236) ([djKooks](https://github.com/djKooks))

**Performance improvements:**

- Ballista: Avoid sleeping between polling for tasks [\#698](https://github.com/apache/arrow-datafusion/pull/698) ([Dandandan](https://github.com/Dandandan))
- Make BallistaContext::collect streaming [\#535](https://github.com/apache/arrow-datafusion/pull/535) ([edrevo](https://github.com/edrevo))

**Closed issues:**

- Confirm git tagging strategy for releases [\#770](https://github.com/apache/arrow-datafusion/issues/770)
- arrow::util::pretty::pretty\_format\_batches missing [\#769](https://github.com/apache/arrow-datafusion/issues/769)
- move the `assert_batches_eq!` macros to a non part of datafusion [\#745](https://github.com/apache/arrow-datafusion/issues/745)
- fix an issue where aliases are not respected in generating downstream schemas in window expr [\#592](https://github.com/apache/arrow-datafusion/issues/592)
- make the planner to print more succinct and useful information in window function explain clause [\#526](https://github.com/apache/arrow-datafusion/issues/526)
- move window frame module to be in `logical_plan` [\#517](https://github.com/apache/arrow-datafusion/issues/517)
- use a more rust idiomatic way of handling nth\_value [\#448](https://github.com/apache/arrow-datafusion/issues/448)
- Make Ballista not depend on arrow directly [\#446](https://github.com/apache/arrow-datafusion/issues/446)
- create a test with more than one partition for window functions [\#435](https://github.com/apache/arrow-datafusion/issues/435)
- Implement hash-partitioned hash aggregate [\#27](https://github.com/apache/arrow-datafusion/issues/27)
- Consider using GitHub pages for DataFusion/Ballista documentation [\#18](https://github.com/apache/arrow-datafusion/issues/18)
- Add Ballista to default cargo workspace [\#17](https://github.com/apache/arrow-datafusion/issues/17)
- Update "repository" in Cargo.toml [\#16](https://github.com/apache/arrow-datafusion/issues/16)
- Consolidate TPC-H benchmarks [\#6](https://github.com/apache/arrow-datafusion/issues/6)
- \[Ballista\] Fix integration test script [\#4](https://github.com/apache/arrow-datafusion/issues/4)
- Ballista should not have separate DataFrame implementation [\#2](https://github.com/apache/arrow-datafusion/issues/2)

**Merged pull requests:**

- Change datatype of tpch keys from Int32 to UInt64 to support sf=1000 [\#836](https://github.com/apache/arrow-datafusion/pull/836) ([andygrove](https://github.com/andygrove))
- Add ballista-examples to docker build [\#829](https://github.com/apache/arrow-datafusion/pull/829) ([andygrove](https://github.com/andygrove))
- Update dependencies: prost to 0.8 and tonic to 0.5 [\#818](https://github.com/apache/arrow-datafusion/pull/818) ([alamb](https://github.com/alamb))
- Move `hash_array` into hash\_utils.rs [\#807](https://github.com/apache/arrow-datafusion/pull/807) ([alamb](https://github.com/alamb))
- Fix: Update clippy lints for Rust 1.54 [\#794](https://github.com/apache/arrow-datafusion/pull/794) ([alamb](https://github.com/alamb))
- MINOR: Remove unused Ballista query execution code path [\#732](https://github.com/apache/arrow-datafusion/pull/732) ([andygrove](https://github.com/andygrove))
- \[fix\] benchmark run with compose [\#666](https://github.com/apache/arrow-datafusion/pull/666) ([rdettai](https://github.com/rdettai))
- bring back dev scripts for ballista [\#648](https://github.com/apache/arrow-datafusion/pull/648) ([Jimexist](https://github.com/Jimexist))
- Remove unnecessary mutex [\#639](https://github.com/apache/arrow-datafusion/pull/639) ([edrevo](https://github.com/edrevo))
- round trip TPCH queries in tests [\#630](https://github.com/apache/arrow-datafusion/pull/630) ([houqp](https://github.com/houqp))
- Fix build [\#627](https://github.com/apache/arrow-datafusion/pull/627) ([andygrove](https://github.com/andygrove))
- in ballista also check for UI prettier changes [\#578](https://github.com/apache/arrow-datafusion/pull/578) ([Jimexist](https://github.com/Jimexist))
- turn on clippy rule for needless borrow [\#545](https://github.com/apache/arrow-datafusion/pull/545) ([Jimexist](https://github.com/Jimexist))
- reuse datafusion physical planner in ballista building from protobuf [\#532](https://github.com/apache/arrow-datafusion/pull/532) ([Jimexist](https://github.com/Jimexist))
- update cargo.toml in python crate and fix unit test due to hash joins [\#483](https://github.com/apache/arrow-datafusion/pull/483) ([Jimexist](https://github.com/Jimexist))
- make `VOLUME` declaration in tpch datagen docker absolute [\#466](https://github.com/apache/arrow-datafusion/pull/466) ([crepererum](https://github.com/crepererum))
- Refactor QueryStageExec in preparation for implementing map-side shuffle [\#459](https://github.com/apache/arrow-datafusion/pull/459) ([andygrove](https://github.com/andygrove))
- Simplified usage of `use arrow` in ballista. [\#447](https://github.com/apache/arrow-datafusion/pull/447) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Benchmark subcommand to distinguish between DataFusion and Ballista [\#402](https://github.com/apache/arrow-datafusion/pull/402) ([jgoday](https://github.com/jgoday))
- \#352: BallistaContext::collect\(\) logging is too noisy [\#394](https://github.com/apache/arrow-datafusion/pull/394) ([jgoday](https://github.com/jgoday))
- cleanup function return type fn [\#350](https://github.com/apache/arrow-datafusion/pull/350) ([Jimexist](https://github.com/Jimexist))
- Update Ballista to use new physical plan formatter utility [\#344](https://github.com/apache/arrow-datafusion/pull/344) ([andygrove](https://github.com/andygrove))
- Update arrow dependencies again [\#341](https://github.com/apache/arrow-datafusion/pull/341) ([alamb](https://github.com/alamb))
- Remove references to Ballista Docker images published to ballistacompute Docker Hub repo [\#326](https://github.com/apache/arrow-datafusion/pull/326) ([andygrove](https://github.com/andygrove))
- Update arrow-rs deps [\#317](https://github.com/apache/arrow-datafusion/pull/317) ([alamb](https://github.com/alamb))
- Update arrow deps [\#269](https://github.com/apache/arrow-datafusion/pull/269) ([alamb](https://github.com/alamb))
- Enable redundant\_field\_names clippy lint [\#261](https://github.com/apache/arrow-datafusion/pull/261) ([Dandandan](https://github.com/Dandandan))
- Update arrow-rs deps \(to fix build due to flatbuffers update\) [\#224](https://github.com/apache/arrow-datafusion/pull/224) ([alamb](https://github.com/alamb))
- update arrow-rs deps to latest master [\#216](https://github.com/apache/arrow-datafusion/pull/216) ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
