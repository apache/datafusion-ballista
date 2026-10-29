# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for run.py and compare.py that need no cluster.

The manifests are checked as data, and a whole run is driven against a fake
kubectl on the PATH that records its calls and serves a runner log.

Run from benchmarks/cluster:  python3 -m unittest -v test_run
"""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

import compare
import run

EXAMPLE_CONFIG = os.path.join(run.HERE, "cluster.example.toml")


def config(**overrides):
    cfg = run.load_cluster_config(EXAMPLE_CONFIG)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(cfg.get(key), dict) and key != "node_selector":
            cfg[key] = {**cfg[key], **value}
        else:
            cfg[key] = value
    return cfg


def args(*argv):
    return run.parse_args(list(argv))


def containers(manifest):
    return manifest["spec"]["template"]["spec"]["containers"]


def flag_value(arguments, flag):
    for i, a in enumerate(arguments):
        if a == flag:
            return arguments[i + 1]
        if a.startswith(flag + "="):
            return a.split("=", 1)[1]
    raise KeyError(flag)


def conf_value(command, key):
    for i, a in enumerate(command):
        if a == "--conf" and command[i + 1].startswith(key + "="):
            return command[i + 1].split("=", 1)[1]
    raise KeyError(key)


class ConfigTest(unittest.TestCase):
    def test_example_config_loads_with_defaults(self):
        cfg = run.load_cluster_config(EXAMPLE_CONFIG)
        self.assertEqual(cfg["namespace"], "ballista-benchmarks")
        self.assertEqual(cfg["images"]["trino"], "trinodb/trino:483")
        self.assertEqual(cfg["data"]["scale_factor"], 1000)
        self.assertEqual(cfg["node_selector"], {"kubernetes.io/arch": "amd64"})

    def test_unknown_settings_are_rejected(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
            f.write('namespace = "x"\nnamepsace = "y"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                run.load_cluster_config(f.name)
            self.assertIn("namepsace", str(ctx.exception))
        finally:
            os.unlink(f.name)

    def test_query_numbers(self):
        self.assertIsNone(run.query_numbers("tpch", None))
        self.assertEqual(run.query_numbers("tpch", "q01,6, Q14"), [1, 6, 14])
        self.assertEqual(run.query_numbers("tpcds", "99"), [99])
        with self.assertRaises(SystemExit):
            run.query_numbers("tpch", "23")
        with self.assertRaises(SystemExit):
            run.query_numbers("tpch", "q1a")

    def test_iterations_default_per_engine(self):
        self.assertEqual(args("--engine", "ballista").iterations, 2)
        self.assertEqual(args("--engine", "spark").iterations, 2)
        self.assertEqual(args("--engine", "trino").iterations, 3)
        self.assertEqual(args("--engine", "trino", "--iterations", "1").iterations, 1)

    def test_trino_runs_tpch_only(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            args("--engine", "trino", "--benchmark", "tpcds")

    def test_missing_image_is_reported(self):
        cfg = config(images={"ballista": ""})
        with self.assertRaises(SystemExit) as ctx:
            run.ballista_manifests(args("--engine", "ballista"), cfg, "r", "s3://b/tpch", None)
        self.assertIn("images.ballista", str(ctx.exception))


class PodSpecTest(unittest.TestCase):
    def test_cluster_settings_reach_every_pod(self):
        cfg = config(env_secret="aws-creds", tolerations=[{"key": "k", "operator": "Exists"}],
                     image_pull_secrets=["regcred"], node_selector={"pool": "bench"})
        for engine in run.ENGINES:
            _, cluster, _, runner, _ = run.MANIFESTS[engine](
                args("--engine", engine), cfg, "r", "s3://b/tpch", None)
            pods = [m for m in cluster + [runner] if m["kind"] in ("Deployment", "Job")]
            self.assertTrue(pods, engine)
            for m in pods:
                spec = m["spec"]["template"]["spec"]
                self.assertEqual(spec["serviceAccountName"], "benchmark", engine)
                self.assertEqual(spec["nodeSelector"], {"pool": "bench"})
                self.assertEqual(spec["tolerations"], [{"key": "k", "operator": "Exists"}])
                self.assertEqual(spec["imagePullSecrets"], [{"name": "regcred"}])
                for c in spec["containers"]:
                    self.assertEqual(c["envFrom"], [{"secretRef": {"name": "aws-creds"}}])
                    self.assertIn({"name": "AWS_REGION", "value": "us-east-1"}, c["env"])
                annotations = m["spec"]["template"]["metadata"]["annotations"]
                self.assertEqual(annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"], "false")
                self.assertEqual(m["metadata"]["labels"]["benchmark-run"], "r")

    def test_no_secret_no_selector(self):
        cfg = config(env_secret="", node_selector={}, tolerations=[], image_pull_secrets=[])
        spec = run.pod_spec(cfg, [{"name": "c"}])
        for key in ("nodeSelector", "tolerations", "imagePullSecrets"):
            self.assertNotIn(key, spec)
        self.assertNotIn("envFrom", spec["containers"][0])

    def test_scratch_volume_is_a_pvc_with_a_storage_class_else_an_empty_dir(self):
        volume = run.scratch_volume(config(), "work", "r", "app", "1000Gi")
        claim = volume["ephemeral"]["volumeClaimTemplate"]["spec"]
        self.assertEqual(claim["storageClassName"], "gp3")
        self.assertEqual(claim["resources"]["requests"]["storage"], "1000Gi")
        self.assertEqual(run.scratch_volume(config(storage_class=""), "work", "r", "app", "1Gi"),
                         {"name": "work", "emptyDir": {}})


class BallistaTest(unittest.TestCase):
    def manifests(self, *argv, numbers=None, benchmark_path="s3://b/tpch"):
        a = args("--engine", "ballista", *argv)
        return run.ballista_manifests(a, config(), "tpch-ballista-abc", benchmark_path, numbers)

    def test_executors_match_the_published_configuration(self):
        _, cluster, deployments, runner, settings = self.manifests()
        executor = next(m for m in cluster if m["metadata"]["name"].endswith("-executor"))
        self.assertEqual(executor["spec"]["replicas"], 32)
        (c,) = containers(executor)
        self.assertEqual(flag_value(c["args"], "--vcores"), "8")
        self.assertEqual(flag_value(c["args"], "--memory-pool-size"), "48103633715")  # 70% of 64 GiB
        self.assertEqual(flag_value(c["args"], "--work-dir"), "/data")
        self.assertEqual(flag_value(c["args"], "--task-scheduling-policy"), "pull-staged")
        self.assertEqual(flag_value(c["args"], "--grpc-server-max-decoding-message-size"), "134217728")
        self.assertEqual(flag_value(c["args"], "--scheduler-host"), "tpch-ballista-abc-scheduler")
        self.assertEqual(c["resources"]["limits"], {"cpu": "8", "memory": "64Gi"})
        scheduler = next(m for m in cluster if m["kind"] == "Deployment" and "scheduler" in m["metadata"]["name"])
        self.assertEqual(flag_value(containers(scheduler)[0]["args"], "--scheduler-policy"), "pull-staged")
        self.assertEqual(deployments, [("tpch-ballista-abc-scheduler", 1), ("tpch-ballista-abc-executor", 32)])
        self.assertEqual(settings["partitions"], 256)

    def test_runner_script(self):
        _, _, _, runner, _ = self.manifests("--iterations", "3", "--ballista-config", "a.b=1",
                                            "--no-partition-cols", numbers=[1, 6])
        script = containers(runner)[0]["command"][2]
        self.assertEqual(subprocess.run(["bash", "-n"], input=script, text=True).returncode, 0)
        self.assertIn("/opt/ballista/tpch benchmark ballista --format parquet --host tpch-ballista-abc-scheduler"
                      " --port 50050 --path s3://b/tpch --iterations 3 --partitions 256 -c a.b=1"
                      " --no-partition-cols -q 1 --output /tmp/out/1 || RC=$?", script)
        self.assertIn("-q 6 --output /tmp/out/6", script)
        self.assertEqual(script.count(run.RESULTS_MARKER), 2)
        self.assertTrue(script.endswith("exit $RC"))

    def test_tpcds_uses_the_tpcds_runner(self):
        a = args("--engine", "ballista", "--benchmark", "tpcds", "--partitions", "512")
        _, _, _, runner, _ = run.ballista_manifests(a, config(), "r", "s3://b/tpcds", None)
        script = containers(runner)[0]["command"][2]
        self.assertIn("/opt/ballista/tpcds --host r-scheduler", script)
        self.assertIn("--partitions 512", script)
        self.assertIn("tpcds-*.json", script)
        self.assertNotIn(" -q ", script)

    def test_run_info(self):
        self.assertEqual(run.ballista_run_info("x\nBALLISTA-RUN-INFO commit=abc123\ny"),
                         {"ballista_commit": "abc123"})
        self.assertEqual(run.ballista_run_info("BALLISTA-RUN-INFO commit=\n"), {})


class SparkTest(unittest.TestCase):
    def manifests(self, engine="spark", *argv, cfg=None):
        a = args("--engine", engine, *argv)
        return run.spark_manifests(a, cfg or config(), "tpch-spark-abc", "s3://b/tpch", [1, 3])

    def test_spark_submit(self):
        image, cluster, deployments, driver, settings = self.manifests()
        self.assertEqual(image, "registry.example.com/spark-bench:4.1.3")
        self.assertEqual(deployments, [])
        command = containers(driver)[0]["command"]
        self.assertEqual(flag_value(command, "--deploy-mode"), "client")
        self.assertEqual(conf_value(command, "spark.executor.instances"), "32")
        self.assertEqual(conf_value(command, "spark.kubernetes.executor.limit.cores"), "8")
        self.assertEqual(conf_value(command, "spark.executor.cores"), "16")
        self.assertEqual(conf_value(command, "spark.executor.memory"), "64G")
        self.assertEqual(conf_value(command, "spark.sql.shuffle.partitions"), "512")
        self.assertEqual(conf_value(command, "spark.driver.memory"), "48g")
        self.assertEqual(conf_value(command, "spark.driver.host"), "$(POD_IP)")
        self.assertEqual(conf_value(command, "spark.kubernetes.executor.label.benchmark-run"), "tpch-spark-abc")
        prefix = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1"
        self.assertEqual(conf_value(command, f"{prefix}.options.storageClass"), "gp3")
        self.assertEqual(conf_value(command, f"{prefix}.options.claimName"), "OnDemand")
        self.assertNotIn("spark.plugins", " ".join(command))
        tail = command[command.index("/opt/bench/scripts/tpcbench.py"):]
        self.assertEqual(flag_value(tail, "--data"), "s3a://b/tpch")
        self.assertEqual(flag_value(tail, "--query"), "1,3")
        names = {m["metadata"]["name"] for m in cluster}
        self.assertEqual(names, {"tpch-spark-abc-scripts", "tpch-spark-abc-queries", "tpch-spark-abc-conf"})
        queries = next(m for m in cluster if m["metadata"]["name"].endswith("-queries"))
        self.assertEqual(len(queries["data"]), 22)
        scripts = next(m for m in cluster if m["metadata"]["name"].endswith("-scripts"))
        self.assertEqual(set(scripts["data"]), {"tpcbench.py", "tpch_expected_rows.py"})

    def test_comet_adds_its_settings_and_spark_conf_wins(self):
        image, _, _, driver, settings = self.manifests("comet", "--spark-conf", "spark.memory.offHeap.size=16G",
                                                       "--spark-conf", "spark.sql.shuffle.partitions=1024")
        self.assertEqual(image, "registry.example.com/comet-bench:4.1.3")
        command = containers(driver)[0]["command"]
        self.assertEqual(conf_value(command, "spark.plugins"), "org.apache.spark.CometPlugin")
        self.assertEqual(conf_value(command, "spark.shuffle.manager"),
                         "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
        self.assertEqual(conf_value(command, "spark.memory.offHeap.size"), "16G")
        self.assertEqual(conf_value(command, "spark.sql.shuffle.partitions"), "1024")
        # Each key is passed once.
        keys = [command[i + 1].split("=", 1)[0] for i, a in enumerate(command) if a == "--conf"]
        self.assertEqual(len(keys), len(set(keys)))

    def test_without_a_storage_class_spark_uses_its_default_local_dirs(self):
        _, _, _, driver, _ = self.manifests(cfg=config(storage_class=""))
        self.assertNotIn("persistentVolumeClaim", " ".join(containers(driver)[0]["command"]))

    def test_executor_template_carries_the_cluster_settings(self):
        cfg = config(env_secret="aws-creds", tolerations=[{"operator": "Exists"}])
        _, cluster, _, _, _ = self.manifests(cfg=cfg)
        conf = next(m for m in cluster if m["metadata"]["name"].endswith("-conf"))
        template = json.loads(conf["data"]["executor-pod.json"])
        self.assertEqual(template["kind"], "Pod")
        self.assertEqual(template["spec"]["tolerations"], [{"operator": "Exists"}])
        self.assertEqual(template["spec"]["containers"][0]["envFrom"], [{"secretRef": {"name": "aws-creds"}}])
        # The apache/spark image runs as 185, and a fresh shuffle volume is owned by root.
        self.assertEqual(template["spec"]["securityContext"], {"fsGroup": 185})

    def test_s3a(self):
        self.assertEqual(run.to_s3a("s3://b/x"), "s3a://b/x")
        self.assertEqual(run.to_s3a("gs://b/x"), "gs://b/x")


class TrinoTest(unittest.TestCase):
    def manifests(self, *argv, cfg=None):
        a = args("--engine", "trino", *argv)
        return run.trino_manifests(a, cfg or config(), "tpch-trino-abc", "s3://b/tpch/sf1000", None)

    def test_memory_settings_fit_the_heap(self):
        per_node, headroom = run.trino_memory_settings(64)
        self.assertEqual((per_node, headroom), (38, 6))
        self.assertLess(per_node + headroom, 64 * 0.8)

    def test_properties(self):
        _, cluster, deployments, runner, settings = self.manifests("--executors", "4")
        etc = next(m for m in cluster if m["metadata"]["name"].endswith("-trino-etc"))["data"]
        self.assertIn("node-scheduler.include-coordinator=false", etc["coordinator.config.properties"])
        self.assertIn("query.max-memory=152GB", etc["worker.config.properties"])
        self.assertIn("query.max-memory-per-node=38GB", etc["worker.config.properties"])
        self.assertNotIn("spill", etc["worker.config.properties"])
        self.assertIn("fs.s3.enabled=true", etc["hive.properties"])
        self.assertIn("s3.region=us-east-1", etc["hive.properties"])
        self.assertIn("hive.metastore.catalog.dir=s3://my-bucket/trino-metastore/483/b-tpch-sf1000-",
                      etc["hive.properties"])
        self.assertEqual(deployments, [("tpch-trino-abc-coordinator", 1), ("tpch-trino-abc-worker", 4)])
        env = {e["name"]: e.get("value") for e in containers(runner)[0]["env"]}
        self.assertEqual(env["EXPECTED_WORKERS"], "4")
        self.assertEqual(env["ANALYZE"], "0")
        self.assertEqual(env["PARTITIONED"], "0")
        self.assertEqual(env["WARMUP"], "1")
        self.assertEqual(env["ITERATIONS"], "3")
        self.assertEqual(settings["metastore_dir"].rsplit("-", 1)[1], "nostats/")

    def test_spill_and_analyze(self):
        _, cluster, _, runner, settings = self.manifests("--trino-spill", "--trino-analyze")
        etc = next(m for m in cluster if m["metadata"]["name"].endswith("-trino-etc"))["data"]
        self.assertIn("spill-enabled=true\nspiller-spill-path=/spill", etc["worker.config.properties"])
        workers = next(m for m in cluster if m["metadata"]["name"].endswith("-worker"))
        spill = next(v for v in workers["spec"]["template"]["spec"]["volumes"] if v["name"] == "spill")
        self.assertIn("ephemeral", spill)
        self.assertTrue(settings["metastore_dir"].endswith("-stats/"))

    def test_metastore_dir_depends_on_the_table_definitions(self):
        partitioned = config(data={"tpch_partitioned": True})
        _, _, _, _, a = self.manifests()
        _, _, _, _, b = self.manifests(cfg=partitioned)
        self.assertNotEqual(a["metastore_dir"], b["metastore_dir"])

    def test_metastore_dir_is_required(self):
        with self.assertRaises(SystemExit) as ctx:
            self.manifests(cfg=config(trino={"metastore_dir": ""}))
        self.assertIn("trino.metastore_dir", str(ctx.exception))


class ResultsTest(unittest.TestCase):
    def test_parse_summaries_reads_every_marker(self):
        logs = (f"noise\n{run.RESULTS_MARKER}\n" + json.dumps({"queries": [{"query": 1}]}) +
                f"\nmore\n{run.RESULTS_MARKER}\n" + json.dumps({"queries": [{"query": 2}]}) + "\n")
        self.assertEqual([s["queries"][0]["query"] for s in run.parse_summaries(logs)], [1, 2])
        self.assertEqual(run.parse_summaries(f"{run.RESULTS_MARKER}\nnot json"), [])

    def test_query_lines_recover_a_run_that_died(self):
        prefix = run.QUERY_RESULT_PREFIXES["trino"]
        logs = f"{prefix}{json.dumps({'query': 3, 'iterations': []})}\n{prefix}{{broken\n"
        self.assertEqual(run.parse_query_lines(logs, prefix), [{"query": 3, "iterations": []}])

    def test_query_result(self):
        q = run.query_result({"query": 1, "iterations": [{"elapsed": 1.0, "row_count": 4},
                                                          {"elapsed": 2.0, "row_count": 4}],
                              "warmup": {"elapsed": 5.0, "error": None}})
        self.assertEqual(q, {"query": 1, "elapsed": [1.0, 2.0], "error": None, "mean": 1.5, "row_count": 4,
                             "warmup": 5.0})
        self.assertEqual(run.query_result({"query": 2, "iterations": []})["error"], "no iterations ran")
        self.assertEqual(run.query_result({"query": 2, "iterations": [], "error": "OOM"})["error"], "OOM")

    def test_compare_table(self):
        a = {"queries": [{"query": 1, "mean": 1.0}, {"query": 2, "mean": 2.5}]}
        b = {"queries": [{"query": 1, "mean": 3.0}, {"query": 2, "error": "boom"}]}
        lines = compare.table([a, b], ["Ballista", "Spark"]).splitlines()
        self.assertEqual(lines[0], "| Query | Ballista (s) | Spark (s) |")
        self.assertEqual(lines[2], "| 1 | 1.00 | 3.00 |")
        self.assertEqual(lines[3], "| 2 | 2.50 | FAIL |")
        self.assertEqual(lines[4], "| **Total** | **3.50** | - |")


FAKE_KUBECTL = textwrap.dedent("""\
    #!/usr/bin/env python3
    # Records each call; answers what run.py asks during a run.
    import json, os, sys
    state = os.environ["FAKE_KUBECTL_DIR"]
    args = sys.argv[1:]
    stdin = sys.stdin.read() if "-f" in args else ""
    with open(os.path.join(state, "calls.jsonl"), "a") as f:
        f.write(json.dumps({"args": args, "stdin": stdin}) + "\\n")
    verb = args[2]  # after --namespace NS
    if verb == "get" and "job" in args:
        print("Complete")
    elif verb == "get":
        print("Running")
    elif verb == "logs" and any(a.startswith("job/") for a in args):
        print(open(os.path.join(state, "runner.log")).read())
    """)


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        bin_dir = os.path.join(self.tmp.name, "bin")
        os.makedirs(bin_dir)
        kubectl = os.path.join(bin_dir, "kubectl")
        with open(kubectl, "w") as f:
            f.write(FAKE_KUBECTL)
        os.chmod(kubectl, 0o755)
        self.env = {"PATH": bin_dir + os.pathsep + os.environ["PATH"], "FAKE_KUBECTL_DIR": self.tmp.name}
        self.results = os.path.join(self.tmp.name, "results")

    def tearDown(self):
        self.tmp.cleanup()

    def write_log(self, text):
        with open(os.path.join(self.tmp.name, "runner.log"), "w") as f:
            f.write(text)

    def calls(self):
        with open(os.path.join(self.tmp.name, "calls.jsonl")) as f:
            return [json.loads(line) for line in f]

    def run_cli(self, *argv):
        return subprocess.run([sys.executable, os.path.join(run.HERE, "run.py"), "--config", EXAMPLE_CONFIG,
                               "--results-dir", self.results, *argv],
                              env={**os.environ, **self.env}, capture_output=True, text=True)

    def test_ballista_run(self):
        summary = {"benchmark_version": "55.0.0", "queries": [
            {"query": 1, "iterations": [{"elapsed": 10.0, "row_count": 4}, {"elapsed": 12.0, "row_count": 4}]}]}
        self.write_log(f"BALLISTA-RUN-INFO commit=abc\n{run.RESULTS_MARKER}\n{json.dumps(summary)}\n")
        result = self.run_cli("--engine", "ballista", "--query", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        verbs = [c["args"][2] for c in self.calls()]
        self.assertEqual(verbs[0], "apply")
        self.assertIn("rollout", verbs)
        self.assertEqual(verbs[-1], "delete")
        applied = [json.loads(c["stdin"]) for c in self.calls() if "apply" in c["args"]]
        kinds = [item["kind"] for manifest in applied for item in manifest["items"]]
        self.assertEqual(kinds, ["Service", "Deployment", "Deployment", "Job"])
        for c in self.calls():
            self.assertEqual(c["args"][:2], ["--namespace", "ballista-benchmarks"])
        (path,) = [p for p in os.listdir(self.results) if p.endswith(".json")]
        with open(os.path.join(self.results, path)) as f:
            results = json.load(f)
        self.assertEqual(results["engine"], "ballista")
        self.assertEqual(results["ballista_commit"], "abc")
        self.assertEqual(results["queries"], [{"query": 1, "elapsed": [10.0, 12.0], "error": None,
                                               "mean": 11.0, "row_count": 4}])
        self.assertTrue(any(p.endswith(".log") for p in os.listdir(self.results)))

    def test_failed_query_fails_the_run_and_still_tears_down(self):
        summary = {"queries": [{"query": 6, "iterations": [], "error": "Resources exhausted"}]}
        self.write_log(f"{run.RESULTS_MARKER}\n{json.dumps(summary)}\n")
        result = self.run_cli("--engine", "spark", "--query", "6")
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAIL", result.stderr)
        self.assertIn("delete", self.calls()[-1]["args"])

    def test_dry_run_prints_the_manifests_and_touches_nothing(self):
        result = self.run_cli("--engine", "trino", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = json.loads(result.stdout)
        self.assertEqual(manifest["kind"], "List")
        self.assertIn("Job", [item["kind"] for item in manifest["items"]])
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name, "calls.jsonl")))


if __name__ == "__main__":
    unittest.main()
