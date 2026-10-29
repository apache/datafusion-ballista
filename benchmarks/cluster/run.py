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

"""Run TPC-H or TPC-DS on Kubernetes with Ballista, Spark, Spark + Comet or Trino.

Each run deploys a fresh cluster for one engine in the namespace that
cluster.toml names, runs the queries from a runner Job inside it, reads the
results from the runner's log, writes them to results/ and deletes everything it
created. Every engine gets the same number of executor (worker) pods with the
same CPU and memory, and reads the same Parquet files.

  ballista  a scheduler, N executors, and the tpch or tpcds runner from this
            directory's parent (benchmarks/), all from one image
  spark     a driver Job running spark/tpcbench.py in client mode, which starts
            N executor pods
  comet     the same, with Comet's settings (conf/comet.conf) and image
  trino     a coordinator, N workers, and a Job running trino/run_queries.py

Only the standard library is used, and kubectl must be on the PATH and point at
the cluster. See README.md for what the cluster needs.
"""

import argparse
import json
import os
import re
import secrets
import shlex
import statistics
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
BENCHMARKS_DIR = os.path.dirname(HERE)
QUERIES_DIRS = {
    "tpch": os.path.join(BENCHMARKS_DIR, "queries"),
    "tpcds": os.path.join(BENCHMARKS_DIR, "queries-tpcds"),
}
NUM_QUERIES = {"tpch": 22, "tpcds": 99}
ENGINES = ["ballista", "spark", "comet", "trino"]

# The runners print this line, then their JSON summary.
RESULTS_MARKER = "===RESULTS-JSON-FILE==="
# ... and, as each query finishes, a line with this prefix, so a run that died
# part way can be recovered from the log.
QUERY_RESULT_PREFIXES = {
    "spark": "SPARK-QUERY-RESULT ",
    "comet": "SPARK-QUERY-RESULT ",
    "trino": "TRINO-QUERY-RESULT ",
}

# Raised from the 16 MiB default: some SF1000 plans (q11, q21, q22) encode
# above it.
BALLISTA_GRPC_MESSAGE_SIZE = 128 * 1024 * 1024
# Each Ballista executor's DataFusion memory pool, as a fraction of its
# container, leaving room for shuffle buffers and the rest of the process.
BALLISTA_MEMORY_POOL_FRACTION = 0.7

# The user and group the apache/spark image runs as. A fresh shuffle volume is
# owned by root, so the pods' fsGroup makes it writable.
SPARK_SECURITY_CONTEXT = {"fsGroup": 185}

# Seconds to wait for each step.
DEPLOY_TIMEOUT = 15 * 60
RUN_TIMEOUT = 6 * 60 * 60


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLUSTER_DEFAULTS = {
    "context": "",
    "namespace": "default",
    "service_account": "default",
    "env_secret": "",
    "region": "us-east-1",
    "storage_class": "",
    "shuffle_volume_size": "1000Gi",
    "node_selector": {},
    "tolerations": [],
    "image_pull_secrets": [],
    "data": {"scale_factor": 1000, "tpch_partitioned": False},
    "images": {"trino": "trinodb/trino:483", "python": "python:3.12-slim"},
    "trino": {"metastore_dir": "", "money_type": "decimal(15, 2)"},
}


def load_cluster_config(path):
    """cluster.toml with the defaults filled in."""
    with open(path, "rb") as f:
        user = tomllib.load(f)
    cfg = {}
    for key, default in CLUSTER_DEFAULTS.items():
        value = user.get(key, default)
        # Sections are merged with their defaults; node_selector is replaced.
        merge = isinstance(default, dict) and key != "node_selector"
        cfg[key] = {**default, **value} if merge else value
    unknown = sorted(set(user) - set(CLUSTER_DEFAULTS))
    if unknown:
        sys.exit(f"{path}: unknown settings {', '.join(unknown)}")
    return cfg


def require(cfg, section, key, why):
    value = cfg[section].get(key)
    if not value:
        sys.exit(f"cluster.toml: set {section}.{key} ({why})")
    return value


def parse_pairs(pairs, flag):
    """KEY=VALUE strings from a repeated flag, as a list of (key, value)."""
    out = []
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key.strip():
            sys.exit(f"{flag} takes KEY=VALUE, got {pair!r}")
        out.append((key.strip(), value.strip()))
    return out


def load_conf_file(path):
    """key=value lines of a Spark .conf file, without comments."""
    pairs = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, _, value = line.partition("=")
                pairs.append((key.strip(), value.strip()))
    return pairs


def to_s3a(path):
    """Spark's S3 connector is the s3a:// scheme."""
    return "s3a://" + path[len("s3://"):] if path.startswith("s3://") else path


def query_numbers(benchmark, query):
    """The query numbers selected by --query ("1", "q01", "1,6,q14"), or None for all."""
    if not query:
        return None
    numbers = []
    for name in query.split(","):
        match = re.fullmatch(r"\s*[qQ]?0*(\d+)\s*", name)
        if not match or not 1 <= int(match.group(1)) <= NUM_QUERIES[benchmark]:
            sys.exit(f"--query: {name!r} is not a {benchmark} query (1 to {NUM_QUERIES[benchmark]})")
        numbers.append(int(match.group(1)))
    return numbers


# ---------------------------------------------------------------------------
# Kubernetes
# ---------------------------------------------------------------------------

class Kube:
    """kubectl against the namespace (and context) in cluster.toml."""

    def __init__(self, cfg, dry_run=False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.base = ["kubectl"]
        if cfg["context"]:
            self.base += ["--context", cfg["context"]]
        self.base += ["--namespace", cfg["namespace"]]

    def run(self, *args, input=None, check=True):
        result = subprocess.run(self.base + list(args), input=input, capture_output=True, text=True)
        if check and result.returncode != 0:
            raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
        return result

    def apply(self, objects):
        manifest = json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}, indent=2)
        if self.dry_run:
            print(manifest)
            return
        self.run("apply", "-f", "-", input=manifest)

    def delete_run(self, run_name):
        """Delete everything labelled with the run, including Spark's executor pods."""
        if self.dry_run:
            return
        self.run("delete", "deployment,service,job,configmap,pod", "-l", f"benchmark-run={run_name}",
                 "--ignore-not-found", "--wait=false", check=False)

    def wait_for_deployment(self, name, timeout=DEPLOY_TIMEOUT):
        return self.run("rollout", "status", f"deployment/{name}", f"--timeout={timeout}s",
                        check=False).returncode == 0

    def describe_pods(self, selector, tail=40):
        """The end of `kubectl describe`: its Events say why a pod is stuck."""
        out = self.run("describe", "pods", "-l", selector, check=False).stdout
        return "\n".join(out.splitlines()[-tail:])

    def wait_for_job(self, name, timeout=RUN_TIMEOUT):
        """Wait for a Job to finish. True if it succeeded.

        A pod that cannot be scheduled or whose image cannot be pulled would look
        like a slow Job, so a pod still Pending after DEPLOY_TIMEOUT fails the wait.
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            conditions = self.run("get", "job", name, "-o", "jsonpath={.status.conditions[*].type}",
                                  check=False).stdout.split()
            if "Complete" in conditions:
                return True
            if "Failed" in conditions:
                return False
            phase = self.run("get", "pods", "-l", f"job-name={name}", "-o",
                             "jsonpath={.items[0].status.phase}", check=False).stdout.strip()
            if phase in ("", "Pending") and time.monotonic() - start > DEPLOY_TIMEOUT:
                print(f"{name} did not start:\n{self.describe_pods(f'job-name={name}')}", file=sys.stderr)
                return False
            time.sleep(15)
        print(f"Timed out after {timeout}s waiting for job {name}", file=sys.stderr)
        return False

    def logs(self, target, tail=-1):
        return self.run("logs", target, f"--tail={tail}", check=False).stdout


def labels(run_name, app):
    return {"app": app, "benchmark-run": run_name}


def metadata(name, run_name, app):
    return {"name": name, "labels": labels(run_name, app)}


def pod_spec(cfg, containers, volumes=(), restart_policy="Always", security_context=None):
    """A pod spec with the scheduling and credential settings from cluster.toml."""
    for container in containers:
        env = container.setdefault("env", [])
        env += [{"name": "AWS_REGION", "value": cfg["region"]},
                {"name": "AWS_DEFAULT_REGION", "value": cfg["region"]}]
        if cfg["env_secret"]:
            container["envFrom"] = [{"secretRef": {"name": cfg["env_secret"]}}]
    spec = {
        "serviceAccountName": cfg["service_account"],
        "enableServiceLinks": False,
        "restartPolicy": restart_policy,
        "containers": containers,
        "volumes": list(volumes),
    }
    if cfg["node_selector"]:
        spec["nodeSelector"] = dict(cfg["node_selector"])
    if cfg["tolerations"]:
        spec["tolerations"] = list(cfg["tolerations"])
    if cfg["image_pull_secrets"]:
        spec["imagePullSecrets"] = [{"name": s} for s in cfg["image_pull_secrets"]]
    if security_context:
        spec["securityContext"] = security_context
    return spec


def pod_template(run_name, app, spec):
    # The cluster autoscaler only reads pod annotations. Losing a pod mid-query
    # fails or hangs the query.
    return {"metadata": {"labels": labels(run_name, app),
                         "annotations": {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}},
            "spec": spec}


def deployment(name, run_name, app, replicas, spec):
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": metadata(name, run_name, app),
        "spec": {"replicas": replicas, "selector": {"matchLabels": labels(run_name, app)},
                 "template": pod_template(run_name, app, spec)},
    }


def service(name, run_name, app, ports, headless=False, publish_not_ready=False):
    spec = {"selector": labels(run_name, app),
            "ports": [{"name": n, "port": p, "targetPort": p} for n, p in ports]}
    if headless:
        spec["clusterIP"] = "None"
    if publish_not_ready:
        spec["publishNotReadyAddresses"] = True
    return {"apiVersion": "v1", "kind": "Service", "metadata": metadata(name, run_name, app), "spec": spec}


def job(name, run_name, app, spec):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata(name, run_name, app),
        "spec": {"backoffLimit": 0, "template": pod_template(run_name, app, spec)},
    }


def configmap(name, run_name, app, data):
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata(name, run_name, app), "data": data}


def scratch_volume(cfg, name, run_name, app, size):
    """A volume of its own for each pod (shuffle files, spill), deleted with the pod.

    With a storage class it is a fresh PersistentVolumeClaim, so the I/O does
    not compete with the node's root disk. Without one it is an emptyDir.
    """
    if not cfg["storage_class"]:
        return {"name": name, "emptyDir": {}}
    return {"name": name, "ephemeral": {"volumeClaimTemplate": {
        "metadata": {"labels": labels(run_name, app)},
        "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": cfg["storage_class"],
                 "resources": {"requests": {"storage": size}}},
    }}}


def resources(cpu, memory_gib, request_cpu=None):
    return {"requests": {"cpu": str(request_cpu or cpu), "memory": f"{memory_gib}Gi"},
            "limits": {"cpu": str(cpu), "memory": f"{memory_gib}Gi"}}


def read_files(directory, names):
    data = {}
    for name in names:
        with open(os.path.join(directory, name)) as f:
            data[name] = f.read()
    return data


def query_files(benchmark):
    directory = QUERIES_DIRS[benchmark]
    names = sorted(n for n in os.listdir(directory) if re.fullmatch(r"q\d+\.sql", n))
    return read_files(directory, names)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

def parse_summaries(logs):
    """Every JSON summary that follows a results marker in a log."""
    summaries = []
    decoder = json.JSONDecoder()
    for part in logs.split(RESULTS_MARKER)[1:]:
        try:
            summaries.append(decoder.raw_decode(part.lstrip())[0])
        except json.JSONDecodeError:
            pass
    return summaries


def parse_query_lines(logs, prefix):
    """The per-query lines, for a runner that died before printing its summary."""
    queries = []
    for line in logs.splitlines():
        if line.startswith(prefix):
            try:
                queries.append(json.loads(line[len(prefix):]))
            except json.JSONDecodeError:
                pass
    return queries


def query_result(q):
    """One query in the common results format."""
    times = [it["elapsed"] for it in q.get("iterations", []) if "elapsed" in it]
    result = {"query": q["query"], "elapsed": times, "error": q.get("error")}
    if not times and not result["error"]:
        result["error"] = "no iterations ran"
    if times:
        result["mean"] = round(statistics.mean(times), 3)
        result["row_count"] = q["iterations"][-1].get("row_count")
    if (q.get("warmup") or {}).get("elapsed") is not None:
        result["warmup"] = q["warmup"]["elapsed"]
    return result


def write_results(results_dir, run, summaries, queries):
    """Write the run's results. Returns the path."""
    run["queries"] = [query_result(q) for q in sorted(queries, key=lambda q: q["query"])]
    run["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    run["runner_summaries"] = summaries
    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, f"{run['run']}.json")
    with open(path, "w") as f:
        json.dump(run, f, indent=2)
    return path


def print_results(run):
    print(f"\n{'query':>6}{'mean (s)':>11}{'rows':>8}  iterations", file=sys.stderr)
    total = 0.0
    for q in run["queries"]:
        if q.get("error"):
            print(f"{q['query']:>6}{'FAIL':>11}{'':>8}  {q['error'][:100]}", file=sys.stderr)
            continue
        total += q["mean"]
        times = ", ".join(f"{t:.2f}" for t in q["elapsed"])
        print(f"{q['query']:>6}{q['mean']:>11.2f}{q.get('row_count', ''):>8}  {times}", file=sys.stderr)
    print(f"{'total':>6}{total:>11.2f}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Ballista
# ---------------------------------------------------------------------------

def ballista_runner_script(args, run_name, data_path, numbers):
    """The shell script the runner Job runs: the tpch or tpcds binary, then its summaries.

    The binary writes its summary to a file, so the script prints it after the
    results marker once the binary exits, whether or not it succeeded. With
    several --query values the binary runs once per query.
    """
    if args.benchmark == "tpch":
        binary = ["/opt/ballista/tpch", "benchmark", "ballista", "--format", "parquet"]
        summary_glob = "tpch-*.json"
    else:
        binary = ["/opt/ballista/tpcds"]
        summary_glob = "tpcds-*.json"
    command = binary + [
        "--host", f"{run_name}-scheduler", "--port", "50050",
        "--path", data_path,
        "--iterations", str(args.iterations),
        "--partitions", str(args.partitions or args.executors * args.vcores),
    ]
    for key, value in parse_pairs(args.ballista_config, "--ballista-config"):
        command += ["-c", f"{key}={value}"]
    if args.no_partition_cols:
        command.append("--no-partition-cols")
    runs = [(str(n), ["-q", str(n)]) for n in numbers] if numbers else [("all", [])]
    lines = [
        "cd /opt/ballista",
        'echo "BALLISTA-RUN-INFO commit=$(sed -n \'s/^COMMIT=//p\' BUILD_INFO 2>/dev/null)"',
        "RC=0",
    ]
    for label, query_args in runs:
        out = f"/tmp/out/{label}"
        lines += [
            f"mkdir -p {out}",
            shlex.join(command + query_args + ["--output", out]) + " || RC=$?",
            f"echo {RESULTS_MARKER}",
            f"cat {out}/{summary_glob} 2>/dev/null || echo '{{}}'",
        ]
    lines.append("exit $RC")
    return "\n".join(lines)


def ballista_manifests(args, cfg, run_name, data_path, numbers):
    image = require(cfg, "images", "ballista", "the image built from docker/ballista.Dockerfile")
    grpc = [f"--grpc-server-max-decoding-message-size={BALLISTA_GRPC_MESSAGE_SIZE}",
            f"--grpc-server-max-encoding-message-size={BALLISTA_GRPC_MESSAGE_SIZE}"]
    scheduler = deployment(f"{run_name}-scheduler", run_name, "ballista-scheduler", 1, pod_spec(cfg, [{
        "name": "scheduler",
        "image": image,
        "command": ["/opt/ballista/ballista-scheduler"],
        "args": ["--bind-host", "0.0.0.0", "--bind-port", "50050",
                 "--external-host", f"{run_name}-scheduler",
                 f"--scheduler-policy={args.scheduling_policy}", *grpc],
        "env": [{"name": "RUST_LOG", "value": "ballista=info"}],
        "ports": [{"containerPort": 50050, "name": "grpc"}],
        "resources": resources(8, 16, request_cpu=4),
        "readinessProbe": {"tcpSocket": {"port": 50050}, "periodSeconds": 10},
    }]))
    pool_bytes = int(args.executor_memory_gib * BALLISTA_MEMORY_POOL_FRACTION * 1024**3)
    executor = deployment(f"{run_name}-executor", run_name, "ballista-executor", args.executors, pod_spec(cfg, [{
        "name": "executor",
        "image": image,
        "command": ["/opt/ballista/ballista-executor"],
        "args": ["--bind-host", "0.0.0.0", "--bind-port", "50051",
                 "--external-host", "$(POD_IP)",
                 "--scheduler-host", f"{run_name}-scheduler", "--scheduler-port", "50050",
                 f"--task-scheduling-policy={args.scheduling_policy}",
                 f"--vcores={args.vcores}",
                 f"--memory-pool-size={pool_bytes}",
                 "--work-dir", "/data", *grpc],
        "env": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                {"name": "RUST_LOG", "value": "ballista=info"}],
        "ports": [{"containerPort": 50051, "name": "flight"}],
        "resources": resources(args.executor_cpus, args.executor_memory_gib),
        "volumeMounts": [{"name": "work", "mountPath": "/data"}],
        "readinessProbe": {"tcpSocket": {"port": 50051}, "periodSeconds": 10},
    }], volumes=[scratch_volume(cfg, "work", run_name, "ballista-executor", cfg["shuffle_volume_size"])]))
    runner = job(f"{run_name}-runner", run_name, "ballista-runner", pod_spec(cfg, [{
        "name": "runner",
        "image": image,
        "command": ["/bin/bash", "-c", ballista_runner_script(args, run_name, data_path, numbers)],
        "env": [{"name": "RUST_LOG", "value": "ballista=info"}],
        "resources": resources(8, 32, request_cpu=4),
    }], restart_policy="Never"))
    cluster = [service(f"{run_name}-scheduler", run_name, "ballista-scheduler", [("grpc", 50050)]),
               scheduler, executor]
    settings = {"executors": args.executors, "executor_cpus": args.executor_cpus,
                "executor_memory_gib": args.executor_memory_gib, "vcores": args.vcores,
                "memory_pool_bytes": pool_bytes,
                "partitions": args.partitions or args.executors * args.vcores,
                "scheduling_policy": args.scheduling_policy,
                "ballista_config": dict(parse_pairs(args.ballista_config, "--ballista-config")),
                "partition_cols": not args.no_partition_cols}
    return image, cluster, [(f"{run_name}-scheduler", 1), (f"{run_name}-executor", args.executors)], runner, settings


def ballista_run_info(logs):
    match = re.search(r"^BALLISTA-RUN-INFO commit=(\S*)$", logs, re.MULTILINE)
    return {"ballista_commit": match.group(1)} if match and match.group(1) else {}


# ---------------------------------------------------------------------------
# Spark and Spark + Comet
# ---------------------------------------------------------------------------

def spark_conf(args, cfg, run_name):
    """The Spark settings of the run, in order; later keys win."""
    conf = load_conf_file(os.path.join(HERE, "conf", "spark.conf"))
    if args.engine == "comet":
        conf += load_conf_file(os.path.join(HERE, "conf", "comet.conf"))
    conf += [
        ("spark.executor.instances", str(args.executors)),
        ("spark.kubernetes.executor.request.cores", str(args.executor_cpus)),
        ("spark.kubernetes.executor.limit.cores", str(args.executor_cpus)),
        ("spark.driver.memory", f"{int(args.driver_memory_gib * 0.75)}g"),
        ("spark.hadoop.fs.s3a.endpoint.region", cfg["region"]),
    ]
    if cfg["storage_class"]:
        prefix = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1"
        conf += [
            (f"{prefix}.options.claimName", "OnDemand"),
            (f"{prefix}.options.storageClass", cfg["storage_class"]),
            (f"{prefix}.options.sizeLimit", cfg["shuffle_volume_size"]),
            (f"{prefix}.mount.path", "/data"),
            (f"{prefix}.mount.readOnly", "false"),
        ]
    conf += parse_pairs(args.spark_conf, "--spark-conf")
    return list(dict(conf).items())


def spark_executor_template(cfg, run_name):
    """The executors' pod template: what cluster.toml asks of every pod.

    Spark fills in the image, command, resources and volumes from its settings.
    """
    spec = pod_spec(cfg, [{"name": "executor"}], security_context=SPARK_SECURITY_CONTEXT)
    return {"apiVersion": "v1", "kind": "Pod", **pod_template(run_name, "spark-executor", spec)}


def spark_manifests(args, cfg, run_name, data_path, numbers):
    key = "comet" if args.engine == "comet" else "spark"
    image = require(cfg, "images", key, f"the image built from docker/{key}.Dockerfile")
    conf = spark_conf(args, cfg, run_name)
    submit = [
        "/opt/spark/bin/spark-submit",
        "--master", "k8s://https://kubernetes.default.svc",
        "--deploy-mode", "client",
        "--name", run_name,
        "--conf", "spark.driver.host=$(POD_IP)",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", "spark.kubernetes.driver.pod.name=$(POD_NAME)",
        "--conf", f"spark.kubernetes.namespace={cfg['namespace']}",
        "--conf", f"spark.kubernetes.container.image={image}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", f"spark.kubernetes.authenticate.executor.serviceAccountName={cfg['service_account']}",
        "--conf", f"spark.kubernetes.executor.podNamePrefix={run_name}",
        "--conf", f"spark.kubernetes.executor.label.benchmark-run={run_name}",
        "--conf", "spark.kubernetes.executor.podTemplateFile=/opt/bench/conf/executor-pod.json",
    ]
    for k, v in conf:
        submit += ["--conf", f"{k}={v}"]
    submit += [
        "/opt/bench/scripts/tpcbench.py",
        "--benchmark", args.benchmark,
        "--data", to_s3a(data_path),
        "--queries", "/opt/bench/queries",
        "--iterations", str(args.iterations),
        "--scale-factor", str(cfg["data"]["scale_factor"]),
    ]
    if numbers:
        submit += ["--query", ",".join(map(str, numbers))]
    if args.no_check_rows:
        submit.append("--no-check-rows")

    driver = job(f"{run_name}-driver", run_name, "spark-driver", pod_spec(cfg, [{
        "name": "driver",
        "image": image,
        "command": submit,
        "env": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                {"name": "PYTHONPATH", "value": "/opt/bench/scripts"}],
        "resources": resources(args.driver_cpus, args.driver_memory_gib),
        "volumeMounts": [{"name": "scripts", "mountPath": "/opt/bench/scripts", "readOnly": True},
                         {"name": "queries", "mountPath": "/opt/bench/queries", "readOnly": True},
                         {"name": "conf", "mountPath": "/opt/bench/conf", "readOnly": True}],
    }], volumes=[
        {"name": "scripts", "configMap": {"name": f"{run_name}-scripts"}},
        {"name": "queries", "configMap": {"name": f"{run_name}-queries"}},
        {"name": "conf", "configMap": {"name": f"{run_name}-conf"}},
    ], restart_policy="Never", security_context=SPARK_SECURITY_CONTEXT))
    scripts = read_files(os.path.join(HERE, "spark"), ["tpcbench.py"])
    scripts.update(read_files(HERE, ["tpch_expected_rows.py"]))
    cluster = [
        configmap(f"{run_name}-scripts", run_name, "spark-driver", scripts),
        configmap(f"{run_name}-queries", run_name, "spark-driver", query_files(args.benchmark)),
        configmap(f"{run_name}-conf", run_name, "spark-driver",
                  {"executor-pod.json": json.dumps(spark_executor_template(cfg, run_name), indent=2)}),
    ]
    settings = {"executors": args.executors, "executor_cpus": args.executor_cpus,
                "driver_cpus": args.driver_cpus, "driver_memory_gib": args.driver_memory_gib,
                "spark_conf": dict(conf)}
    return image, cluster, [], driver, settings


# ---------------------------------------------------------------------------
# Trino
# ---------------------------------------------------------------------------

def trino_memory_settings(container_gib):
    """(query.max-memory-per-node, memory.heap-headroom-per-node) in GB for a container.

    The image sizes the JVM heap at 80% of the container memory, and Trino
    refuses to start unless the two settings add up to less than the heap. The
    runner runs one query at a time, so a query may use most of the heap; the
    headroom covers memory Trino does not track.
    """
    heap_gib = container_gib * 0.8
    return int(heap_gib * 0.75), int(heap_gib * 0.12)


def trino_metastore_dir(cfg, image, data_path, analyze, fingerprint):
    """Directory of the Hive file metastore for this Trino version, dataset and table definitions.

    The file metastore refuses to read metadata written by another Trino
    version, so the version (the image tag) is part of the path. So are the
    table definitions and whether statistics are collected, so a run never
    picks up tables or statistics made under different settings, while later
    runs with the same settings reuse the tables, partitions and statistics.
    """
    root = require(cfg, "trino", "metastore_dir", "a writable s3:// directory for Trino's file metastore")
    version = image.rsplit("/", 1)[-1].partition(":")[2] or "latest"
    dataset = re.sub(r"[^\w.-]+", "-", data_path.split("://", 1)[-1]).strip("-")
    return f"{root.rstrip('/')}/{version}/{dataset}-{fingerprint}-{'stats' if analyze else 'nostats'}/"


def trino_properties(args, cfg, run_name, metastore_dir, workers_gib, coordinator_gib):
    worker_per_node, worker_headroom = trino_memory_settings(workers_gib)
    coordinator_per_node, coordinator_headroom = trino_memory_settings(coordinator_gib)
    spill = ["spill-enabled=true"] if args.trino_spill else []

    def config(coordinator, per_node, headroom, spill_path):
        lines = [f"coordinator={'true' if coordinator else 'false'}"]
        if coordinator:
            # The coordinator plans and schedules but never scans data.
            lines.append("node-scheduler.include-coordinator=false")
        lines += [
            "http-server.http.port=8080",
            f"discovery.uri=http://{run_name}-coordinator:8080",
            # No single query can use more than every worker's share.
            f"query.max-memory={worker_per_node * args.executors}GB",
            f"query.max-memory-per-node={per_node}GB",
            f"memory.heap-headroom-per-node={headroom}GB",
        ]
        if spill:
            lines += spill + [f"spiller-spill-path={spill_path}"]
        return "\n".join(lines) + "\n"

    # The file metastore reads one metadata file per partition and lists every
    # partition for every query, and splitting a scan lists every partition
    # directory in S3. The data never changes, so all of that is cached for the
    # length of a run. The Hive connector's dynamic filter wait defaults to 0,
    # so a dynamic filter usually arrives after the splits of the big tables are
    # generated; 1s is the Iceberg connector's default.
    hive = "\n".join([
        "connector.name=hive",
        "hive.metastore=file",
        f"hive.metastore.catalog.dir={metastore_dir}",
        "fs.s3.enabled=true",
        f"s3.region={cfg['region']}",
        "hive.metastore-cache-ttl=24h",
        "hive.metastore-stats-cache-ttl=24h",
        "hive.file-status-cache-tables=tpch.*",
        "hive.file-status-cache-expire-time=24h",
        "hive.dynamic-filtering.wait-timeout=1s",
    ]) + "\n"
    return {
        "coordinator.config.properties": config(True, coordinator_per_node, coordinator_headroom, "/data/trino/spill"),
        "worker.config.properties": config(False, worker_per_node, worker_headroom, "/spill"),
        "hive.properties": hive,
    }


def trino_manifests(args, cfg, run_name, data_path, numbers):
    sys.path.insert(0, os.path.join(HERE, "trino"))
    from tpch_tables import ddl_fingerprint

    image = cfg["images"]["trino"]
    partitioned = bool(cfg["data"]["tpch_partitioned"])
    money_type = cfg["trino"]["money_type"]
    metastore_dir = trino_metastore_dir(cfg, image, data_path, args.trino_analyze,
                                        ddl_fingerprint(partitioned, money_type))
    coordinator_gib = 32
    properties = trino_properties(args, cfg, run_name, metastore_dir, args.executor_memory_gib, coordinator_gib)
    # The user the image runs as.
    security = {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000}
    health = {"exec": {"command": ["/usr/lib/trino/bin/health-check"]}, "periodSeconds": 10, "timeoutSeconds": 5}
    startup = {**health, "periodSeconds": 5, "failureThreshold": 72}

    def trino_container(role):
        mounts = [
            # Only these files are replaced; the image's jvm.config (heap = 80% of
            # the container) and node.properties (node.id = the pod name) are kept.
            {"name": "etc", "mountPath": "/etc/trino/config.properties", "subPath": f"{role}.config.properties"},
            {"name": "etc", "mountPath": "/etc/trino/catalog/hive.properties", "subPath": "hive.properties"},
            {"name": "data", "mountPath": "/data/trino"},
        ]
        if role == "worker":
            # A separate volume from the data directory, which holds the logs.
            mounts.append({"name": "spill", "mountPath": "/spill"})
        cpu, gib = (8, coordinator_gib) if role == "coordinator" else (args.executor_cpus, args.executor_memory_gib)
        return {"name": "trino", "image": image, "ports": [{"containerPort": 8080, "name": "http"}],
                "resources": resources(cpu, gib), "volumeMounts": mounts,
                "startupProbe": startup, "readinessProbe": health}

    etc = {"name": "etc", "configMap": {"name": f"{run_name}-trino-etc"}}
    data = {"name": "data", "emptyDir": {}}
    spill = (scratch_volume(cfg, "spill", run_name, "trino-worker", "200Gi") if args.trino_spill
             else {"name": "spill", "emptyDir": {}})
    coordinator = deployment(f"{run_name}-coordinator", run_name, "trino-coordinator", 1, pod_spec(
        cfg, [trino_container("coordinator")], volumes=[etc, data], security_context=security))
    workers = deployment(f"{run_name}-worker", run_name, "trino-worker", args.executors, pod_spec(
        cfg, [trino_container("worker")], volumes=[etc, data, spill], security_context=security))

    session = parse_pairs(args.trino_session, "--trino-session")
    runner_env = {
        "TRINO_URL": f"http://{run_name}-coordinator:8080",
        "TRINO_SCHEMA": "tpch",
        "DATA_PATH": data_path,
        "PARTITIONED": "1" if partitioned else "0",
        "MONEY_TYPE": money_type,
        "SCALE_FACTOR": str(cfg["data"]["scale_factor"]),
        "EXPECTED_WORKERS": str(args.executors),
        "ITERATIONS": str(args.iterations),
        "WARMUP": "0" if args.no_warmup else "1",
        "CHECK_ROWS": "0" if args.no_check_rows else "1",
        "QUERIES": ",".join(map(str, numbers or [])),
        "ANALYZE": "1" if args.trino_analyze else "0",
        "SESSION_PROPERTIES": ",".join(f"{k}={v}" for k, v in session),
        "QUERY_TIMEOUT_S": str(args.query_timeout),
    }
    runner = job(f"{run_name}-runner", run_name, "trino-runner", pod_spec(cfg, [{
        "name": "runner",
        "image": cfg["images"]["python"],
        "command": ["python3", "-u", "/opt/bench/scripts/run_queries.py"],
        "env": [{"name": k, "value": v} for k, v in runner_env.items()],
        "resources": resources(2, 2, request_cpu=1),
        "volumeMounts": [{"name": "scripts", "mountPath": "/opt/bench/scripts", "readOnly": True},
                         {"name": "queries", "mountPath": "/opt/bench/queries", "readOnly": True}],
    }], volumes=[
        {"name": "scripts", "configMap": {"name": f"{run_name}-scripts"}},
        {"name": "queries", "configMap": {"name": f"{run_name}-queries"}},
    ], restart_policy="Never"))

    scripts = read_files(os.path.join(HERE, "trino"), ["run_queries.py", "tpch_tables.py"])
    scripts.update(read_files(HERE, ["tpch_expected_rows.py"]))
    cluster = [
        configmap(f"{run_name}-trino-etc", run_name, "trino", properties),
        configmap(f"{run_name}-scripts", run_name, "trino-runner", scripts),
        configmap(f"{run_name}-queries", run_name, "trino-runner", query_files("tpch")),
        # Workers announce themselves to this address, including the
        # coordinator's own announcement, so it must resolve before it is Ready.
        service(f"{run_name}-coordinator", run_name, "trino-coordinator", [("http", 8080)],
                publish_not_ready=True),
        coordinator,
        workers,
    ]
    settings = {"workers": args.executors, "worker_cpus": args.executor_cpus,
                "worker_memory_gib": args.executor_memory_gib, "metastore_dir": metastore_dir,
                "partitioned": partitioned, "money_type": money_type, "analyze": args.trino_analyze,
                "warmup": not args.no_warmup, "spill": args.trino_spill, "session_properties": dict(session)}
    return image, cluster, [(f"{run_name}-coordinator", 1), (f"{run_name}-worker", args.executors)], runner, settings


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------

MANIFESTS = {"ballista": ballista_manifests, "spark": spark_manifests, "comet": spark_manifests,
             "trino": trino_manifests}


def run_benchmark(args, cfg):
    data_path = require(cfg, "data", args.benchmark, f"the s3:// directory of the {args.benchmark} tables")
    numbers = query_numbers(args.benchmark, args.query)
    run_name = f"{args.benchmark}-{args.engine}-{secrets.token_hex(3)}"
    image, cluster, deployments, runner, settings = MANIFESTS[args.engine](args, cfg, run_name, data_path, numbers)
    runner_name = runner["metadata"]["name"]
    kube = Kube(cfg, dry_run=args.dry_run)

    print(f"{args.engine} {args.benchmark} run {run_name}", file=sys.stderr)
    print(f"  image:      {image}", file=sys.stderr)
    print(f"  data:       {data_path}", file=sys.stderr)
    print(f"  queries:    {', '.join(map(str, numbers)) if numbers else 'all'} x {args.iterations}",
          file=sys.stderr)
    print(f"  executors:  {args.executors} x {args.executor_cpus} CPU, {args.executor_memory_gib} GiB",
          file=sys.stderr)
    if args.dry_run:
        kube.apply(cluster + [runner])
        return 0

    run = {
        "run": run_name,
        "engine": args.engine,
        "benchmark": args.benchmark,
        "scale_factor": cfg["data"]["scale_factor"],
        "data": data_path,
        "image": image,
        "iterations": args.iterations,
        "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "settings": settings,
    }
    failed = True
    try:
        kube.apply(cluster)
        for name, replicas in deployments:
            print(f"Waiting for {name} ({replicas} pods)...", file=sys.stderr)
            if not kube.wait_for_deployment(name):
                print(f"{name} is not ready:\n{kube.describe_pods(f'benchmark-run={run_name}')}\n"
                      f"{kube.logs(f'deployment/{name}', tail=50)}", file=sys.stderr)
                raise SystemExit(f"{name} did not start")
        kube.apply([runner])
        print(f"Running the queries. Follow them with:\n"
              f"  kubectl -n {cfg['namespace']} logs -f job/{runner_name}", file=sys.stderr)
        succeeded = kube.wait_for_job(runner_name)

        logs = kube.logs(f"job/{runner_name}")
        os.makedirs(args.results_dir, exist_ok=True)
        with open(os.path.join(args.results_dir, f"{run_name}.log"), "w") as f:
            f.write(logs)
        summaries = parse_summaries(logs)
        queries = [q for s in summaries for q in s.get("queries", [])]
        if not queries and args.engine in QUERY_RESULT_PREFIXES:
            queries = parse_query_lines(logs, QUERY_RESULT_PREFIXES[args.engine])
        if args.engine == "ballista":
            run.update(ballista_run_info(logs))
        for s in summaries:
            if s.get("error"):
                print(f"Runner failed: {s['error']}", file=sys.stderr)
        if queries:
            path = write_results(args.results_dir, run, summaries, queries)
            print_results(run)
            print(f"\nResults: {path}", file=sys.stderr)
        else:
            print(f"No results in the runner's log. Its end:\n{logs[-3000:]}", file=sys.stderr)
        failed = not succeeded or not queries or any(q.get("error") for q in run.get("queries", []))
    finally:
        if args.keep:
            print(f"\nLeaving the cluster up (--keep). Delete it with:\n"
                  f"  kubectl -n {cfg['namespace']} delete deployment,service,job,configmap,pod "
                  f"-l benchmark-run={run_name}", file=sys.stderr)
        else:
            print("\nDeleting the cluster...", file=sys.stderr)
            kube.delete_run(run_name)
    return 1 if failed else 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=os.path.join(HERE, "cluster.toml"),
                        help="Cluster settings (default: cluster.toml next to this script; "
                             "start from cluster.example.toml)")
    parser.add_argument("--engine", choices=ENGINES, required=True)
    parser.add_argument("--benchmark", choices=sorted(NUM_QUERIES), default="tpch")
    parser.add_argument("--query", help="Query numbers to run, comma-separated (default: all)")
    parser.add_argument("--iterations", type=int,
                        help="Timed iterations of each query (default: 2; 3 for trino, after a warm-up run)")
    parser.add_argument("--executors", type=int, default=32,
                        help="Executor pods (Trino: worker pods) (default: 32)")
    parser.add_argument("--executor-cpus", type=int, default=8, help="CPUs of each executor pod (default: 8)")
    parser.add_argument("--executor-memory-gib", type=int, default=64,
                        help="Memory of each Ballista executor and Trino worker pod (default: 64). Spark's "
                             "executor memory is in conf/spark.conf")
    parser.add_argument("--results-dir", default=os.path.join(HERE, "results"))
    parser.add_argument("--dry-run", action="store_true", help="Print the manifests and stop")
    parser.add_argument("--keep", action="store_true", help="Do not delete the cluster after the run")
    parser.add_argument("--no-check-rows", action="store_true",
                        help="Spark and Trino: do not check row counts (tpch_expected_rows.py)")

    ballista = parser.add_argument_group("Ballista")
    ballista.add_argument("--vcores", type=int, default=8, help="Tasks each executor runs at once (default: 8)")
    ballista.add_argument("--partitions", type=int,
                          help="target_partitions (default: executors x vcores)")
    ballista.add_argument("--scheduling-policy", choices=["pull-staged", "push-staged"], default="pull-staged",
                          help="Task scheduling policy of the scheduler and executors (default: pull-staged)")
    ballista.add_argument("--ballista-config", action="append", default=[], metavar="KEY=VALUE",
                          help="Session setting for the runner (-c), e.g. ballista.execution.mode=pipelined. "
                               "Can be repeated")
    ballista.add_argument("--no-partition-cols", action="store_true",
                          help="Register the tables without their Hive partition columns")

    spark = parser.add_argument_group("Spark and Comet")
    spark.add_argument("--driver-cpus", type=int, default=16, help="CPUs of the driver pod (default: 16)")
    spark.add_argument("--driver-memory-gib", type=int, default=64,
                       help="Memory of the driver pod (default: 64); spark.driver.memory is 75%% of it")
    spark.add_argument("--spark-conf", action="append", default=[], metavar="KEY=VALUE",
                       help="Spark setting, over conf/spark.conf and conf/comet.conf. Can be repeated")

    trino = parser.add_argument_group("Trino (TPC-H only)")
    trino.add_argument("--trino-analyze", action="store_true",
                       help="Collect table statistics with ANALYZE (reads all the data once, then kept "
                            "in the metastore)")
    trino.add_argument("--trino-spill", action="store_true", help="Let workers spill to disk")
    trino.add_argument("--trino-session", action="append", default=[], metavar="KEY=VALUE",
                       help="Session property for every query. Can be repeated")
    trino.add_argument("--no-warmup", action="store_true",
                       help="Do not run each query once untimed before the timed iterations")
    trino.add_argument("--query-timeout", type=int, default=3600, help="Seconds per query (default: 3600)")

    args = parser.parse_args(argv)
    if args.iterations is None:
        args.iterations = 3 if args.engine == "trino" else 2
    if args.engine == "trino" and args.benchmark != "tpch":
        parser.error("--engine trino runs TPC-H only")
    if args.engine == "trino" and args.executor_memory_gib < 16:
        parser.error("--executor-memory-gib must be at least 16 for Trino")
    return args


def main():
    args = parse_args()
    if not os.path.exists(args.config):
        sys.exit(f"{args.config} not found: copy cluster.example.toml to cluster.toml and fill it in")
    sys.exit(run_benchmark(args, load_cluster_config(args.config)))


if __name__ == "__main__":
    main()
