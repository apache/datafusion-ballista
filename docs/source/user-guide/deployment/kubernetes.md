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

# Deploying Ballista with Kubernetes

Ballista can be deployed to any Kubernetes cluster using the following instructions. These instructions assume that
you are already comfortable managing Kubernetes deployments.

The Ballista deployment consists of:

- k8s deployment for one or more scheduler processes
- k8s deployment for one or more executor processes
- k8s service to route traffic to the schedulers
- k8s persistent volume and persistent volume claims to make local data accessible to Ballista
- _(optional)_ a [keda](http://keda.sh) instance for autoscaling the number of executors

## Testing Locally

[Microk8s](https://microk8s.io/) is recommended for installing a local k8s cluster. Once Microk8s is installed, DNS
must be enabled using the following command.

```bash
microk8s enable dns
```

## Build Docker Images

To create the required Docker images please refer to the [docker deployment page](docker.md).

## Publishing Docker Images

Once the images have been built, you can retag them and can push them to your favourite Docker registry.

```bash
docker tag apache/datafusion-ballista-scheduler:latest <your-repo>/datafusion-ballista-scheduler:latest
docker tag apache/datafusion-ballista-executor:latest <your-repo>/datafusion-ballista-executor:latest
docker push <your-repo>/datafusion-ballista-scheduler:latest
docker push <your-repo>/datafusion-ballista-executor:latest
```

## Create Persistent Volume and Persistent Volume Claim

Copy the following yaml to a `pv.yaml` file and apply to the cluster to create a persistent volume and a persistent
volume claim so that the specified host directory is available to the containers. This is where any data should be
located so that Ballista can execute queries against it.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: data-pv
  labels:
    type: local
spec:
  storageClassName: manual
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/mnt"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pv-claim
spec:
  storageClassName: manual
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 3Gi
```

To apply this yaml:

```bash
kubectl apply -f pv.yaml
```

You should see the following output:

```bash
persistentvolume/data-pv created
persistentvolumeclaim/data-pv-claim created
```

## Deploying a Ballista Cluster

Copy the following yaml to a `cluster.yaml` file and change `<your-image>` with the name of your Ballista Docker image.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ballista-scheduler
  labels:
    app: ballista-scheduler
spec:
  ports:
    - port: 50050
      name: scheduler
    - port: 80
      name: scheduler-ui
  selector:
    app: ballista-scheduler
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ballista-scheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ballista-scheduler
  template:
    metadata:
      labels:
        app: ballista-scheduler
        ballista-cluster: ballista
    spec:
      containers:
        - name: ballista-scheduler
          image: <your-repo>/datafusion-ballista-scheduler:latest
          args: ["--bind-port=50050"]
          ports:
            - containerPort: 50050
              name: flight
          livenessProbe:
            httpGet:
              path: /healthz
              port: 50050
            failureThreshold: 3
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 50050
            failureThreshold: 3
            periodSeconds: 5
          volumeMounts:
            - mountPath: /mnt
              name: data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data-pv-claim
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ballista-executor
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ballista-executor
  template:
    metadata:
      labels:
        app: ballista-executor
        ballista-cluster: ballista
    spec:
      containers:
        - name: ballista-executor
          image: <your-repo>/datafusion-ballista-executor:latest
          args:
            - "--bind-port=50051"
            - "--scheduler-host=ballista-scheduler"
            - "--scheduler-port=50050"
          ports:
            - containerPort: 50051
              name: flight
            - containerPort: 50053
              name: health
          livenessProbe:
            httpGet:
              path: /healthz
              port: 50053
            failureThreshold: 3
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 50053
            failureThreshold: 3
            periodSeconds: 5
          volumeMounts:
            - mountPath: /mnt
              name: data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data-pv-claim
```

```bash
kubectl apply -f cluster.yaml
```

This should show the following output:

```
service/ballista-scheduler created
deployment.apps/ballista-scheduler created
deployment.apps/ballista-executor created
```

You can also check status by running `kubectl get pods`:

```bash
$ kubectl get pods
NAME                                 READY   STATUS    RESTARTS   AGE
ballista-executor-78cc5b6486-4rkn4   0/1     Pending   0          42s
ballista-executor-78cc5b6486-7crdm   0/1     Pending   0          42s
ballista-scheduler-879f874c5-rnbd6   0/1     Pending   0          42s
```

You can view the scheduler logs with `kubectl logs ballista-scheduler-<pod-id>`:

```
$ kubectl logs ballista-scheduler-<pod-id>
INFO ballista_scheduler::scheduler_process: Ballista v52.0.0 Scheduler listening on 0.0.0.0:50050
INFO ballista_scheduler::scheduler_server::grpc: Received register_executor request for ExecutorMetadata { id: "b5e81711-1c5c-46ec-8522-d8b359793188", host: "10.1.23.149", port: 50051 }
INFO ballista_scheduler::scheduler_server::grpc: Received register_executor request for ExecutorMetadata { id: "816e4502-a876-4ed8-b33f-86d243dcf63f", host: "10.1.23.150", port: 50051 }
```

## Health Probes and Rolling Upgrades

Both the scheduler and executor expose two Kubernetes-style HTTP probe endpoints:

- **`/healthz`** — liveness. Always returns `200 OK` while the process is
  running. Failing this signals only "the process is dead," which is the sole
  case where a restart can actually recover. Do **not** make liveness reflect
  scheduler connectivity — a flapping scheduler would then take the whole
  executor fleet down with it in a restart cascade.
- **`/readyz`** — readiness. Returns `200 OK` when the pod is prepared to
  accept traffic (scheduler: at least `--min-ready-executors` executors
  registered; executor: last heartbeat to the scheduler succeeded).
  A failing `/readyz` removes the pod from `Service` endpoints but does
  **not** trigger a restart, so it is safe to gate on cluster state.

The scheduler serves both probes on the same port as its gRPC/REST API
(default `50050`). The executor serves them on a dedicated HTTP port
(`--bind-health-port`, default `50053`) since the Arrow Flight port is not
HTTP.

### Protocol version and forced upgrades

Every executor sends a compile-time `BALLISTA_PROTOCOL_VERSION` on
registration and heartbeat. The scheduler compares against its own compiled
value and rejects mismatches with `FailedPrecondition`; the executor then
flips `/readyz` off and, after several consecutive failures, self-terminates
so the operator (or Deployment controller) can bring a matching version up.

Bump `BALLISTA_PROTOCOL_VERSION` only on releases that change the
executor↔scheduler wire format. Most releases will not bump it.

### Rolling both Deployments together

The scheduler and executor live in separate Deployments; there is no single
"app roll." When you cut a release that bumps `BALLISTA_PROTOCOL_VERSION`,
update **both** Deployments' image tags in the same change and apply them
together. Kubernetes will roll each Deployment independently under its own
rollout policy, but the fleet stays serving throughout:

| scheduler ↔ executor | outcome                                                                      |
| -------------------- | ---------------------------------------------------------------------------- |
| old ↔ old            | works (old scheduler pre-dates the check)                                    |
| old ↔ new            | works (old scheduler ignores the unknown field)                              |
| new ↔ new            | works                                                                        |
| new ↔ old            | rejected — new scheduler fails `/readyz` until enough new executors register |

Because the _old_ scheduler is permissive (it never learned to check the
field), executors that reach it during the transition are always accepted.
The rejection happens only when a _new_ scheduler sees an _old_ executor,
which is the case the version check is designed to protect against —
mismatched pairs never exchange work, so they cannot corrupt state.

Give each Deployment a `maxSurge` so Kubernetes brings new pods up before
tearing old ones down; this keeps executor capacity at roughly 100% during
the roll:

```yaml
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 25%
      maxUnavailable: 0
```

The scheduler's `/readyz` gate (`--min-ready-executors`, default `1`) means
a fresh scheduler pod does not receive `Service` traffic until at least one
matching-version executor has registered with it, so clients keep hitting
the old scheduler until the new one has real capacity behind it. Set
`--min-ready-executors=0` if you want the scheduler to advertise readiness
immediately (e.g. single-node development clusters).

## Port Forwarding

If you want to run applications outside of the cluster and have them connect to the scheduler then it is necessary to
set up port forwarding.

First, check that the `ballista-scheduler` service is running.

```bash
$ kubectl get services
NAME                 TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)     AGE
kubernetes           ClusterIP   10.152.183.1    <none>        443/TCP     26h
ballista-scheduler   ClusterIP   10.152.183.21   <none>        50050/TCP   24m
```

Use the following command to set up port-forwarding.

```bash
kubectl port-forward service/ballista-scheduler 50050:50050
```

## Deleting the Ballista Cluster

Run the following kubectl command to delete the cluster.

```bash
kubectl delete -f cluster.yaml
```

## Autoscaling Executors

Ballista supports autoscaling for executors through [Keda](http://keda.sh). Keda allows for the scaling of a
deployment through custom metrics which are exposed through the Ballista scheduler, and it
can even scale the number of executors down to 0 if there is no activity in the cluster.

Keda can be installed in your kubernetes cluster through a single command line:

```bash
kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.7.1/keda-2.7.1.yaml
```

Once you have deployed Keda on your cluster, you can now deploy a new kubernetes object called `ScaledObject`
which will let Keda know how to scale your executors. In order to do that, copy the following YAML into a
`scale.yaml` file:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ballista-executor
spec:
  scaleTargetRef:
    name: ballista-executor
  minReplicaCount: 0
  maxReplicaCount: 5
  triggers:
    - type: external
      metadata:
        # Change this DNS if the scheduler isn't deployed in the "default" namespace
        scalerAddress: ballista-scheduler.default.svc.cluster.local:50050
```

And then deploy it into the cluster:

```bash
kubectl apply -f scale.yaml
```

If the cluster is inactive, Keda will now scale the number of executors down to 0, and will scale them up when
you launch a query. Please note that Keda will perform a scan once every 30 seconds, so it might take a bit to
scale the executors.

Please visit Keda's [documentation page](https://keda.sh/docs/2.7/concepts/scaling-deployments/) for more information.
