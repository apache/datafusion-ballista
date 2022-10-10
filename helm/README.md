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

# Ballista Helm Chart

Helm is a convenient way to install applications into Kubernetes. It can work against any Kubeneretes environement,
including all the major cloud providers. 
For the purposes of this documentation, we will use Kubernetes-in-Docker (kind) to install locally.

## Prerequisites

```shell
sudo apt install docker.io docker-compose
sudo snap install go kubectl helm --classic
go install sigs.k8s.io/kind@v0.16.0
~/go/bin/kind create cluster
```

## Build

```shell
dev/build-ballista-docker.sh
```

## Deploy

```shell
# See https://iximiuz.com/en/posts/kubernetes-kind-load-docker-image/
# https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster
~/go/bin/kind load docker-image ballista-scheduler:latest
~/go/bin/kind load docker-image ballista-executor:latest

cd helm/ballista
helm repo add bitnami https://charts.bitnami.com/bitnami
helm dep update 
helm dep build 
helm install ballista .
```

## Access Scheduler Web UI

Run the following command to redirect localhost port 8080 to port 80 in the scheduler container and then view the scheduler UI at http://localhost:8080. 

kubectl port-forward ballista-scheduler-0 8080:80

## Connect

```shell
kubectl port-forward ballista-scheduler-0 50050:50050
sqline # ... see FlightSQL instructions
```

## Teardown

```shell
helm uninstall ballista
```