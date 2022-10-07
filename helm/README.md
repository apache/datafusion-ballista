# Ballista Helm Chart

Helm is a convenient way to install applications into Kubernetes. It can work against any Kubeneretes environement,
including all the major cloud providers. For the purposes of this documentation, we will use microk8s to install locally.

## Prerequisites

```shell
sudo apt install kubectl docker.io docker-compose
sudo snap install go --classic
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

## Teardown

```shell
helm uninstall ballista
```