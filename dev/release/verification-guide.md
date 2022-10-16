# Verifying a Ballista Release Candidate

An important part of the release process is having multiple people verify the release candidate so that we catch any
issues that do not show up by simply running the automated verification script (which just checks signatures of the
release tarball and runs `cargo test`). This is not sufficient for testing a distributed platform such as Ballista, so
this guide exists to list other ways of verifying a release more thoroughly.

The following artifacts could potentially be published after the vote passes, so ideally we should test all of these
when voting. It may not be practical for everyone to check all of these items, so these are just suggestions rather
than requirements.

## Rust Crates

- The `verify-release-candidate.sh` script should be sufficient to ensure that we can at least compile and publish
  the crates. This is the minimum level of verification that is required when voting on a release candidate.

## Integration Tests

- Running `./dev/integration-test.sh` will start a cluster and run some benchmark queries.

## Python Bindings

- Build the Python bindings
- Run an example from the user guide
- Run the benchmarks using the [Python benchmark script](../../benchmarks/tpch.py)

## Scheduler Web User Interface

- View the UI when running queries

## Docker Images

- Build the Docker images
- Start a cluster using docker-compose

## Kubernetes Support

- Start a Ballista cluster in k8s, either using the [Helm chart](../../helm/README.md), or the
  [yaml provided](../../docs/source/user-guide/deployment/kubernetes.md) in the user guide

## User Guide

- Check that the user guide can be built using the [release process instructions](README.md)
