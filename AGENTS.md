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

# AGENTS.md

Guidance for AI coding agents working in Apache DataFusion Ballista, a distributed query
engine built on Apache DataFusion. This file only points at the docs that are the source
of truth. Read them rather than relying on memory.

## Guides

User guide (how Ballista is used and configured):

- [Introduction](docs/source/user-guide/introduction.md)
- [Configuration](docs/source/user-guide/configs.md)
- [Tuning guide](docs/source/user-guide/tuning-guide.md)
- [Extending components](docs/source/user-guide/extending-components.md)

Contributor guide (how Ballista is built and changed):

- [Development](docs/source/contributors-guide/development.md): build, test, lint, golden
  files, DataFusion upgrades, upgrade notes
- [Architecture](docs/source/contributors-guide/architecture.md)
- [Code organization](docs/source/contributors-guide/code-organization.md)
- [User personas](docs/source/contributors-guide/user-personas.md): the audiences a change
  must not regress, with red flags to check in every pull request
- [CONTRIBUTING.md](CONTRIBUTING.md)

## Before opening a pull request

- Run `dev/rust_lint.sh` (format and clippy).
- Fill in every section of `.github/pull_request_template.md`.
- A breaking public API change needs the `api-change` label and an entry in
  `docs/source/upgrading/`.

## Skills

Task-specific skills live in `.ai/skills/`. Each is a directory with a `SKILL.md`.

- [`pr-review`](.ai/skills/pr-review/SKILL.md): review a pull request, covering correctness
  and tests plus the Ballista persona, public API and benchmark checks. Use it whenever
  asked to review a PR.
