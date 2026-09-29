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

# Python Client Release Process

The Ballista Python client, published as the [`ballista`](https://pypi.org/project/ballista/) package
on PyPI, is released separately from the Rust crates, with its own release candidate and vote.

The Python client depends on [datafusion-python](https://github.com/apache/datafusion-python), which
usually publishes a new major version several weeks after DataFusion does. Releasing the Python
client separately means the Rust release does not have to wait for datafusion-python. See
[#2511](https://github.com/apache/datafusion-ballista/issues/2511) for the background.

This document only covers what is specific to the Python client. Steps that are the same as for the
Rust release, such as setting up a signing key, are described in the
[Rust release process](../../../dev/release/README.md).

## Overview

- **When.** The Python client is released after the Rust crates it depends on have been published to
  crates.io, and once datafusion-python has published the matching major version.
- **What.** The source release only contains the `python/` directory. `python/Cargo.toml` depends on
  the published `ballista` crates rather than on the Rust workspace, so the wheels contain exactly
  the Rust code that was voted on in the Rust release.
- **Version.** The Python client has the same version as the `ballista` crates it depends on. For
  example, the Python client built against the Ballista 55.0.0 crates is released as `ballista`
  55.0.0 on PyPI.
- **Branches and tags.** There are no separate release branches for the Python client. Its release
  candidates are tagged on the same release branch as the Rust release, for example `branch-55`,
  with a `python-` prefix: `python-55.0.0-rc1` for a release candidate and `python-55.0.0` for the
  release.
- **Wheels.** Pushing a `python-*-rc*` tag runs the `Python Release Build` workflow
  (`.github/workflows/build.yml`), which builds the wheels and the sdist that are published to PyPI
  once the vote passes.

## Who Can Create Releases?

| Task                                                  | Role Required |
| ----------------------------------------------------- | ------------- |
| Create PRs that prepare the Python client for release | None          |
| Create release candidate tag                          | Committer     |
| Upload release candidate wheels to TestPyPI           | PMC           |
| Create release candidate tarball and publish to SVN   | PMC           |
| Start and call the vote on the mailing list           | PMC           |
| Publish release tarball to SVN                        | PMC           |
| Publish wheels to PyPI                                | PMC           |

## Prepare the Python Client

Create a PR against `main` that moves `python/` to the new release:

- Set `version` in `python/Cargo.toml` to the version being released.
- Depend on the published `ballista`, `ballista-core`, `ballista-executor` and `ballista-scheduler`
  crates, pinned to that version, for example `ballista = { version = "=55.0.0" }`. The source
  release only contains `python/`, so there must be no `path` dependencies. `create-tarball.sh`
  checks this.
- Update `datafusion-python` to its new release, and update `datafusion`, `datafusion-proto` and
  `pyo3` to the versions that datafusion-python uses, so that everything links the same DataFusion.
- Update the `datafusion` requirement in `python/pyproject.toml`.
- Refresh the lock files by running `cargo update` and `uv lock` in `python/`.

Once the PR is merged, cherry-pick it onto the release branch.

## Create a Release Candidate

### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, etc. Python client release
candidates are numbered independently of the Rust ones.

### Create the git tag

Tag the head of the release branch and push the tag:

```shell
git fetch apache
git checkout apache/branch-55
git tag python-55.0.0-rc1
git push apache python-55.0.0-rc1
```

Pushing the tag starts the `Python Release Build` workflow. Wait for it to succeed before
continuing.

### Download the wheels

The wheels and the sdist that are voted on, and later uploaded to PyPI, are the ones built by the
release candidate's CI run. They are never rebuilt.

One-time setup:

- Create accounts on [pypi.org](https://pypi.org) and [test.pypi.org](https://test.pypi.org)
  (separate accounts).
- Ask an existing maintainer of the `ballista` PyPI project, listed on the project page, to add you
  as a maintainer. The request should be made on the dev mailing list so it is publicly tracked.
- Generate project-scoped API tokens for both PyPI and TestPyPI.
- Configure `~/.pypirc`:

  ```ini
  [distutils]
  index-servers =
      pypi
      testpypi

  [pypi]
  username = __token__
  password = pypi-...

  [testpypi]
  repository = https://test.pypi.org/legacy/
  username = __token__
  password = pypi-...
  ```

- Restrict the permissions on `~/.pypirc` so the API tokens are not world-readable:

  ```bash
  chmod 600 ~/.pypirc
  ```

- Install `twine` and `requests` (the latter is used by
  `python/dev/release/download-python-wheels.py`):

  ```bash
  pip install twine requests
  ```

Export the release version and RC number so the rest of this document can be copy-pasted without
manual edits:

```bash
export BALLISTA_VERSION=55.0.0       # PEP 440 release version; matches the wheels
export BALLISTA_RC_NUM=1              # which RC tag CI built the wheels from
export GH_TOKEN=...                   # GitHub PAT with read access to actions
```

From the root of the repository:

```bash
mkdir ballista-pypi-${BALLISTA_VERSION}-rc${BALLISTA_RC_NUM}
cd ballista-pypi-${BALLISTA_VERSION}-rc${BALLISTA_RC_NUM}
python ../python/dev/release/download-python-wheels.py python-${BALLISTA_VERSION}-rc${BALLISTA_RC_NUM}
ls *.whl *.tar.gz       # confirm filenames carry the right version
```

Keep this directory until the release is published, because these are the files that get uploaded
to PyPI after the vote.

> **Artifact retention warning:** GitHub Actions artifacts default to 90-day retention. If the
> downloaded files are lost after that window, the voted-on wheels are unrecoverable and you must
> cut a new RC and revote. Check the run's `expires_at` on
> `https://github.com/apache/datafusion-ballista/actions` if in doubt.

> **GPG signing needs an interactive terminal.** The script signs each artifact with
> `gpg --detach-sig`, which prompts for the key passphrase. From a non-interactive shell the prompt
> fails with `gpg: signing failed: Inappropriate ioctl for device` and the script aborts after the
> first artifact. Either run from an interactive shell, or configure `gpg-agent` with
> `pinentry-mode loopback` and a cached passphrase. The wheels and sdist are downloaded before the
> signing step, so for a TestPyPI upload the traceback is harmless (PyPI does not accept `.asc` files
> anyway).

The merged artifact should contain one of each of the following files (file naming uses
[PEP 425](https://peps.python.org/pep-0425/) tags; the `manylinux_X_Y` glibc tag depends on the
Linux runner image and changes over time, so glob it rather than pinning a specific value):

- `ballista-${BALLISTA_VERSION}-cp310-abi3-manylinux_*_x86_64.whl`
- `ballista-${BALLISTA_VERSION}-cp310-abi3-manylinux_*_aarch64.whl`
- `ballista-${BALLISTA_VERSION}-cp310-abi3-macosx_*_arm64.whl`
- `ballista-${BALLISTA_VERSION}.tar.gz` (sdist)

> **Verify every expected file is present.** The `merge-build-artifacts` job in
> `.github/workflows/build.yml` has been observed to silently drop wheels when merging the
> per-platform artifacts. If any file from the list above is missing from the merged `dist`
> artifact, fall back to downloading the individual per-platform artifacts directly from the
> workflow run:
>
> ```bash
> gh run download <run-id> --repo apache/datafusion-ballista \
>   --name dist-manylinux-aarch64 \
>   --name dist-manylinux-x86_64 \
>   --name dist-macos-latest \
>   --name dist-sdist
> ```
>
> Then re-sign each downloaded file with `gpg --detach-sig` and regenerate the `.sha256` / `.sha512`
> checksums the same way `download-python-wheels.py` does. Do **not** proceed with an incomplete
> set of files.
>
> If only the sdist is missing, it can also be rebuilt locally from the RC tag (the `build-sdist`
> job uploads it as `dist-sdist` so this should not normally be needed):
>
> ```bash
> git checkout python-${BALLISTA_VERSION}-rc${BALLISTA_RC_NUM}
> cd python
> uv run --no-project maturin sdist --out dist
> ```

Check the metadata of the downloaded files:

```bash
twine check *.whl *.tar.gz
```

The `download-python-wheels.py` script also writes `.asc` GPG signatures and `.sha256` / `.sha512`
checksum files alongside each artifact. PyPI rejects them, so pass explicit globs to `twine` so
only the wheels and sdist are considered.

### Upload the wheels to TestPyPI

Uploading the release candidate to TestPyPI lets voters install the exact files that will be
published to PyPI, and catches the common ways a PyPI upload goes wrong. PyPI uploads are
immutable: once a version is published it cannot be replaced or re-uploaded, only yanked.

```bash
twine upload --repository testpypi *.whl *.tar.gz

# Wheels are cp310-abi3 so the venv needs Python >= 3.10. Using `python -m venv`
# with macOS's stock /usr/bin/python3 (3.9) silently picks no wheel and pip
# reports a misleading "No matching distribution found".
python3.10 -m venv /tmp/ballista-pypi-smoke
source /tmp/ballista-pypi-smoke/bin/activate
pip install -i https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    ballista==${BALLISTA_VERSION}
python -c "from ballista import BallistaSessionContext; print('ok')"
deactivate
```

`--extra-index-url` is required because TestPyPI does not mirror dependencies like `pyarrow` and
`datafusion`.

TestPyPI also accepts each filename only once, so the wheels of a second release candidate for the
same version cannot be uploaded there. In that case skip this step and remove the TestPyPI link from
the vote email.

### Create, sign, and upload the source tarball

Make sure your signing key is in the `KEYS` files, as described in
[Create, sign, and upload artifacts](../../../dev/release/README.md#create-sign-and-upload-artifacts)
for the Rust release. Then run `create-tarball.sh` with the version and RC number:

```shell
./python/dev/release/create-tarball.sh 55.0.0 1
```

The `create-tarball.sh` script

1. checks that `python/Cargo.toml` at the tag has the expected version and no `path` dependencies,

2. creates a tarball of the `python/` directory at the tag, runs the Apache RAT license check, signs
   it, and uploads it to the [datafusion dev](https://dist.apache.org/repos/dist/dev/datafusion)
   location on the apache distribution svn server,

3. provides you an email template to send to dev@datafusion.apache.org for release voting.

### Vote on Release Candidate artifacts

Send the email output from the script to dev@datafusion.apache.org. The vote stays open for at
least 72 hours, and for the release to become "official" it needs at least three PMC members to
vote +1 on it.

## Verifying Release Candidates

`python/dev/release/verify-release-candidate.sh` downloads the source tarball from the ASF dev SVN,
verifies its GPG signature and checksums, builds the Python client and runs the Python tests. It
needs [uv](https://docs.astral.sh/uv/getting-started/installation/), and it installs a Rust
toolchain in a temporary directory. Run it like:

```shell
./python/dev/release/verify-release-candidate.sh 55.0.0 1
```

### (Optional) Verify the wheels from TestPyPI

If the release manager has uploaded the RC's wheels to
[test.pypi.org](https://test.pypi.org/project/ballista/), verifiers can install them in a throwaway
virtualenv to sanity-check the artifacts that will ship to real PyPI. The wheels there are
byte-identical to what would be uploaded to pypi.org if the vote passes.

The wheels are built as `cp310-abi3`, so the venv needs Python ≥ 3.10:

```bash
export BALLISTA_VERSION=55.0.0    # version under vote

python3.10 -m venv /tmp/ballista-rc-verify
source /tmp/ballista-rc-verify/bin/activate
pip install -i https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    ballista==${BALLISTA_VERSION}
python -c "from ballista import BallistaSessionContext; print('ok')"
deactivate
```

### If the release is not approved

If the release is not approved, fix whatever the problem is, merge the fix into `main` and the
release branch, and try again with the next RC number.

## Finalize the Release

NOTE: steps in this section can only be done by PMC members.

### Call the vote

Call the vote on the DataFusion dev list by replying to the RC voting thread. The reply should have
a new subject constructed by adding `[RESULT]` prefix to the old subject line.

Sample announcement template:

```
The vote has passed with <NUMBER> +1 votes. Thank you to all who helped
with the release verification.
```

### Publish the source tarball

Move the artifacts to the release location in SVN, e.g.
https://dist.apache.org/repos/dist/release/datafusion/datafusion-ballista-python-55.0.0/, using the
`release-tarball.sh` script:

```shell
./python/dev/release/release-tarball.sh 55.0.0 1
```

### Create the release git tag

Tag the same release candidate commit with the final release tag:

```shell
git checkout python-55.0.0-rc1
git tag python-55.0.0
git push apache python-55.0.0
```

### Publish the wheels to PyPI

Only approved releases should be published to PyPI, in order to conform to Apache Software
Foundation governance standards. Upload the files that were voted on, from the directory they were
downloaded to in [Download the wheels](#download-the-wheels):

```bash
twine upload *.whl *.tar.gz
```

If the upload fails partway through, re-run with `--skip-existing` to retry only the files that did
not get through.

Confirm the new version appears at `https://pypi.org/project/ballista/${BALLISTA_VERSION}/`. Then in
another fresh virtual environment:

```bash
python3.10 -m venv /tmp/ballista-pypi-verify
source /tmp/ballista-pypi-verify/bin/activate
pip install ballista==${BALLISTA_VERSION}
python -c "from ballista import BallistaSessionContext; print('ok')"
deactivate
```

#### Recovery

**`twine check` fails.** The artifacts shipped from CI are malformed (bad metadata, missing
`LICENSE.txt`, etc.). Do not proceed. Open an issue, fix in `python/pyproject.toml` or the
`generate-license` job, cut a new RC, re-vote. Do not hand-edit wheels.

**TestPyPI smoke install or import fails.** Same recovery, the wheels are broken, so cut a new RC.
The TestPyPI version stays published forever. You can yank it with
`twine yank --repository testpypi ballista ${BALLISTA_VERSION}` so it does not resolve, but the
filename is permanently consumed on TestPyPI.

**PyPI upload fails partway.** Some wheels uploaded, others did not. Re-run with `--skip-existing`:

```bash
twine upload --skip-existing *.whl *.tar.gz
```

If a _broken_ file actually made it to PyPI, it cannot be replaced.
`twine yank ballista ${BALLISTA_VERSION}` removes the version from `pip install ballista`
resolution, but the version number is permanently consumed. Recovery requires bumping to
`${BALLISTA_VERSION}.post1` and starting over from [Create a Release Candidate](#create-a-release-candidate),
since post-releases must also be voted on.

### Add the release to Apache Reporter

Add the release to https://reporter.apache.org/addrelease.html?datafusion with a version name
prefixed with `BALLISTA-PYTHON-`, for example `BALLISTA-PYTHON-55.0.0`.

### Delete old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

Release candidates should be deleted once the release is published:

```bash
svn ls https://dist.apache.org/repos/dist/dev/datafusion | grep ballista-python
svn delete -m "delete old Ballista Python RC" https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-ballista-python-55.0.0-rc1/
```

Only the latest release should be available. Delete old releases after publishing the new release:

```bash
svn ls https://dist.apache.org/repos/dist/release/datafusion | grep ballista-python
svn delete -m "delete old Ballista Python release" https://dist.apache.org/repos/dist/release/datafusion/datafusion-ballista-python-<old-version>
```
