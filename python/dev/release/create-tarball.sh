#!/bin/bash
#
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
#

# Adapted from dev/release/create-tarball.sh, which does the same for the
# Rust crates.

# This script creates a signed source tarball of the Ballista Python client in
# dev/dist/apache-datafusion-ballista-python-<version>-rc<rc>/, uploads it to
# the "dev" area of the dist.apache.org datafusion repository and prints an
# email for sending to the dev@datafusion.apache.org list for a formal vote.
#
# The tarball only contains the python/ directory, so python/Cargo.toml must
# depend on the published ballista crates rather than on the Rust workspace.
#
# See python/dev/release/README.md for full release instructions
#
# Requirements:
#
# 1. gpg setup for signing and have uploaded your public
# signature to https://pgp.mit.edu/
#
# 2. Logged into the apache svn server with the appropriate
# credentials
#
# 3. Java, to run the Apache RAT license check
#

set -e
set -u

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../../" && pwd)"

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <version> <rc>"
    echo "ex. $0 55.0.0 1"
    exit 1
fi

version=$1
rc=$2
tag="python-${version}-rc${rc}"

release_hash=$(cd "${SOURCE_TOP_DIR}" && git rev-list --max-count=1 "${tag}" 2>/dev/null || true)
if [ -z "${release_hash}" ]; then
    echo "Cannot continue: unknown git tag: ${tag}"
    exit 1
fi

cargo_toml=$(cd "${SOURCE_TOP_DIR}" && git show "${release_hash}:python/Cargo.toml")

# The tarball does not contain the Rust workspace, so a path dependency on it
# would leave the source release unbuildable.
if echo "${cargo_toml}" | grep -v '^[[:space:]]*#' | grep -Eq '(^|[{,[:space:]])path[[:space:]]*='; then
    echo "Cannot continue: python/Cargo.toml at ${tag} has path dependencies."
    echo "Depend on the published ballista crates instead, for example:"
    echo "  ballista = { version = \"=${version}\" }"
    exit 1
fi

crate_version=$(echo "${cargo_toml}" | sed -En 's/^version[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' | head -1)
if [ "${crate_version}" != "${version}" ]; then
    echo "Cannot continue: python/Cargo.toml at ${tag} has version ${crate_version}, expected ${version}"
    exit 1
fi

ballista_version=$(echo "${cargo_toml}" | sed -En 's/^ballista[[:space:]]*=[[:space:]]*(\{[^"]*)?"=?([^"]+)".*/\2/p' | head -1)

release=apache-datafusion-ballista-python-${version}
distdir=${SOURCE_TOP_DIR}/dev/dist/${release}-rc${rc}
tarname=${release}.tar.gz
tarball=${distdir}/${tarname}
url="https://dist.apache.org/repos/dist/dev/datafusion/${release}-rc${rc}"

echo "Attempting to create ${tarball} from tag ${tag}"

# create <tarball> containing the files in python/ at $release_hash
# the files in the tarball are prefixed with ${release}
# (e.g. apache-datafusion-ballista-python-55.0.0/pyproject.toml)
mkdir -p "${distdir}"
(cd "${SOURCE_TOP_DIR}" && git archive --prefix="${release}/" "${release_hash}:python" | gzip > "${tarball}")

echo "Running rat license checker on ${tarball}"
"${SOURCE_TOP_DIR}/dev/release/run-rat.sh" "${tarball}"

echo "Signing tarball and creating checksums"
gpg --armor --output "${tarball}.asc" --detach-sig "${tarball}"
# create signing with relative path of tarball
# so that they can be verified with a command such as
#  shasum --check apache-datafusion-ballista-python-55.0.0.tar.gz.sha512
(cd "${distdir}" && shasum -a 256 "${tarname}") > "${tarball}.sha256"
(cd "${distdir}" && shasum -a 512 "${tarname}") > "${tarball}.sha512"

echo "Uploading to apache dist/dev to ${url}"
svn co --depth=empty https://dist.apache.org/repos/dist/dev/datafusion "${SOURCE_TOP_DIR}/dev/dist"
svn add "${distdir}"
svn ci -m "Apache DataFusion Ballista Python ${version} ${rc}" "${distdir}"

echo "Draft email for dev@datafusion.apache.org mailing list"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@datafusion.apache.org
Subject: [VOTE] Release Apache DataFusion Ballista Python ${version} RC${rc}
Hi,

I would like to propose a release of the Apache DataFusion Ballista Python
client version ${version}. It is built against the Apache DataFusion Ballista
${ballista_version:-<unknown>} crates published to crates.io.

This release candidate is based on commit: ${release_hash} [1]
The proposed release tarball and signatures are hosted at [2].
The changes to the Python client are listed at [3].
The Python wheels built from this release candidate are published to TestPyPI at [4].

Please download, verify checksums and signatures, run the unit tests, and vote
on the release. The vote will be open for at least 72 hours.

Only votes from PMC members are binding, but all members of the community are
encouraged to test the release and vote with "(non-binding)".

The standard verification procedure is documented at https://github.com/apache/datafusion-ballista/blob/main/python/dev/release/README.md#verifying-release-candidates.

[ ] +1 Release this as Apache DataFusion Ballista Python ${version}
[ ] +0
[ ] -1 Do not release this as Apache DataFusion Ballista Python ${version} because...

Here is my vote: +1

[1]: https://github.com/apache/datafusion-ballista/tree/${release_hash}
[2]: ${url}
[3]: https://github.com/apache/datafusion-ballista/commits/${release_hash}/python
[4]: https://test.pypi.org/project/ballista/${version}/
MAIL
echo "---------------------------------------------------------"
