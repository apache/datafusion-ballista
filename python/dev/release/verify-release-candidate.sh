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

# Adapted from dev/release/verify-release-candidate.sh, which does the same
# for the Rust crates.
#
# Requires uv (https://docs.astral.sh/uv/) to check python/Cargo.lock, build
# the Python client and run its tests.

case $# in
  2) VERSION="$1"
     RC_NUMBER="$2"
     ;;
  *) echo "Usage: $0 X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -e
set -x
set -o pipefail

DATAFUSION_DIST_URL='https://dist.apache.org/repos/dist/dev/datafusion'

if ! type uv >/dev/null 2>&1; then
  echo "uv is required, see https://docs.astral.sh/uv/getting-started/installation/"
  exit 1
fi

download_dist_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name $DATAFUSION_DIST_URL/$1
}

download_rc_file() {
  download_dist_file apache-datafusion-ballista-python-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  download_dist_file KEYS
  gpg --import KEYS
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha256
  download_rc_file ${dist_name}.tar.gz.sha512
  verify_dir_artifact_signatures
}

verify_dir_artifact_signatures() {
  # verify the signature and the checksums of each artifact
  find . -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd "$(dirname "$artifact")"
    base_artifact=$(basename $artifact)
    ${sha256_verify} $base_artifact.sha256 || exit 1
    ${sha512_verify} $base_artifact.sha512 || exit 1
    popd
  done
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${DATAFUSION_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${DATAFUSION_TMPDIR} for details."
    fi
  }

  if [ -z "${DATAFUSION_TMPDIR}" ]; then
    # clean up automatically if DATAFUSION_TMPDIR is not defined
    DATAFUSION_TMPDIR=$(mktemp -d -t "$1.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${DATAFUSION_TMPDIR}"
  fi
}

test_source_distribution() {
  # the source release only contains the Python client, so every Rust
  # dependency has to come from crates.io
  uv run --no-project --python '>=3.11' python dev/release/check-cargo-lock.py "${VERSION}" Cargo.lock

  # install rust toolchain in a similar fashion like test-miniconda, outside
  # the source tree so that pytest does not pick up files from the crates
  export RUSTUP_HOME=${DATAFUSION_TMPDIR}/test-rustup
  export CARGO_HOME=${DATAFUSION_TMPDIR}/test-rustup

  curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path --profile minimal

  export PATH=$RUSTUP_HOME/bin:$PATH
  source $RUSTUP_HOME/env

  # build the Python client and run its tests, the same way CI does
  uv sync --dev --no-install-package ballista
  uv run pytest python/tests
}

TEST_SUCCESS=no

setup_tempdir "datafusion-ballista-python-${VERSION}"
echo "Working in sandbox ${DATAFUSION_TMPDIR}"
cd ${DATAFUSION_TMPDIR}

dist_name="apache-datafusion-ballista-python-${VERSION}"
import_gpg_keys
fetch_archive ${dist_name}
tar xf ${dist_name}.tar.gz
pushd ${dist_name}
    test_source_distribution
popd

TEST_SUCCESS=yes
echo 'Release candidate looks good!'
exit 0
