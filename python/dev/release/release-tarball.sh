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

# Adapted from dev/release/release-tarball.sh, which does the same for the
# Rust crates.

# This script copies a Python client tarball from the "dev" area of the
# dist.apache.datafusion repository to the "release" area
#
# This script should only be run after the release has been approved
# by the DataFusion PMC committee.
#
# See python/dev/release/README.md for full release instructions


set -e
set -u

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  echo "ex. $0 55.0.0 1"
  exit 1
fi

version=$1
rc=$2

read -r -p "Proceed to release Python client tarball for ${version}-rc${rc}? [y/N]: " answer
answer=${answer:-no}
if [ "${answer}" != "y" ]; then
  echo "Cancelled tarball release!"
  exit 1
fi

rc_url=https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-ballista-python-${version}-rc${rc}
release_url=https://dist.apache.org/repos/dist/release/datafusion/datafusion-ballista-python-${version}

# a server-side copy, so neither area has to be checked out
echo "Copy ${rc_url} to ${release_url}"
svn cp -m "Apache DataFusion Ballista Python ${version}" "${rc_url}" "${release_url}"

echo "Success! The release is available here:"
echo "  ${release_url}"
