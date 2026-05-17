#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script that download python release artifacts from Github
#
# dependencies:
# pip install requests


import sys
import os
import argparse
import requests
import zipfile
import subprocess
import hashlib
import io


def main():
    parser = argparse.ArgumentParser(
        description='Download python binary wheels from release candidate workflow runs.')
    parser.add_argument('tag', type=str, help='datafusion RC release tag')
    args = parser.parse_args()

    tag = args.tag
    ghp_token = os.environ.get("GH_TOKEN")
    if not ghp_token:
        print(
            "ERROR: Personal Github token is required to download workflow artifacts. "
            "Please specify a token through GH_TOKEN environment variable.")
        sys.exit(1)

    print(f"Downloading latest python wheels for RC tag {tag}...")

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {ghp_token}",
    }

    # Resolve the tag to a commit SHA. Filtering runs by branch name does not
    # work reliably: when the RC tag points at a commit that is also the head
    # of a release branch (e.g. branch-51), GitHub associates the successful
    # workflow run with the branch rather than the tag, and the branch query
    # only returns the (possibly cancelled) tag-triggered run.
    commit_url = f"https://api.github.com/repos/apache/datafusion-ballista/commits/{tag}"
    resp = requests.get(commit_url, headers=headers)
    resp.raise_for_status()
    sha = resp.json()["sha"]
    print(f"Tag {tag} resolves to commit {sha}")

    url = f"https://api.github.com/repos/apache/datafusion-ballista/actions/runs?head_sha={sha}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    artifacts_url = None
    for run in resp.json()["workflow_runs"]:
        if run["name"] != "Python Release Build":
            continue
        if run.get("conclusion") != "success":
            continue
        artifacts_url = run["artifacts_url"]
        break

    if artifacts_url is None:
        print(
            f"ERROR: Could not find a successful 'Python Release Build' run for "
            f"commit {sha}. Re-run the workflow or cut a new RC.")
        sys.exit(1)
    print(f"Found artifacts url: {artifacts_url}")

    download_url = None
    artifacts = requests.get(artifacts_url, headers=headers).json()["artifacts"]
    for artifact in artifacts:
        if artifact["name"] != "dist":
            continue
        download_url = artifact["archive_download_url"]

    if download_url is None:
        print(f"ERROR: Could not resolve python wheel download URL from list of artifacts: {artifacts}")
        sys.exit(1)
    print(f"Extracting archive from: {download_url}...")

    resp = requests.get(download_url, headers=headers, stream=True)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    zf.extractall("./")

    for entry in os.listdir("./"):
        if entry.endswith(".whl") or entry.endswith(".tar.gz"):
            print(f"Sign and checksum artifact: {entry}")
            subprocess.check_output([
                "gpg", "--armor",
                "--output", entry+".asc",
                "--detach-sig", entry,
            ])

            sha256 = hashlib.sha256()
            sha512 = hashlib.sha512()
            with open(entry, "rb") as fd:
                while True:
                    data = fd.read(65536)
                    if not data:
                        break
                    sha256.update(data)
                    sha512.update(data)
            with open(entry+".sha256", "w") as fd:
                fd.write(sha256.hexdigest())
                fd.write("  ")
                fd.write(entry)
                fd.write("\n")
            with open(entry+".sha512", "w") as fd:
                fd.write(sha512.hexdigest())
                fd.write("  ")
                fd.write(entry)
                fd.write("\n")


if __name__ == "__main__":
    main()
