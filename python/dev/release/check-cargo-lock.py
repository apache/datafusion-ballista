#!/usr/bin/env python3

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

# Checks that python/Cargo.lock is ready for a Python client release: the
# client has the version being released, it wraps the ballista crates of the
# same version, and every Rust dependency comes from crates.io. The source
# release only contains python/, so a path or git dependency would build Rust
# code that was never voted on, or not build at all.
#
# Usage: check-cargo-lock.py <version> [<path to Cargo.lock>]
#
# Reads Cargo.lock from stdin when no path is given. Needs Python 3.11 or later.

import sys
import tomllib

CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"


def main():
    if len(sys.argv) not in (2, 3):
        sys.exit(f"Usage: {sys.argv[0]} <version> [<path to Cargo.lock>]")

    version = sys.argv[1]
    if len(sys.argv) == 3:
        with open(sys.argv[2], "rb") as f:
            packages = tomllib.load(f)["package"]
    else:
        packages = tomllib.load(sys.stdin.buffer)["package"]

    errors = []
    if not any(p["name"] == "pyballista" for p in packages):
        errors.append("pyballista is missing from Cargo.lock")
    for p in packages:
        name, pkg_version = p["name"], p["version"]
        if name == "pyballista":
            if pkg_version != version:
                errors.append(f"pyballista is {pkg_version}, expected {version}")
            continue
        if p.get("source") != CRATES_IO:
            source = p.get("source", "a path dependency")
            errors.append(f"{name} {pkg_version} comes from {source}, not crates.io")
        elif (name == "ballista" or name.startswith("ballista-")) and (
            pkg_version != version
        ):
            errors.append(f"{name} is {pkg_version}, expected {version}")

    if errors:
        sys.exit("Cargo.lock is not ready for release:\n  " + "\n  ".join(errors))
    print(f"Cargo.lock is ready for release {version}")


if __name__ == "__main__":
    main()
