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

# Sync the vendored DataFusion proto files with the version pinned in Cargo.lock.
#
# `ballista/core/proto/ballista.proto` imports `datafusion.proto` and
# `datafusion_common.proto`. Those two files are NOT compiled into Rust here --
# `ballista/core/build.rs` maps their packages to the real `datafusion-proto` /
# `datafusion-proto-common` crates via `extern_path`. They exist only so that
# `protoc` can resolve `ballista.proto`'s imports at build time. Because of that
# they must stay compatible with the DataFusion version Ballista depends on.
#
# The `datafusion-proto` / `datafusion-proto-common` crates ship their `.proto`
# files, but expose no `links`/`DEP_*` path for a build script to consume, so we
# vendor a copy. This script keeps that copy honest: it reads the exact crate
# source resolved by `cargo metadata` and copies the `.proto` files in, rewriting
# the one import path that differs from our flat layout.
#
# Usage:
#   dev/update_datafusion_proto.py            # update the vendored files in place
#   dev/update_datafusion_proto.py --check    # fail if vendored files are stale (CI)

import argparse
import difflib
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.absolute()
VENDOR_DIR = REPO_ROOT / "ballista" / "core" / "proto"

# vendored file name -> (crate name, file name within the crate's `proto/` dir)
#
# DataFusion 55 moved the logical-plan proto out of `datafusion-proto` into the
# new `datafusion-proto-models` crate.  We list both names so the script keeps
# working on v54 (datafusion-proto) and v55+ (datafusion-proto-models).
FILES = {
    "datafusion.proto": (
        ["datafusion-proto-models", "datafusion-proto"],
        "datafusion.proto",
    ),
    "datafusion_common.proto": (
        ["datafusion-proto-common"],
        "datafusion_common.proto",
    ),
}

# Every crate that might be referenced in FILES must be looked up.
_ALL_CRATES = {
    crate
    for crate_list, _ in FILES.values()
    for crate in crate_list
}


def crate_source_dirs():
    """Map crate name -> (source directory, version), as resolved in Cargo.lock."""
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(out.stdout)
    dirs = {}
    for pkg in metadata["packages"]:
        if pkg["name"] in _ALL_CRATES:
            # manifest_path points at the crate's Cargo.toml; protos sit alongside it.
            dirs[pkg["name"]] = (Path(pkg["manifest_path"]).parent, pkg["version"])
    return dirs


def rewrite(content: str) -> str:
    """Rewrite crate-relative imports to our flat vendored layout.

    Upstream imports the common proto by its repo path, e.g.
    `import "datafusion/proto-common/proto/datafusion_common.proto";`
    but we vendor it flat next to `datafusion.proto`.
    """
    return re.sub(
        r'import\s+"[^"]*datafusion_common\.proto";',
        'import "datafusion_common.proto";',
        content,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the vendored files are up to date; do not write. Exits 1 on drift.",
    )
    args = parser.parse_args()

    dirs = crate_source_dirs()
    stale = []
    for vendor_name, (crate_list, src_name) in FILES.items():
        # Try each crate in preference order; use the first one that ships the file.
        src = None
        chosen_crate = None
        chosen_version = None
        for crate in crate_list:
            if crate not in dirs:
                continue
            crate_dir, version = dirs[crate]
            candidate = crate_dir / "proto" / src_name
            if candidate.exists():
                src = candidate
                chosen_crate = crate
                chosen_version = version
                break

        if src is None:
            # None of the candidate crates ship the expected file.  This is
            # always an error: a missing source means the check cannot run and
            # proto drift would go undetected.
            tried = ", ".join(
                f"{c}@{dirs[c][1]}" if c in dirs else f"{c} (not in Cargo.lock)"
                for c in crate_list
            )
            sys.exit(
                f"error: could not find proto/{src_name} in any of: {tried}\n"
                f"  Update FILES in this script if the crate was renamed."
            )

        expected = rewrite(src.read_text())
        dest = VENDOR_DIR / vendor_name

        if args.check:
            current = dest.read_text() if dest.exists() else ""
            if current != expected:
                stale.append(vendor_name)
                diff = difflib.unified_diff(
                    current.splitlines(keepends=True),
                    expected.splitlines(keepends=True),
                    fromfile=f"vendored/{vendor_name}",
                    tofile=f"{chosen_crate}@{chosen_version}/proto/{src_name}",
                )
                sys.stderr.writelines(diff)
        else:
            dest.write_text(expected)
            print(
                f"updated {dest.relative_to(REPO_ROOT)} "
                f"from {chosen_crate}@{chosen_version}"
            )

    if args.check and stale:
        sys.exit(
            "\nVendored DataFusion proto file(s) out of date: "
            + ", ".join(stale)
            + "\nRun `dev/update_datafusion_proto.py` and commit the result."
        )


if __name__ == "__main__":
    main()
