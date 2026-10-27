#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
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


"""Configure this Ballista checkout to use its local DataFusion SPJ prerequisite.

Usage: uv run dev/configure_storage_partition_join.py ../datafusion-spj
Then add --config .local/spj-cargo.toml to Cargo commands in this checkout.
"""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("datafusion", type=Path)
    args = parser.parse_args()
    source = args.datafusion.resolve(strict=True)
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=source, text=True,
    ))
    packages = [p for p in metadata["packages"] if p["name"].startswith("datafusion") and p.get("publish") != []]
    core = next(p for p in packages if p["name"] == "datafusion")
    if core["version"] != "55.2.0":
        raise SystemExit("This SPJ stack is based on DataFusion 55.2.0")
    if "pub struct GroupedPartitioning" not in (source / "datafusion/physical-expr/src/partitioning.rs").read_text():
        raise SystemExit("Check out the DataFusion spj/distribution-contract branch first")
    root = Path(__file__).resolve().parent.parent
    config = root / ".local/spj-cargo.toml"
    config.parent.mkdir(exist_ok=True)
    # JSON quoted strings are valid TOML basic strings; no shell interpolation.
    lines = ["[patch.crates-io]"]
    for package in sorted(packages, key=lambda p: p["name"]):
        directory = str(Path(package["manifest_path"]).parent)
        lines.append(f'{package["name"]} = {{ path = {json.dumps(directory, ensure_ascii=False)} }}')
    config.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Configured {config} for {source}")


if __name__ == "__main__":
    main()
