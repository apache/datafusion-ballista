#!/usr/bin/env python3

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

"""One-shot splitter that turns the monolithic root CHANGELOG.md into
one file per release under docs/source/changelog/<version>.md.

Run from the repo root:

    python3 dev/release/split-changelog.py

Idempotent: re-running overwrites existing per-version files.
"""

import dataclasses
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "CHANGELOG.md"
DEST_DIR = REPO_ROOT / "docs" / "source" / "changelog"

DROP_VERSIONS = {"6.0.0", "6.0.0-rc0", "7.0.0", "7.0.0-rc2", "7.1.0-rc1"}

# Matches headings like:
#   ## [53.0.0](https://github.com/apache/datafusion-ballista/tree/53.0.0) (2026-05-19)
#   ## [ballista-0.7.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.7.0) (2022-05-12)
# Also tolerates the malformed entry:
#   ## [43.0.0](<https://.../tree/43.0.0-rc2> (2025-01-07)
# We only need the version slug from inside the first [ ].
HEADING_RE = re.compile(r"^## \[(?P<version>[^\]]+)\]", re.MULTILINE)

ASF_HEADER = """<!--
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
"""


@dataclasses.dataclass
class Section:
    version: str  # raw version slug from the heading, e.g. "ballista-0.7.0"
    body: str     # content from heading line (inclusive) to next heading (exclusive)


def split_sections(text: str) -> list[Section]:
    """Carve the changelog into per-version Section objects."""
    sections: list[Section] = []
    matches = list(HEADING_RE.finditer(text))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        sections.append(
            Section(version=m.group("version"), body=text[start:end].rstrip() + "\n")
        )
    return sections


def normalize_version(version: str) -> str:
    """Strip the legacy 'ballista-' filename prefix."""
    return version[len("ballista-"):] if version.startswith("ballista-") else version


def render(section: Section) -> str:
    """Render a section as a standalone per-version Markdown file."""
    version = normalize_version(section.version)
    # Drop the original heading line (we replace it with our own title)
    # and strip any leading blank lines that follow.
    body_lines = section.body.splitlines()
    # Drop the first line (the `## [version](...)` heading)
    body_lines = body_lines[1:]
    # Skip leading blank lines
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    body = "\n".join(body_lines).rstrip() + "\n"

    parts = [
        ASF_HEADER,
        "",
        f"# Apache DataFusion Ballista {version} Changelog",
        "",
        body,
    ]
    return "\n".join(parts)


def main() -> int:
    if not SRC.exists():
        print(f"error: {SRC} not found", file=sys.stderr)
        return 1
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    text = SRC.read_text()
    sections = split_sections(text)
    written = 0
    skipped = 0
    for s in sections:
        if s.version in DROP_VERSIONS:
            skipped += 1
            print(f"skip {s.version}", file=sys.stderr)
            continue
        out_name = f"{normalize_version(s.version)}.md"
        out_path = DEST_DIR / out_name
        out_path.write_text(render(s))
        written += 1
        print(f"wrote {out_path.relative_to(REPO_ROOT)}", file=sys.stderr)

    print(f"\n{written} files written, {skipped} skipped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
