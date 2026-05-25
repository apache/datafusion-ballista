"""Tests for split-changelog.py.

These tests exercise the splitter against the live CHANGELOG.md in the
repo root. Run from repo root with:

    python3 -m pytest dev/release/tests/test_split_changelog.py -v
"""

import importlib.util
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "dev" / "release" / "split-changelog.py"

spec = importlib.util.spec_from_file_location("split_changelog", SCRIPT)
split_changelog = importlib.util.module_from_spec(spec)
sys.modules["split_changelog"] = split_changelog
spec.loader.exec_module(split_changelog)


def test_drop_list_skips_umbrella_repo_entries():
    dropped = {"6.0.0", "6.0.0-rc0", "7.0.0", "7.0.0-rc2", "7.1.0-rc1"}
    assert dropped == split_changelog.DROP_VERSIONS


def test_strip_ballista_prefix():
    assert split_changelog.normalize_version("ballista-0.7.0") == "0.7.0"
    assert split_changelog.normalize_version("0.12.0") == "0.12.0"
    assert split_changelog.normalize_version("53.0.0") == "53.0.0"


def test_split_sections_finds_all_kept_versions():
    text = (REPO_ROOT / "CHANGELOG.md").read_text()
    sections = split_changelog.split_sections(text)
    versions = [s.version for s in sections]
    # 24 raw headings - 5 dropped umbrella-repo entries = 19 kept files
    kept = [v for v in versions if v not in split_changelog.DROP_VERSIONS]
    assert len(kept) == 19, f"expected 19 kept sections, got {len(kept)}: {kept}"
    # Spot-check first and last
    assert kept[0] == "53.0.0"
    assert kept[-1] == "ballista-0.5.0"


def test_render_section_has_asf_header_and_title():
    text = (REPO_ROOT / "CHANGELOG.md").read_text()
    sections = split_changelog.split_sections(text)
    s = next(s for s in sections if s.version == "53.0.0")
    out = split_changelog.render(s)
    assert out.startswith("<!--\nLicensed to the Apache Software Foundation"), \
        "output must start with the ASF license header"
    assert "# Apache DataFusion Ballista 53.0.0 Changelog" in out
    assert "**Implemented enhancements:**" in out
    # Carry over the Full Changelog link line
    assert "compare/52.0.0...53.0.0" in out


def test_render_strips_ballista_prefix_from_title():
    text = (REPO_ROOT / "CHANGELOG.md").read_text()
    sections = split_changelog.split_sections(text)
    s = next(s for s in sections if s.version == "ballista-0.7.0")
    out = split_changelog.render(s)
    assert "# Apache DataFusion Ballista 0.7.0 Changelog" in out
    assert "ballista-0.7.0" not in out.splitlines()[19], \
        "title line must not contain the ballista- prefix"
