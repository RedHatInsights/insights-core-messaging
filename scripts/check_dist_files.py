#!/usr/bin/env python3
"""Check that every .py file in insights_messaging/ (excluding tests/) is
included in exactly one distribution defined under packages/."""

import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO_ROOT / "insights_messaging"
PACKAGES_DIR = REPO_ROOT / "packages"

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def find_source_files():
    return sorted(
        f.relative_to(REPO_ROOT)
        for f in SOURCE_DIR.rglob("*.py")
        if "tests" not in f.relative_to(SOURCE_DIR).parts
    )


def resolve_force_include(pyproject_path):
    pkg_dir = pyproject_path.parent
    with open(pyproject_path, "rb") as fh:
        data = tomllib.load(fh)

    force_include = (
        data.get("tool", {})
        .get("hatch", {})
        .get("build", {})
        .get("targets", {})
        .get("wheel", {})
        .get("force-include", {})
    )

    files = set()
    for src in force_include:
        src_path = (pkg_dir / src).resolve()
        if src_path.is_dir():
            files.update(f.relative_to(REPO_ROOT) for f in src_path.rglob("*.py"))
        elif src_path.is_file() and src_path.suffix == ".py":
            files.add(src_path.relative_to(REPO_ROOT))
    return files


def main():
    source_files = set(find_source_files())
    file_to_dists = defaultdict(list)

    for pyproject in sorted(PACKAGES_DIR.glob("*/pyproject.toml")):
        dist_name = pyproject.parent.name
        for f in resolve_force_include(pyproject):
            file_to_dists[f].append(dist_name)

    errors = False

    missing = sorted(source_files - set(file_to_dists))
    if missing:
        errors = True
        print("Files not included in any distribution:")
        for f in missing:
            print(f"  {f}")

    duplicated = sorted(f for f, dists in file_to_dists.items() if len(dists) > 1)
    if duplicated:
        errors = True
        print("Files included in multiple distributions:")
        for f in duplicated:
            print(f"  {f} -> {', '.join(file_to_dists[f])}")

    extra = sorted(set(file_to_dists) - source_files)
    if extra:
        errors = True
        print("Files listed in distributions but not found on disk:")
        for f in extra:
            print(f"  {f} -> {', '.join(file_to_dists[f])}")

    if errors:
        sys.exit(1)

    print("All files are included in exactly one distribution.")


if __name__ == "__main__":
    main()
