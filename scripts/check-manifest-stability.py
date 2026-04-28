#!/usr/bin/env python3
"""
check-manifest-stability.py -- Ensure no stable MANIFEST.yaml IDs are removed.

Compares the MANIFEST.yaml on the PR branch (HEAD) against the base branch.
If any ID present in base is missing from HEAD without a major version bump,
the check fails.

Usage: python scripts/check-manifest-stability.py
  Requires git and the base branch to be fetchable.
  Uses GITHUB_BASE_REF env var in CI, falls back to 'main'.
"""
import os
import subprocess
import sys
from pathlib import Path

import yaml


def get_ids(content: str) -> set:
    """Extract all capability IDs from MANIFEST.yaml content."""
    data = yaml.safe_load(content)
    if not data or "capabilities" not in data:
        return set()
    return {cap["id"] for cap in data["capabilities"]}


def get_version(content: str) -> str:
    """Extract version string from MANIFEST.yaml content."""
    data = yaml.safe_load(content)
    return data.get("version", "0.0.0") if data else "0.0.0"


def is_major_bump(old_version: str, new_version: str) -> bool:
    """Check if version change is a major bump (e.g., 1.x.x -> 2.x.x)."""
    try:
        old_major = int(old_version.split(".")[0])
        new_major = int(new_version.split(".")[0])
        return new_major > old_major
    except (ValueError, IndexError):
        return False


def main():
    manifest_path = Path("MANIFEST.yaml")

    if not manifest_path.exists():
        print("ERROR: MANIFEST.yaml not found in current directory", file=sys.stderr)
        sys.exit(1)

    # Read current (HEAD) version
    head_content = manifest_path.read_text()
    head_ids = get_ids(head_content)
    head_version = get_version(head_content)

    # Get base branch from CI environment or default to main
    base_ref = os.environ.get("GITHUB_BASE_REF", "main")

    # Read base version of MANIFEST.yaml
    try:
        result = subprocess.run(
            ["git", "show", f"origin/{base_ref}:MANIFEST.yaml"],
            capture_output=True,
            text=True,
            check=True,
        )
        base_content = result.stdout
    except subprocess.CalledProcessError:
        # MANIFEST.yaml doesn't exist on base branch -- first introduction
        print(f"MANIFEST.yaml not found on origin/{base_ref} -- first introduction. OK.")
        sys.exit(0)

    base_ids = get_ids(base_content)
    base_version = get_version(base_content)

    # Check for removed IDs
    removed = base_ids - head_ids
    added = head_ids - base_ids

    if removed:
        if is_major_bump(base_version, head_version):
            print(f"Major version bump ({base_version} -> {head_version}): "
                  f"{len(removed)} ID(s) removed (allowed).")
            for rid in sorted(removed):
                print(f"  REMOVED (major bump): {rid}")
            sys.exit(0)
        else:
            print(f"ERROR: {len(removed)} stable ID(s) removed without major version bump!")
            print(f"  Current version: {head_version} (base: {base_version})")
            print(f"  To remove IDs, bump the major version (e.g., {head_version} -> "
                  f"{int(head_version.split('.')[0]) + 1}.0.0)")
            for rid in sorted(removed):
                print(f"  REMOVED: {rid}")
            sys.exit(1)

    # Report status
    print(f"Manifest stability check passed.")
    print(f"  Version: {head_version}")
    print(f"  IDs: {len(head_ids)} ({len(added)} added, 0 removed)")


if __name__ == "__main__":
    main()
