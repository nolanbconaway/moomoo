#!/usr/bin/env python3
"""Tag a release for a monorepo package if the version doesn't already exist as a tag."""

import argparse
import subprocess
import sys
from pathlib import Path


def run(*args: str) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True
    ).stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("module", help="Module directory name, e.g. 'pg'")
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip confirmation prompt before pushing the tag",
    )
    args = parser.parse_args()

    # assert that the module directory exists
    if not Path(args.module).is_dir():
        print(f"Error: Module directory '{args.module}' does not exist.")
        sys.exit(1)

    version = run("make", "--no-print-directory", "-C", args.module, "version")
    if not version:
        print(f"Error: could not determine version for module '{args.module}'")
        sys.exit(1)

    tag = f"{args.module}-v{version}"

    run("git", "fetch", "--tags")
    if run("git", "tag", "--list", tag):
        print(f"Tag {tag} already exists, skipping.")
        return

    if not args.yes:
        reply = input(f"Create and push tag '{tag}'? [y/N] ").strip().lower()
        if reply != "y":
            print("Aborted.")
            sys.exit(1)

    run("git", "tag", tag)
    run("git", "push", "origin", tag)
    print(f"Created and pushed tag {tag}")


if __name__ == "__main__":
    main()
