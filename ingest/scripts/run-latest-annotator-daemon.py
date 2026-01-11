#! /usr/bin/env python3
"""Runs the latest version of the annotator daemon from GitHub.

Does the following:

1. Fetches the latest version number from GitHub.
2, Checks if that version is currently running.
3. If running, exits.
4. If not running, checks that the latest version is built locally.
5. If not built, exits with an error.
6. If built, kills any existing annotator daemon processes, and starts the latest version.
"""

import argparse
import json
import os
import subprocess
import sys
import time

GH_URL = (
    "https://raw.githubusercontent.com/nolanbconaway/moomoo/main/ingest/src/moomoo_ingest/version"
)
CONTAINER_NAME = "moomoo-annotator-daemon-latest"


def check_currently_running() -> dict | None:
    """Check if any moomoo-ingest container is running."""
    cmd = ["docker", "ps", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")
    for line in output.splitlines():
        container = json.loads(line)
        if container["Image"].startswith("moomoo-ingest-v"):
            print("Detected running moomoo-ingest container")
            version = container["Image"].split("moomoo-ingest-v")[-1]
            container["moomoo_version"] = version
            return container

    print("No running moomoo-ingest container detected")
    return None


def get_gh_version() -> str:
    """Get the latest moomoo-ingest version from GitHub."""
    try:
        output = subprocess.check_output(["curl", "--silent", GH_URL]).decode("utf-8")
    except subprocess.CalledProcessError as e:
        print(f"Failed to get the latest moomoo-ingest version from GitHub: {e}")
        return None

    res = output.strip()
    print(f"Latest moomoo-ingest version: {res}")
    return res


def check_version_available(version: str) -> bool:
    """Check if the latest moomoo-ingest version is available."""
    cmd = ["docker", "image", "ls", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")

    images = [
        json.loads(line)
        for line in output.splitlines()
        if json.loads(line)["Repository"].startswith("moomoo-ingest-v")
    ]
    if not images:
        print("No moomoo-ingest images found")
        return False

    res = any(image["Repository"] == f"moomoo-ingest-v{version}" for image in images)

    if not res:
        print(f"moomoo-ingest-v{version} not found")
    else:
        print(f"moomoo-ingest-v{version} found")

    return res


def docker_run(version: str, detach: bool, restart: bool) -> None:
    """Run the moomoo annotator daemon container."""
    postgres_uri = os.environ["MOOMOO_DOCKER_POSTGRES_URI"]
    moomoo_dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    moomoo_contact_email = os.environ["MOOMOO_CONTACT_EMAIL"]
    repo_name = f"moomoo-ingest-v{version}"
    cmd = [
        "docker",
        "run",
        "--add-host=host.docker.internal:host-gateway",
        "--env",
        f"MOOMOO_DBT_SCHEMA={moomoo_dbt_schema}",
        "--env",
        f"MOOMOO_CONTACT_EMAIL={moomoo_contact_email}",
        "--env",
        "PYTHONUNBUFFERED=1",
        "--env",
        f"MOOMOO_POSTGRES_URI={postgres_uri}",
        "--name",
        CONTAINER_NAME,
    ]

    if detach:
        cmd += ["--detach"]

    cmd += ["--restart=unless-stopped"] if restart else ["--rm"]

    cmd += [repo_name, "moomoo-ingest", "annotation-daemon"]
    # print(shlex.join(cmd))
    subprocess.run(cmd, check=True)


def stop_existing_containers(version: str) -> None:
    """Stop any existing moomoo-ingest containers."""
    cmd = ["docker", "ps", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")
    for line in output.splitlines():
        container = json.loads(line)
        if container["Image"].startswith(f"moomoo-ingest-v{version}"):
            container_id = container["ID"]
            print(f"Stopping container {container_id}")
            subprocess.check_output(["docker", "rm", "-f", container_id])


def tail_logs(container_id: str, n: int = 10) -> None:
    """Tail the logs of a container."""
    cmd = ["docker", "logs", "--tail", str(n), container_id]
    output = subprocess.check_output(cmd).decode("utf-8")
    print(output)


def parse_arguments():
    """Parse command-line arguments using argparse."""
    parser = argparse.ArgumentParser(description="Run the latest HTTP daemon.")
    parser.add_argument(
        "--force-stop",
        action="store_true",
        help="Force stop existing containers.",
        default=False,
    )
    parser.add_argument(
        "--no-force-stop",
        action="store_false",
        dest="force_stop",
        help="Do not force stop existing containers.",
        default=True,
    )
    parser.add_argument(
        "--detach",
        action="store_true",
        help="Run the container in detached mode.",
        default=True,
    )
    parser.add_argument(
        "--no-detach",
        action="store_false",
        dest="detach",
        help="Run the container in attached mode.",
        default=False,
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Restart the container automatically on failure.",
        default=True,
    )
    parser.add_argument(
        "--no-restart",
        action="store_false",
        dest="restart",
        help="Do not restart the container automatically on failure.",
        default=False,
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5600,
        help="Port on which to run the HTTP server (default: 5600).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    force_stop = args.force_stop
    detach = args.detach
    restart = args.restart

    print("Fetching the latest moomoo-ingest version from GitHub...")
    gh_version = get_gh_version()
    if gh_version is None:
        print("Failed to get the latest moomoo-ingest version from GitHub")
        sys.exit(1)

    print("Checking currently running moomoo-ingest container...")
    running_container = check_currently_running()
    running_version = None if running_container is None else running_container["moomoo_version"]
    if running_version == gh_version and not force_stop:
        print(f"Latest moomoo-ingest version {gh_version} is already running. Exiting.")
        container_id = running_container["ID"]
        print("Follow the logs with:\n")
        print(f"    docker logs --follow {container_id}")
        print()
        print("Tailing the last 10 lines of logs:\n")
        tail_logs(container_id, n=10)
        sys.exit(0)

    print("Checking if the latest moomoo-ingest version is built locally...")
    if not check_version_available(gh_version):
        print(f"Latest moomoo-ingest version {gh_version} is not built locally. Exiting.")
        sys.exit(1)

    if running_version is not None or force_stop:
        print("Stopping existing moomoo-ingest containers...")
        stop_existing_containers(running_version)

        # check no longer running
        if check_currently_running() is not None:
            print("Failed to stop existing moomoo-ingest container. Exiting.")
            sys.exit(1)

    if detach:
        print(f"Starting moomoo-ingest version {gh_version} in detached mode.")
    else:
        print(f"Starting moomoo-ingest version {gh_version}. Press Ctrl+C to exit.")
    docker_run(gh_version, detach=detach, restart=restart)

    # can exit here if not detached, as logs will be shown in the foreground
    if not detach:
        return

    # wait 5s and check running. if not, exit with error. otherwise get 10 log lines
    print("Waiting 5 seconds for the new container to start...")
    time.sleep(5)

    print("Checking if the new moomoo-ingest container is running...")
    new_container = check_currently_running()
    if new_container is None or new_container["moomoo_version"] != gh_version:
        print("Failed to start the new moomoo-ingest container. Exiting.")
        sys.exit(1)

    container_id = new_container["ID"]
    print(f"New moomoo-ingest container {container_id} is running.")
    print(f"Tailing the lines of logs for container {container_id}.")
    print("See more logs with:\n")
    print(f"    docker logs --follow {container_id}")
    print()

    tail_logs(new_container["ID"], n=10)


if __name__ == "__main__":
    main()
