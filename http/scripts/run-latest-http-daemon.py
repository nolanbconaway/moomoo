#! /usr/bin/env python3
"""Runs the latest version of the http server from GitHub.

Does the following:

1. Fetches the latest version number from GitHub.
2. Checks if that version is currently running.
3. If running, exits.
4. If not running, checks that the latest version is built locally.
5. If not built, exits with an error.
6. If built, kills any existing http server processes, and starts the latest version.
"""

import json
import os
import subprocess
import sys
import time

import click

GH_URL = (
    "https://raw.githubusercontent.com/nolanbconaway/moomoo/main/http/src/moomoo_http/version"
)
CONTAINER_NAME = "moomoo-http-daemon-latest"


def check_currently_running() -> dict | None:
    """Check if any moomoo-http container is running."""
    cmd = ["docker", "ps", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")
    for line in output.splitlines():
        container = json.loads(line)
        if container["Image"].startswith("moomoo-http-v"):
            print("Detected running moomoo-http container")
            version = container["Image"].split("moomoo-http-v")[-1]
            container["moomoo_version"] = version
            return container

    print("No running moomoo-http container detected")
    return None


def get_gh_version() -> str:
    """Get the latest moomoo-http version from GitHub."""
    try:
        output = subprocess.check_output(["curl", "--silent", GH_URL]).decode("utf-8")
    except subprocess.CalledProcessError as e:
        print(f"Failed to get the latest moomoo-http version from GitHub: {e}")
        return None

    res = output.strip()
    print(f"Latest moomoo-http version: {res}")
    return res


def check_version_available(version: str) -> bool:
    """Check if the latest moomoo-http version is available."""
    cmd = ["docker", "image", "ls", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")

    images = [
        json.loads(line)
        for line in output.splitlines()
        if json.loads(line)["Repository"].startswith("moomoo-http-v")
    ]
    if not images:
        print("No moomoo-http images found")
        return False

    res = any(image["Repository"] == f"moomoo-http-v{version}" for image in images)

    if not res:
        print(f"moomoo-http-v{version} not found")
    else:
        print(f"moomoo-http-v{version} found")

    return res


def docker_run(version: str, detach: bool, restart: bool) -> None:
    """Run the moomoo http server container."""
    postgres_uri = os.environ["MOOMOO_DOCKER_POSTGRES_URI"]
    moomoo_dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    repo_name = f"moomoo-http-v{version}"
    cmd = [
        "docker",
        "run",
        "--add-host=host.docker.internal:host-gateway",
        "--env",
        f"MOOMOO_POSTGRES_URI={postgres_uri}",
        "--env",
        f"MOOMOO_DBT_SCHEMA={moomoo_dbt_schema}",
        "--env",
        "PYTHONUNBUFFERED=1",
        "--publish=5600:8080",
        "--name",
        CONTAINER_NAME,
    ]

    if detach:
        cmd += ["--detach"]

    cmd += ["--restart=unless-stopped"] if restart else ["--rm"]

    cmd += [repo_name, "make", "http"]
    subprocess.run(cmd, check=True)


def stop_existing_containers(version: str) -> None:
    """Stop any existing moomoo-http containers."""
    cmd = ["docker", "ps", "--format=json"]
    output = subprocess.check_output(cmd).decode("utf-8")
    for line in output.splitlines():
        container = json.loads(line)
        if container["Image"].startswith(f"moomoo-http-v{version}"):
            container_id = container["ID"]
            print(f"Stopping container {container_id}")
            subprocess.check_output(["docker", "stop", container_id])


def tail_logs(container_id: str, n: int = 10) -> None:
    """Tail the logs of a container."""
    cmd = ["docker", "logs", "--tail", str(n), container_id]
    output = subprocess.check_output(cmd).decode("utf-8")
    print(output)


@click.command()
@click.option(
    "--force-stop/-no-force-stop",
    "force_stop",
    is_flag=True,
    default=False,
    help="Force stop existing containers.",
)
@click.option(
    "--detach/--no-detach",
    "detach",
    is_flag=True,
    default=True,
    help="Run the container in detached mode.",
)
@click.option(
    "--restart/--no-restart",
    "restart",
    is_flag=True,
    default=True,
    help="Whether to restart the container automatically on failure.",
)
def main(force_stop: bool, detach: bool, restart: bool) -> None:
    print("Fetching the latest moomoo-http version from GitHub...")
    gh_version = get_gh_version()
    if gh_version is None:
        print("Failed to get the latest moomoo-http version from GitHub")
        sys.exit(1)

    print("Checking currently running moomoo-http container...")
    running_container = check_currently_running()
    running_version = None if running_container is None else running_container["moomoo_version"]
    if running_version == gh_version and not force_stop:
        print(f"Latest moomoo-http version {gh_version} is already running. Exiting.")
        container_id = running_container["ID"]
        print("Follow the logs with:\n")
        print(f"    docker logs --follow {container_id}")
        print()
        print("Tailing the last 10 lines of logs:\n")
        tail_logs(container_id, n=10)
        sys.exit(0)

    print("Checking if the latest moomoo-http version is built locally...")
    if not check_version_available(gh_version):
        print(f"Latest moomoo-http version {gh_version} is not built locally. Exiting.")
        sys.exit(1)

    if running_version is not None or force_stop:
        print("Stopping existing moomoo-http containers...")
        stop_existing_containers(running_version)

        # check no longer running
        if check_currently_running() is not None:
            print("Failed to stop existing moomoo-http container. Exiting.")
            sys.exit(1)

    if detach:
        print(f"Starting moomoo-http version {gh_version} in detached mode.")
    else:
        print(f"Starting moomoo-http version {gh_version}. Press Ctrl+C to exit.")
    docker_run(gh_version, detach=detach, restart=restart)

    # can exit here if not detached, as logs will be shown in the foreground
    if not detach:
        return

    # wait 5s and check running. if not, exit with error. otherwise get 10 log lines
    print("Waiting 5 seconds for the new container to start...")
    time.sleep(5)

    print("Checking if the new moomoo-http container is running...")
    new_container = check_currently_running()
    if new_container is None or new_container["moomoo_version"] != gh_version:
        print("Failed to start the new moomoo-http container. Exiting.")
        sys.exit(1)

    container_id = new_container["ID"]
    print(f"New moomoo-http container {container_id} is running.")
    print(f"Tailing the lines of logs for container {container_id}.")
    print("See more logs with:\n")
    print(f"    docker logs --follow {container_id}")
    print()

    tail_logs(new_container["ID"], n=10)


if __name__ == "__main__":
    main()
