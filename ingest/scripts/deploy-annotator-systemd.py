"""Deploy the annotator daemon as a systemd service.

Uses the run-latest-annotator-daemon.py script to run the latest annotator, setting up management
with systemd using the systemd/template-moomoo-annotator.service file.
"""

import getpass
import subprocess
import tempfile
import time
from pathlib import Path

SYSTEMD_NAME = "moomoo-annotator.service"
SYSTEMD_TARGET = Path(f"/etc/systemd/system/{SYSTEMD_NAME}")


def run_command(command, *args, **kwargs):
    """Use subprocess to run a command, printing it first.'
    
    Needed here because we need sudo for some commands.
    """
    print("Running: " + " ".join(map(str, command)))
    return subprocess.check_output(command, *args, **kwargs)


def main() -> None:
    repo_path = Path(__file__).parent.parent.resolve()
    template_path = repo_path / "systemd/template-moomoo-annotator.service"
    envvfile_path = (repo_path / ".env").resolve()
    annotator_daemon_launcher_path = (
        repo_path / "scripts" / "run-latest-annotator-daemon.py"
    ).resolve()

    assert template_path.exists(), f"Template file not found: {template_path}"
    assert envvfile_path.exists(), f".env file not found: {envvfile_path}"
    assert (
        annotator_daemon_launcher_path.exists()
    ), f"Annotator daemon launcher not found: {annotator_daemon_launcher_path}"

    service_txt = template_path.read_text().format(
        MOOMOO_INGEST_ENVVFILE=str(envvfile_path),
        MOOMOO_ANNOTATOR_DAEMON_LAUNCHER=str(annotator_daemon_launcher_path),
        MOOMOO_INGEST_PATH=str(repo_path),
        WHOAMI=getpass.getuser(),
    )

    print("Generated service file content:\n")
    print(service_txt)
    print()
    print(f"Deploying service file to {SYSTEMD_TARGET}")

    with tempfile.NamedTemporaryFile("w", delete=False) as tmpfile:
        tmpfile.write(service_txt)
        tmpfile.flush()
        run_command(["sudo", "cp", tmpfile.name, str(SYSTEMD_TARGET)])

    print("Reloading systemd daemon...")
    run_command(["sudo", "systemctl", "daemon-reload"])

    print("Enabling annotator service...")
    run_command(["sudo", "systemctl", "enable", SYSTEMD_NAME])

    print("Starting annotator service...")
    run_command(["sudo", "systemctl", "start", SYSTEMD_NAME])

    time.sleep(2)
    print("Checking annotator service status...")
    status_output = run_command(["sudo", "systemctl", "status", SYSTEMD_NAME])
    print(status_output.decode())


if __name__ == "__main__":
    main()
