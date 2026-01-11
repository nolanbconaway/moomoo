"""Deploy the http server as a systemd service.

Uses the run-latest-http-daemon.py script to run the latest http server, setting up management
with systemd using the systemd/template-moomoo-http.service file.
"""

import getpass
import subprocess
import tempfile
import time
from pathlib import Path

SYSTEMD_NAME = "moomoo-http.service"
SYSTEMD_TARGET = Path(f"/etc/systemd/system/{SYSTEMD_NAME}")


def run_command(command, *args, **kwargs):
    """Use subprocess to run a command, printing it first.'

    Needed here because we need sudo for some commands.
    """
    print("Running: " + " ".join(map(str, command)))
    return subprocess.check_output(command, *args, **kwargs)


def main() -> None:
    repo_path = Path(__file__).parent.parent.resolve()
    template_path = repo_path / "systemd/template-moomoo-http.service"
    envvfile_path = (repo_path / ".env").resolve()
    http_daemon_launcher_path = (
        repo_path / "scripts" / "run-latest-http-daemon.py"
    ).resolve()

    assert template_path.exists(), f"Template file not found: {template_path}"
    assert envvfile_path.exists(), f".env file not found: {envvfile_path}"
    assert (
        http_daemon_launcher_path.exists()
    ), f"HTTP daemon launcher not found: {http_daemon_launcher_path}"

    service_txt = template_path.read_text().format(
        MOOMOO_HTTP_ENVVFILE=str(envvfile_path),
        MOOMOO_HTTP_DAEMON_LAUNCHER=str(http_daemon_launcher_path),
        MOOMOO_HTTP_PATH=str(repo_path),
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

    print("Enabling http service...")
    run_command(["sudo", "systemctl", "enable", SYSTEMD_NAME])

    print("Starting http service...")
    run_command(["sudo", "systemctl", "start", SYSTEMD_NAME])

    time.sleep(2)
    print("Checking http service status...")
    status_output = run_command(["sudo", "systemctl", "status", SYSTEMD_NAME])
    print(status_output.decode())


if __name__ == "__main__":
    main()
