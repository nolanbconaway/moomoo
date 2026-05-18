"""A reusable docker container implementation for testing."""

import io
import shutil
import tarfile
import time
from pathlib import Path

import httpx
from testcontainers.core.container import DockerContainer

TESTS_DIR = Path(__file__).parent
MUSIC_DIR = TESTS_DIR / "resources" / "music"
SEED_DB = TESTS_DIR / "resources" / "navidrome.db"

SUBSONIC_PARAMS = {
    "u": "admin",
    "p": "admin",
    "v": "1.16.1",
    "c": "pytest",
    "f": "json",
}


class NavidromeContainer(DockerContainer):
    def __init__(self, data_dir: Path) -> None:
        super().__init__("deluan/navidrome:latest")

        # seed the data dir with the desired data
        self.data_dir = data_dir
        shutil.copy2(SEED_DB, data_dir / "navidrome.db")

        (
            self.with_env("ND_DEVAUTOCREATEADMINPASSWORD", "admin")
            .with_env("ND_AUTHREQUESTLIMIT", "0")
            .with_env("ND_DATAFOLDER", "/data")
            .with_env("ND_MUSICFOLDER", "/music")
            .with_env("ND_SCANNER_ENABLED", "false")
            .with_env("ND_SCANNER_SCANONSTARTUP", "false")
            .with_env("ND_LOGLEVEL", "info")
            .with_env("ND_ENABLEGRAVATAR", "false")
            .with_env("ND_LASTFM_ENABLED", "false")
            .with_env("ND_SPOTIFY_ID", "")
            .with_volume_mapping(str(MUSIC_DIR), "/music", "ro")
            .with_volume_mapping(str(data_dir), "/data", "rw")
            .with_volume_mapping(str(SEED_DB), "/seed/navidrome.db", "ro")
            .with_exposed_ports(4533)
        )

    @property
    def url(self) -> str:
        return f"http://localhost:{self.get_exposed_port(4533)}"

    def wait_for_http(self, timeout: float = 60.0, interval: float = 0.5) -> None:
        deadline = time.monotonic() + timeout
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                resp = httpx.get(f"{self.url}/rest/ping", params=SUBSONIC_PARAMS, timeout=2)
                if resp.status_code == 200:
                    return
            except httpx.HTTPError as exc:
                last_exc = exc
            time.sleep(interval)
        raise TimeoutError(f"Navidrome did not become ready within {timeout}s") from last_exc

    def wait_for_scan(self, timeout: float = 120.0) -> int:
        """Trigger a full scan and block until complete. Returns track count."""
        httpx.get(f"{self.url}/rest/startScan", params={**SUBSONIC_PARAMS, "fullScan": "true"})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = httpx.get(f"{self.url}/rest/getScanStatus", params=SUBSONIC_PARAMS)
            status = r.json()["subsonic-response"]["scanStatus"]
            if not status["scanning"]:
                return status["count"]
            time.sleep(1)
        raise TimeoutError(f"Scan did not complete within {timeout}s")

    def checkpoint(self) -> None:
        """Flush WAL to main DB file."""
        self.get_wrapped_container().exec_run(
            "sqlite3 /data/navidrome.db 'PRAGMA wal_checkpoint(TRUNCATE)'"
        )

    def restore_seed(self) -> None:
        """Reset the DB to the seed state."""
        self.get_wrapped_container().exec_run(
            "sh -c 'cp /seed/navidrome.db /data/navidrome.db "
            "&& rm -f /data/navidrome.db-wal /data/navidrome.db-shm'"
        )

    def extract_db(self) -> bytes:
        """Return the current DB as bytes, checkpointing first."""
        self.checkpoint()
        raw, _ = self.get_wrapped_container().get_archive("/data/navidrome.db")
        tar_bytes = io.BytesIO(b"".join(raw))
        with tarfile.open(fileobj=tar_bytes) as tar:
            f = tar.extractfile(tar.getmember("navidrome.db"))
            return f.read()
