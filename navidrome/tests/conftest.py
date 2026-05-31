import logging
from collections.abc import Generator
from pathlib import Path

import pytest
from loguru import logger

from moomoo_navidrome.navidrome import NavidromeDBClient

from .navidrome_container import SUBSONIC_PARAMS, NavidromeContainer


@pytest.fixture(autouse=True)
def caplog_for_loguru(caplog):
    """Make loguru work with caplog."""

    class PropagateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropagateHandler(), format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(scope="session")
def navidrome_data_dir(tmp_path_factory) -> Path:
    """A temporary directory for Navidrome to store its data."""
    data_dir = tmp_path_factory.mktemp("navidrome_data")
    return data_dir


@pytest.fixture(scope="session")
def navidrome_container(navidrome_data_dir) -> Generator[NavidromeContainer, None, None]:
    with NavidromeContainer(navidrome_data_dir) as container:
        container.wait_for_http()
        yield container


@pytest.fixture(autouse=True)
def setup_environment(monkeypatch, navidrome_data_dir, navidrome_container):
    """Automatically set up environment variables for tests and setup a clean db state."""
    envvars = [
        "LISTENBRAINZ_USERNAME",
        "LISTENBRAINZ_USER_TOKEN",
        "MOOMOO_DBT_SCHEMA",
    ]
    for v in envvars:
        monkeypatch.setenv(v, f"FAKE_{v}")

    # set navidrome connection envvars based on the test container
    monkeypatch.setenv("NAVIDROME_DB_PATH", str(navidrome_data_dir / "navidrome.db"))
    monkeypatch.setenv("NAVIDROME_USERNAME", SUBSONIC_PARAMS["u"])
    monkeypatch.setenv("NAVIDROME_PASSWORD", SUBSONIC_PARAMS["p"])
    monkeypatch.setenv("NAVIDROME_URL", navidrome_container.url)

    yield

    # reset the navidrome state to overwrite any changes made during the test
    wal_file = navidrome_data_dir / "navidrome.db-wal"
    if wal_file.exists() and wal_file.stat().st_size > 0:
        navidrome_container.restore_seed()
        navidrome_container.restart()  # restart after restoring seed so Navidrome reloads
        monkeypatch.setenv("NAVIDROME_URL", navidrome_container.url)


@pytest.fixture
def songs() -> dict[str, Path]:
    """All known song ids in the seeded db."""
    with NavidromeDBClient().connect() as conn:
        cursor = conn.cursor()
        cursor.execute("select id as song_id, path from media_file")
        return {row["song_id"]: Path(row["path"]) for row in cursor.fetchall()}
