import os
from collections.abc import Generator, Iterator
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from testcontainers.core.container import DockerContainer

from moomoo_navidrome.navidrome import NavidromeDBClient, NavidromeHTTPClient

from .navidrome_container import SUBSONIC_PARAMS, NavidromeContainer


@pytest.fixture(scope="session")
def navidrome_data_dir(tmp_path_factory) -> Path:
    """A temporary directory for Navidrome to store its data."""
    data_dir = tmp_path_factory.mktemp("navidrome_data")
    return data_dir


@pytest.fixture(scope="session")
def navidrome_container(navidrome_data_dir) -> Generator[DockerContainer, None, None]:
    with NavidromeContainer(navidrome_data_dir) as container:
        container.wait_for_http()
        yield container


@pytest.fixture(scope="session")
def navidrome_session(navidrome_container) -> Generator[httpx.Client, None, None]:
    """An httpx.Client pre-configured with the Navidrome base URL and Subsonic auth params.

    Shared across the test session — don't mutate it in tests.
    """
    patch_env = {
        "NAVIDROME_URL": navidrome_container.url,
        "NAVIDROME_USERNAME": SUBSONIC_PARAMS["u"],
        "NAVIDROME_PASSWORD": SUBSONIC_PARAMS["p"],
    }
    with patch.dict(os.environ, patch_env), NavidromeHTTPClient() as client:
        yield client


@pytest.fixture
def clean_navidrome(navidrome_container: NavidromeContainer) -> Iterator[None]:
    """Reset the Navidrome database to a known state before each test."""
    # maybe seed db added as a dependency so that we are sure it runs before cleaning.

    navidrome_container.restore_seed()
    yield
    navidrome_container.restore_seed()


@pytest.fixture
def db_client(navidrome_data_dir: Path, clean_navidrome: None) -> NavidromeDBClient:
    """A NavidromeDBClient connected to the test container's data."""
    return NavidromeDBClient(navidrome_data_dir / "navidrome.db")


@pytest.fixture
def http_client(
    navidrome_session: NavidromeHTTPClient, db_client: NavidromeDBClient
) -> NavidromeHTTPClient:
    """Per-test alias for navidrome_session.

    If a test needs isolation (different user, modified params, etc.), override this fixture
    locally — don't mutate navidrome_session directly.

    Has a db client dependency so that the navidrome db cleaner resolves in the right order.
    """
    # copy the init db file to the data dir before each test to reset the state
    return navidrome_session
