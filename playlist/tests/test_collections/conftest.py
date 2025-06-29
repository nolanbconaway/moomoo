import pytest

from moomoo_playlist.collections.create_collections import create_collections


@pytest.fixture(autouse=True)
def setup_collections(session):
    """Create the regestered collections before running tests."""
    create_collections(username="test", session=session, silent=True)
    yield
