import pytest
from moomoo_playlist.db import get_session
from moomoo_playlist.ddl import PlaylistCollection


@pytest.fixture(autouse=True)
def create_storage():
    with get_session() as session:
        PlaylistCollection.metadata.create_all(session.bind)
        yield
