"""Test the shared playlist utilities."""
import uuid
from unittest.mock import patch

import pytest
from moomoo_http.db import Base, MoomooPlaylist, db
from moomoo_http.playlist_generator import BasePlaylistGenerator
from moomoo_http.routes.playlist import PlaylistArgs, get_playlist_result
from werkzeug.datastructures import TypeConversionDict

from ...conftest import load_local_files_table


class FakePlaylistGenerator(BasePlaylistGenerator):
    """Fake playlist generator."""

    name = "fake"

    def get_playlist(self, limit: int, **_) -> tuple:
        """Fake playlist generator."""
        return [f"test/{i}" for i in range(limit)], ["test/0"]


@pytest.fixture(autouse=True)
def create_storage():
    """Create the storage table."""
    Base.metadata.create_all(db.engine)


@pytest.fixture(autouse=True)
def load_local_files_table__fixed():
    """Preload each test with a local files table."""
    data = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=uuid.uuid4())
        for i in range(10)
    ]
    load_local_files_table(data=data)


def test_playlist_args__from_request():
    """Test the playlist args from request constructor."""

    class Request:
        """Fake request."""

        args = None

    request = Request()

    request.args = TypeConversionDict(n="0", seed="0", shuffle="0")
    args = PlaylistArgs.from_request(request)
    assert args.n == 0
    assert args.seed == 0
    assert args.shuffle is False

    request.args = TypeConversionDict(n="1", seed="1", shuffle="true")
    args = PlaylistArgs.from_request(request)
    assert args.n == 1
    assert args.seed == 1
    assert args.shuffle is True


def test_get_playlist_result():
    """Test the composition of a playlist from files."""
    # basic
    generator = FakePlaylistGenerator()
    args = PlaylistArgs(n=3, seed=0, shuffle=True)
    res = get_playlist_result(generator=generator, args=args, username="a")
    assert res["success"]
    assert res["playlist"] == [f"test/{i}" for i in range(3)]
    assert res["source_paths"] == ["test/0"]

    # test storage
    assert db.session.query(MoomooPlaylist).count() == 1
    row = db.session.query(MoomooPlaylist).first()
    assert row.username == "a"
    assert row.playlist == res["playlist"]
    assert row.source_paths == res["source_paths"]

    # handle error on get_playlist
    with patch.object(generator, "get_playlist", side_effect=Exception("test")):
        res, status = get_playlist_result(generator=generator, args=args, username="a")
        assert status == 500
        assert res["success"] is False
        assert res["error"] == "Exception: test"
        assert db.session.query(MoomooPlaylist).count() == 1  # still only one row

    # handle error on insert does not raise
    with patch.object(db.session, "add", side_effect=Exception("test")):
        res = get_playlist_result(generator=generator, args=args, username="a")
        assert res["success"] is True
        assert res["playlist"] == [f"test/{i}" for i in range(3)]
        assert res["source_paths"] == ["test/0"]
        assert db.session.query(MoomooPlaylist).count() == 1  # still only one row
