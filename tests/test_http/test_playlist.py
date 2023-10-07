import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from flask.testing import FlaskClient

from moomoo.db import MoomooPlaylist
from moomoo.http.app import create_app
from moomoo.utils_ import PlaylistResult

from ..conftest import load_local_files_table


@pytest.fixture
def http_app() -> FlaskClient:
    """Create a test client for the http app."""
    app = create_app()
    return app.test_client()


@pytest.fixture(autouse=True)
def create_storage():
    MoomooPlaylist.create()


@pytest.fixture(autouse=True)
def load_local_files_table__fixed():
    """Preload each test with a local files table."""
    data = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=uuid.uuid4())
        for i in range(10)
    ]
    load_local_files_table(data=data)


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    resp = http_app.get("/playlist/from-files", query_string=dict(path="test/3949"))
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No listenbrainz-username header provided."

    resp = http_app.get(
        "/playlist/from-files",
        query_string=dict(),
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No filepaths provided."

    query_string = "&".join([f"path=test{i}" for i in range(1000)])
    resp = http_app.get(
        "/playlist/from-files",
        query_string=query_string,
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "Too many filepaths provided (>500)."


def test_invalid_filepaths(http_app: FlaskClient):
    """Test that an error is returned when invalid filepaths are provided."""
    resp = http_app.get(
        "/playlist/from-files",
        query_string=dict(path="test/3949"),
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No paths requested (or found via request)." in resp.json["error"]

    with patch(
        "moomoo.playlist.PlaylistGenerator.get_playlist",
        side_effect=Exception("test exception message"),
    ) as mock:
        resp = http_app.get(
            "/playlist/from-files",
            query_string=dict(path="test/3949"),
            headers={"listenbrainz-username": "a"},
        )
        assert resp.status_code == 500
        assert resp.json["success"] is False
        assert "test exception message" in resp.json["error"]
        assert mock.call_count == 1

    with patch(
        "moomoo.playlist.PlaylistGenerator.get_playlist",
        return_value=PlaylistResult(playlist=[], source_paths=[Path("test/3949")]),
    ) as mock:
        resp = http_app.get(
            "/playlist/from-files",
            query_string=dict(path="test/3949"),
            headers={"listenbrainz-username": "a"},
        )
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlist"] == []
        assert resp.json["source_paths"] == ["test/3949"]
        assert mock.call_count == 1


def test_from_files_playlist(http_app: FlaskClient):
    """Test the composition of a playlist from files."""
    resp = http_app.get(
        "/playlist/from-files",
        query_string=dict(path="test/5", n=3, shuffle=False),
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 3
    assert resp.json["playlist"] == ["test/4", "test/6", "test/3"]
    assert resp.json["source_paths"] == ["test/5"]

    # multiple paths
    query_string = "&".join([f"path=test/{i}" for i in [4, 5]] + ["n=2", "shuffle=0"])
    resp = http_app.get(
        "/playlist/from-files",
        query_string=query_string,
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 2
    assert resp.json["playlist"] == ["test/3", "test/6"]
    assert resp.json["source_paths"] == ["test/4", "test/5"]


def test_playlist_storage(http_app: FlaskClient):
    """Test that the playlist is stored in the database."""
    resp = http_app.get(
        "/playlist/from-files",
        query_string=dict(path="test/5", n=3, shuffle=False),
        headers={"listenbrainz-username": "a"},
    )

    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 3

    res = MoomooPlaylist.select_star()
    assert len(res) == 1
    assert res[0]["username"] == "a"
    assert res[0]["playlist"] == resp.json["playlist"]
    assert res[0]["source_paths"] == resp.json["source_paths"]

    resp = http_app.get(
        "/playlist/from-files",
        query_string=dict(path="test/5", n=3, shuffle=False),
        headers={"listenbrainz-username": "a"},
    )
    assert len(MoomooPlaylist.select_star()) == 2
