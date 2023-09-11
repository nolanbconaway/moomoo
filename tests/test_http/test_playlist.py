import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from flask.testing import FlaskClient

from moomoo.http.app import create_app
from moomoo.utils_ import PlaylistResult

from ..conftest import load_local_files_table


@pytest.fixture
def http_app() -> FlaskClient:
    """Create a test client for the http app."""
    app = create_app()
    return app.test_client()


@pytest.fixture(autouse=True)
def load_local_files_table__fixed(monkeypatch):
    """Preload each test with a local files table."""
    schema = "test"
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", schema)
    data = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=uuid.uuid4())
        for i in range(10)
    ]
    load_local_files_table(data=data, schema=schema)


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when no files are provided."""
    resp = http_app.get("/playlist/from-files", query_string=dict())
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No filepaths provided."

    query_string = "&".join([f"path=test{i}" for i in range(1000)])
    resp = http_app.get("/playlist/from-files", query_string=query_string)
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "Too many filepaths provided (>500)."

    resp = http_app.get("/playlist/from-parent-path", query_string=dict())
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No path provided."


@pytest.mark.parametrize(
    "endpoint", ["/playlist/from-files", "/playlist/from-parent-path"]
)
def test_invalid_filepaths(http_app: FlaskClient, endpoint: str):
    """Test that an error is returned when invalid filepaths are provided."""
    resp = http_app.get(endpoint, query_string=dict(path="test/3949"))
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No paths requested (or found via request)." in resp.json["error"]

    with patch(
        "moomoo.playlist.PlaylistGenerator.get_playlist",
        side_effect=Exception("test exception message"),
    ) as mock:
        resp = http_app.get(endpoint, query_string=dict(path="test/3949"))
        assert resp.status_code == 500
        assert resp.json["success"] is False
        assert "test exception message" in resp.json["error"]
        assert mock.call_count == 1

    with patch(
        "moomoo.playlist.PlaylistGenerator.get_playlist",
        return_value=PlaylistResult(playlist=[], source_paths=[Path("test/3949")]),
    ) as mock:
        resp = http_app.get(endpoint, query_string=dict(path="test/3949"))
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlist"] == []
        assert resp.json["source_paths"] == ["test/3949"]
        assert mock.call_count == 1


def test_from_files_playlist(http_app: FlaskClient):
    resp = http_app.get(
        "/playlist/from-files", query_string=dict(path="test/5", n=3, shuffle=False)
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 3
    assert resp.json["playlist"] == ["test/4", "test/6", "test/3"]
    assert resp.json["source_paths"] == ["test/5"]

    # multiple paths
    query_string = "&".join([f"path=test/{i}" for i in [4, 5]] + ["n=2", "shuffle=0"])
    resp = http_app.get("/playlist/from-files", query_string=query_string)
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 2
    assert resp.json["playlist"] == ["test/3", "test/6"]
    assert resp.json["source_paths"] == ["test/4", "test/5"]


def test_from_parent_path_playlist(http_app: FlaskClient):
    resp = http_app.get(
        "/playlist/from-parent-path", query_string=dict(path="test/2", n=10)
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlist"]) == 9  # all but file 2
    assert resp.json["source_paths"] == ["test/2"]
