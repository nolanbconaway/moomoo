from pathlib import Path
from unittest.mock import patch

import pytest
from flask.testing import FlaskClient
from moomoo_http.db import Base, db

from ...conftest import load_local_files_table


@pytest.fixture(autouse=True)
def create_storage():
    """Create the storage table."""
    Base.metadata.create_all(db.engine)

    # load an empty table so queries don't fail. we dont need any data for these tests.
    load_local_files_table([])


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    resp = http_app.get("/playlist/from-mbids", query_string=dict(path="test/3949"))
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No listenbrainz-username header provided."

    resp = http_app.get(
        "/playlist/from-mbids",
        query_string=dict(),
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No filepaths provided."

    query_string = "&".join([f"path=test{i}" for i in range(1000)])
    resp = http_app.get(
        "/playlist/from-mbids",
        query_string=query_string,
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "Too many filepaths provided (>500)."


def test_invalid_mbids(http_app: FlaskClient):
    """Test that an error is returned when invalid mbids are provided."""
    patch_obj = "moomoo_http.playlist_generator.FromFilesPlaylistGenerator.get_playlist"
    resp = http_app.get(
        "/playlist/from-mbids",
        query_string=dict(path="test/3949"),
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No paths requested (or found via request)." in resp.json["error"]

    with patch(patch_obj, side_effect=Exception("test exception")) as mock:
        resp = http_app.get(
            "/playlist/from-mbids",
            query_string=dict(path="test/3949"),
            headers={"listenbrainz-username": "a"},
        )
        assert resp.status_code == 500
        assert resp.json["success"] is False
        assert "test exception" in resp.json["error"]
        assert mock.call_count == 1

    with patch(patch_obj, return_value=([], [Path("test/3949")])) as mock:
        resp = http_app.get(
            "/playlist/from-mbids",
            query_string=dict(path="test/3949"),
            headers={"listenbrainz-username": "a"},
        )
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlist"] == []
        assert resp.json["source_paths"] == ["test/3949"]
        assert mock.call_count == 1
