from unittest.mock import patch

import pytest
from flask.testing import FlaskClient
from moomoo_http.playlist_generator import Playlist

from ...conftest import load_local_files_table

plist_obj = "moomoo_http.playlist_generator.FromFilesPlaylistGenerator.get_playlist"


@pytest.fixture(autouse=True)
def create_storage():
    # load an empty table so queries don't fail. we dont need any data for these tests.
    load_local_files_table([])


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    resp = http_app.get("/playlist/from-files", query_string=dict())
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No filepaths provided."

    query_string = "&".join([f"path=test{i}" for i in range(1000)])
    resp = http_app.get("/playlist/from-files", query_string=query_string)
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "Too many filepaths provided (>500)."


def test_success(http_app: FlaskClient):
    """Quick process test when everything works."""
    playlist = Playlist(tracks=[])
    with patch(plist_obj, return_value=playlist) as mock:
        resp = http_app.get("/playlist/from-files", query_string=dict(path="test/3949"))
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlists"] == [playlist.to_dict()]
        assert mock.call_count == 1
