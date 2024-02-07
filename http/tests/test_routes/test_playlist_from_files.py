from unittest.mock import patch

from flask.testing import FlaskClient
from moomoo_playlist import Playlist

plist_obj = "moomoo_playlist.FromFilesPlaylistGenerator.get_playlist"


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    resp = http_app.get("/playlist/from-files", query_string=dict())
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "No paths provided."


def test_success(http_app: FlaskClient):
    """Quick process test when everything works."""
    playlist = Playlist(tracks=[])
    with patch(plist_obj, return_value=playlist) as mock:
        resp = http_app.get("/playlist/from-files", query_string=dict(path="test/3949"))
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlists"] == [{"playlist": []}]
        assert mock.call_count == 1
