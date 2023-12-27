from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_http.playlist_generator import Playlist

plist_obj = "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.get_playlist"
list_obj = "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.list_source_paths"


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    resp = http_app.get("/playlist/from-mbids", query_string=dict(mbid=uuid4().hex))
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
    assert resp.json["error"] == "No mbids provided."

    query_string = "&".join([f"mbid={uuid4().hex}" for i in range(1000)])
    resp = http_app.get(
        "/playlist/from-mbids",
        query_string=query_string,
        headers={"listenbrainz-username": "a"},
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "Too many mbids provided (>500)."


def test_success(http_app: FlaskClient):
    """Quick process test when everything works."""
    playlist = Playlist(playlist=[], source_paths=[Path("test/3949")])
    with patch(plist_obj, return_value=playlist) as mock:
        resp = http_app.get(
            "/playlist/from-mbids",
            query_string=dict(mbid=uuid4().hex),
            headers={"listenbrainz-username": "a"},
        )
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert resp.json["playlist"] == []
        assert resp.json["source_paths"] == ["test/3949"]
        assert mock.call_count == 1
