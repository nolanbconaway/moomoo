from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_pg import Playlist, PlaylistCollection

from moomoo_http.db import db


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/revisit-tracks/")
    assert resp.status_code == 404


def test_get(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    collection = PlaylistCollection(
        collection_id=uuid4(), username="aaa", collection_name="revisit-tracks"
    )
    playlist = Playlist.Data(
        title="test",
        description="test",
        tracks=[{"filepath": "aaa"}, {"filepath": "bbb"}, {"filepath": "ccc"}],
    )

    db.session.add(collection)
    collection.replace_playlists([playlist], session=db.session)
    db.session.commit()

    resp = http_app.get("/playlist/revisit-tracks/aaa")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 1
    assert len(resp.json["playlists"][0]["playlist"]) == 3
    assert sorted(resp.json["playlists"][0]["playlist"], key=lambda x: x["filepath"]) == [
        {"filepath": "aaa"},
        {"filepath": "bbb"},
        {"filepath": "ccc"},
    ]
