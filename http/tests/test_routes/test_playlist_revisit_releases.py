from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_pg import Playlist, PlaylistCollection

from moomoo_http.db import db


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/revisit-releases/")
    assert resp.status_code == 404


def test_get(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    collection = PlaylistCollection(
        collection_id=uuid4(), username="aaa", collection_name="revisit-releases"
    )
    playlists = [
        Playlist.Data(title="test", description="test", tracks=[{"filepath": "aaa"}]),
        Playlist.Data(
            title="test",
            description="test",
            tracks=[{"filepath": "bbb"}, {"filepath": "ccc"}],
        ),
    ]

    db.session.add(collection)
    collection.replace_playlists(playlists, session=db.session)
    db.session.commit()

    resp = http_app.get("/playlist/revisit-releases/aaa")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 2
    assert resp.json["playlists"][0]["playlist"] == [{"filepath": "aaa"}]
    assert resp.json["playlists"][1]["playlist"] == [
        {"filepath": "bbb"},
        {"filepath": "ccc"},
    ]
