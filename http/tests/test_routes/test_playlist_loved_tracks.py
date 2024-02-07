from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_http.db import db
from moomoo_playlist.ddl import PlaylistCollection, PlaylistCollectionItem


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/loved/")
    assert resp.status_code == 404


def test_no_playlist_errors(http_app: FlaskClient):
    # no loved playlist collection available
    resp = http_app.get("/playlist/loved/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "Collection loved-tracks collection not found for aaa." in resp.json["error"]

    # a collection available, but no playlists
    collection = PlaylistCollection(username="aaa", collection_name="loved-tracks")
    db.session.add(collection)
    db.session.commit()

    resp = http_app.get("/playlist/loved/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No loved-tracks playlists found for aaa." in resp.json["error"]


def test_success(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    collection = PlaylistCollection(
        collection_id=uuid4(), username="aaa", collection_name="loved-tracks"
    )
    collection_item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        collection_order_index=0,
        title="test",
        description="test",
        playlist=[{"filepath": "aaa"}, {"filepath": "bbb"}, {"filepath": "ccc"}],
    )
    db.session.add(collection)
    db.session.add(collection_item)
    db.session.commit()

    resp = http_app.get("/playlist/loved/aaa")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 1
    assert len(resp.json["playlists"][0]["playlist"]) == 3
    assert sorted(
        resp.json["playlists"][0]["playlist"], key=lambda x: x["filepath"]
    ) == [{"filepath": "aaa"}, {"filepath": "bbb"}, {"filepath": "ccc"}]
