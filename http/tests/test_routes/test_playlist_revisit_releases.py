from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_http.db import db
from moomoo_playlist.ddl import PlaylistCollection, PlaylistCollectionItem


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/revisit-releases/")
    assert resp.status_code == 404


def test_no_playlist_errors(http_app: FlaskClient):
    # no loved playlist collection available
    resp = http_app.get("/playlist/revisit-releases/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert (
        "Collection revisit-releases collection not found for aaa."
        in resp.json["error"]
    )

    # a collection available, but no playlists
    collection = PlaylistCollection(username="aaa", collection_name="revisit-releases")
    db.session.add(collection)
    db.session.commit()

    resp = http_app.get("/playlist/revisit-releases/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No revisit-releases playlists found for aaa." in resp.json["error"]


def test_success(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    collection = PlaylistCollection(
        collection_id=uuid4(), username="aaa", collection_name="revisit-releases"
    )
    items = [
        PlaylistCollectionItem(
            collection_id=collection.collection_id,
            collection_order_index=0,
            title="test",
            description="test",
            playlist=[{"filepath": "aaa"}],
        ),
        PlaylistCollectionItem(
            collection_id=collection.collection_id,
            collection_order_index=1,
            title="test",
            description="test",
            playlist=[{"filepath": "bbb"}, {"filepath": "ccc"}],
        ),
    ]
    db.session.add(collection)
    db.session.add_all(items)
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
