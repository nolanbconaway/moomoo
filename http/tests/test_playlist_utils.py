"""Test the base app."""

from moomoo_playlist import Playlist, Track
from moomoo_playlist.ddl import PlaylistCollection, PlaylistCollectionItem

from moomoo_http.db import db
from moomoo_http.routes.playlist import PlaylistResponse


def test_PlaylistResponse__serialize_playlist():
    p = Playlist(tracks=[Track(filepath="test.mp3")])
    res = PlaylistResponse.serialize_playlist(p)
    assert res == {"playlist": [{"filepath": "test.mp3"}]}

    p.title = "test"
    p.description = "test"
    res = PlaylistResponse.serialize_playlist(p)
    assert res == {
        "playlist": [{"filepath": "test.mp3"}],
        "title": "test",
        "description": "test",
    }


def test_PlaylistResponse__to_serializable():
    pr = PlaylistResponse(
        success=True, playlists=[Playlist(tracks=[Track(filepath="test.mp3")])]
    )
    res = pr.to_serializable()
    assert res == {
        "success": True,
        "playlists": [{"playlist": [{"filepath": "test.mp3"}]}],
    }

    pr = PlaylistResponse(success=False, error="test")
    res = pr.to_serializable()
    assert res == {"success": False, "error": "test"}


def test_PlaylistResponse__to_http():
    pr = PlaylistResponse(
        success=True, playlists=[Playlist(tracks=[Track(filepath="test.mp3")])]
    )
    res = pr.to_http()
    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.json == {
        "success": True,
        "playlists": [{"playlist": [{"filepath": "test.mp3"}]}],
    }

    pr = PlaylistResponse(success=False, error="test")
    res = pr.to_http()
    assert res.status_code == 500
    assert res.content_type == "application/json"
    assert res.json == {"success": False, "error": "test"}


def test_PlaylistResponce__from_user_collection():
    """Test handling of a PlaylistResponse from a user collection."""
    # no collection
    res = PlaylistResponse.from_user_collection(
        collection_name="test-collection", username="test-name", session=db.session
    )
    assert res.success is False
    assert res.error == "Collection test-collection collection not found for test-name."

    # collection, no playlists
    collection = PlaylistCollection(
        username="test-name", collection_name="test-collection"
    )
    db.session.add(collection)
    db.session.commit()

    res = PlaylistResponse.from_user_collection(
        collection_name="test-collection", username="test-name", session=db.session
    )
    assert res.success is False
    assert res.error == "No test-collection playlists found for test-name."

    # collection, playlists
    items = [
        PlaylistCollectionItem(
            collection_id=collection.collection_id,
            collection_order_index=0,
            playlist=[{"filepath": "aaa"}],
        ),
        PlaylistCollectionItem(
            collection_id=collection.collection_id,
            collection_order_index=1,
            playlist=[{"filepath": "bbb"}, {"filepath": "ccc"}],
        ),
    ]
    db.session.add(collection)
    db.session.add_all(items)
    db.session.commit()

    res = PlaylistResponse.from_user_collection(
        collection_name="test-collection", username="test-name", session=db.session
    )
    assert res.success is True
    assert len(res.playlists) == 2
    assert res.playlists[0].tracks == [Track(filepath="aaa")]
    assert res.playlists[1].tracks == [Track(filepath="bbb"), Track(filepath="ccc")]
