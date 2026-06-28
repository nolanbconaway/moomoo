"""Test the base app."""

from pathlib import Path

from moomoo_pg import Playlist, PlaylistCollection, PlaylistTrack

from moomoo_http.db import db
from moomoo_http.routes.playlist import PlaylistResponse


def test_PlaylistResponse__serialize_playlist():
    p = Playlist.Data(tracks=[dict(filepath=Path("test.mp3"))], title=None, description=None)
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
    playlist = Playlist.Data(tracks=[dict(filepath=Path("test.mp3"))], title=None, description=None)
    pr = PlaylistResponse(success=True, playlists=[playlist])
    res = pr.to_serializable()
    assert res == {
        "success": True,
        "playlists": [{"playlist": [{"filepath": "test.mp3"}]}],
    }

    pr = PlaylistResponse(success=False, error="test")
    res = pr.to_serializable()
    assert res == {"success": False, "error": "test"}


def test_PlaylistResponse__to_http():
    playlist = Playlist.Data(tracks=[dict(filepath=Path("test.mp3"))], title=None, description=None)
    pr = PlaylistResponse(success=True, playlists=[playlist])
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
    collection = PlaylistCollection(username="test-name", collection_name="test-collection")
    db.session.add(collection)
    db.session.commit()

    res = PlaylistResponse.from_user_collection(
        collection_name="test-collection", username="test-name", session=db.session
    )
    assert res.success is False
    assert res.error == "No test-collection playlists found for test-name."

    # collection, playlists
    playlists = [
        Playlist.Data(tracks=[{"filepath": "aaa"}], title=None, description=None),
        Playlist.Data(
            tracks=[{"filepath": "bbb"}, {"filepath": "ccc"}], title=None, description=None
        ),
    ]
    db.session.add(collection)
    collection.replace_playlists(playlists, session=db.session)
    db.session.commit()

    res = PlaylistResponse.from_user_collection(
        collection_name="test-collection", username="test-name", session=db.session
    )
    assert res.success is True
    assert len(res.playlists) == 2
    assert res.playlists[0].tracks == [PlaylistTrack.Data(filepath="aaa")]
    assert res.playlists[1].tracks == [
        PlaylistTrack.Data(filepath="bbb"),
        PlaylistTrack.Data(filepath="ccc"),
    ]
