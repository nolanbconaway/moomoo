import uuid

import pytest
from moomoo_playlist.ddl import PlaylistCollection, PlaylistCollectionItem
from moomoo_playlist.playlist import Playlist
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


def test_collection__unique_constraint(session: Session):
    """Test the unique constraint on collection_name and username."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    # create a new collection with the same name
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    with pytest.raises(IntegrityError):
        session.commit()


def test_collection_item__unique_constraint(session: Session):
    """Test the unique constraint on collection_id and collection_order_index."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    playlist = Playlist([]).serialize_tracks()
    item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=playlist,
    )
    session.add(item)
    session.commit()

    # create a new item with the same collection_id and collection_order_index
    item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=playlist,
    )
    session.add(item)
    with pytest.raises(IntegrityError):
        session.commit()


def test_collection_item__playlist_round_trip(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    track = {"filepath": "test/test.mp3", "artist_mbid": str(uuid.uuid4())}
    playlist = Playlist([track])
    item = PlaylistCollectionItem.from_playlist(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=playlist,
    )
    session.add(item)
    session.commit()

    # round trip test
    item = session.get(PlaylistCollectionItem, item.playlist_id)
    assert item.to_playlist() == playlist


def test_collection_playlists(session: Session):
    """Test the playlists property."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    assert collection.playlists == []

    track = {"filepath": "test/test.mp3", "artist_mbid": str(uuid.uuid4())}
    playlist = Playlist([track])
    item = PlaylistCollectionItem.from_playlist(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=playlist,
    )
    session.add(item)
    session.commit()

    assert collection.playlists == [playlist]


def test_get_collection_by_name(session: Session):
    collection = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )
    assert collection.username == "test"
    assert collection.collection_name == "test"
    assert collection.items == []

    collection_2 = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )

    assert collection == collection_2

    # test refresh_interval_hours setting with an existing collection
    collection = PlaylistCollection.get_collection_by_name(
        username="test",
        collection_name="test",
        session=session,
        refresh_interval_hours=24,
    )
    assert collection.refresh_interval_hours is None  # not overridden

    collection = PlaylistCollection.get_collection_by_name(
        username="test",
        collection_name="test2",
        session=session,
        refresh_interval_hours=48,
    )
    assert collection.refresh_interval_hours == 48  # set on create


def test_replace_playlists(session: Session):
    collection = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )
    playlist = Playlist(tracks=[], title="test title", description="test description")
    assert collection.items == []

    collection.replace_playlists([playlist], session=session)
    assert len(collection.items) == 1
    assert collection.items[0].title == playlist.title
    assert collection.items[0].description == playlist.description
    assert collection.items[0].playlist == []
    assert collection.items[0].collection_order_index == 0
    assert collection.items[0].collection_id == collection.collection_id

    # test stale handler
    collection = PlaylistCollection.get_collection_by_name(
        username="test",
        collection_name="test_stale",
        session=session,
        refresh_interval_hours=24,
    )

    # no updates yet
    assert collection.is_stale
    collection.replace_playlists([playlist], session=session)
    assert collection.is_fresh

    # test force handler
    assert collection.replace_playlists([playlist], session=session) is False
    assert collection.replace_playlists([playlist], session=session, force=True) is True
