import datetime
import uuid
from unittest.mock import patch

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
    # raised if collection does not exist
    with pytest.raises(ValueError):
        PlaylistCollection.get_collection_by_name(
            username="test", collection_name="test", session=session
        )

    # works if collection exists
    collection = PlaylistCollection(
        username="test", collection_name="test", refresh_at_hours_utc=[1, 2, 3]
    )
    session.add(collection)
    session.commit()

    result = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )

    assert result.collection_id == collection.collection_id
    assert result.username == collection.username
    assert result.collection_name == collection.collection_name
    assert result.refresh_at_hours_utc == collection.refresh_at_hours_utc


def test_last_refresh_target():
    """Test the last_refresh_target property."""
    # none if refresh_at_hours_utc is null
    collection = PlaylistCollection(username="test", collection_name="test")
    assert collection.last_refresh_target is None

    collection.refresh_at_hours_utc = [6, 18]

    # set now to 12:00, should return today's 6:00
    now = datetime.datetime(2024, 5, 18, 12, tzinfo=datetime.timezone.utc)
    with patch("moomoo_playlist.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 6, tzinfo=datetime.timezone.utc
        )

    # set now to 00:00, should return yesterday's 18:00
    now = datetime.datetime(2024, 5, 18, 0, tzinfo=datetime.timezone.utc)
    with patch("moomoo_playlist.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 17, 18, tzinfo=datetime.timezone.utc
        )
    # set now to 21:00, should return today's 18:00
    now = datetime.datetime(2024, 5, 18, 21, tzinfo=datetime.timezone.utc)
    with patch("moomoo_playlist.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 18, tzinfo=datetime.timezone.utc
        )

    # exact match returns the same time
    now = datetime.datetime(2024, 5, 18, 6, tzinfo=datetime.timezone.utc)
    with patch("moomoo_playlist.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 6, tzinfo=datetime.timezone.utc
        )


def test_is_stale(session: Session):
    """Test the is_stale property."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()
    assert collection.is_stale

    # always stale if refresh_at_hours_utc is null
    collection.refreshed_at_utc = datetime.datetime.now(datetime.timezone.utc)
    assert collection.is_stale

    # set now to 12:00, refresh at 6:O
    collection.refresh_at_hours_utc = [6]
    now = datetime.datetime(2024, 5, 18, 12, tzinfo=datetime.timezone.utc)
    with patch("moomoo_playlist.ddl.now_utc", return_value=now):
        # stale if target at 6:00, refreshed at 5:00
        collection.refreshed_at_utc = now.replace(hour=5)
        assert collection.is_stale

        # not stale if target at 6:00, refreshed at 6:00
        collection.refreshed_at_utc = now.replace(hour=6)
        assert not collection.is_stale

        # not stale if target at 6:00, refreshed at 7:00
        collection.refreshed_at_utc = now.replace(hour=7)
        assert not collection.is_stale


def test_replace_playlists(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

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
    collection = PlaylistCollection(
        username="test2", collection_name="test2", refresh_at_hours_utc=[12]
    )
    session.add(collection)
    session.commit()

    # no updates yet
    assert collection.is_stale
    collection.replace_playlists([playlist], session=session)
    assert collection.is_fresh

    # test force handler
    assert collection.replace_playlists([playlist], session=session) is False
    assert collection.replace_playlists([playlist], session=session, force=True) is True
