from moomoo_playlist.ddl import PlaylistCollection
from moomoo_playlist.playlist import Playlist
from sqlalchemy.orm import Session


def test_get_collection_by_name(session: Session):
    collection = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )
    assert collection.username == "test"
    assert collection.collection_name == "test"
    assert collection.playlists == []

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
    assert collection.playlists == []

    collection.replace_playlists([playlist], session=session)
    assert len(collection.playlists) == 1
    assert collection.playlists[0].title == playlist.title
    assert collection.playlists[0].description == playlist.description
    assert collection.playlists[0].playlist == []
    assert collection.playlists[0].collection_order_index == 0
    assert collection.playlists[0].collection_id == collection.collection_id

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
