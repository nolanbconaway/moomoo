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
