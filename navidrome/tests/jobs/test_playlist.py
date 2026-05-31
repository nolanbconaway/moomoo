import datetime
from uuid import uuid4

from moomoo_playlist.ddl import PlaylistCollection, PlaylistCollectionItem

from moomoo_navidrome.jobs.playlist import (
    TITLE_PREFIX,
    drop_all_moomoo_playlists,
    sync_playlists_collection,
)
from moomoo_navidrome.navidrome import NavidromeHTTPClient


def test_drop_all_moomoo_playlists():
    client = NavidromeHTTPClient()
    # no moomoo playlists to start with
    assert drop_all_moomoo_playlists(client) == 0

    # add one
    client.create_playlist(name=f"{TITLE_PREFIX} test 1", song_ids=[], comment="test")
    assert drop_all_moomoo_playlists(client) == 1

    # then no more
    assert drop_all_moomoo_playlists(client) == 0


def test_sync_playlists_collection():
    client = NavidromeHTTPClient()
    collection = PlaylistCollection(collection_id=uuid4(), collection_name="test", username="fake")
    playlist_item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        playlist_id=uuid4(),
        collection_order_index=0,
        title="Test Playlist",
        description="A test playlist",
        playlist=[],
        create_at_utc=datetime.datetime.now(datetime.timezone.utc),
    )
    collection.items.append(playlist_item)

    # no playlist with this title
    assert not client.fetch_playlists()

    # sync the collection, should create a new playlist
    sync_playlists_collection(client, collection)
    playlists = client.fetch_playlists()
    assert len(playlists) == 1
    signed_at = playlists[0].signature.signed_at

    # sync again without changes, should skip
    sync_playlists_collection(client, collection)
    playlists = client.fetch_playlists()
    assert len(playlists) == 1
    assert playlists[0].signature.signed_at == signed_at

    # force run, signed_at should update
    sync_playlists_collection(client, collection, force=True)
    playlists = client.fetch_playlists()
    assert len(playlists) == 1
    assert playlists[0].signature.signed_at > signed_at
