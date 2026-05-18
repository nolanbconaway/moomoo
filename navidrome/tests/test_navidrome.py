from pathlib import Path

import pytest

from moomoo_navidrome.models import SubsonicStatusError
from moomoo_navidrome.navidrome import NavidromeDBClient, NavidromeHTTPClient


@pytest.fixture
def song_ids(db_client: NavidromeDBClient) -> list[str]:
    """All known song ids in the seeded db."""
    with db_client.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("select id as song_id from media_file")
        return [row["song_id"] for row in cursor.fetchall()]


def test_NavidromeHTTPClient__auto_raise_error(http_client: NavidromeHTTPClient):
    # make a valid request
    response = http_client.get("/rest/ping")
    assert response.status_code == 200

    # bad response should have SubsonicStatusError raised
    with pytest.raises(SubsonicStatusError):
        http_client.get("/rest/getPlaylists", params={"u": "invalid", "p": "invalid"})

    http_client.get("/rest/startScan", params={"fullScan": "true"})
    import time

    time.sleep(0)


def test_NavidromeHTTPClient__fetch_playlists(http_client: NavidromeHTTPClient):
    # no playlists in the seeded db
    assert not http_client.fetch_playlists()

    http_client.create_playlist("Test Playlist", "Test comment", [])
    playlists = http_client.fetch_playlists()
    assert len(playlists) == 1


def test_NavidromeHTTPClient__get_playlist_by_id(http_client: NavidromeHTTPClient):
    # no playlists in the seeded db
    assert http_client.get_playlist_by_id("invalid") is None

    # add one and then fetch it
    playlist_id = http_client.create_playlist("Test Playlist", "Test comment", [])
    playlist = http_client.get_playlist_by_id(playlist_id)
    assert playlist.playlist_id == playlist_id


def test_NavidromeHTTPClient__delete_playlist(http_client: NavidromeHTTPClient):
    # error if playlist doesn't exist
    with pytest.raises(SubsonicStatusError):
        http_client.delete_playlist("invalid")

    # add one and then delete it
    playlist_id = http_client.create_playlist("Test Playlist", "Test comment", [])
    assert http_client.delete_playlist(playlist_id)
    assert http_client.get_playlist_by_id(playlist_id) is None


def test_NavidromeHTTPClient__add_songs_to_playlist(
    http_client: NavidromeHTTPClient, song_ids: list[str]
):
    # error if playlist doesn't exist
    with pytest.raises(SubsonicStatusError):
        http_client.add_songs_to_playlist("invalid", song_ids)

    # add one and then add songs to it
    playlist_id = http_client.create_playlist("Test Playlist", "Test comment", [])
    http_client.add_songs_to_playlist(playlist_id, song_ids, chunk_size=2)  # force multiple chunks

    playlist = http_client.get_playlist_by_id(playlist_id)
    assert [i.song_id for i in playlist.songs] == song_ids


def test_NavidromeHTTPClient__create_playlist(
    http_client: NavidromeHTTPClient, song_ids: list[str]
):
    playlist_id = http_client.create_playlist("Test Playlist", "Test comment", song_ids)
    playlist = http_client.get_playlist_by_id(playlist_id)
    assert playlist is not None
    assert playlist.playlist_id == playlist_id
    assert playlist.name == "Test Playlist"
    assert playlist.comment == "Test comment"
    assert [i.song_id for i in playlist.songs] == song_ids


def test_NavidromeDBClient__resolve_paths_to_ids(db_client: NavidromeDBClient):
    # invalid path returns nothing
    assert db_client.resolve_paths_to_ids([Path("invalid/path.mp3")]) == {}

    # list the paths relative to the resources/music directory in the seeded db
    paths = list((Path(__file__).parent.parent / "resources" / "music").glob("**/*.flac"))

    # should have an id for each path
    resolved = db_client.resolve_paths_to_ids(paths, chunk_size=2)  # force multiple chunks
    assert len(resolved) == len(paths)


def test_NavidromeDBClient__get_song_ids(db_client: NavidromeDBClient):
    # nothing if bad paths
    assert db_client.get_song_ids([Path("invalid/path.mp3")]) == []

    # list the paths relative to the resources/music directory in the seeded db
    paths = list((Path(__file__).parent.parent / "resources" / "music").glob("**/*.flac"))
    assert len(db_client.get_song_ids(paths)) == len(paths)
