from pathlib import Path

import pytest

from moomoo_navidrome.models import SubsonicStatusError
from moomoo_navidrome.navidrome import NavidromeDBClient, NavidromeHTTPClient


def test_NavidromeHTTPClient__auto_raise_error():
    client = NavidromeHTTPClient()

    # make a valid request
    response = client.get("/rest/ping")
    assert response.status_code == 200

    # bad response should have SubsonicStatusError raised
    with pytest.raises(SubsonicStatusError):
        client.get("/rest/getPlaylists", params={"u": "invalid", "p": "invalid"})

    client.get("/rest/startScan", params={"fullScan": "true"})
    import time

    time.sleep(0)


def test_NavidromeHTTPClient__fetch_playlists():
    client = NavidromeHTTPClient()
    # no playlists in the seeded db
    assert not client.fetch_playlists()

    client.create_playlist("Test Playlist", "Test comment", [])
    playlists = client.fetch_playlists()
    assert len(playlists) == 1


def test_NavidromeHTTPClient__get_playlist_by_id():
    client = NavidromeHTTPClient()
    # no playlists in the seeded db
    assert client.get_playlist_by_id("invalid") is None

    # add one and then fetch it
    playlist_id = client.create_playlist("Test Playlist", "Test comment", [])
    playlist = client.get_playlist_by_id(playlist_id)
    assert playlist.playlist_id == playlist_id


def test_NavidromeHTTPClient__delete_playlist():
    client = NavidromeHTTPClient()
    # error if playlist doesn't exist
    with pytest.raises(SubsonicStatusError):
        client.delete_playlist("invalid")

    # add one and then delete it
    playlist_id = client.create_playlist("Test Playlist", "Test comment", [])
    assert client.delete_playlist(playlist_id)
    assert client.get_playlist_by_id(playlist_id) is None


def test_NavidromeHTTPClient__add_songs_to_playlist(songs: dict):
    client = NavidromeHTTPClient()
    # error if playlist doesn't exist
    with pytest.raises(SubsonicStatusError):
        client.add_songs_to_playlist("invalid", list(songs.keys()))

    # add one and then add songs to it
    playlist_id = client.create_playlist("Test Playlist", "Test comment", [])
    client.add_songs_to_playlist(
        playlist_id, list(songs.keys()), chunk_size=2
    )  # force multiple chunks

    playlist = client.get_playlist_by_id(playlist_id)
    assert [i.song_id for i in playlist.songs] == list(songs.keys())


def test_NavidromeHTTPClient__create_playlist(songs: dict):
    client = NavidromeHTTPClient()
    playlist_id = client.create_playlist("Test Playlist", "Test comment", list(songs.keys()))
    playlist = client.get_playlist_by_id(playlist_id)
    assert playlist is not None
    assert playlist.playlist_id == playlist_id
    assert playlist.name == "Test Playlist"
    assert playlist.comment == "Test comment"
    assert [i.song_id for i in playlist.songs] == list(songs.keys())


def test_NavidromeDBClient__resolve_paths_to_ids():
    db_client = NavidromeDBClient()

    # invalid path returns nothing
    assert db_client.resolve_paths_to_ids([Path("invalid/path.mp3")]) == {}

    # list the paths relative to the resources/music directory in the seeded db
    paths = list((Path(__file__).parent.parent / "resources" / "music").glob("**/*.flac"))

    # should have an id for each path
    resolved = db_client.resolve_paths_to_ids(paths, chunk_size=2)  # force multiple chunks
    assert len(resolved) == len(paths)


def test_NavidromeDBClient__get_song_ids():
    db_client = NavidromeDBClient()

    # nothing if bad paths
    assert db_client.get_song_ids([Path("invalid/path.mp3")]) == []

    # list the paths relative to the resources/music directory in the seeded db
    paths = list((Path(__file__).parent.parent / "resources" / "music").glob("**/*.flac"))
    assert len(db_client.get_song_ids(paths)) == len(paths)


def test_NavidromeDBClient__list_loved_files(songs):
    db_client = NavidromeDBClient()
    http_client = NavidromeHTTPClient()

    # no loved songs in the seeded db
    assert db_client.list_loved_files() == set()

    song_id = next(iter(songs.keys()))  # get an arbitrary song id from the seeded db
    song_path = songs[song_id]

    # love a song and check it's listed
    http_client.get("/rest/star", params={"id": song_id})
    assert db_client.list_loved_files() == {Path(song_path)}
