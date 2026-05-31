import datetime

import pytest
from click.testing import CliRunner

from moomoo_navidrome.jobs.play_queue import QueuePlaylist, QueueSignature, cli
from moomoo_navidrome.navidrome import NavidromeHTTPClient


def test_QueueSignature__signature():
    signature = QueueSignature(ts="2024-01-01T00:00:00Z", synced_at="2024-01-01T01:00:00Z")
    assert signature.signature.startswith("#moomoo-inbox=")


def test_QueueSignature__from_comment():
    now = datetime.datetime.now(datetime.timezone.utc)
    signature = QueueSignature(ts=now, synced_at=now)
    comment = f"Some comment\n{signature.signature}\nAnother comment"
    parsed = QueueSignature.from_comment(comment)
    assert parsed == signature


def test_QueuePlaylist__sign():
    now = datetime.datetime.now(datetime.timezone.utc)
    http = NavidromeHTTPClient()
    playlist_id = http.create_playlist("Test Playlist", "A playlist for testing", song_ids=[])

    signature = QueueSignature(ts=now, synced_at=now)
    signature.sign(client=http, playlist_id=playlist_id)

    playlist = http.get_playlist_by_id(playlist_id)
    parsed = QueueSignature.from_comment(playlist.comment)
    assert parsed == signature


def test_QueuePlaylist__fetch():
    now = datetime.datetime.now(datetime.timezone.utc)
    http = NavidromeHTTPClient()

    # no playlists with the tag should raise an error
    with pytest.raises(RuntimeError, match=f"Playlist with tag {QueueSignature.tag} not found."):
        QueuePlaylist.fetch(http)

    # add a playlist with the tag
    signature = QueueSignature(ts=now, synced_at=now)
    playlist_id = http.create_playlist("Test Playlist", signature.signature, song_ids=[])
    playlist = QueuePlaylist.fetch(http)
    assert playlist.playlist_id == playlist_id

    # add a second one, should error
    http.create_playlist("Test Playlist 2", signature.signature, song_ids=[])
    with pytest.raises(RuntimeError, match="Multiple playlists with tag"):
        QueuePlaylist.fetch(http)


def test_cli_sign(caplog):
    runner = CliRunner()

    # no playlists with the tag should error
    result = runner.invoke(cli, ["sign"])
    assert result.exit_code != 0
    assert f"No playlist with tag {QueueSignature.tag} found." in str(result.exception)

    # add a playlist with the tag
    http = NavidromeHTTPClient()
    http.create_playlist("Test Playlist", QueueSignature.tag, song_ids=[])
    result = runner.invoke(cli, ["sign"])
    assert result.exit_code == 0
    assert "Playlist 'Test Playlist' signed with timestamp" in caplog.text

    # add a second one, should error
    http.create_playlist("Test Playlist 2", QueueSignature.tag, song_ids=[])
    result = runner.invoke(cli, ["sign"])
    assert result.exit_code != 0
    assert f"Multiple playlists with tag {QueueSignature.tag} found." in str(result.exception)


def test_cli_sync(caplog, monkeypatch, songs):
    runner = CliRunner()

    # add a playlist with the tag
    now = datetime.datetime.now(datetime.timezone.utc)
    signature = QueueSignature(ts=now, synced_at=now)
    http = NavidromeHTTPClient()
    playlist_id = http.create_playlist("Test Playlist", signature.signature, song_ids=[])

    # mock get_latest_ts to return a random time. only used in the comment.
    monkeypatch.setattr("moomoo_navidrome.jobs.play_queue.get_latest_ts", lambda: now)

    # no songs in get_files_added_since
    monkeypatch.setattr("moomoo_navidrome.jobs.play_queue.get_files_added_since", lambda _: [])
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 0 new songs since last sync." in caplog.text
    assert http.get_playlist_by_id(playlist_id).songs == []

    # some songs in get_files_added_since
    path = next(iter(songs.values()))
    monkeypatch.setattr("moomoo_navidrome.jobs.play_queue.get_files_added_since", lambda _: [path])
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 1 new songs since last sync." in caplog.text
    assert len(http.get_playlist_by_id(playlist_id).songs) == 1
