from pathlib import Path

import pytest
from click.testing import CliRunner

from moomoo_navidrome.jobs.loves import cli
from moomoo_navidrome.navidrome import NavidromeHTTPClient


@pytest.fixture(autouse=True)
def mock_check_files_in_db(monkeypatch):
    """Mock check_files_in_db to just return the input filepaths."""
    monkeypatch.setattr("moomoo_navidrome.jobs.loves.check_files_in_db", lambda fs: set(fs))


@pytest.fixture(autouse=True)
def no_submit_feedback(monkeypatch):
    """Mock submit_listenbrainz_feedback to do nothing."""
    monkeypatch.setattr(
        "moomoo_navidrome.jobs.loves.submit_listenbrainz_feedback", lambda *_, **__: None
    )


def test_sync_loves(monkeypatch, caplog, songs: dict[str, Path]):
    """Test the sync command with various scenarios."""
    runner = CliRunner()

    # no loves anywhere
    monkeypatch.setattr("moomoo_navidrome.jobs.loves.list_db_loves", lambda: set())
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 0 loved tracks in navidrome." in caplog.text
    assert "Found 0 loved tracks in listenbrainz." in caplog.text
    assert "Loved tracks are already in sync." in caplog.text

    song_ids = list(songs.keys())
    song_paths = list(songs.values())

    # mock list db loves to return one loved track
    monkeypatch.setattr("moomoo_navidrome.jobs.loves.list_db_loves", lambda: {song_paths[0]})
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 0 loved tracks in navidrome." in caplog.text
    assert "Found 1 loved tracks in listenbrainz." in caplog.text
    assert "Starring 1 tracks on navidrome..." in caplog.text
    assert "No tracks to star on listenbrainz." in caplog.text

    # run it again and should find no new tracks to star
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 1 loved tracks in navidrome." in caplog.text
    assert "Found 1 loved tracks in listenbrainz." in caplog.text
    assert "Loved tracks are already in sync." in caplog.text

    # star one on navidrome but not on listenbrainz and test the other direction
    http_client = NavidromeHTTPClient()
    http_client.get("/rest/star", params={"id": song_ids[1]})
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "Found 2 loved tracks in navidrome." in caplog.text
    assert "Found 1 loved tracks in listenbrainz." in caplog.text
    assert "Starring 1 tracks on listenbrainz..." in caplog.text
    assert "No tracks to star on navidrome." in caplog.text
