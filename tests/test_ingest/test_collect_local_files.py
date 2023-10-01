from click.testing import CliRunner

from moomoo.db import LocalFile
from moomoo.ingest import collect_local_files

from ..conftest import RESOURCES


def test_parse_audio_file():
    path = RESOURCES / "test.mp3"
    res = collect_local_files.parse_audio_file(path)["json_data"]
    assert res["title"] == "fake"
    assert res["artist"] == "me"
    assert res["album"] == "out"
    assert res["album_artist"] == "please"


def test_list_audio_files():
    res = collect_local_files.list_audio_files(RESOURCES)
    assert len(res) == 1
    assert res[0].name == "test.mp3"


def test_cli_main__not_table_exists_error():
    runner = CliRunner()
    result = runner.invoke(collect_local_files.main, [str(RESOURCES)])
    assert result.exit_code != 0

    name = LocalFile.table_name()
    assert f"Table {name} does not exist" in result.output


def test_cli_main__no_files(monkeypatch):
    monkeypatch.setattr(collect_local_files, "list_audio_files", lambda *a: [])
    runner = CliRunner()
    LocalFile.create()

    result = runner.invoke(collect_local_files.main, [str(RESOURCES)])
    assert result.exit_code == 0
    assert "No audio files found. Exiting." in result.output


def test_cli_main__insert_serial():
    runner = CliRunner()
    LocalFile.create()

    result = runner.invoke(collect_local_files.main, [str(RESOURCES), "--procs=1"])
    assert result.exit_code == 0
    assert "Parsing audio files serially" in result.output
    assert "Inserting" in result.output

    rows = LocalFile.select_star()
    assert len(rows) == 1
    assert rows[0]["json_data"]["title"] == "fake"


def test_cli_main__insert_mp(monkeypatch):
    n, filepath = 4, RESOURCES / "test.mp3"

    runner = CliRunner()
    LocalFile.create()

    # just copy the existing file
    monkeypatch.setattr(
        collect_local_files, "list_audio_files", lambda *a: [filepath] * n
    )

    result = runner.invoke(collect_local_files.main, [str(RESOURCES), "--procs=2"])
    assert result.exit_code == 0
    assert "Parsing audio files in 2 processes" in result.output
    assert "Inserting" in result.output

    rows = LocalFile.select_star()
    assert len(rows) == 1  # upsert!
    assert rows[0]["json_data"]["title"] == "fake"
