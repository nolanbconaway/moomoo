import shutil
from pathlib import Path

from click.testing import CliRunner
from moomoo_ingest import collect_local_files
from moomoo_ingest.db import LocalFile

from .conftest import RESOURCES


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
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


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
    assert rows[0]["json_data"]["title"] == rows[0]["recording_name"] == "fake"


def test_cli_main__insert_mp(tmpdir):
    # make a bunch of copies of the test file
    tmp_path = Path(tmpdir) / "media"
    tmp_path.mkdir()
    for i in range(10):
        shutil.copy(RESOURCES / "test.mp3", tmp_path / f"{i}.mp3")

    runner = CliRunner()
    LocalFile.create()

    result = runner.invoke(collect_local_files.main, [str(tmp_path), "--procs=2"])
    assert result.exit_code == 0
    assert "Parsing audio files in 2 processes" in result.output
    assert "Inserting" in result.output

    rows = LocalFile.select_star()
    assert len(rows) == 10
    assert rows[0]["json_data"]["title"] == rows[0]["recording_name"] == "fake"
