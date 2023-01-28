import json

import pytest
from click.testing import CliRunner

from moomoo.ingest import collect_local_files

from ..conftest import RESOURCES


@pytest.fixture(autouse=True)
def mock_insert(monkeypatch):
    monkeypatch.setattr(collect_local_files, "insert", lambda *args, **kwargs: ...)


def test_parse_audio_file():
    path = RESOURCES / "test.mp3"
    res = json.loads(collect_local_files.parse_audio_file(path)["json_data"])
    assert res["title"] == "fake"
    assert res["artist"] == "me"
    assert res["album"] == "out"
    assert res["album_artist"] == "please"


@pytest.mark.parametrize("procs", [1, 2])
def test_cli_main(procs):
    runner = CliRunner()

    result = runner.invoke(
        collect_local_files.main,
        [str(RESOURCES), "--table=FAKE", "--schema=FAKE", f"--procs={procs}"],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output
