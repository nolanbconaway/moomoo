from click.testing import CliRunner

from moomoo.db import FileEmbedding
from moomoo.ml import score_local_files

from ..conftest import RESOURCES


def test_cli_main__not_table_exists_error():
    runner = CliRunner()
    result = runner.invoke(score_local_files.main, [str(RESOURCES)])
    assert result.exit_code != 0

    name = FileEmbedding.table_name()
    assert f"Table {name} does not exist" in result.output


def test_main__new_table():
    """Test main with a fresh table."""
    runner = CliRunner()
    FileEmbedding.create()

    result = runner.invoke(score_local_files.main, [str(RESOURCES)])
    assert result.exit_code == 0
    assert "Found 1 unscored file(s)." in result.output
    assert "Scoring" in result.output
    res = FileEmbedding.select_star()
    assert len(res) == 1


def test_main__skip_already_scored():
    FileEmbedding.create()
    FileEmbedding(
        filepath="test.mp3",
        success=False,
        fail_reason="uhoh",
        duration_seconds=None,
        embedding=None,
    ).insert()

    runner = CliRunner()
    result = runner.invoke(score_local_files.main, [str(RESOURCES)])

    assert result.exit_code == 0
    assert "Found 0 unscored file(s)." in result.output
    assert "Nothing to do" in result.output

    res = FileEmbedding.select_star()
    assert len(res) == 1
    assert res[0]["filepath"] == "test.mp3"
    assert res[0]["success"] is False
    assert res[0]["fail_reason"] == "uhoh"
