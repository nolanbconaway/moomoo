from click.testing import CliRunner
from moomoo_ml.db import FileEmbedding, LocalFileExcludeRegex, get_session
from moomoo_ml.scorer.cli import score_local_files

from ..conftest import RESOURCES


def test_score_local_files():
    """Test main with a fresh table."""

    # db is empty
    with get_session() as session:
        res = list(session.query(FileEmbedding.filepath))
        assert len(res) == 0

    runner = CliRunner()
    result = runner.invoke(score_local_files, [str(RESOURCES)])

    assert result.exit_code == 0
    assert "Found 1 unscored file(s)." in result.output
    assert "Scoring" in result.output

    # db has one row bc only one file in resources
    with get_session() as session:
        res = list(session.query(FileEmbedding.filepath))
        assert len(res) == 1


def test_score_local_files__skip_already_scored():
    """Test that it skips already scored files."""

    # add the test.mp3
    item = FileEmbedding(
        filepath="test.mp3",
        success=False,
        fail_reason="uhoh",
        duration_seconds=None,
        embedding=None,
    )
    with get_session() as session:
        session.add(item)
        session.commit()
        res = list(session.query(FileEmbedding.filepath))
        assert len(res) == 1

    runner = CliRunner()
    result = runner.invoke(score_local_files, [str(RESOURCES)])
    assert result.exit_code == 0
    assert "Found 0 unscored file(s)." in result.output
    assert "Nothing to do" in result.output

    # should be the same as before
    with get_session() as session:
        res = list(session.query(FileEmbedding))
        assert len(res) == 1
        assert res[0].filepath == "test.mp3"
        assert res[0].success is False
        assert res[0].fail_reason == "uhoh"


def test_score_local_files__skip_regex_exclude():
    """Test that it skips files that match the regex exclude."""
    exclude = LocalFileExcludeRegex(pattern="test", note="exclude test files")
    with get_session() as session:
        session.add(exclude)
        session.commit()

    runner = CliRunner()
    result = runner.invoke(score_local_files, [str(RESOURCES)])
    assert result.exit_code == 0
    assert "Found 1 unscored file(s)." in result.output
    assert "Found 0 file(s) after filtering by regex." in result.output
    assert "Nothing to do" in result.output
