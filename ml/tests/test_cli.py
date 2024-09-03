from pathlib import Path

import numpy as np
from click.testing import CliRunner
from moomoo_ml.cli import (
    build_conditioner,
    condition_new_files,
    get_db_embeddings,
    score_local_files,
    version,
)
from moomoo_ml.db import (
    BaseTable,
    ConditionedEmbedding,
    FileEmbedding,
    LocalFileExcludeRegex,
    get_session,
)

from .conftest import RESOURCES


def create_db():
    with get_session() as session:
        engine = session.get_bind()
        BaseTable.metadata.create_all(engine)


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(version)
    assert result.exit_code == 0
    assert "." in result.output


def test_score_local_files():
    """Test main with a fresh table."""
    create_db()

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
    create_db()

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
    create_db()
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


def test_build_conditioner(monkeypatch, tmp_path):
    # patch result of get_db_embeddings
    paths = [Path(f"{i}.mp3") for i in range(2000)]
    embeds = np.random.rand(1000, 1024)
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", lambda: (paths, embeds))

    # run the command
    runner = CliRunner()
    result = runner.invoke(build_conditioner, ["--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    assert "Building conditioner" in result.output
    assert len(list(tmp_path.iterdir())) == 1


def test_condition_new_files(monkeypatch, tmp_path):
    def fake_result(n):
        """make a monkeypatched function that returns n fake embeddings."""

        def wrapper(**_):
            return [Path(f"{i}.mp3") for i in range(n)], np.random.rand(n, 1024)

        return wrapper

    create_db()
    runner = CliRunner()

    # build the conditioner
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", fake_result(2000))
    result = runner.invoke(build_conditioner, ["--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    conditioner_id = next(iter(tmp_path.iterdir())).with_suffix("").name

    # error if wrong conditioner_id
    result = runner.invoke(condition_new_files, ["fake", "--artifacts", str(tmp_path)])
    assert result.exit_code != 0
    assert "Conditioner fake not found" in result.output

    # no data
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", lambda **_: [[], np.array([])])
    result = runner.invoke(condition_new_files, [conditioner_id, "--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    assert "No new files to condition." in result.output

    # condition the files
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", fake_result(10))
    result = runner.invoke(condition_new_files, [conditioner_id, "--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    assert "Saving 10 conditioned embeddings." in result.output

    # check the db
    with get_session() as session:
        res = session.query(ConditionedEmbedding).all()
        assert len(res) == 10
        assert all(i.conditioner_id == conditioner_id for i in res)


def test_get_db_embeddings():
    create_db()

    # test no data, no embeddings
    paths, embeds = get_db_embeddings()
    assert len(paths) == len(embeds) == 0

    # add some data
    with get_session() as session:
        session.add(
            FileEmbedding(
                filepath="1.mp3",
                success=True,
                fail_reason=None,
                duration_seconds=1.0,
                embedding=np.random.rand(1024).tolist(),
            )
        )
        session.add(
            FileEmbedding(
                filepath="2.mp3",
                success=True,
                fail_reason=None,
                duration_seconds=1.0,
                embedding=np.random.rand(1024).tolist(),
            )
        )
        session.add(
            ConditionedEmbedding(
                filepath="1.mp3",
                conditioner_id="123",
                embedding=np.random.rand(50).tolist(),
            )
        )
        session.commit()

    # test get all embeddings
    paths, embeds = get_db_embeddings()
    assert len(paths) == len(embeds) == 2
    assert embeds.shape == (2, 1024)
    assert set(paths) == {Path("1.mp3"), Path("2.mp3")}

    # test unconditioned_by
    paths, embeds = get_db_embeddings(unconditioned_by="123")
    assert len(paths) == len(embeds) == 1
    assert embeds.shape == (1, 1024)
    assert set(paths) == {Path("2.mp3")}
