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
from moomoo_ml.db import BaseTable, FileEmbedding, LocalFileExcludeRegex, get_session

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
    # make artifacts dir
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()

    # patch click.confirm to always return True
    monkeypatch.setattr("click.confirm", lambda *_, **__: True)

    # patch result of get_db_embeddings
    paths = [Path(f"{i}.mp3") for i in range(2000)]
    embeds = np.random.rand(1000, 1024)
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", lambda: (paths, embeds))

    # run the command
    runner = CliRunner()
    result = runner.invoke(build_conditioner, ["--artifacts", str(artifacts)])
    assert result.exit_code == 0
    assert "Building conditioner" in result.output
    assert len(list(artifacts.iterdir())) == 1


def test_condition_new_files(monkeypatch, tmp_path):
    # patch click.confirm to always return True
    monkeypatch.setattr("click.confirm", lambda *_, **__: True)

    def fake_result(n):
        """make a monkeypatched function that returns n fake embeddings."""

        def wrapper(**_):
            return [Path(f"{i}.mp3") for i in range(n)], np.random.rand(n, 1024)

        return wrapper

    # add some data
    create_db()
    with get_session() as session:
        for i in range(50):
            vec = np.random.rand(1024)
            session.add(FileEmbedding(filepath=f"{i}.mp3", success=True, embedding=vec.tolist()))
        session.commit()

    runner = CliRunner()

    # build the conditioner. this should update the model info file which is patched to a tmp dir
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", fake_result(2000))
    result = runner.invoke(build_conditioner, ["--artifacts", str(tmp_path)])
    assert result.exit_code == 0

    # no data
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", lambda **_: [[], np.array([])])
    result = runner.invoke(condition_new_files, ["--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    assert "No new files to condition." in result.output

    # condition the files
    monkeypatch.setattr("moomoo_ml.cli.get_db_embeddings", fake_result(10))
    result = runner.invoke(condition_new_files, ["--artifacts", str(tmp_path)])
    assert result.exit_code == 0
    assert "Saving 10 conditioned embeddings." in result.output

    # check the db. only files 1-10 should have conditioned embeddings because the fake result
    # function only returns 10 embeddings
    with get_session() as session:
        res = session.query(FileEmbedding).all()
        assert len(res) == 50

        condition = lambda r: int(r.filepath.split(".")[0]) < 10  # noqa: E731
        assert all([r.conditioned_embedding is not None for r in res if condition(r)])
        assert all([r.conditioned_embedding is None for r in res if not condition(r)])


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
                conditioned_embedding=np.random.rand(50).tolist(),
            )
        )
        session.add(
            FileEmbedding(
                filepath="2.mp3",
                success=True,
                fail_reason=None,
                duration_seconds=1.0,
                embedding=np.random.rand(1024).tolist(),
                conditioned_embedding=None,
            )
        )

        session.commit()

    # test get all embeddings
    paths, embeds = get_db_embeddings()
    assert len(paths) == len(embeds) == 2
    assert embeds.shape == (2, 1024)
    assert set(paths) == {Path("1.mp3"), Path("2.mp3")}

    # test unconditioned_by
    paths, embeds = get_db_embeddings(unconditioned=True)
    assert len(paths) == len(embeds) == 1
    assert embeds.shape == (1, 1024)
    assert set(paths) == {Path("2.mp3")}
