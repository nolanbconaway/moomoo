from pathlib import Path

import numpy as np
from click.testing import CliRunner
from moomoo_ml.conditioner.cli import build_conditioner, condition_new_files
from moomoo_ml.db import FileEmbedding, get_session


def test_build_conditioner(monkeypatch, artifacts_path):
    # patch click.confirm to always return True
    monkeypatch.setattr("click.confirm", lambda *_, **__: True)

    # patch result of get_db_embeddings
    paths = [Path(f"{i}.mp3") for i in range(2000)]
    embeds = np.random.rand(1000, 1024)
    monkeypatch.setattr(FileEmbedding, "fetch_numpy_embeddings", lambda: (paths, embeds))

    # run the command
    runner = CliRunner()
    result = runner.invoke(build_conditioner)
    assert result.exit_code == 0
    assert "Building conditioner" in result.output
    assert len(list(artifacts_path.iterdir())) == 1


def test_condition_new_files(monkeypatch):
    def fake_result(n):
        """make a monkeypatched function that returns n fake embeddings."""

        def wrapper(**_):
            return [Path(f"{i}.mp3") for i in range(n)], np.random.rand(n, 1024)

        return wrapper

    # add some data
    with get_session() as session:
        for i in range(50):
            vec = np.random.rand(1024)
            session.add(FileEmbedding(filepath=f"{i}.mp3", success=True, embedding=vec.tolist()))
        session.commit()

    runner = CliRunner()

    # build the conditioner. this should update the model info file which is patched to a tmp dir
    monkeypatch.setattr(FileEmbedding, "fetch_numpy_embeddings", fake_result(2000))
    result = runner.invoke(build_conditioner, ["--update-info"])
    assert result.exit_code == 0

    # no data
    monkeypatch.setattr(FileEmbedding, "fetch_numpy_embeddings", lambda **_: [[], np.array([])])
    result = runner.invoke(condition_new_files)
    assert result.exit_code == 0
    assert "No new files to condition." in result.output

    # condition the files
    monkeypatch.setattr(FileEmbedding, "fetch_numpy_embeddings", fake_result(10))
    result = runner.invoke(condition_new_files)
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
