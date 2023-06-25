from moomoo.ml.scorer import Model

from ..conftest import RESOURCES


def test_score_single_path():
    path = RESOURCES / "test.mp3"
    model = Model.from_artifacts()
    result = model.score(path)
    assert result.success
    assert result.embedding.shape == (1024,)
    assert result.fail_reason is None


def test_score_single_path__error(monkeypatch):
    path = RESOURCES / "test.mp3"

    def f(*_):
        raise ValueError("test error")

    monkeypatch.setattr(Model, "get_input", f)

    model = Model.from_artifacts()
    result = model.score(path)
    assert not result.success
    assert result.embedding is None
    assert result.fail_reason == "test error"
