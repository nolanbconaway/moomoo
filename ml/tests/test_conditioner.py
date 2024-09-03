import numpy as np
import pytest
from moomoo_ml.conditioner import Model


@pytest.fixture
def random_data() -> np.ndarray:
    rs = np.random.RandomState(0)
    return rs.randn(100, 1024)


def test_is_fitted(random_data: np.ndarray):
    model = Model()
    assert not model.is_fitted

    model.fit(random_data)
    assert model.is_fitted


def test_conditioner_id(random_data: np.ndarray):
    model = Model()
    with pytest.raises(ValueError):
        model.conditioner_id  # noqa: B018

    model.fit(random_data)
    cid = model.conditioner_id
    assert isinstance(cid, str)

    # check deterministic
    model2 = Model()
    model2.fit(random_data)
    assert model2.conditioner_id == cid


def test_save_to_artifacts(random_data: np.ndarray, tmp_path: str):
    artifacts = tmp_path / "artifacts"
    model = Model()
    model.fit(random_data)
    model.save_to_artifacts(artifacts)
    assert (artifacts / f"{model.conditioner_id}.pkl").exists()


def test_load_from_artifacts(random_data: np.ndarray, tmp_path: str):
    artifacts = tmp_path / "artifacts"
    model = Model()
    model.fit(random_data)
    model.save_to_artifacts(artifacts)

    model2 = Model.load_from_artifacts(model.conditioner_id, artifacts)
    assert np.allclose(model.components_, model2.components_)
    assert model2.is_fitted

    with pytest.raises(FileNotFoundError):
        Model.load_from_artifacts("not_found", artifacts)
