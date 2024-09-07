import json
from pathlib import Path

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


def test_conditioner_hash(random_data: np.ndarray):
    model = Model()
    with pytest.raises(ValueError):
        model.hash  # noqa: B018

    model.fit(random_data)
    cid = model.hash
    assert isinstance(cid, str)

    # check deterministic
    model2 = Model()
    model2.fit(random_data)
    assert model2.hash == cid


def test_conditioner_read_write_model_info(random_data: np.ndarray):
    # no data, empty dict
    info = Model.read_model_info()
    assert info == {}

    # write some data
    model = Model()
    model.fit(random_data)
    model.update_model_info()

    # read it back
    info = Model.read_model_info()
    assert info["name"] == model.name
    assert info["hash"] == model.hash

    # writing again should not change the file
    model.update_model_info()
    assert info == Model.read_model_info()


def test_save_to_artifacts(random_data: np.ndarray, tmp_path: Path):
    artifacts = tmp_path / "artifacts"
    model = Model()
    model.fit(random_data)
    model.save_to_artifacts(artifacts)
    assert (artifacts / model.filename).exists()


def test_load_from_artifacts(random_data: np.ndarray, tmp_path: Path, conditioner_info_file: Path):
    artifacts = tmp_path / "artifacts"
    model = Model()
    model.fit(random_data)
    model.save_to_artifacts(artifacts)
    model.update_model_info()

    model2 = Model.load_from_artifacts(artifacts)
    assert np.allclose(model.components_, model2.components_)
    assert model2.is_fitted

    # change the model info file so that it doesn't match the model
    model_info = Model.read_model_info()
    model_info["name"] = "different"
    conditioner_info_file.write_text(json.dumps(model_info))
    with pytest.raises(ValueError) as exc:
        Model.load_from_artifacts(artifacts)
    assert "Model name or hash does not match saved model." in str(exc.value)

    # clear out the artifacts directory. should raise FileNotFoundError
    for f in artifacts.glob("*.pkl"):
        f.unlink()
    with pytest.raises(FileNotFoundError):
        Model.load_from_artifacts(artifacts)
