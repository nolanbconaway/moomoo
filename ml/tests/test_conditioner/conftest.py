from pathlib import Path

import pytest
from moomoo_ml.conditioner.conditioner import Model

# add some mocks to avoid writing to the actual filesystem


@pytest.fixture(autouse=True)
def info_file(tmp_path: Path, monkeypatch) -> Path:
    """Patch the conditioner info dir to a temporary directory."""
    p = tmp_path / "test_info/cinfo.json"
    p.parent.mkdir(parents=True)
    monkeypatch.setattr(Model, "INFO_FILE", p)
    return p


@pytest.fixture(autouse=True)
def artifacts_path(tmp_path: Path, monkeypatch) -> Path:
    """Patch the conditioner artifacts dir to a temporary directory."""
    p = tmp_path / "test_artifacts"
    p.mkdir(parents=True)
    monkeypatch.setattr("moomoo_ml.conditioner.cli.DEFAULT_ARTIFACTS_PATH", p)
    return p
