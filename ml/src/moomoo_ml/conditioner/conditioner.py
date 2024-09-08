"""Code to condition raw scores given the dataset.

This generally involves dimensionality reduction of the raw embeddings which are 1024 vectors
using available media library to extract more relevant features.
"""

import datetime
import json
import pickle
from hashlib import md5
from pathlib import Path

from sklearn.decomposition import PCA


class Model(PCA):
    N_DIMS = 50
    INFO_FILE = Path(__file__).parent / "model-info.json"

    def __init__(self):
        """Override PCA init to set n_components and random state."""
        super().__init__(n_components=self.N_DIMS, random_state=0)

    @property
    def is_fitted(self) -> bool:
        """Return whether the model is fitted."""
        return hasattr(self, "components_")

    @property
    def name(self) -> str:
        """Return the architecture name of the model."""
        return f"pca_d{self.N_DIMS}"

    @property
    def hash(self) -> str:
        """Return a unique identifier for this conditioner."""
        if not self.is_fitted:
            raise ValueError("Model not fitted.")

        return md5(self.components_.data.tobytes()).hexdigest()[:6]

    @property
    def filename(self) -> str:
        """Return the filename for this model."""
        return f"{self.name}_{self.hash}.pkl"

    @classmethod
    def read_model_info(cls) -> dict:
        """Read the model info file."""
        if not cls.INFO_FILE.exists():
            return {}
        return json.loads(cls.INFO_FILE.read_text())

    def save_to_artifacts(self, artifacts: Path):
        """Save the model to the artifacts directory."""
        if not self.is_fitted:
            raise ValueError("Model not fitted.")

        artifacts.mkdir(exist_ok=True)
        with open(artifacts / self.filename, "wb") as f:
            pickle.dump(self, f)

    def update_model_info(self):
        """Update the model info file."""
        if not self.is_fitted:
            raise ValueError("Model not fitted.")

        # check if model info file already matches self
        model_info = self.read_model_info()
        if model_info and (self.name == model_info["name"] and self.hash == model_info["hash"]):
            return

        # update model info
        model_info = {
            "name": self.name,
            "hash": self.hash,
            "filename": str(self.filename),
            "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self.INFO_FILE.write_text(json.dumps(model_info, indent=2))

    @classmethod
    def load_from_artifacts(cls, artifacts: Path) -> "Model":
        """Load the model from the artifacts directory."""
        model_info = cls.read_model_info()
        filename, name, hash_ = model_info["filename"], model_info["name"], model_info["hash"]
        with open(artifacts / filename, "rb") as f:
            model = pickle.load(f)

        # check name and hash match
        if model.name != name or model.hash != hash_:
            raise ValueError("Model name or hash does not match saved model.")

        return model
