"""Code to condition raw scores given the dataset.

This generally involves dimensionality reduction of the raw embeddings which are 1024 vectors
using available media library to extract more relevant features.
"""

import pickle
from hashlib import md5
from pathlib import Path

from sklearn.decomposition import PCA

N_DIMS = 50


class Model(PCA):
    def __init__(self):
        """Override PCA init to set n_components and random state."""
        super().__init__(n_components=N_DIMS, random_state=0)

    @property
    def is_fitted(self) -> bool:
        """Return whether the model is fitted."""
        return hasattr(self, "components_")

    @property
    def conditioner_id(self) -> str:
        """Return a unique identifier for this conditioner."""
        if not self.is_fitted:
            raise ValueError("Model not fitted.")

        md5_hash = md5(self.components_.data.tobytes()).hexdigest()[:6]
        return f"pca_d{N_DIMS}_{md5_hash}"

    def save_to_artifacts(self, artifacts: Path = Path("artifacts")):
        """Save the model to the artifacts directory."""
        if not self.is_fitted:
            raise ValueError("Model not fitted.")

        artifacts.mkdir(exist_ok=True)
        with open(artifacts / f"{self.conditioner_id}.pkl", "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load_from_artifacts(
        cls, conditioner_id: str, artifacts: Path = Path("artifacts")
    ) -> "Model":
        """Load the model from the artifacts directory."""
        with open(artifacts / f"{conditioner_id}.pkl", "rb") as f:
            model = pickle.load(f)
        return model
