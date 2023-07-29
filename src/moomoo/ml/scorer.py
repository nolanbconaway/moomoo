"""Scoring utilities for the model."""
import dataclasses
import math
import os
import warnings
from pathlib import Path
from typing import Optional

import librosa
import numpy as np
import torch
from transformers import AutoModel, BatchFeature, Wav2Vec2FeatureExtractor
from transformers.modeling_outputs import BaseModelOutput


@dataclasses.dataclass
class EmbeddingResult:
    """Storage for the result of embedding a song."""

    success: bool
    fail_reason: Optional[str] = None
    embedding: Optional[np.ndarray] = None
    duration_seconds: Optional[float] = None


@dataclasses.dataclass
class Model:
    """Model class for scoring."""

    model: AutoModel
    processor: Wav2Vec2FeatureExtractor
    device: str = None
    max_duration_s: float = 120

    def __post_init__(self):
        """Set the device, move model to it."""
        if self.device is None:
            if "MOOMOO_ML_DEVICE" in os.environ:
                self.device = os.environ["MOOMOO_ML_DEVICE"]
            else:
                self.device = "cuda" if torch.cuda.is_available() else "cpu"

        self.model = self.model.to(self.device)

    @classmethod
    def from_artifacts(cls, artifacts: Path = Path("artifacts"), **kw) -> "Model":
        """Load the model from artifacts."""
        processor = Wav2Vec2FeatureExtractor.from_pretrained(
            artifacts, trust_remote_code=True, revision="na"  # prevents warning
        )
        model = AutoModel.from_pretrained(
            artifacts, trust_remote_code=True, revision="na"  # prevents warning
        )
        return cls(model=model, processor=processor, **kw)

    @property
    def sampling_rate(self) -> int:
        """Sampling rate of the model."""
        return self.processor.sampling_rate

    def aggregate(self, output: BaseModelOutput) -> np.ndarray:
        """Aggregate the output of the model to a vector, move it to the cpu."""
        return output.last_hidden_state.squeeze().mean(axis=0).cpu().numpy()

    def get_input(self, p: Path) -> BatchFeature:
        """Get input for the model.

        Consists of up to 120s of audio from the middle of the song. Pass the input to
        the network via **input.
        """
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            audio, _ = librosa.load(p, sr=self.sampling_rate)

        # grab at most 120s from the middle of the song
        sample_size = min(self.max_duration_s * self.sampling_rate, audio.shape[0])
        middle_point = audio.shape[0] / 2
        lb = int(math.ceil(middle_point - (sample_size / 2)))
        ub = int(math.floor(middle_point + (sample_size / 2)))

        return self.processor(
            torch.from_numpy(audio[lb:ub]),
            sampling_rate=self.sampling_rate,
            return_tensors="pt",
        ).to(self.device)

    def score(self, p: Path) -> EmbeddingResult:
        """Score a song.

        Returns an EmbeddingResult with the embedding. Any exception is caught and
        returned as a fail reason.
        """
        try:
            inputs = self.get_input(p)
            if inputs is None:
                return EmbeddingResult(
                    success=False, fail_reason="Failed to parse input"
                )

            duration_seconds: float = round(
                inputs.input_values.shape[1] / self.sampling_rate, 3
            )

            with torch.no_grad():
                output = self.model(**inputs)

            if output is None:
                return EmbeddingResult(
                    success=False,
                    fail_reason="No output.",
                    duration_seconds=duration_seconds,
                )

            output = self.aggregate(output)

        except Exception as e:
            if str(e):
                fail_reason = f"{type(e).__name__}: {e}"
            else:
                fail_reason = f"{type(e).__name__}"
            return EmbeddingResult(success=False, fail_reason=fail_reason)

        return EmbeddingResult(
            success=True, embedding=output, duration_seconds=duration_seconds
        )
