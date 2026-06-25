from pathlib import Path

import numpy as np
from moomoo_pg import FileEmbedding, get_session

from moomoo_ml.db import fetch_numpy_embeddings


def test_fetch_numpy_embeddings():
    """Test the fetch_numpy_embeddings function."""
    # test no data, no embeddings
    paths, embeds = fetch_numpy_embeddings()
    assert len(paths) == len(embeds) == 0

    # add some data
    with get_session() as session:
        session.add(
            FileEmbedding(
                filepath=Path("1.mp3"),
                success=True,
                fail_reason=None,
                duration_seconds=1.0,
                embedding=np.random.rand(1024).tolist(),
                conditioned_embedding=np.random.rand(50).tolist(),
            )
        )
        session.add(
            FileEmbedding(
                filepath=Path("2.mp3"),
                success=True,
                fail_reason=None,
                duration_seconds=1.0,
                embedding=np.random.rand(1024).tolist(),
                conditioned_embedding=None,
            )
        )

        session.commit()

    # test get all embeddings
    paths, embeds = fetch_numpy_embeddings()
    assert len(paths) == len(embeds) == 2
    assert embeds.shape == (2, 1024)
    assert set(paths) == {Path("1.mp3"), Path("2.mp3")}

    # test only_unconditioned
    paths, embeds = fetch_numpy_embeddings(only_unconditioned=True)
    assert len(paths) == len(embeds) == 1
    assert embeds.shape == (1, 1024)
    assert set(paths) == {Path("2.mp3")}
