from pathlib import Path

import numpy as np
import psycopg
from moomoo_ml.db import FileEmbedding, get_session


def test_pg_connect_mocked(postgresql: psycopg.Connection):
    """Make sure the pg_connect function is mocked as expected.

    The postgresql fixture is provided by the pytest-postgresql plugin, and
    points to a fresh, temporary database.
    """
    with get_session() as session:
        engine = session.get_bind()
        assert engine.url.username == postgresql.info.user
        assert engine.url.host == postgresql.info.host
        assert engine.url.port == postgresql.info.port
        assert engine.url.database == postgresql.info.dbname


def test_FileEmbedding__fetch_numpy_embeddings():
    """Test the FileEmbedding.fetch_numpy_embeddings method."""
    # test no data, no embeddings
    paths, embeds = FileEmbedding.fetch_numpy_embeddings()
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
    paths, embeds = FileEmbedding.fetch_numpy_embeddings()
    assert len(paths) == len(embeds) == 2
    assert embeds.shape == (2, 1024)
    assert set(paths) == {Path("1.mp3"), Path("2.mp3")}

    # test only_unconditioned
    paths, embeds = FileEmbedding.fetch_numpy_embeddings(only_unconditioned=True)
    assert len(paths) == len(embeds) == 1
    assert embeds.shape == (1, 1024)
    assert set(paths) == {Path("2.mp3")}
