"""Connectivity utils for the database."""

from pathlib import Path

import numpy as np
from moomoo_pg import FileEmbedding, get_session
from sqlalchemy import text


def fetch_numpy_embeddings(only_unconditioned: bool = False) -> tuple[list[Path], np.ndarray]:
    """Get embeddings from the database.

    Returns a list of Paths and a 2d numpy array of embeddings. Each row in the array
    corresponds to the embedding of the file at the same index in the list.

    If only_unconditioned is provided, only embeddings which have not been conditioned will be
    returned.
    """
    if only_unconditioned:
        conditioner_sql = FileEmbedding.conditioned_embedding.is_(None)
    else:
        conditioner_sql = text("true")

    with get_session() as session:
        query = (
            session.query(FileEmbedding)
            .filter(FileEmbedding.success.is_(True))
            .filter(conditioner_sql)
            .order_by(FileEmbedding.filepath)
        )

        if not query.count():
            paths, embeddings = [], []
        else:
            paths, embeddings = zip(*[(i.filepath, i.embedding) for i in query.all()], strict=True)

    return paths, np.array(embeddings)
