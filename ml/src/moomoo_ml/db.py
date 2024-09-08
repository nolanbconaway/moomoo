"""Connectivity utils for the database."""

import datetime
import os
import re
from pathlib import Path
from typing import Annotated, ClassVar

import numpy as np
from pgvector.sqlalchemy import Vector
from sqlalchemy import create_engine, func, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def get_session() -> Session:
    """Get a sqlalchemy session for the db."""
    return Session(bind=create_engine(os.environ["MOOMOO_POSTGRES_URI"]))


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map: ClassVar[dict] = {
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        Annotated[list[float], 1024]: Vector(1024),
        Annotated[list[float], 50]: Vector(50),
    }


class FileEmbedding(BaseTable):
    """Model for local_music_files table."""

    __tablename__ = "local_music_embeddings"

    filepath: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    success: Mapped[bool] = mapped_column(nullable=False)
    fail_reason: Mapped[str] = mapped_column(nullable=True)
    duration_seconds: Mapped[float] = mapped_column(nullable=True)
    embedding: Mapped[Annotated[list[float], 1024]] = mapped_column(nullable=True)
    conditioned_embedding: Mapped[Annotated[list[float], 50]] = mapped_column(nullable=True)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def fetch_numpy_embeddings(
        cls, only_unconditioned: bool = False
    ) -> tuple[list[Path], np.ndarray]:
        """Get embeddings from the database.

        Returns a list of Paths and a 2d numpy array of embeddings. Each row in the array
        corresponds to the embedding of the file at the same index in the list.

        If only_unconditioned is provided, only embeddings which have not been conditioned will be
        returned.
        """
        if only_unconditioned:
            conditioner_sql = cls.conditioned_embedding.is_(None)
        else:
            conditioner_sql = text("true")

        with get_session() as session:
            query = (
                session.query(cls)
                .filter(cls.success.is_(True))
                .filter(conditioner_sql)
                .order_by(cls.filepath)
            )

            if not query.count():
                paths, embeddings = [], []
            else:
                paths, embeddings = zip(*[(Path(i.filepath), i.embedding) for i in query.all()])

        return paths, np.array(embeddings)


class LocalFileExcludeRegex(BaseTable):
    """Model containing regex patterns to exclude from local music files."""

    __tablename__ = "local_music_files_exclude_regex"

    pattern: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    note: Mapped[str] = mapped_column(nullable=True)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def fetch_all_regex(cls) -> list[re.Pattern]:
        """Return a list of compiled regex patterns."""
        with get_session() as session:
            return [re.compile(i.pattern) for i in session.query(cls).all()]
