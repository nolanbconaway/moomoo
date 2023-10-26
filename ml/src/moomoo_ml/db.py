"""Connectivity utils for the database."""
import datetime
import os

from pgvector.sqlalchemy import Vector
from sqlalchemy import create_engine, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def get_session() -> Session:
    """Get a sqlalchemy session for the db."""
    return Session(bind=create_engine(os.environ["MOOMOO_POSTGRES_URI"]))


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map = {
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        list[float]: Vector(1024),
    }


class FileEmbedding(BaseTable):
    """Model for local_music_files table."""

    __tablename__ = "local_music_embeddings"

    filepath: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    success: Mapped[bool] = mapped_column(nullable=False)
    fail_reason: Mapped[str] = mapped_column(nullable=True)
    duration_seconds: Mapped[float] = mapped_column(nullable=True)
    embedding: Mapped[list[float]] = mapped_column(nullable=True)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )
