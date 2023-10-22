"""Database module for moomoo_http."""
import datetime
from typing import Optional

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

TYPE_ANNOTATION_MAP = {
    str: postgresql.VARCHAR,
    datetime.datetime: postgresql.TIMESTAMP(timezone=True),
    list[str]: postgresql.ARRAY(postgresql.VARCHAR),
}


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


Base.registry.update_type_annotation_map(TYPE_ANNOTATION_MAP)

db = SQLAlchemy(model_class=Base)


def execute_sql_fetchall(
    session: Session, sql: str, params: Optional[dict] = None
) -> list[dict]:
    """Execute a SQL statement and return all results via dict cursors."""
    res = session.execute(text(sql), params)
    if res.returns_rows:
        return list(map(dict, res.mappings()))
    return []


class MoomooPlaylist(Base):
    """Model for moomoo_playlists table."""

    __tablename__ = "moomoo_playlists"

    id: Mapped[int] = mapped_column(
        primary_key=True, nullable=False, autoincrement=True
    )
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    generator: Mapped[str] = mapped_column(nullable=False, index=True)
    source_paths: Mapped[list[str]] = mapped_column(nullable=False)
    playlist: Mapped[list[str]] = mapped_column(nullable=False)
    ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )
