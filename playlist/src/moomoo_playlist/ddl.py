"""Datatbase models for playlist storage."""
import datetime
from typing import ClassVar

from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from .playlist import Playlist


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map: ClassVar[dict] = {
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        list: postgresql.JSONB,
    }


class SavedPlaylist(BaseTable):
    """Model for moomoo_saved_playlists table."""

    __tablename__ = "moomoo_saved_playlists"

    playlist_id: Mapped[int] = mapped_column(
        nullable=False, primary_key=True, autoincrement=True
    )
    playlist: Mapped[list] = mapped_column(nullable=False)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    title: Mapped[str] = mapped_column(nullable=True)
    description: Mapped[str] = mapped_column(nullable=True)
    collection_name: Mapped[str] = mapped_column(nullable=False, index=True)
    collection_update_ts: Mapped[datetime.datetime] = mapped_column(
        nullable=False, index=True
    )

    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def save_collection(
        cls,
        playlists: list[Playlist],
        username: str,
        collection_name: str,
        session: Session,
        collection_update_ts: datetime.datetime | None = None,
    ) -> "SavedPlaylist":
        """Save a collection of playlists to the database."""
        if collection_update_ts is None:
            collection_update_ts = datetime.datetime.utcnow().replace(
                tzinfo=datetime.timezone.utc
            )
        plists = [
            cls(
                playlist=playlist.serialize_list(),
                username=username,
                title=playlist.title,
                description=playlist.description,
                collection_name=collection_name,
                collection_update_ts=collection_update_ts,
            )
            for playlist in playlists
        ]

        session.add_all(plists)
        session.commit()
