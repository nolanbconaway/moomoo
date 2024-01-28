"""Datatbase models for playlist storage."""
import datetime
from typing import ClassVar
from uuid import UUID, uuid4

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


class PlaylistCollection(BaseTable):
    """Model for moomoo_playlist_collections table."""

    __tablename__ = "moomoo_playlist_collections"

    playlist_id: Mapped[UUID] = mapped_column(
        nullable=False, primary_key=True, default=uuid4
    )
    playlist: Mapped[list] = mapped_column(nullable=False)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    title: Mapped[str] = mapped_column(nullable=True)
    description: Mapped[str] = mapped_column(nullable=True)
    collection_name: Mapped[str] = mapped_column(nullable=False, index=True)
    collection_order_index: Mapped[int] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )

    @classmethod
    def save_collection(
        cls,
        playlists: list[Playlist],
        username: str,
        collection_name: str,
        session: Session,
        insert_ts_utc: datetime.datetime | None = None,
    ) -> "PlaylistCollection":
        """Save an ordered collection of playlists to the database."""
        if insert_ts_utc is None:
            insert_ts_utc = datetime.datetime.utcnow().replace(
                tzinfo=datetime.timezone.utc
            )

        plists = [
            cls(
                playlist=playlist.serialize_list(),
                username=username,
                title=playlist.title,
                description=playlist.description,
                collection_name=collection_name,
                collection_order_index=i,
            )
            for i, playlist in enumerate(playlists)
        ]

        # drop all existing playlists for this user and collection
        session.query(cls).filter_by(
            username=username, collection_name=collection_name
        ).delete()

        session.add_all(plists)
        session.commit()
