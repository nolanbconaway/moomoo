"""Datatbase models for playlist storage."""

import datetime
from typing import ClassVar
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from .logger import get_logger
from .playlist import Playlist

logger = get_logger().bind(module=__name__)


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

    collection_id: Mapped[UUID] = mapped_column(
        nullable=False, primary_key=True, default=uuid4
    )
    collection_name: Mapped[str] = mapped_column(nullable=False, index=True)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    update_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )
    playlists: Mapped[list["PlaylistCollectionItem"]] = relationship(
        back_populates="collection"
    )

    @classmethod
    def get_collection_by_name(
        cls, username: str, collection_name: str, session: Session
    ) -> "PlaylistCollection":
        """Get a playlist collection by name, creating it if it doesn't exist."""
        collection = (
            session.query(cls)
            .filter_by(username=username, collection_name=collection_name)
            .one_or_none()
        )

        if collection is None:
            logger.info(
                f"Creating collection '{collection_name}' for user '{username}'."
            )
            collection = cls(username=username, collection_name=collection_name)
            session.add(collection)
            session.commit()

        return collection

    def replace_playlists(
        self, playlists: list[Playlist], session: Session
    ) -> "PlaylistCollection":
        """Replace all playlists in the collection with the given list."""
        logger.info(
            f"Replacing playlists in collection '{self.collection_name}' for user"
            + self.username
        )
        # drop all existing playlists for this user and collection
        session.query(PlaylistCollectionItem).filter_by(
            collection_id=self.collection_id
        ).delete()

        items = [
            PlaylistCollectionItem(
                collection_id=self.collection_id,
                collection_order_index=i,
                title=playlist.title,
                description=playlist.description,
                playlist=playlist.serialize_list(),
            )
            for i, playlist in enumerate(playlists)
        ]

        session.add_all(items)
        session.commit()


class PlaylistCollectionItem(BaseTable):
    """Model for moomoo_playlist_collection_items table."""

    __tablename__ = "moomoo_playlist_collection_items"

    playlist_id: Mapped[UUID] = mapped_column(
        nullable=False, primary_key=True, default=uuid4
    )
    collection_id: Mapped[UUID] = mapped_column(
        ForeignKey(PlaylistCollection.collection_id)
    )
    collection_order_index: Mapped[int] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=True)
    description: Mapped[str] = mapped_column(nullable=True)
    playlist: Mapped[list] = mapped_column(nullable=False)
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    update_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    collection: Mapped["PlaylistCollection"] = relationship(back_populates="playlists")
