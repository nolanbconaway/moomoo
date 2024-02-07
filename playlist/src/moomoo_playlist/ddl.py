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
    refresh_interval_hours: Mapped[int] = mapped_column(nullable=True)
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    playlists_refreshed_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=True, index=True
    )
    playlists: Mapped[list["PlaylistCollectionItem"]] = relationship(
        back_populates="collection"
    )

    @classmethod
    def get_collection_by_name(
        cls,
        username: str,
        collection_name: str,
        session: Session,
        refresh_interval_hours: int | None = None,
    ) -> "PlaylistCollection":
        """Get a playlist collection by name, creating it if it doesn't exist.

        kwargs are passed to the constructor if the collection is created. This is where
        the refresh_interval_hours can be set.
        """
        collection = (
            session.query(cls)
            .filter_by(username=username, collection_name=collection_name)
            .one_or_none()
        )

        if collection is None:
            logger.info(
                f"Creating collection '{collection_name}' for user '{username}'."
            )
            collection = cls(
                username=username,
                collection_name=collection_name,
                refresh_interval_hours=refresh_interval_hours,
            )
            session.add(collection)
            session.commit()

        elif refresh_interval_hours is not None:
            # raise warning if the refresh interval was supplied and is not the
            # same as the existing collection's refresh interval
            if collection.refresh_interval_hours != refresh_interval_hours:
                logger.warning(
                    f"Collection '{collection_name}' for user '{username}' already "
                    + "exists with a different refresh interval. "
                    + f"{refresh_interval_hours} != {collection.refresh_interval_hours}"
                )

        return collection

    @property
    def is_stale(self) -> bool:
        """Check if the collection is stale and needs to be refreshed.

        This is always True if the refresh interval is None.
        """
        if self.refresh_interval_hours is None:
            return True

        if self.playlists_refreshed_at_utc is None:
            return True

        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        delta_hours = (now - self.playlists_refreshed_at_utc).total_seconds() / 3600
        return delta_hours >= self.refresh_interval_hours

    @property
    def is_fresh(self) -> bool:
        """Check if the collection is fresh and does not need to be refreshed."""
        return not self.is_stale

    def replace_playlists(
        self, playlists: list[Playlist], session: Session, force: bool = False
    ) -> int:
        """Replace all playlists in the collection with the given list.

        Set force=True to replace the playlists even if the collection is not stale.

        Returns a boolean indicating if the playlists were replaced (True = replaced,
        False = skipped).
        """
        logger.info(
            f"Replacing playlists in collection '{self.collection_name}' for user "
            + self.username
        )

        if self.is_fresh and not force:
            logger.info(
                f"Collection '{self.collection_name}' for user '{self.username}' is "
                "fresh; skipping."
            )
            return False

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

        # update the collection's refreshed at time
        self.playlists_refreshed_at_utc = func.current_timestamp()
        session.commit()

        logger.info(f"Saved {len(playlists)} playlist(s) to database.")
        return True


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

    collection: Mapped["PlaylistCollection"] = relationship(back_populates="playlists")
