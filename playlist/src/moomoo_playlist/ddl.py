"""Datatbase models for playlist storage."""

import datetime
from typing import ClassVar
from uuid import UUID, uuid4

from sqlalchemy import ForeignKey, UniqueConstraint, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from .logger import get_logger
from .playlist import Playlist, Track

logger = get_logger().bind(module=__name__)


def now_utc() -> datetime.datetime:
    """Get the current time in UTC.

    Split out into a function for easier mocking in tests.
    """
    return datetime.datetime.now(datetime.timezone.utc)


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map: ClassVar[dict] = {
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        list: postgresql.JSONB,
        list[int]: postgresql.ARRAY(postgresql.INTEGER),
    }


class PlaylistCollection(BaseTable):
    """Model for moomoo_playlist_collections table."""

    __tablename__ = "moomoo_playlist_collections"

    collection_id: Mapped[UUID] = mapped_column(
        nullable=False, primary_key=True, default=uuid4
    )
    collection_name: Mapped[str] = mapped_column(nullable=False, index=True)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    refresh_at_hours_utc: Mapped[list[int]] = mapped_column(nullable=True)
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    refreshed_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=True, index=True
    )

    items: Mapped[list["PlaylistCollectionItem"]] = relationship(
        back_populates="collection"
    )

    # add unique constraint for username and collection_name
    __table_args__ = (UniqueConstraint("username", "collection_name"), {})

    @property
    def playlists(self) -> list[Playlist]:
        """Get the playlists in this collection."""
        return [item.to_playlist() for item in self.items]

    @classmethod
    def get_collection_by_name(
        cls, username: str, collection_name: str, session: Session
    ) -> "PlaylistCollection":
        """Get a playlist collection by name, raising an error if it does not exist."""
        collection = (
            session.query(cls)
            .filter_by(username=username, collection_name=collection_name)
            .one_or_none()
        )

        if collection is None:
            raise ValueError(
                f"Collection '{collection_name}' for user '{username}' does not exist."
            )

        return collection

    @property
    def last_refresh_target(self) -> datetime.datetime | None:
        """Get the most recent refresh target time for this collection."""
        if not self.refresh_at_hours_utc:
            return None

        # check all possible refresh times for the last two days, in case we run this at
        # like 00:01 or something
        now = now_utc()
        refresh_times = [
            datetime.datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=hour,
                tzinfo=datetime.timezone.utc,
            )
            for hour in list(set(self.refresh_at_hours_utc))
            for date in [now - datetime.timedelta(days=1), now]
            if 0 <= hour < 24
        ]

        return max([i for i in refresh_times if i <= now])

    @property
    def is_stale(self) -> bool:
        """Check if the collection is stale and needs to be refreshed.

        This is always True if the refresh_at_hours_utc is None, or if the playlists
        have never been refreshed.
        """
        if not self.refreshed_at_utc or not self.refresh_at_hours_utc:
            return True

        return self.refreshed_at_utc < self.last_refresh_target

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
            PlaylistCollectionItem.from_playlist(
                collection_id=self.collection_id,
                collection_order_index=i,
                playlist=playlist,
            )
            for i, playlist in enumerate(playlists)
        ]

        session.add_all(items)

        # update the collection's refreshed at time
        self.refreshed_at_utc = func.current_timestamp()
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

    collection: Mapped["PlaylistCollection"] = relationship(back_populates="items")

    # unique constraint for collection_id and collection_order_index
    __table_args__ = (UniqueConstraint("collection_id", "collection_order_index"), {})

    @classmethod
    def from_playlist(
        cls, playlist: Playlist, collection_id: UUID, collection_order_index: int
    ) -> "PlaylistCollectionItem":
        """Create a collection item from a playlist."""
        return cls(
            collection_id=collection_id,
            collection_order_index=collection_order_index,
            title=playlist.title,
            description=playlist.description,
            playlist=playlist.serialize_tracks(),
        )

    def to_playlist(self) -> Playlist:
        """Convert this collection item to a playlist."""
        return Playlist(
            tracks=[Track(**track) for track in self.playlist],
            title=self.title,
            description=self.description,
        )
