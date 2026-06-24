"""Define database ddl and dml operations."""

import datetime
import re
from pathlib import Path
from typing import Annotated, Any, ClassVar, NewType
from uuid import UUID, uuid4

from pgvector.sqlalchemy import Vector
from pydantic import BaseModel
from sqlalchemy import Compiled, ForeignKey, UniqueConstraint, func, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.types import String, TypeDecorator

from .db import execute_sql_fetchall, get_engine, get_session


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


# make some new python types for real and smallint
RealFloat = NewType("RealFloat", float)
SmallInt = NewType("SmallInt", int)


class PathType(TypeDecorator):
    """Stores pathlib.Path objects as strings in the database."""

    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return Path(value)


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map: ClassVar[dict] = {
        dict[str, Any]: postgresql.JSONB(none_as_null=True),
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        UUID: postgresql.UUID,
        list: postgresql.JSONB,
        list[int]: postgresql.ARRAY(postgresql.INTEGER),
        list[str]: postgresql.ARRAY(postgresql.VARCHAR),
        list[float]: Vector(1024),
        Annotated[list[float], 1024]: Vector(1024),
        Annotated[list[float], 50]: Vector(50),
        RealFloat: postgresql.REAL,
        SmallInt: postgresql.SMALLINT,
        Path: PathType,
    }

    @classmethod
    def table_name(cls) -> str:
        """Return the table name."""
        return cls.__tablename__

    @classmethod
    def primary_key(cls) -> list[str]:
        """Return the primary key columns."""
        return [c.name for c in cls.__table__.primary_key.columns]

    @classmethod
    def columns(cls) -> list[str]:
        """Return the column names."""
        return [c.name for c in cls.__table__.columns]

    def as_dict(self) -> dict[str, Any]:
        """Return a dict of the table's columns."""
        return {i: getattr(self, i) for i in self.columns()}

    @classmethod
    def create(cls, if_not_exists: bool = False, drop: bool = False) -> None:
        """Create the table."""
        if drop:
            cls.drop(if_exists=True)
        cls.metadata.create_all(get_engine(), checkfirst=if_not_exists, tables=[cls.__table__])

    @classmethod
    def drop(cls, if_exists: bool = False) -> None:
        """Drop the table."""
        cls.metadata.drop_all(get_engine(), checkfirst=if_exists, tables=[cls.__table__])

    @classmethod
    def exists(cls) -> bool:
        """Return True if the table exists."""
        return inspect(get_engine()).has_table(cls.table_name())

    def insert(
        self,
        session: Session = None,
        commit: bool | None = None,
    ) -> None:
        """Insert a row into the table.

        Commits the transaction by default if no session is provided, otherwise leaves it to the
        caller.
        """
        if commit is None:
            commit = session is None

        def f(s: Session):
            s.add(self)
            if commit:
                s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    @classmethod
    def bulk_insert(
        cls,
        rows: list[dict],
        session: Session = None,
        commit: bool | None = None,
    ) -> None:
        """Bulk insert rows into the table.

        Is MUCH faster than inserting one row at a time. Commits the transaction by default if no
        session is provided, otherwise leaves it to the caller.
        """
        if commit is None:
            commit = session is None

        def f(s: Session):
            s.execute(insert(cls), rows)
            if commit:
                s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    def upsert(
        self,
        update_cols: list[str] | None = None,
        session: Session = None,
        commit: bool | None = None,
    ) -> None:
        """Upsert a row into the table.

        Set update_cols to a list of columns to update on conflict. Defaults to all
        columns except the primary key.

        Commits the transaction by default if no session is provided, otherwise leaves it to the
        caller.
        """
        if commit is None:
            commit = session is None

        pk = self.primary_key()
        if not pk:
            raise ValueError("Cannot upsert a row without a primary key.")

        if not update_cols:
            update_cols = [i for i in self.columns() if i not in pk]

        data = self.as_dict()
        updates = {i: data[i] for i in update_cols}
        stmt = (
            insert(self.__class__)
            .values(**data)
            .on_conflict_do_update(index_elements=pk, set_=updates)
        )

        def f(s: Session):
            s.execute(stmt)
            if commit:
                s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    @classmethod
    def ddl(cls) -> list[Compiled]:
        """Return DDL for a table.

        Returns a list of Compiled objects, which can be executed directly or printed
        via str().
        """
        table = cls.__table__
        engine = get_engine()
        create = CreateTable(table).compile(engine)
        indexes = [CreateIndex(index).compile(engine) for index in table.indexes]
        return [create, *indexes]

    @classmethod
    def select_star(cls, where: str | None = None, **kw) -> list[dict]:
        """Return a select * query for the table, passing kw to execute_sql_fetchall.

        Optionally pass a where clause.
        """
        sql = f"select * from {cls.table_name()}"
        if where:
            sql += f" where {where}"
        return execute_sql_fetchall(sql, **kw)


class ListenBrainzListen(BaseTable):
    """Model for listenbrainz_listens table."""

    __tablename__ = "listenbrainz_listens"

    listen_md5: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    json_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    listen_at_ts_utc: Mapped[datetime.datetime] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def last_listen_for_user(cls, username: str) -> datetime.datetime | None:
        """Get the last listen timestamp from the user in the db.

        Returns None if the user has no listens.
        """
        stmt = select(func.max(cls.listen_at_ts_utc)).where(cls.username == username)
        with get_session() as session:
            return session.execute(stmt).scalar()


class ListenBrainzSimilarUserActivity(BaseTable):
    """Model for listenbrainz_similar_user_activity table."""

    __tablename__ = "listenbrainz_similar_user_activity"

    payload_id: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    from_username: Mapped[str] = mapped_column(nullable=False, index=True)
    to_username: Mapped[str] = mapped_column(nullable=False, index=True)
    entity: Mapped[str] = mapped_column(nullable=False, index=True)
    time_range: Mapped[str] = mapped_column(nullable=False, index=True)
    user_similarity: Mapped[float] = mapped_column(nullable=False)
    json_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class MusicBrainzAnnotation(BaseTable):
    """Model for musicbrainz_annotations table."""

    __tablename__ = "musicbrainz_annotations"

    mbid: Mapped[UUID] = mapped_column(primary_key=True, nullable=False)
    entity: Mapped[str] = mapped_column(nullable=False, index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(nullable=True)
    ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class ListenBrainzArtistStats(BaseTable):
    """Model for listenbrainz_artist_stats table."""

    __tablename__ = "listenbrainz_artist_stats"

    mbid: Mapped[UUID] = mapped_column(primary_key=True, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(nullable=True)
    ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class LocalFile(BaseTable):
    """Model for local_music_files table."""

    __tablename__ = "local_music_files"

    filepath: Mapped[Path] = mapped_column(primary_key=True, nullable=False)
    recording_md5: Mapped[str] = mapped_column(nullable=True, index=True)
    recording_name: Mapped[str] = mapped_column(nullable=True)
    release_name: Mapped[str] = mapped_column(nullable=True)
    artist_name: Mapped[str] = mapped_column(nullable=True)
    json_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    file_created_at: Mapped[datetime.datetime] = mapped_column(nullable=False)
    file_modified_at: Mapped[datetime.datetime] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


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


class LocalFileBirthTimestamp(BaseTable):
    """Model for local_music_files_birth_timestamps table."""

    __tablename__ = "local_music_files_birth_timestamps"

    filepath: Mapped[Path] = mapped_column(primary_key=True, nullable=False)
    birth_at: Mapped[datetime.datetime] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def bulk_upsert_on_conflict_do_nothing(
        cls, rows: list[dict], session: Session = None, commit: bool | None = None
    ) -> None:
        """Bulk upsert rows into the table, doing nothing on conflict.

        Commits the transaction by default if no session is provided, otherwise leaves it to the
        caller.
        """
        if not rows:
            return

        if commit is None:
            commit = session is None

        stmt = insert(cls).values(rows).on_conflict_do_nothing(index_elements=["filepath"])

        def f(s: Session):
            s.execute(stmt)
            if commit:
                s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)


class FileEmbedding(BaseTable):
    """Model for local_music_files table."""

    __tablename__ = "local_music_embeddings"

    filepath: Mapped[Path] = mapped_column(primary_key=True, nullable=False)
    success: Mapped[bool] = mapped_column(nullable=False)
    fail_reason: Mapped[str] = mapped_column(nullable=True)
    duration_seconds: Mapped[float] = mapped_column(nullable=True)
    embedding: Mapped[Annotated[list[float], 1024]] = mapped_column(nullable=True)
    conditioned_embedding: Mapped[Annotated[list[float], 50]] = mapped_column(nullable=True)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class MessyBrainzNameMap(BaseTable):
    """Model for messybrainz_name_map table."""

    __tablename__ = "messybrainz_name_map"

    recording_md5: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    recording_name: Mapped[str] = mapped_column(nullable=False)
    artist_name: Mapped[str] = mapped_column(nullable=False)
    release_name: Mapped[str] = mapped_column(nullable=False)
    success: Mapped[bool] = mapped_column(nullable=False, index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(nullable=True)
    ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class ListenBrainzUserFeedback(BaseTable):
    """Model for listenbrainz_user_feedback table."""

    __tablename__ = "listenbrainz_user_feedback"

    feedback_md5: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    score: Mapped[int] = mapped_column(nullable=False)
    recording_mbid: Mapped[UUID] = mapped_column(nullable=False)
    feedback_at: Mapped[datetime.datetime] = mapped_column(nullable=False, index=True)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


class ListenBrainzDataDump(BaseTable):
    __tablename__ = "listenbrainz_data_dumps"

    slug: Mapped[str] = mapped_column(primary_key=True, unique=True, nullable=False)
    ftp_path: Mapped[str] = mapped_column(nullable=False, unique=True)
    ftp_modify_ts: Mapped[datetime.datetime] = mapped_column(nullable=False)
    date: Mapped[datetime.date] = mapped_column(nullable=False)
    start_timestamp: Mapped[datetime.datetime] = mapped_column(nullable=False)
    end_timestamp: Mapped[datetime.datetime] = mapped_column(nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    refreshed_at: Mapped[datetime.datetime] = mapped_column(nullable=True)

    records: Mapped[list["ListenBrainzDataDumpRecord"]] = relationship(back_populates="dump")

    def replace_records(self, records: list[dict], session: Session) -> None:
        """Replace all records in the dump with the given records."""
        # add dump id
        for record in records:
            record["slug"] = self.slug

        session.query(ListenBrainzDataDumpRecord).filter(
            ListenBrainzDataDumpRecord.slug == self.slug
        ).delete()

        if records:
            ListenBrainzDataDumpRecord.bulk_insert(records, session=session, commit=False)

        self.refreshed_at = func.current_timestamp()


class ListenBrainzDataDumpRecord(BaseTable):
    __tablename__ = "listenbrainz_data_dump_records"
    __table_args__ = (UniqueConstraint("slug", "user_id", "artist_mbid"), {})

    dump_record_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    slug: Mapped[str] = mapped_column(
        ForeignKey(ListenBrainzDataDump.slug), nullable=False, index=True
    )
    user_id: Mapped[int] = mapped_column(nullable=False)
    artist_mbid: Mapped[UUID] = mapped_column(nullable=False)
    listen_count: Mapped[int] = mapped_column(nullable=False)

    dump: Mapped["ListenBrainzDataDump"] = relationship(back_populates="records")


class ListenBrainzCollaborativeFilteringScore(BaseTable):
    __tablename__ = "listenbrainz_collaborative_filtering_scores"
    __table_args__ = (UniqueConstraint("artist_mbid_a", "artist_mbid_b"), {})

    mbid_pair_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    artist_mbid_a: Mapped[UUID] = mapped_column(nullable=False, index=True)
    artist_mbid_b: Mapped[UUID] = mapped_column(nullable=False, index=True)
    score_value: Mapped[RealFloat] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )

    @classmethod
    def reset_pk(
        cls,
        session: Session | None = None,
        commit: bool | None = None,
    ) -> None:
        """Reset the primary key to allow for re-insertions.

        Commits the transaction by default if no session is provided, otherwise leaves it to the
        caller.
        """
        if commit is None:
            commit = session is None

        name = cls.table_name()
        sql = f"SELECT setval(pg_get_serial_sequence('{name}', 'mbid_pair_id'), 1, false);"

        def f(session: Session):
            session.execute(text(sql))
            if commit:
                session.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)


class MusicBrainzDataDump(BaseTable):
    """Model for musicbrainz_data_dumps table."""

    __tablename__ = "musicbrainz_data_dumps"

    slug: Mapped[str] = mapped_column(primary_key=True, unique=True, nullable=False)
    packet_number: Mapped[int] = mapped_column(nullable=False)
    entity: Mapped[str] = mapped_column(nullable=False)
    dump_timestamp: Mapped[datetime.datetime] = mapped_column(nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    refreshed_at: Mapped[datetime.datetime] = mapped_column(nullable=True)

    records: Mapped[list["MusicBrainzDataDumpRecord"]] = relationship(back_populates="dump")

    def replace_records(self, records: list[dict], session: Session) -> None:
        """Replace all records in the dump with the given records."""
        # add dump id
        for record in records:
            record["slug"] = self.slug

        session.query(MusicBrainzDataDumpRecord).filter(
            MusicBrainzDataDumpRecord.slug == self.slug
        ).delete()

        if records:
            MusicBrainzDataDumpRecord.bulk_insert(records, session=session)

        self.refreshed_at = func.current_timestamp()


class MusicBrainzDataDumpRecord(BaseTable):
    """Model for musicbrainz_data_dump_records table."""

    __tablename__ = "musicbrainz_data_dump_records"
    __table_args__ = (UniqueConstraint("slug", "mbid"), {})

    dump_record_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    slug: Mapped[str] = mapped_column(
        ForeignKey(MusicBrainzDataDump.slug), nullable=False, index=True
    )
    mbid: Mapped[UUID] = mapped_column(nullable=False, index=True)
    json_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)

    dump: Mapped["MusicBrainzDataDump"] = relationship(back_populates="records")


class AnnotationQueueLog(BaseTable):
    """Model for annotation_queue_log table."""

    __tablename__ = "annotation_queue_log"

    log_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    source: Mapped[str] = mapped_column(nullable=False, index=True)
    entity: Mapped[str] = mapped_column(nullable=False, index=True)
    as_of_ts_utc: Mapped[datetime.datetime] = mapped_column(nullable=False, index=True)
    queue_size: Mapped[int] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )

    @classmethod
    def last_update_timestamp(cls, session: Session, source: str) -> datetime.datetime | None:
        """Get the last update timestamp in the log for the given source.

        Returns None if there are no log entries.
        """
        stmt = select(func.max(cls.as_of_ts_utc)).filter(cls.source == source)
        return session.execute(stmt).scalar()


class PlaylistCollection(BaseTable):
    """Model for moomoo_playlist_collections table."""

    __tablename__ = "moomoo_playlist_collections"

    collection_id: Mapped[UUID] = mapped_column(nullable=False, primary_key=True, default=uuid4)
    collection_name: Mapped[str] = mapped_column(nullable=False, index=True)
    username: Mapped[str] = mapped_column(nullable=False, index=True)
    refresh_at_hours_utc: Mapped[list[int]] = mapped_column(nullable=True)
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )
    refreshed_at_utc: Mapped[datetime.datetime] = mapped_column(nullable=True, index=True)

    items: Mapped[list["Playlist"]] = relationship(back_populates="collection")

    # add unique constraint for username and collection_name
    __table_args__ = (UniqueConstraint("username", "collection_name"), {})

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

    @property
    def ordered_items(self) -> list["Playlist"]:
        """Return the playlists in the collection, ordered by collection_order_index."""
        return sorted(self.items, key=lambda x: x.collection_order_index)

    def make_playlist(
        self,
        tracks: list["PlaylistTrack.Data"],
        title: str | None = None,
        description: str | None = None,
    ) -> "Playlist":
        """Make a new playlist in the collection from the given data."""
        return Playlist.from_tracks(
            collection_id=self.collection_id,
            collection_order_index=len(self.items),
            title=title,
            description=description,
            tracks=tracks,
        )

    def replace_playlists(
        self,
        playlists: list["Playlist.Data"],
        session: Session,
        force: bool = False,
    ) -> bool:
        """Replace all playlists in the collection with the given list.

        Set force=True to replace the playlists even if the collection is not stale.

        Returns a boolean indicating if the playlists were replaced (True = replaced,
        False = skipped).
        """

        if self.is_fresh and not force:
            return False

        playlists = [
            Playlist.from_data(p, collection_id=self.collection_id, collection_order_index=i)
            for i, p in enumerate(playlists)
        ]

        # drop all existing playlists for this user and collection
        session.query(Playlist).filter_by(collection_id=self.collection_id).delete()
        session.add_all(playlists)

        # update the collection's refreshed at time
        self.refreshed_at_utc = func.current_timestamp()

        return True


class Playlist(BaseTable):
    """Model for moomoo_playlist_collection_items table."""

    __tablename__ = "moomoo_playlist_collection_items"

    class Data(BaseModel):
        """Playlist fields independent of collection context."""

        title: str | None
        description: str | None
        tracks: list["PlaylistTrack.Data"]

    playlist_id: Mapped[UUID] = mapped_column(nullable=False, primary_key=True, default=uuid4)
    collection_id: Mapped[UUID] = mapped_column(ForeignKey(PlaylistCollection.collection_id))
    collection_order_index: Mapped[int] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=True)
    description: Mapped[str] = mapped_column(nullable=True)
    playlist: Mapped[list] = mapped_column(nullable=True)  # TODO: remove once clients migrate.
    create_at_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp()
    )

    collection: Mapped["PlaylistCollection"] = relationship(back_populates="items")
    tracks: Mapped[list["PlaylistTrack"]] = relationship(back_populates="playlist")

    # unique constraint for collection_id and collection_order_index
    __table_args__ = (UniqueConstraint("collection_id", "collection_order_index"), {})

    @property
    def ordered_tracks(self) -> list["PlaylistTrack"]:
        """Return the tracks in the playlist, ordered by track_order_index."""
        return sorted(self.tracks, key=lambda x: x.track_order_index)

    @property
    def data(self) -> Data:
        """The playlist data without any collection context, appropriate for client consumption."""
        return self.Data(
            title=self.title,
            description=self.description,
            tracks=[track.data for track in self.ordered_tracks],
        )

    @property
    def seeds(self) -> list["PlaylistTrack"]:
        """Return the seed tracks in the playlist."""
        return [t for t in self.ordered_tracks if t.is_seed]

    def model_dump(self, **kwargs) -> dict[str, Any]:
        """Expose the model dump from the data here, so that this can be ducktyped when needed."""
        return self.data.model_dump(**kwargs)

    @classmethod
    def from_tracks(
        cls,
        collection_id: UUID,
        collection_order_index: int,
        title: str | None,
        description: str | None,
        tracks: list["PlaylistTrack.Data"],
    ) -> "Playlist":
        """Construct a Playlist from a list of tracks."""
        # convert everything to data, then back into PlaylistTrack objects
        tracks = [PlaylistTrack.from_data(t, track_order_index=i) for i, t in enumerate(tracks)]
        playlist = cls(
            collection_id=collection_id,
            collection_order_index=collection_order_index,
            title=title,
            description=description,
            playlist=[track.to_dict() for track in tracks],
        )
        playlist.tracks = tracks
        return playlist

    @classmethod
    def from_data(
        cls, data: Data | dict, collection_id: UUID, collection_order_index: int
    ) -> "Playlist":
        """Construct a Playlist from a PlaylistData object and collection context."""
        if isinstance(data, dict):
            # in case of nested pydantic model dumps.
            data = cls.Data(**data)

        return cls.from_tracks(
            collection_id=collection_id,
            collection_order_index=collection_order_index,
            **data.model_dump(),
        )

    def append_tracks(self, tracks: list["PlaylistTrack.Data"], session: Session) -> None:
        """Add tracks to the end of the playlist."""
        idx = len(self.tracks)
        for i, track in enumerate(tracks):
            track = PlaylistTrack.from_data(track, track_order_index=idx + i)
            track.playlist_id = self.playlist_id
            session.add(track)


class PlaylistTrack(BaseTable):
    """Model for moomoo_playlist_tracks table."""

    __tablename__ = "moomoo_playlist_tracks"

    class Data(BaseModel):
        """Track fields independent of playlist/ordering context."""

        filepath: Path
        recording_mbid: UUID | None = None
        release_mbid: UUID | None = None
        release_group_mbid: UUID | None = None
        artist_mbid: UUID | None = None
        album_artist_mbid: UUID | None = None
        track_length_seconds: int | None = None
        match_distance: float | None = None
        is_seed: bool | None = None

    track_id: Mapped[UUID] = mapped_column(nullable=False, primary_key=True, default=uuid4)
    playlist_id: Mapped[UUID] = mapped_column(ForeignKey(Playlist.playlist_id))
    track_order_index: Mapped[int] = mapped_column(nullable=False)

    filepath: Mapped[Path] = mapped_column(nullable=False)

    # optional metadata about the track
    recording_mbid: Mapped[UUID] = mapped_column(nullable=True)
    release_mbid: Mapped[UUID] = mapped_column(nullable=True)
    release_group_mbid: Mapped[UUID] = mapped_column(nullable=True)
    artist_mbid: Mapped[UUID] = mapped_column(nullable=True)
    album_artist_mbid: Mapped[UUID] = mapped_column(nullable=True)
    track_length_seconds: Mapped[int] = mapped_column(nullable=True)
    match_distance: Mapped[float] = mapped_column(nullable=True)
    is_seed: Mapped[bool] = mapped_column(nullable=True)

    # backref to playlist
    playlist: Mapped["Playlist"] = relationship(back_populates="tracks")

    # unique constraint for playlist_id and track_order_index
    __table_args__ = (UniqueConstraint("playlist_id", "track_order_index"), {})

    @property
    def data(self) -> Data:
        """Return the track data as a PlaylistTrack.Data object."""
        return self.Data(
            filepath=self.filepath,
            recording_mbid=self.recording_mbid,
            release_mbid=self.release_mbid,
            release_group_mbid=self.release_group_mbid,
            artist_mbid=self.artist_mbid,
            album_artist_mbid=self.album_artist_mbid,
            track_length_seconds=self.track_length_seconds,
            match_distance=self.match_distance,
            is_seed=self.is_seed,
        )

    @classmethod
    def from_data(cls, data: Data | dict, track_order_index: int) -> "PlaylistTrack":
        if isinstance(data, dict):
            # in case of nested pydantic model dumps.
            data = cls.Data(**data)
        return cls(track_order_index=track_order_index, **data.model_dump())

    def model_dump(self, **kwargs) -> dict[str, Any]:
        """Expose the model dump from the data here, so that this can be ducktyped when needed."""
        return self.data.model_dump(**kwargs)

    def to_dict(self) -> dict:
        """Convert to a dictionary, appropriate for json serialization."""
        return self.data.model_dump(exclude_none=True, mode="json")


# list all subclasses of BaseTable and put them in a list for easy access
TABLES: list[type[BaseTable]] = []
for subclass in BaseTable.__subclasses__():
    TABLES.append(subclass)

__all__ = ["BaseTable", "TABLES", *(i.__name__ for i in TABLES)]
