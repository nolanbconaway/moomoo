"""Define database ddl and dml operations."""

import datetime
import re
from typing import Any, ClassVar, NewType
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import Compiled, ForeignKey, UniqueConstraint, func, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.schema import CreateIndex, CreateTable

from .connection import execute_sql_fetchall, get_engine, get_session

# make some new python types for real and smallint
RealFloat = NewType("RealFloat", float)
SmallInt = NewType("SmallInt", int)


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map: ClassVar[dict] = {
        dict[str, Any]: postgresql.JSONB(none_as_null=True),
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        list[float]: Vector(1024),
        UUID: postgresql.UUID,
        list[str]: postgresql.ARRAY(postgresql.VARCHAR),
        RealFloat: postgresql.REAL,
        SmallInt: postgresql.SMALLINT,
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

    def dict(self) -> dict[str, Any]:
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

    def insert(self, session: Session = None) -> None:
        """Insert a row into the table."""

        def f(s: Session):
            s.add(self)
            s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    @classmethod
    def bulk_insert(cls, rows: list[dict], session: Session = None, commit: bool = True) -> None:
        """Bulk insert rows into the table.

        Is MUCH faster than inserting one row at a time.
        """

        def f(s: Session):
            s.execute(insert(cls), rows)
            if commit:
                s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    def upsert(self, update_cols: list[str] | None = None, session: Session = None) -> None:
        """Upsert a row into the table.

        Set update_cols to a list of columns to update on conflict. Defaults to all
        columns except the primary key.
        """
        pk = self.primary_key()
        if not pk:
            raise ValueError("Cannot upsert a row without a primary key.")

        if not update_cols:
            update_cols = [i for i in self.columns() if i not in pk]

        data = self.dict()
        updates = {i: data[i] for i in update_cols}
        stmt = (
            insert(self.__class__)
            .values(**data)
            .on_conflict_do_update(index_elements=pk, set_=updates)
        )

        def f(s: Session):
            s.execute(stmt)
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

    filepath: Mapped[str] = mapped_column(primary_key=True, nullable=False)
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
            ListenBrainzDataDumpRecord.bulk_insert(records, session=session)

        self.refreshed_at = func.current_timestamp()
        session.commit()


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
    def reset_pk(cls, session: Session | None = None, commit: bool = True) -> None:
        """Reset the primary key to allow for re-insertions."""
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
        session.commit()


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


TABLES: tuple[BaseTable] = (
    ListenBrainzListen,
    LocalFile,
    LocalFileExcludeRegex,
    ListenBrainzSimilarUserActivity,
    MusicBrainzAnnotation,
    ListenBrainzArtistStats,
    MessyBrainzNameMap,
    ListenBrainzUserFeedback,
    ListenBrainzDataDump,
    ListenBrainzDataDumpRecord,
    ListenBrainzCollaborativeFilteringScore,
    MusicBrainzDataDump,
    MusicBrainzDataDumpRecord,
    AnnotationQueueLog,
)
