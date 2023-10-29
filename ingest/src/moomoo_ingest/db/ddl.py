"""Define database ddl and dml operations."""
import datetime
from typing import Any
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import Compiled, func, inspect, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.schema import CreateIndex, CreateTable

from .connection import execute_sql_fetchall, get_engine, get_session


class BaseTable(DeclarativeBase):
    """Base class for all database models."""

    type_annotation_map = {
        dict[str, Any]: postgresql.JSONB(none_as_null=True),
        str: postgresql.VARCHAR,
        datetime.datetime: postgresql.TIMESTAMP(timezone=True),
        list[float]: Vector(1024),
        UUID: postgresql.UUID,
        list[str]: postgresql.ARRAY(postgresql.VARCHAR),
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
        cls.metadata.create_all(
            get_engine(), checkfirst=if_not_exists, tables=[cls.__table__]
        )

    @classmethod
    def drop(cls, if_exists: bool = False) -> None:
        """Drop the table."""
        cls.metadata.drop_all(
            get_engine(), checkfirst=if_exists, tables=[cls.__table__]
        )

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
    def bulk_insert(cls, rows: list[dict], session: Session = None) -> None:
        """Bulk insert rows into the table.

        Is MUCH faster than inserting one row at a time.
        """

        def f(s: Session):
            s.execute(insert(cls), rows)
            s.commit()

        if session is None:
            with get_session() as session:
                f(session)
        else:
            f(session)

    def upsert(self, update_cols: list[str] = None, session: Session = None) -> None:
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
        return [create] + indexes

    @classmethod
    def select_star(cls, where: str = None, **kw) -> list[dict]:
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
    def last_listen_for_user(cls, username: str) -> datetime.datetime:
        """Get the last listen timestamp from the user in the db."""
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
    json_data: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    file_created_at: Mapped[datetime.datetime] = mapped_column(nullable=False)
    file_modified_at: Mapped[datetime.datetime] = mapped_column(nullable=False)
    insert_ts_utc: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.current_timestamp(), index=True
    )


TABLES: tuple[BaseTable] = (
    ListenBrainzListen,
    LocalFile,
    ListenBrainzSimilarUserActivity,
    MusicBrainzAnnotation,
    ListenBrainzArtistStats,
)
