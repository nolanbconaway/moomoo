"""DDL for replicant tables from the MusicBrainz database into the moomoo database.

Every table knows how to copy itself: `ids_to_copy` (override per table) says which
rows are in scope, and the inherited `copy()` on MusicBrainzBase actually runs it.
Subclasses only ever provide `ids_to_copy` and, where it's not "id", `copy_identifier_column`.
"""

import datetime
from typing import ClassVar
from uuid import UUID

from cachetools import FIFOCache, cached
from sqlalchemy import MetaData, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from .mbids import MbidCollection


def get_dbapi_connection(session: Session):
    return session.connection().connection.dbapi_connection


def copy_rows(
    mbz_session: Session,
    app_session: Session,
    select_sql: str,
    params: dict,
    target_table: str,
) -> None:
    mbz_conn = get_dbapi_connection(mbz_session)
    app_conn = get_dbapi_connection(app_session)

    with (
        mbz_conn.cursor() as src_cur,
        src_cur.copy(f"COPY ({select_sql}) TO STDOUT", params) as src_copy,
        app_conn.cursor() as dest_cur,
        dest_cur.copy(f"COPY musicbrainz.{target_table} FROM STDIN") as dest_copy,
    ):
        for chunk in src_copy:
            dest_copy.write(chunk)


class MusicBrainzBase(DeclarativeBase):
    metadata = MetaData(schema="musicbrainz")

    # name of the column used to identify rows for copying by id
    copy_identifier_column: ClassVar[str] = "id"

    # configuration to enable full table copy, skipping id resolution.
    is_full_copy: ClassVar[bool] = False

    @classmethod
    def column_names(cls) -> list[str]:
        return [column.name for column in cls.__table__.columns]

    @classmethod
    def primary_key_columns(cls) -> list[str]:
        return [column.name for column in cls.__table__.primary_key.columns]

    @classmethod
    def ids_to_copy(
        cls, mbz_session: Session, mbids: MbidCollection
    ) -> set[int] | set[UUID] | None:
        """Ids to scope this table's copy by. None means: copy the whole table."""
        return None

    @classmethod
    def _do_copy_with_filter(
        cls,
        mbz_session: Session,
        app_session: Session,
        dest_schema: str,
        where_sql: str,
        params: dict,
    ) -> None:
        mbz_conn = get_dbapi_connection(mbz_session)
        app_conn = get_dbapi_connection(app_session)
        columns_sql = ", ".join(cls.column_names())
        select_sql = f"SELECT {columns_sql} FROM musicbrainz.{cls.__tablename__} {where_sql}"

        # write from mbz to a staging table for this copy
        staging_table = f"staging_{cls.__tablename__}"
        pk_sql = ", ".join(cls.primary_key_columns())
        app_session.execute(
            text(
                f"""
                CREATE TEMP TABLE {staging_table} (LIKE {dest_schema}.{cls.__tablename__})
                ON COMMIT DROP
                """
            )
        )
        with (
            mbz_conn.cursor() as src_cur,
            src_cur.copy(f"COPY ({select_sql}) TO STDOUT", params) as src_copy,
            app_conn.cursor() as dest_cur,
            dest_cur.copy(f"COPY {staging_table} ({columns_sql}) FROM STDIN") as dest_copy,
        ):
            for chunk in src_copy:
                dest_copy.write(chunk)

        # insert from the staging table into the destination table
        app_session.execute(
            text(f"""
                INSERT INTO {dest_schema}.{cls.__tablename__} ({columns_sql})
                SELECT {columns_sql} FROM {staging_table}
                ON CONFLICT ({pk_sql}) DO NOTHING
            """)
        )

    @classmethod
    def copy(
        cls,
        mbz_session: Session,
        app_session: Session,
        mbids: MbidCollection,
        dest_schema: str,
    ) -> None:
        if cls.is_full_copy:
            where_sql, params = "", {}
        else:
            ids = cls.ids_to_copy(mbz_session, mbids)
            if not ids:
                return
            where_sql = f"WHERE {cls.copy_identifier_column} = ANY(%(ids)s)"
            params = {"ids": list(ids)}

        cls._do_copy_with_filter(
            mbz_session=mbz_session,
            app_session=app_session,
            dest_schema=dest_schema,
            where_sql=where_sql,
            params=params,
        )


class ArtistCredit(MusicBrainzBase):
    __tablename__ = "artist_credit"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    artist_count: Mapped[int]
    ref_count: Mapped[int | None]
    created: Mapped[datetime.datetime | None]

    @classmethod
    @cached(FIFOCache(maxsize=5), key=lambda cls, mbz_session, mbids: mbids.collection_hash)
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        artist_credit_ids: set[int] = set()

        # any credited artist from any release group
        release_group_ids = ReleaseGroup.ids_to_copy(mbz_session, mbids)
        if release_group_ids:
            rows = mbz_session.execute(
                text("SELECT artist_credit FROM musicbrainz.release_group WHERE id = ANY(:ids)"),
                {"ids": list(release_group_ids)},
            )
            artist_credit_ids |= {row.artist_credit for row in rows}

        # any credited artist from any release
        release_ids = Release.ids_to_copy(mbz_session, mbids)
        if release_ids:
            rows = mbz_session.execute(
                text("SELECT artist_credit FROM musicbrainz.release WHERE id = ANY(:ids)"),
                {"ids": list(release_ids)},
            )
            artist_credit_ids |= {row.artist_credit for row in rows}

        # any credited artist from any recording and its associated tracks
        recording_ids = Recording.ids_to_copy(mbz_session, mbids)
        if recording_ids:
            rows = mbz_session.execute(
                text("SELECT artist_credit FROM musicbrainz.recording WHERE id = ANY(:ids)"),
                {"ids": list(recording_ids)},
            )
            artist_credit_ids |= {row.artist_credit for row in rows}

            rows = mbz_session.execute(
                text("SELECT artist_credit FROM musicbrainz.track WHERE recording = ANY(:ids)"),
                {"ids": list(recording_ids)},
            )
            artist_credit_ids |= {row.artist_credit for row in rows}

        return artist_credit_ids


class ArtistCreditName(MusicBrainzBase):
    __tablename__ = "artist_credit_name"
    copy_identifier_column = "artist_credit"

    artist_credit: Mapped[int] = mapped_column(primary_key=True)
    position: Mapped[int] = mapped_column(primary_key=True)
    artist: Mapped[int]
    name: Mapped[str]
    join_phrase: Mapped[str]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        # same identifiers as ArtistCredit, just a different column name
        return ArtistCredit.ids_to_copy(mbz_session, mbids)


class Release(MusicBrainzBase):
    __tablename__ = "release"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    artist_credit: Mapped[int]
    release_group: Mapped[int]
    status: Mapped[int | None]
    packaging: Mapped[int | None]
    language: Mapped[int | None]
    script: Mapped[int | None]
    barcode: Mapped[str | None]
    comment: Mapped[str]
    quality: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    @cached(FIFOCache(maxsize=5), key=lambda cls, mbz_session, mbids: mbids.collection_hash)
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        release_mbids = mbids.by_entity["release"]
        release_ids: set[int] = set()

        # the ids for the releases directly identified by their MBIDs
        if release_mbids:
            rows = mbz_session.execute(
                text("SELECT id FROM musicbrainz.release WHERE gid = ANY(:gids)"),
                {"gids": release_mbids},
            )
            release_ids |= {row.id for row in rows}

        # any release for any artist
        artist_ids = Artist.ids_to_copy(mbz_session, mbids)
        if artist_ids:
            rows = mbz_session.execute(
                text("""
                    SELECT r.id
                    FROM musicbrainz.release r
                    JOIN musicbrainz.artist_credit_name acn ON acn.artist_credit = r.artist_credit
                    WHERE acn.artist = ANY(:artist_ids)
                """),
                {"artist_ids": list(artist_ids)},
            )
            release_ids |= {row.id for row in rows}

        # any release for any release group or any of the releases already identified
        release_group_mbids = mbids.by_entity["release-group"]
        rows = mbz_session.execute(
            text("""
                SELECT id FROM musicbrainz.release_group WHERE gid = ANY(:gids)
                UNION DISTINCT
                SELECT release_group FROM musicbrainz.release WHERE id = ANY(:release_ids)
            """),
            {"gids": release_group_mbids, "release_ids": list(release_ids)},
        )
        release_group_ids = {row.id for row in rows}

        if release_group_ids:
            rows = mbz_session.execute(
                text("SELECT id FROM musicbrainz.release WHERE release_group = ANY(:group_ids)"),
                {"group_ids": list(release_group_ids)},
            )
            release_ids |= {row.id for row in rows}

        return release_ids


class ReleaseGroup(MusicBrainzBase):
    __tablename__ = "release_group"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    artist_credit: Mapped[int]
    type: Mapped[int | None]
    comment: Mapped[str]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    @cached(FIFOCache(maxsize=5), key=lambda cls, mbz_session, mbids: mbids.collection_hash)
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        release_group_mbids = mbids.by_entity["release-group"]
        release_group_ids: set[int] = set()

        # the ids for the release groups directly identified by their MBIDs
        if release_group_mbids:
            rows = mbz_session.execute(
                text("SELECT id FROM musicbrainz.release_group WHERE gid = ANY(:gids)"),
                {"gids": release_group_mbids},
            )
            release_group_ids |= {row.id for row in rows}

        # any release group for any release
        release_ids = Release.ids_to_copy(mbz_session, mbids)
        if release_ids:
            rows = mbz_session.execute(
                text("SELECT release_group FROM musicbrainz.release WHERE id = ANY(:ids)"),
                {"ids": list(release_ids)},
            )
            release_group_ids |= {row.release_group for row in rows}

        return release_group_ids


class ReleaseGroupMeta(MusicBrainzBase):
    __tablename__ = "release_group_meta"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    release_count: Mapped[int]
    first_release_date_year: Mapped[int | None]
    first_release_date_month: Mapped[int | None]
    first_release_date_day: Mapped[int | None]
    rating: Mapped[int | None]
    rating_count: Mapped[int | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return ReleaseGroup.ids_to_copy(mbz_session, mbids)


class ReleaseStatus(MusicBrainzBase):
    __tablename__ = "release_status"
    is_full_copy: ClassVar[bool] = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    parent: Mapped[int | None]
    child_order: Mapped[int]
    description: Mapped[str | None]


class ReleaseFirstReleaseDate(MusicBrainzBase):
    __tablename__ = "release_first_release_date"
    copy_identifier_column = "release"

    release: Mapped[int] = mapped_column(primary_key=True)
    year: Mapped[int | None]
    month: Mapped[int | None]
    day: Mapped[int | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Release.ids_to_copy(mbz_session, mbids)


class ReleaseCountry(MusicBrainzBase):
    __tablename__ = "release_country"
    copy_identifier_column = "release"

    release: Mapped[int] = mapped_column(primary_key=True)
    country: Mapped[int] = mapped_column(primary_key=True)
    date_year: Mapped[int | None]
    date_month: Mapped[int | None]
    date_day: Mapped[int | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Release.ids_to_copy(mbz_session, mbids)


class ReleaseUnknownCountry(MusicBrainzBase):
    __tablename__ = "release_unknown_country"
    copy_identifier_column = "release"

    release: Mapped[int] = mapped_column(primary_key=True)
    date_year: Mapped[int | None]
    date_month: Mapped[int | None]
    date_day: Mapped[int | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Release.ids_to_copy(mbz_session, mbids)


class Artist(MusicBrainzBase):
    __tablename__ = "artist"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    sort_name: Mapped[str]
    begin_date_year: Mapped[int | None]
    begin_date_month: Mapped[int | None]
    begin_date_day: Mapped[int | None]
    end_date_year: Mapped[int | None]
    end_date_month: Mapped[int | None]
    end_date_day: Mapped[int | None]
    type: Mapped[int | None]
    area: Mapped[int | None]
    gender: Mapped[int | None]
    comment: Mapped[str]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    @cached(FIFOCache(maxsize=5), key=lambda cls, mbz_session, mbids: mbids.collection_hash)
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        artist_mbids = mbids.by_entity["artist"]
        if not artist_mbids:
            return set()

        # just any artist with the given MBIDs
        rows = mbz_session.execute(
            text("SELECT id FROM musicbrainz.artist WHERE gid = ANY(:gids)"),
            {"gids": artist_mbids},
        )
        return {row.id for row in rows}


class Recording(MusicBrainzBase):
    __tablename__ = "recording"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    artist_credit: Mapped[int]
    length: Mapped[int | None]
    comment: Mapped[str]
    last_updated: Mapped[datetime.datetime | None]
    video: Mapped[bool]

    @classmethod
    @cached(FIFOCache(maxsize=5), key=lambda cls, mbz_session, mbids: mbids.collection_hash)
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        recording_mbids = mbids.by_entity["recording"]
        if not recording_mbids:
            return set()

        # just any recording with the given MBIDs
        rows = mbz_session.execute(
            text("SELECT id FROM musicbrainz.recording WHERE gid = ANY(:gids)"),
            {"gids": recording_mbids},
        )
        return {row.id for row in rows}


class ArtistTag(MusicBrainzBase):
    __tablename__ = "artist_tag"
    copy_identifier_column = "artist"

    artist: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Artist.ids_to_copy(mbz_session, mbids)


class ReleaseTag(MusicBrainzBase):
    __tablename__ = "release_tag"
    copy_identifier_column = "release"

    release: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Release.ids_to_copy(mbz_session, mbids)


class RecordingTag(MusicBrainzBase):
    __tablename__ = "recording_tag"
    copy_identifier_column = "recording"

    recording: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Recording.ids_to_copy(mbz_session, mbids)


class ReleaseGroupTag(MusicBrainzBase):
    __tablename__ = "release_group_tag"
    copy_identifier_column = "release_group"

    release_group: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return ReleaseGroup.ids_to_copy(mbz_session, mbids)


class Tag(MusicBrainzBase):
    __tablename__ = "tag"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str]
    ref_count: Mapped[int]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        tag_ids: set[int] = set()

        # (tag join table, its entity column, the entity's ids for this batch)
        sources = [
            ("artist_tag", "artist", Artist.ids_to_copy(mbz_session, mbids)),
            ("release_tag", "release", Release.ids_to_copy(mbz_session, mbids)),
            ("recording_tag", "recording", Recording.ids_to_copy(mbz_session, mbids)),
            ("release_group_tag", "release_group", ReleaseGroup.ids_to_copy(mbz_session, mbids)),
        ]
        for table, column, entity_ids in sources:
            if not entity_ids:
                continue
            rows = mbz_session.execute(
                text(f"SELECT tag FROM musicbrainz.{table} WHERE {column} = ANY(:ids)"),
                {"ids": list(entity_ids)},
            )
            tag_ids |= {row.tag for row in rows}

        return tag_ids


class Medium(MusicBrainzBase):
    __tablename__ = "medium"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    release: Mapped[int]
    position: Mapped[int]
    format: Mapped[int | None]
    name: Mapped[str]
    track_count: Mapped[int]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        recording_ids = Recording.ids_to_copy(mbz_session, mbids)
        if not recording_ids:
            return set()
        rows = mbz_session.execute(
            text("SELECT medium FROM musicbrainz.track WHERE recording = ANY(:ids)"),
            {"ids": list(recording_ids)},
        )
        return {row.medium for row in rows}


class Track(MusicBrainzBase):
    __tablename__ = "track"
    copy_identifier_column = "recording"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    recording: Mapped[int]
    medium: Mapped[int]
    position: Mapped[int]
    number: Mapped[str]
    name: Mapped[str]
    artist_credit: Mapped[int]
    length: Mapped[int | None]
    is_data_track: Mapped[bool]
    last_updated: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return Recording.ids_to_copy(mbz_session, mbids)


class ArtistGidRedirect(MusicBrainzBase):
    __tablename__ = "artist_gid_redirect"

    is_full_copy: ClassVar[bool] = True

    gid: Mapped[UUID] = mapped_column(primary_key=True)
    new_id: Mapped[int]
    created: Mapped[datetime.datetime | None]


class ReleaseGidRedirect(MusicBrainzBase):
    __tablename__ = "release_gid_redirect"
    is_full_copy: ClassVar[bool] = True

    gid: Mapped[UUID] = mapped_column(primary_key=True)
    new_id: Mapped[int]
    created: Mapped[datetime.datetime | None]


class ReleaseGroupGidRedirect(MusicBrainzBase):
    __tablename__ = "release_group_gid_redirect"
    is_full_copy: ClassVar[bool] = True

    gid: Mapped[UUID] = mapped_column(primary_key=True)
    new_id: Mapped[int]
    created: Mapped[datetime.datetime | None]


class RecordingGidRedirect(MusicBrainzBase):
    __tablename__ = "recording_gid_redirect"
    copy_identifier_column = "gid"

    gid: Mapped[UUID] = mapped_column(primary_key=True)
    new_id: Mapped[int]
    created: Mapped[datetime.datetime | None]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[UUID]:
        # filtered by the old gid itself, so only the redirects for library mbids are copied
        return set(mbids.by_entity["recording"])


class ReleaseGroupPrimaryType(MusicBrainzBase):
    __tablename__ = "release_group_primary_type"

    is_full_copy: ClassVar[bool] = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    parent: Mapped[int | None]
    child_order: Mapped[int]
    description: Mapped[str | None]


class ReleaseGroupSecondaryType(MusicBrainzBase):
    __tablename__ = "release_group_secondary_type"

    is_full_copy: ClassVar[bool] = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    gid: Mapped[UUID]
    name: Mapped[str]
    parent: Mapped[int | None]
    child_order: Mapped[int]
    description: Mapped[str | None]


class ReleaseGroupSecondaryTypeJoin(MusicBrainzBase):
    __tablename__ = "release_group_secondary_type_join"
    copy_identifier_column = "release_group"

    release_group: Mapped[int] = mapped_column(primary_key=True)
    secondary_type: Mapped[int] = mapped_column(primary_key=True)
    created: Mapped[datetime.datetime]

    @classmethod
    def ids_to_copy(cls, mbz_session: Session, mbids: MbidCollection) -> set[int]:
        return ReleaseGroup.ids_to_copy(mbz_session, mbids)


class ArtistType(MusicBrainzBase):
    __tablename__ = "artist_type"
    is_full_copy: ClassVar[bool] = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str]
    parent: Mapped[int | None]
    child_order: Mapped[int]
    description: Mapped[str | None]
    gid: Mapped[UUID]
