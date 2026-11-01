from uuid import uuid1

from moomoo_pg import get_session
from sqlalchemy import text

from moomoo_ingest.annotate_mbids import Mbid
from moomoo_ingest.musicbrainz.ddl import Artist, ReleaseStatus, copy_rows
from moomoo_ingest.musicbrainz.mbids import MbidCollection


def create_destination_table(session, schema: str, table: str):
    session.execute(text(f"CREATE SCHEMA {schema}"))
    session.execute(text(f"CREATE TABLE {schema}.{table} (LIKE musicbrainz.{table} INCLUDING ALL)"))
    session.commit()


def test_copy_rows():
    suffix = uuid1().hex
    src_table = f"copy_rows_source_{suffix}"
    dst_table = f"copy_rows_target_{suffix}"

    with get_session() as mbz_session, get_session() as app_session:
        app_session.execute(text(f"CREATE TABLE musicbrainz.{src_table} (id integer)"))
        app_session.execute(text(f"CREATE TABLE musicbrainz.{dst_table} (id integer)"))
        app_session.execute(
            text(f"INSERT INTO musicbrainz.{src_table} VALUES (:id)"),
            [{"id": 1}, {"id": 2}],
        )
        app_session.commit()

        copy_rows(
            mbz_session=mbz_session,
            app_session=app_session,
            select_sql=f"SELECT id FROM musicbrainz.{src_table} WHERE id >= %(min_id)s",
            params={"min_id": 2},
            target_table=dst_table,
        )
        app_session.commit()

        result = app_session.execute(text(f"SELECT id FROM musicbrainz.{dst_table}")).all()
        assert result == [(2,)]


def test_MusicBrainzBase__copy__full_table():
    dest_schema = f"copy_full_{uuid1().hex}"
    gid = uuid1()

    with get_session() as mbz_session, get_session() as app_session:
        create_destination_table(app_session, dest_schema, "release_status")
        mbz_session.execute(
            text("""
                INSERT INTO musicbrainz.release_status (id, gid, name, child_order)
                VALUES (:id, :gid, :name, :child_order)
            """),
            {"id": 1, "gid": gid, "name": "Official", "child_order": 1},
        )
        mbz_session.commit()

        ReleaseStatus.copy(
            mbz_session=mbz_session,
            app_session=app_session,
            mbids=MbidCollection([]),
            dest_schema=dest_schema,
        )
        app_session.commit()

        result = app_session.execute(
            text(f"SELECT id, gid, name, child_order FROM {dest_schema}.release_status")
        ).all()
        assert result == [(1, gid, "Official", 1)]


def test_MusicBrainzBase__copy__mbid_scoped_table():
    dest_schema = f"copy_scoped_{uuid1().hex}"
    gid = uuid1()

    with get_session() as mbz_session, get_session() as app_session:
        create_destination_table(app_session, dest_schema, "artist")
        mbz_session.execute(
            text("""
                INSERT INTO musicbrainz.artist (id, gid, name, sort_name, comment)
                VALUES (:id, :gid, :name, :sort_name, :comment)
            """),
            {"id": 1, "gid": gid, "name": "Artist", "sort_name": "Artist", "comment": ""},
        )
        mbz_session.commit()

        Artist.copy(
            mbz_session=mbz_session,
            app_session=app_session,
            mbids=MbidCollection([Mbid(mbid=gid, entity="artist")]),
            dest_schema=dest_schema,
        )
        app_session.commit()

        result = app_session.execute(text(f"SELECT id, gid, name FROM {dest_schema}.artist")).all()
        assert result == [(1, gid, "Artist")]
