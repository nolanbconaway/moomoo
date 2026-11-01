from uuid import uuid1

from moomoo_pg import get_session
from sqlalchemy import text

from moomoo_ingest.musicbrainz.ddl import copy_rows


def test_copy_rows():
    suffix = uuid1().hex
    src_table = f"copy_rows_source_{suffix}"
    dst_table = f"copy_rows_target_{suffix}"

    with get_session() as mbz_session, get_session() as app_session:
        app_session.execute(text("CREATE SCHEMA IF NOT EXISTS musicbrainz"))
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
            dst_table=dst_table,
        )
        app_session.commit()

        result = app_session.execute(text(f"SELECT id FROM musicbrainz.{dst_table}")).all()
        assert result == [(2,)]
