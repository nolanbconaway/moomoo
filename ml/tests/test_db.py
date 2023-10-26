import psycopg

from moomoo_ml import db


def test_pg_connect_mocked(postgresql: psycopg.Connection):
    """Make sure the pg_connect function is mocked as expected.

    The postgresql fixture is provided by the pytest-postgresql plugin, and
    points to a fresh, temporary database.
    """
    with db.get_session() as session:
        engine = session.get_bind()
        assert engine.url.username == postgresql.info.user
        assert engine.url.host == postgresql.info.host
        assert engine.url.port == postgresql.info.port
        assert engine.url.database == postgresql.info.dbname
