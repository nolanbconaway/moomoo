import psycopg

from moomoo_http.db import db


def test_pg_connect_mocked(postgresql: psycopg.Connection):
    """Make sure the pg_connect function is mocked as expected.

    The postgresql fixture is provided by the pytest-postgresql plugin, and
    points to a fresh, temporary database.
    """
    assert db.engine.url.username == postgresql.info.user
    assert db.engine.url.host == postgresql.info.host
    assert db.engine.url.port == postgresql.info.port
    assert db.engine.url.database == postgresql.info.dbname
