"""Common fixtures for all tests."""

import logging
import time
from collections.abc import Generator

import psycopg
import pytest
from sqlalchemy.orm import Session

from moomoo_pg.db import get_engine, get_session
from moomoo_pg.ddl import TABLES

# Suppress harmless AdminShutdown errors during test cleanup when pytest-postgresql
# tears down the test database while SQLAlchemy's pool is still finalizing connections
logging.getLogger("sqlalchemy.pool").setLevel(logging.CRITICAL)


@pytest.fixture(autouse=True)
def setup_db(monkeypatch, postgresql: psycopg.Connection) -> Generator[str, None, None]:
    """Mock the internal db connection function to use the test db.

    Returns an endless supply of connections to the test db.
    """
    # convert the dsn into a sqlalchemy uri
    uri = f"postgresql+psycopg://{postgresql.info.user}@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    monkeypatch.setenv("MOOMOO_POSTGRES_URI", uri)

    # make sure the test schema exists and the vector extension is loaded
    with postgresql.cursor() as cur:
        cur.execute("create schema if not exists dbt")
        cur.execute("create extension if not exists vector")

        # set utc timezone
        cur.execute(f"ALTER USER {postgresql.info.user} SET timezone='UTC'")

        postgresql.commit()

    # quick test that the connection is mocked correctly
    engine = get_engine()
    assert engine.url.username == postgresql.info.user
    assert engine.url.host == postgresql.info.host
    assert engine.url.port == postgresql.info.port
    assert engine.url.database == postgresql.info.dbname

    # create each table
    for table in TABLES:
        table.create()

    yield uri

    engine.dispose()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Disable all calls to time.sleep."""
    monkeypatch.setattr(time, "sleep", lambda x: None)


@pytest.fixture
def session(setup_db) -> Generator[Session, None, None]:
    # setup db as a dependency here, so that we are sure we have the right db connection.
    with get_session() as session:
        yield session
