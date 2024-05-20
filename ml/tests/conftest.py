"""Common fixtures for all tests."""

from pathlib import Path

import psycopg
import pytest

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    monkeypatch.setenv("MOOMOO_ML_DEVICE", "cpu")


@pytest.fixture(autouse=True)
def mock_db(monkeypatch, postgresql: psycopg.Connection):
    """Mock the internal db connection function to use the test db.

    Returns an endless supply of connections to the test db.
    """
    # convert the dsn into a sqlalchemy uri
    user, host, port, dbname = (
        postgresql.info.user,
        postgresql.info.host,
        postgresql.info.port,
        postgresql.info.dbname,
    )
    uri = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"
    monkeypatch.setenv("MOOMOO_POSTGRES_URI", uri)

    # make sure the test schema exists and the vector extension is loaded
    cur = postgresql.cursor()
    cur.execute("create schema if not exists test")
    cur.execute("create extension if not exists vector schema test")
    cur.execute(f"alter database {postgresql.info.dbname} set search_path to test")

    # set utc timezone
    cur.execute(f"ALTER USER {postgresql.info.user} SET timezone='UTC'")

    postgresql.commit()

    return uri
