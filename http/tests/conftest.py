"""Common fixtures for all tests."""

import psycopg
import pytest
from flask.testing import FlaskClient
from moomoo_http.app import create_app
from moomoo_http.db import db
from moomoo_playlist.ddl import BaseTable, PlaylistCollection, PlaylistCollectionItem


@pytest.fixture(autouse=True)
def override_env_variables(monkeypatch):
    """Override env variables for testing."""
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", "dbt")


@pytest.fixture(autouse=True)
def mock_db(monkeypatch, postgresql: psycopg.Connection):
    """Mock the internal db connection function to use the test db.

    Returns an endless supply of connections to the test db.
    """
    # convert the dsn into a sqlalchemy uri
    uri = "postgresql+psycopg://{}@{}:{}/{}".format(
        postgresql.info.user,
        postgresql.info.host,
        postgresql.info.port,
        postgresql.info.dbname,
    )
    monkeypatch.setenv("MOOMOO_POSTGRES_URI", uri)

    # make sure the test schema exists and the vector extension is loaded
    cur = postgresql.cursor()
    cur.execute("create schema if not exists dbt")
    cur.execute("create extension if not exists vector schema public")
    cur.execute("create extension if not exists vector schema dbt")

    # set utc timezone
    cur.execute(f"ALTER USER {postgresql.info.user} SET timezone='UTC'")

    postgresql.commit()

    return uri


@pytest.fixture
def http_app(mock_db) -> FlaskClient:
    """Create a test client for the http app."""
    app = create_app()
    return app.test_client()


@pytest.fixture(autouse=True)
def app_context(http_app):
    """Make sure the app context is created for each test."""
    with http_app.application.app_context():
        yield


@pytest.fixture(autouse=True)
def playlist_collection_tables(app_context):
    """Create the playlist collection tables."""
    BaseTable.metadata.create_all(
        bind=db.engine,
        tables=[PlaylistCollection.__table__, PlaylistCollectionItem.__table__],
    )
