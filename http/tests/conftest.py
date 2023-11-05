"""Common fixtures for all tests."""
import os

import psycopg
import pytest
from moomoo_http.app import create_app
from moomoo_http.db import db
from sqlalchemy import text


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
    uri = "postgresql+psycopg://{0}@{1}:{2}/{3}".format(
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


def load_local_files_table(data: list[dict]):
    """Load a fresh version of the local files table.

    This is managed by dbt in prod, but needed for tests.

    Input rows are dicts with keys:

        - filepath: str
        - embedding_success: bool
        - embedding: list[float]
        - artist_mbid: uuid
        - embedding_duration_seconds: int
    """
    with create_app().app_context():
        schema = os.environ["MOOMOO_DBT_SCHEMA"]

        sql = f"""
            create table {schema}.local_files (
                filepath text primary key
                , embedding_success bool
                , embedding vector
                , artist_mbid uuid
                , embedding_duration_seconds int
            )
        """
        db.session.execute(text(sql))

        sql = f"""
            insert into {schema}.local_files (
                filepath
                , embedding_success
                , embedding
                , artist_mbid
                , embedding_duration_seconds
            )
            values (
                :filepath, true, :embedding , :artist_mbid, 90
            )
        """
        for i in data:
            db.session.execute(text(sql), i)
        db.session.commit()
