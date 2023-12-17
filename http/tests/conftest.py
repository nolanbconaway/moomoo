"""Common fixtures for all tests."""
import os
from copy import deepcopy
from uuid import uuid4

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


def load_local_files_table(data: list[dict]):
    """Load a fresh version of the local files table.

    This is managed by dbt in prod, but needed for tests.

    Input rows are dicts with keys:

        - filepath: str
        - embedding: list[float]
        - artist_mbid: uuid (optional)
        - album_artist_mbid: uuid (optional)
        - embedding_success: bool (optional)
        - embedding_duration_seconds: int (optional)
    """
    with create_app().app_context():
        schema = os.environ["MOOMOO_DBT_SCHEMA"]

        sql = f"""
            create table {schema}.local_files (
                filepath text primary key
                , embedding_success bool
                , embedding vector
                , recording_mbid uuid
                , artist_mbid uuid
                , album_artist_mbid uuid
                , embedding_duration_seconds int
            )
        """
        db.session.execute(text(sql))

        sql = f"""
            insert into {schema}.local_files (
                filepath
                , embedding_success
                , embedding
                , recording_mbid
                , artist_mbid
                , album_artist_mbid
                , embedding_duration_seconds
            )
            values (
                :filepath
                , true
                , :embedding
                , :recording_mbid
                , :artist_mbid
                , :album_artist_mbid
                , :embedding_duration_seconds
            )
        """
        for i in data:
            i = deepcopy(i)
            if "embedding_success" not in i:
                i["embedding_success"] = i["embedding"] is not None
            if "recording_mbid" not in i:
                i["recording_mbid"] = str(uuid4())
            if "artist_mbid" not in i:
                i["artist_mbid"] = str(uuid4())
            if "album_artist_mbid" not in i:
                i["album_artist_mbid"] = i["artist_mbid"]
            if "embedding_duration_seconds" not in i:
                i["embedding_duration_seconds"] = 90
            db.session.execute(text(sql), i)
        db.session.commit()
