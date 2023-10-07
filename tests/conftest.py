"""Common fixtures for all tests."""
import os
import time
from pathlib import Path

import musicbrainzngs
import psycopg
import pytest
from sqlalchemy import text

from moomoo.db import get_session

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    monkeypatch.setenv("MOOMOO_INGEST_SCHEMA", "test")
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", "dbt")
    monkeypatch.setenv("MOOMOO_ML_DEVICE", "cpu")
    monkeypatch.setenv("MOOMOO_CONTACT_EMAIL", "not-real")  # musicbrainzngs mocked


@pytest.fixture(autouse=True)
def disable_musicbrainzngs_calls(monkeypatch):
    def f(*_, **__):
        raise RuntimeError("musicbrainzngs called unexpectedly")

    monkeypatch.setattr(musicbrainzngs, "get_recording_by_id", f)
    monkeypatch.setattr(musicbrainzngs, "get_release_by_id", f)
    monkeypatch.setattr(musicbrainzngs, "get_artist_by_id", f)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


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
    cur.execute("create schema if not exists test")
    cur.execute("create schema if not exists dbt")
    cur.execute("create extension if not exists vector schema test")

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
    with get_session() as session:
        schema = os.environ["MOOMOO_DBT_SCHEMA"]

        sql = f"""
            create table {schema}.local_files_flat (
                filepath text primary key
                , embedding_success bool
                , embedding vector
                , artist_mbid uuid
                , embedding_duration_seconds int
            )
        """
        session.execute(text(sql))

        sql = f"""
            insert into {schema}.local_files_flat (
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
            session.execute(text(sql), i)
        session.commit()
