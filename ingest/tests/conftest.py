"""Common fixtures for all tests."""

import os
import time
from pathlib import Path

import musicbrainzngs
import psycopg
import pytest
from moomoo_ingest.db import get_session
from sqlalchemy import text

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    """Remove relevant environment variables before each test."""
    monkeypatch.setenv("MOOMOO_CONTACT_EMAIL", "not-real")  # musicbrainzngs mocked
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", "dbt")
    monkeypatch.delenv("MOOMOO_MEDIA_LIBRARY", raising=False)


@pytest.fixture(autouse=True)
def disable_musicbrainzngs_calls(monkeypatch):
    """Disable all calls to musicbrainzngs."""

    def f(*_, **__):
        raise RuntimeError("musicbrainzngs called unexpectedly")

    monkeypatch.setattr(musicbrainzngs, "get_recording_by_id", f)
    monkeypatch.setattr(musicbrainzngs, "get_release_by_id", f)
    monkeypatch.setattr(musicbrainzngs, "get_release_group_by_id", f)
    monkeypatch.setattr(musicbrainzngs, "get_artist_by_id", f)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Disable all calls to time.sleep."""
    monkeypatch.setattr(time, "sleep", lambda x: None)


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
    cur.execute("create extension if not exists vector")

    # set utc timezone
    cur.execute(f"ALTER USER {postgresql.info.user} SET timezone='UTC'")

    postgresql.commit()

    return uri


def load_mbids_table(data: list[dict]):
    """Load the dbt schema mbids table with data.

    This is managed by dbt in prod, but needed for tests.

    Input rows are dicts with keys:

        - mbid: str
        - entity: str
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    with get_session() as session:
        sql = f"""create table if not exists {schema}.mbids (
            mbid uuid primary key, entity varchar
        )
        """
        session.execute(text(sql))

        sql = f"insert into {schema}.mbids (mbid, entity) values (:mbid, :entity)"
        for i in data:
            session.execute(text(sql), i)

        session.commit()
