"""Common fixtures for all tests."""

import time
from pathlib import Path

import musicbrainzngs
import psycopg
import pytest
from moomoo_pg import TABLES, get_engine

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    """Remove relevant environment variables before each test."""
    monkeypatch.setenv("MOOMOO_CONTACT_EMAIL", "not-real")  # musicbrainzngs mocked
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", "dbt")
    monkeypatch.delenv("MOOMOO_MEDIA_LIBRARY", raising=False)
    monkeypatch.setenv("LISTENBRAINZ_USER_TOKEN", "not-real")


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
def disable_listenbrainz_auth(monkeypatch):
    monkeypatch.setattr("liblistenbrainz.ListenBrainz.set_auth_token", lambda *_: None)


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
    uri = f"postgresql+psycopg://{postgresql.info.user}@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    monkeypatch.setenv("MOOMOO_POSTGRES_URI", uri)
    monkeypatch.setenv("MUSICBRAINZ_POSTGRES_URI", uri)

    # make sure the test schema exists and the vector extension is loaded
    cur = postgresql.cursor()
    cur.execute("create schema if not exists dbt")
    cur.execute("create extension if not exists vector")

    # set utc timezone
    cur.execute(f"ALTER USER {postgresql.info.user} SET timezone='UTC'")

    postgresql.commit()

    # create each table
    try:
        for table in TABLES:
            table.create()
        yield uri
    finally:
        get_engine().dispose()
