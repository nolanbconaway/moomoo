"""Common fixtures for all tests."""
import time
from pathlib import Path

import musicbrainzngs
import psycopg
import pytest

import moomoo.utils_

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "dbname=fake user=fake password=fake host=fake")
    monkeypatch.setenv("MOOMOO_ML_DEVICE", "cpu")
    monkeypatch.setenv("CONTACT_EMAIL", "not-real")  # musicbrainzngs mocked


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

    def f(*_, **__):
        conn = psycopg.connect(postgresql.info.dsn)
        with conn.cursor() as cursor:
            cursor.execute("create extension if not exists vector")
            cursor.execute("create schema if not exists test")

        conn.commit()
        return conn

    monkeypatch.setattr(moomoo.utils_, "_pg_connect", f)
