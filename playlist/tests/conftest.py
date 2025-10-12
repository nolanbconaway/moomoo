"""Common fixtures for all tests."""

import os
from collections.abc import Generator
from copy import deepcopy
from uuid import uuid4

import psycopg
import pytest
import tenacity
from sqlalchemy import text
from sqlalchemy.orm import Session

from moomoo_playlist.db import get_session
from moomoo_playlist.ddl import BaseTable


@pytest.fixture(autouse=True)
def nosleep(monkeypatch):
    """Disable sleep in tenacity."""
    monkeypatch.setattr(tenacity.nap.time, "sleep", lambda *_: None)


@pytest.fixture(autouse=True)
def mock_db(monkeypatch, postgresql: psycopg.Connection) -> str:
    """Mock the internal db connection function to use the test db.

    Returns an endless supply of connections to the test db.
    """
    user, host, port, dbname = (
        postgresql.info.user,
        postgresql.info.host,
        postgresql.info.port,
        postgresql.info.dbname,
    )
    # convert the dsn into a sqlalchemy uri
    uri = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"
    monkeypatch.setenv("MOOMOO_POSTGRES_URI", uri)
    monkeypatch.setenv("MOOMOO_DBT_SCHEMA", "dbt")

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
def session(mock_db: str) -> Generator[Session, None, None]:
    """Return a fresh session to the test db."""
    # require mock_db to ensure it's set up
    assert mock_db
    with get_session() as session:
        yield session


@pytest.fixture(autouse=True)
def create_tables(session: Session):
    """Create the tables in the test db."""
    BaseTable.metadata.create_all(session.bind)

    # create the listenbrainz_collaborative_filtering_scores table
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        create table if not exists {schema}.listenbrainz_collaborative_filtering_scores (
            artist_mbid_a uuid not null
            , artist_mbid_b uuid not null
            , score_value float not null
        )
        """
    session.execute(text(sql))


def load_local_files_table(data: list[dict]):
    """Load a fresh version of the local files table.

    This is managed by dbt in prod, but needed for tests.

    Input rows are dicts with keys:

        - filepath: str
        - embedding: list[float]
        - recording_mbid: uuid (optional)
        - release_mbid: uuid (optional)
        - release_group_mbid: uuid (optional)
        - artist_mbid: uuid (optional)
        - album_artist_mbid: uuid (optional)
        - artist_name: str (optional)
        - album_artist_name: str (optional)
        - track_name: str (optional)
        - embedding_success: bool (optional)
        - embedding_duration_seconds: int (optional)
    """
    with get_session() as session:
        schema = os.environ["MOOMOO_DBT_SCHEMA"]

        sql = f"""
            create table {schema}.local_files (
                filepath text primary key
                , embedding_success bool
                , embedding vector
                , recording_mbid uuid
                , release_mbid uuid
                , release_group_mbid uuid
                , artist_mbid uuid
                , album_artist_mbid uuid
                , artist_name text
                , album_artist_name text
                , track_name text
                , embedding_duration_seconds int
            )
        """
        session.execute(text(sql))

        sql = f"""
            insert into {schema}.local_files (
                filepath
                , embedding_success
                , embedding
                , recording_mbid
                , release_mbid
                , release_group_mbid
                , artist_mbid
                , album_artist_mbid
                , artist_name
                , album_artist_name
                , track_name
                , embedding_duration_seconds
            )
            values (
                :filepath
                , :embedding_success
                , :embedding
                , :recording_mbid
                , :release_mbid
                , :release_group_mbid
                , :artist_mbid
                , :album_artist_mbid
                , :artist_name
                , :album_artist_name
                , :track_name
                , :embedding_duration_seconds
            )
        """
        for i in data:
            i = deepcopy(i)
            if "embedding_success" not in i:
                i["embedding_success"] = i["embedding"] is not None
            i["recording_mbid"] = i.get("recording_mbid", str(uuid4()))
            i["release_mbid"] = i.get("release_mbid", str(uuid4()))
            i["release_group_mbid"] = i.get("release_group_mbid", str(uuid4()))
            i["artist_mbid"] = i.get("artist_mbid", str(uuid4()))
            if "album_artist_mbid" not in i:
                i["album_artist_mbid"] = i["artist_mbid"]
            i["artist_name"] = i.get("artist_name", "artist_name")
            i["album_artist_name"] = i.get("album_artist_name", "album_artist_name")
            i["track_name"] = i.get("track_name", "track_name")
            i["embedding_duration_seconds"] = i.get("embedding_duration_seconds", 90)
            session.execute(text(sql), i)
        session.commit()


def load_listenbrainz_collaborative_filtering_scores(data: list[dict]):
    """Load a data info the listenbrainz_collaborative_filtering_scores table.

    The table is not recreated, just cleared.

    Input rows are dicts with keys:

        - artist_mbid_a: uuid
        - artist_mbid_b: uuid
        - score_value: float
    """
    with get_session() as session:
        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        session.execute(
            text(f"delete from {schema}.listenbrainz_collaborative_filtering_scores")
        )
        sql = f"""
            insert into {schema}.listenbrainz_collaborative_filtering_scores (
                artist_mbid_a, artist_mbid_b, score_value
            )
            values (
                :artist_mbid_a, :artist_mbid_b, :score_value
            )
        """
        for i in data:
            session.execute(text(sql), i)
        session.commit()
