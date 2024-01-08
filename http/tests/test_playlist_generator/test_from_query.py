from pathlib import Path
from typing import Optional

import pytest
from moomoo_http.app import create_app
from moomoo_http.db import db
from moomoo_http.playlist_generator import NoFilesRequestedError, Track
from moomoo_http.playlist_generator import QueryPlaylistGenerator as Gen
from sqlalchemy import text


@pytest.fixture(autouse=True)
def app_context():
    """Make sure the app context is created for each test."""
    with create_app().app_context():
        yield


def execute_sql(sql: str, params: Optional[dict] = None):
    """Execute a SQL query. Wrap in text() to avoid SQL injection."""
    db.session.execute(text(sql), params=params)
    db.session.commit()


def test_fetch_filepaths():
    """Test that fetch_filepaths works."""
    sql = """
    select filepath
    from (
        select 'a' as filepath union
        select 'b' as filepath union
        select 'c' as filepath
    ) _
    order by filepath
    """

    assert Gen(sql=sql).fetch_filepaths(session=db.session) == [
        Path("a"),
        Path("b"),
        Path("c"),
    ]


def test_get_playlist():
    """Test that get_playlist works."""
    sql = """
    select filepath
    from (
        select 'a' as filepath union
        select 'b' as filepath union
        select 'c' as filepath
    ) _
    order by filepath
    """

    plist = Gen(sql).get_playlist(session=db.session)
    assert plist.playlist == [
        Track(filepath=Path("a")),
        Track(filepath=Path("b")),
        Track(filepath=Path("c")),
    ]
    assert plist.description is None

    # test none requested
    sql = """
    select filepath
    from (
        select 'a' as filepath union
        select 'b' as filepath union
        select 'c' as filepath
    ) _
    where 1 = 0
    order by filepath
    """
    with pytest.raises(NoFilesRequestedError):
        Gen(sql).get_playlist(session=db.session)
