from pathlib import Path

import pytest
from moomoo_playlist.generator import NoFilesRequestedError, Track
from moomoo_playlist.generator import QueryPlaylistGenerator as Gen
from sqlalchemy.orm import Session


def test_fetch_filepaths(session: Session):
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

    assert Gen(sql=sql).fetch_filepaths(session=session) == [
        Path("a"),
        Path("b"),
        Path("c"),
    ]


def test_get_playlist(session: Session):
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

    plist = Gen(sql).get_playlist(session=session)
    assert plist.playlist == [
        Track(filepath=Path("a")),
        Track(filepath=Path("b")),
        Track(filepath=Path("c")),
    ]

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
        Gen(sql).get_playlist(session=session)
