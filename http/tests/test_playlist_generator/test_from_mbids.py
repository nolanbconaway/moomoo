import os
from pathlib import Path
from typing import Optional
from unittest.mock import patch
from uuid import uuid4

import pytest
from moomoo_http.app import create_app
from moomoo_http.db import db
from moomoo_http.playlist_generator import (
    FromMbidsPlaylistGenerator,
    NoFilesRequestedError,
    PlaylistTrack,
)
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


def test__files_for_recording_mbids():
    """Test that _files_for_recording_mbids works.

    This is the base function for all the other _files_for_* functions, so this is
    really the only place we need to test file grabbing. The rest can be tested with
    mocks to this function.
    """
    # populate {schema}.file_recording_map
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(filepath=f"test/{i}", recording_mbid=uuid4()) for i in range(10)]

    execute_sql(
        f"create table {schema}.file_recording_map (filepath text, recording_mbid uuid)"
    )
    execute_sql(
        f"""
        insert into {schema}.file_recording_map (filepath, recording_mbid)
        values (:filepath, :recording_mbid)
        """,
        records,
    )

    # test none requested
    assert (
        FromMbidsPlaylistGenerator._files_for_recording_mbids(
            mbids=[], session=db.session
        )
        == []
    )

    # test some matched
    mbids = [records[0]["recording_mbid"], records[1]["recording_mbid"]]
    assert FromMbidsPlaylistGenerator._files_for_recording_mbids(
        mbids=mbids, session=db.session
    ) == [Path(f"test/{0}"), Path(f"test/{1}")]

    # test none matched
    mbids = [uuid4()]
    assert (
        FromMbidsPlaylistGenerator._files_for_recording_mbids(
            mbids=mbids, session=db.session
        )
        == []
    )

    # test duplicate files don't get returned. add a duplicate mbid for the first file
    # and test what gets returned when we request both mbids leading to the file
    mbid = uuid4()
    execute_sql(
        f"""
        insert into {schema}.file_recording_map (filepath, recording_mbid)
        values (:filepath, :recording_mbid)
        """,
        dict(filepath=records[0]["filepath"], recording_mbid=mbid),
    )
    assert FromMbidsPlaylistGenerator._files_for_recording_mbids(
        mbids=[mbid, records[0]["recording_mbid"]], session=db.session
    ) == [Path(f"test/{0}")]


@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_recording_mbids"
)
def test__files_for_release_mbids(mock_files_for_recording_mbids):
    """Test that _files_for_release_mbids works."""
    # mock files_for_recording_mbids
    # no need to return anything. just testing this is called with the right params
    mock_files_for_recording_mbids.return_value = []

    # populate {schema}.recording_release_long
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(recording_mbid=uuid4(), release_mbid=uuid4()) for _ in range(10)]

    execute_sql(
        f"""
        create table {schema}.recording_release_long (
            recording_mbid uuid, release_mbid uuid
        )
        """
    )
    execute_sql(
        f"""
        insert into {schema}.recording_release_long (recording_mbid, release_mbid)
        values (:recording_mbid, :release_mbid)
        """,
        records,
    )

    # test none requested skips files_for_recording_mbids
    FromMbidsPlaylistGenerator._files_for_release_mbids(mbids=[], session=db.session)
    assert mock_files_for_recording_mbids.call_count == 0

    # test some matched
    release_mbids = [records[0]["release_mbid"], records[1]["release_mbid"]]
    recording_mbids = [records[0]["recording_mbid"], records[1]["recording_mbid"]]
    FromMbidsPlaylistGenerator._files_for_release_mbids(
        mbids=release_mbids, session=db.session
    )

    assert mock_files_for_recording_mbids.call_count == 1
    assert set(mock_files_for_recording_mbids.call_args[0][0]) == set(recording_mbids)

    # test none matched
    release_mbids = [uuid4()]
    FromMbidsPlaylistGenerator._files_for_release_mbids(
        mbids=release_mbids, session=db.session
    )

    assert mock_files_for_recording_mbids.call_args[0][0] == []


@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_recording_mbids"
)
def test__files_for_release_group_mbids(mock_files_for_recording_mbids):
    """Test that _files_for_release_group_mbids works."""
    # mock files_for_recording_mbids
    # no need to return anything. just testing this is called with the right params
    mock_files_for_recording_mbids.return_value = []

    # populate {schema}.recording_release_long
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [
        dict(recording_mbid=uuid4(), release_group_mbid=uuid4()) for _ in range(10)
    ]

    execute_sql(
        f"""
        create table {schema}.recording_release_long (
            recording_mbid uuid, release_group_mbid uuid
        )
        """
    )
    execute_sql(
        f"""
        insert into {schema}.recording_release_long (recording_mbid, release_group_mbid)
        values (:recording_mbid, :release_group_mbid)
        """,
        records,
    )

    # test none requested skips files_for_recording_mbids

    FromMbidsPlaylistGenerator._files_for_release_group_mbids(
        mbids=[], session=db.session
    )

    assert mock_files_for_recording_mbids.call_count == 0

    # test some matched
    release_group_mbids = [
        records[0]["release_group_mbid"],
        records[1]["release_group_mbid"],
    ]
    recording_mbids = [records[0]["recording_mbid"], records[1]["recording_mbid"]]

    FromMbidsPlaylistGenerator._files_for_release_group_mbids(
        mbids=release_group_mbids, session=db.session
    )

    assert mock_files_for_recording_mbids.call_count == 1
    assert set(mock_files_for_recording_mbids.call_args[0][0]) == set(recording_mbids)

    # test none matched
    release_group_mbids = [uuid4()]
    FromMbidsPlaylistGenerator._files_for_release_group_mbids(
        mbids=release_group_mbids, session=db.session
    )

    assert mock_files_for_recording_mbids.call_args[0][0] == []


@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_release_group_mbids"
)
def test__files_for_artist_mbids(mock_files_for_release_group_mbids):
    """Test that _files_for_artist_mbids works.

    This currently works by getting all the release group mbids for the artist, then
    getting the files for those release groups.
    """
    # mock files_for_release_group_mbids. no need to return anything.
    # just testing this is called with the right params
    mock_files_for_release_group_mbids.return_value = []

    # populate {schema}.release_artists_long
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(release_group_mbid=uuid4(), artist_mbid=uuid4()) for _ in range(10)]

    execute_sql(
        f"""
        create table {schema}.release_artists_long (
            release_group_mbid uuid, artist_mbid uuid
        )
        """
    )
    execute_sql(
        f"""
        insert into {schema}.release_artists_long (release_group_mbid, artist_mbid)
        values (:release_group_mbid, :artist_mbid)
        """,
        records,
    )

    # test none requested skips files_for_release_group_mbids
    FromMbidsPlaylistGenerator._files_for_artist_mbids(mbids=[], session=db.session)
    assert mock_files_for_release_group_mbids.call_count == 0

    # test some matched
    artist_mbids = [records[0]["artist_mbid"], records[1]["artist_mbid"]]
    release_group_mbids = [
        records[0]["release_group_mbid"],
        records[1]["release_group_mbid"],
    ]

    FromMbidsPlaylistGenerator._files_for_artist_mbids(
        mbids=artist_mbids, session=db.session
    )

    assert mock_files_for_release_group_mbids.call_count == 1
    assert set(mock_files_for_release_group_mbids.call_args[0][0]) == set(
        release_group_mbids
    )

    # test none matched
    artist_mbids = [uuid4()]
    FromMbidsPlaylistGenerator._files_for_artist_mbids(
        mbids=artist_mbids, session=db.session
    )
    assert mock_files_for_release_group_mbids.call_args[0][0] == []


def test_list_source_paths():
    raise NotImplementedError


@patch("moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.list_source_paths")
def test_get_playlist__no_files_error(mock_list_source_paths):
    """Test that get_playlist errors when no files are requested."""
    mock_list_source_paths.return_value = []
    with pytest.raises(NoFilesRequestedError):
        FromMbidsPlaylistGenerator(uuid4()).get_playlist(db.session)


@patch("moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.list_source_paths")
@patch("moomoo_http.playlist_generator.base.stream_similar_tracks")
def test_get_playlist(mock_stream_similar_tracks, mock_list_source_paths):
    """Test that get_playlist works."""
    # mock listed sources and stream
    mock_list_source_paths.return_value = [Path("test/0")]

    mock_stream_similar_tracks.return_value = [
        PlaylistTrack(
            filepath=Path(f"test/{i}"),
            artist_mbid=f"{i}",
            album_artist_mbid=f"{i}",
            distance=i,
        )
        for i in range(1, 100)
    ]

    pg = FromMbidsPlaylistGenerator(Path("test/0"))
    playlist, source_paths = pg.get_playlist(limit=2, shuffle=False, session=db.session)
    assert playlist == [Path("test/1"), Path("test/2")]
    assert source_paths == [Path("test/0")]

    # up the limit
    playlist, source_paths = pg.get_playlist(limit=4, shuffle=False, session=db.session)
    assert playlist == [Path("test/1"), Path("test/2"), Path("test/3"), Path("test/4")]
    assert source_paths == [Path("test/0")]

    # add a seed
    playlist, source_paths = pg.get_playlist(
        limit=2, shuffle=False, seed_count=1, session=db.session
    )
    assert playlist == [Path("test/0"), Path("test/1")]
    assert source_paths == [Path("test/0")]


def test_source_limit_handler():
    raise NotImplementedError
