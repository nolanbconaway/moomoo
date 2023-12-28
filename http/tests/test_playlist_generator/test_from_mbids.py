import os
from pathlib import Path
from typing import Optional
from unittest.mock import patch
from uuid import uuid4

import pytest
from moomoo_http.app import create_app
from moomoo_http.db import db

# use an alias to limit so many multi-line statements
from moomoo_http.playlist_generator import FromMbidsPlaylistGenerator as Gen
from moomoo_http.playlist_generator import NoFilesRequestedError, Track
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
    """Test that _files_for_recording_mbids works."""
    # populate {schema}.map__file_recording
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(filepath=f"test/{i}", recording_mbid=uuid4()) for i in range(10)]

    execute_sql(
        f"""
        create table {schema}.map__file_recording (
            filepath text, recording_mbid uuid
        )
        """
    )
    execute_sql(
        f"""
        insert into {schema}.map__file_recording (filepath, recording_mbid)
        values (:filepath, :recording_mbid)
        """,
        records,
    )

    # test none requested
    assert Gen._files_for_recording_mbids(mbids=[], session=db.session) == []

    # test some matched
    mbids = [records[0]["recording_mbid"], records[1]["recording_mbid"]]
    assert Gen._files_for_recording_mbids(mbids=mbids, session=db.session) == [
        Path(f"test/{0}"),
        Path(f"test/{1}"),
    ]

    # test none matched
    mbids = [uuid4()]
    assert Gen._files_for_recording_mbids(mbids=mbids, session=db.session) == []


def test__files_for_release_mbids():
    """Test that _files_for_release_mbids works."""
    # populate {schema}.map__file_release
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(filepath=f"test/{i}", release_mbid=uuid4()) for i in range(10)]

    execute_sql(
        f"create table {schema}.map__file_release (filepath text, release_mbid uuid)"
    )
    execute_sql(
        f"""
        insert into {schema}.map__file_release (filepath, release_mbid)
        values (:filepath, :release_mbid)
        """,
        records,
    )

    # test none requested
    assert Gen._files_for_release_mbids(mbids=[], session=db.session) == []

    # test some matched
    mbids = [records[0]["release_mbid"], records[1]["release_mbid"]]
    assert Gen._files_for_release_mbids(mbids=mbids, session=db.session) == [
        Path(f"test/{0}"),
        Path(f"test/{1}"),
    ]

    # test none matched
    mbids = [uuid4()]
    assert Gen._files_for_release_mbids(mbids=mbids, session=db.session) == []


def test__files_for_release_group_mbids():
    """Test that _files_for_release_group_mbids works."""
    # populate {schema}.map__file_release_group
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [
        dict(filepath=f"test/{i}", release_group_mbid=uuid4()) for i in range(10)
    ]

    execute_sql(
        f"""
        create table {schema}.map__file_release_group (
            filepath text, release_group_mbid uuid
        )
        """
    )
    execute_sql(
        f"""
        insert into {schema}.map__file_release_group (filepath, release_group_mbid)
        values (:filepath, :release_group_mbid)
        """,
        records,
    )

    # test none requested
    assert Gen._files_for_release_group_mbids(mbids=[], session=db.session) == []

    # test some matched
    mbids = [records[0]["release_group_mbid"], records[1]["release_group_mbid"]]
    assert Gen._files_for_release_group_mbids(mbids=mbids, session=db.session) == [
        Path(f"test/{0}"),
        Path(f"test/{1}"),
    ]

    # test none matched
    mbids = [uuid4()]
    assert Gen._files_for_release_group_mbids(mbids=mbids, session=db.session) == []


def test__files_for_artist_mbids():
    """Test that _files_for_artist_mbids works."""
    # populate {schema}.map__file_artist
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    records = [dict(filepath=f"test/{i}", artist_mbid=uuid4()) for i in range(10)]

    execute_sql(
        f"create table {schema}.map__file_artist (filepath text, artist_mbid uuid)"
    )
    execute_sql(
        f"""
        insert into {schema}.map__file_artist (filepath, artist_mbid)
        values (:filepath, :artist_mbid)
        """,
        records,
    )

    # test none requested
    assert Gen._files_for_artist_mbids(mbids=[], session=db.session) == []

    # test some matched
    mbids = [records[0]["artist_mbid"], records[1]["artist_mbid"]]
    assert Gen._files_for_artist_mbids(mbids=mbids, session=db.session) == [
        Path(f"test/{0}"),
        Path(f"test/{1}"),
    ]

    # test none matched
    mbids = [uuid4()]
    assert Gen._files_for_artist_mbids(mbids=mbids, session=db.session) == []


@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_recording_mbids"
)
@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_release_mbids"
)
@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_release_group_mbids"
)
@patch(
    "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator._files_for_artist_mbids"
)
def test_list_source_paths(
    patch_artist, patch_release_group, patch_release, patch_recording
):
    """Test that list_source_paths works."""

    # reset mocks to default
    def reset_mocks():
        patch_artist.reset_mock()
        patch_release_group.reset_mock()
        patch_release.reset_mock()
        patch_recording.reset_mock()

        patch_artist.return_value = []
        patch_release_group.return_value = []
        patch_release.return_value = []
        patch_recording.return_value = []

    reset_mocks()

    # make an mbids table
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    execute_sql(f"create table {schema}.mbids (mbid uuid, entity varchar)")

    # test with mbids that dont map to entities
    generator = Gen(uuid4())
    assert generator.list_source_paths(session=db.session) == []
    assert patch_recording.call_count == 0
    assert patch_release.call_count == 0
    assert patch_release_group.call_count == 0
    assert patch_artist.call_count == 0

    # test mbids with entities but no files
    mbids = [uuid4()]
    generator = Gen(*mbids)
    execute_sql(
        f"insert into {schema}.mbids (mbid, entity) values (:mbid, 'recording')",
        dict(mbid=mbids[0]),
    )
    assert generator.list_source_paths(session=db.session) == []
    assert patch_recording.call_count == 1
    assert patch_recording.call_args[0][0] == mbids
    assert patch_release.call_count == 1
    assert patch_release.call_args[0][0] == []
    assert patch_release_group.call_count == 1
    assert patch_release_group.call_args[0][0] == []
    assert patch_artist.call_count == 1
    assert patch_artist.call_args[0][0] == []
    reset_mocks()

    # test dedupe
    patch_recording.return_value = patch_recording.return_value = [Path("test/0")]
    assert generator.list_source_paths(session=db.session) == [Path("test/0")]
    reset_mocks()

    # test limit handler
    n = generator.limit_source_paths
    patch_recording.return_value = patch_recording.return_value = [
        Path(f"test/{i}") for i in range(n * 2)
    ]
    assert len(generator.list_source_paths(session=db.session)) == n


@patch("moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.list_source_paths")
def test_get_playlist__no_files_error(mock_list_source_paths):
    """Test that get_playlist errors when no files are requested."""
    mock_list_source_paths.return_value = []
    with pytest.raises(NoFilesRequestedError):
        Gen(uuid4()).get_playlist(db.session)


@patch("moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.list_source_paths")
@patch("moomoo_http.playlist_generator.base.stream_similar_tracks")
def test_get_playlist(mock_stream_similar_tracks, mock_list_source_paths):
    """Test that get_playlist works."""
    # mock listed sources and stream
    mock_list_source_paths.return_value = [Path("test/0")]

    mock_stream_similar_tracks.return_value = [
        Track(
            filepath=Path(f"test/{i}"),
            artist_mbid=f"{i}",
            album_artist_mbid=f"{i}",
            distance=i,
        )
        for i in range(1, 100)
    ]

    pg = Gen(Path("test/0"))
    playlist = pg.get_playlist(limit=2, shuffle=False, session=db.session)
    assert [i.filepath for i in playlist.playlist] == [Path("test/1"), Path("test/2")]

    # up the limit
    playlist = pg.get_playlist(limit=4, shuffle=False, session=db.session)
    assert [i.filepath for i in playlist.playlist] == [
        Path("test/1"),
        Path("test/2"),
        Path("test/3"),
        Path("test/4"),
    ]

    # add a seed
    playlist = pg.get_playlist(limit=2, shuffle=False, seed_count=1, session=db.session)
    assert [i.filepath for i in playlist.playlist] == [Path("test/0"), Path("test/1")]
