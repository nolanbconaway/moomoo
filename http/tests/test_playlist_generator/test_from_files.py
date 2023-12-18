from pathlib import Path
from unittest.mock import patch

import pytest
from moomoo_http.app import create_app
from moomoo_http.db import db
from moomoo_http.playlist_generator import (
    FromFilesPlaylistGenerator,
    NoFilesRequestedError,
    PlaylistTrack,
)

from ..conftest import load_local_files_table


@pytest.fixture(autouse=True)
def app_context():
    """Make sure the app context is created for each test."""
    with create_app().app_context():
        yield


@pytest.mark.parametrize(
    "paths, expect",
    [
        ([], []),
        (["test4"], []),
        (["test1"], [Path("test1")]),
        (["test1", "test2"], [Path("test1"), Path("test2")]),
    ],
)
def test_list_source_paths__multi_paths(paths, expect):
    """Test that from_files lists the correct requested paths.

    Runs a db query to get the list of requested paths.
    """
    rows = [dict(filepath=p, embedding=None) for p in paths]
    load_local_files_table(rows)
    ps = FromFilesPlaylistGenerator(Path("test1"), Path("test2")).list_source_paths(
        db.session
    )
    assert set(ps) == set(expect)


@pytest.mark.parametrize(
    "paths, expect",
    [
        ([], []),
        (["not-test"], []),
        (["test"], [Path("test")]),
        (["test1", "test2"], [Path("test1"), Path("test2")]),
    ],
)
def test_list_source_paths__parent_path(paths, expect):
    rows = [dict(filepath=p, embedding=None) for p in paths]
    load_local_files_table(data=rows)
    ps = FromFilesPlaylistGenerator(Path("test")).list_source_paths(db.session)
    assert set(ps) == set(expect)


def test_get_playlist__no_files_error():
    """Test that get_playlist errors when no files are requested."""
    load_local_files_table(data=[])
    with pytest.raises(NoFilesRequestedError):
        FromFilesPlaylistGenerator(Path("not_real")).get_playlist(db.session)


@patch("moomoo_http.playlist_generator.FromFilesPlaylistGenerator.list_source_paths")
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

    pg = FromFilesPlaylistGenerator(Path("test/0"))
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
    """Test that the source path limit is enforced."""
    n = FromFilesPlaylistGenerator.limit_source_paths

    # if the user adds more than the limit, it should be randomly sampled on init
    pg = FromFilesPlaylistGenerator(*[Path(f"test/{i}") for i in range(n + 5)])
    assert len(pg.files) == n

    # if a parent path adds more than the limit, it should be randomly sampled on list
    rows = [dict(filepath=f"test/{i}", embedding=str([i] * 10)) for i in range(n * 2)]
    load_local_files_table(data=rows)

    pg = FromFilesPlaylistGenerator(Path("test"))
    assert len(pg.list_source_paths(session=db.session)) == n