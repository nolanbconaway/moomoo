import os
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from moomoo.playlist.playlist_generator import NoFilesRequestedError, PlaylistGenerator

from ..conftest import load_local_files_table


def test_sql_errors():
    # no filepath
    with pytest.raises(ValueError):
        PlaylistGenerator("select * from table")

    # no select
    with pytest.raises(ValueError):
        PlaylistGenerator("filepath from table")

    # all good
    PlaylistGenerator("select filepath from table")


@pytest.mark.parametrize(
    "paths, expect",
    [
        ([], []),
        (["test4"], []),
        (["test1"], [Path("test1")]),
        (["test1", "test2"], [Path("test1"), Path("test2")]),
    ],
)
def test_from_files(paths, expect):
    rows = [
        dict(
            filepath=p,
            embedding_success=False,
            embedding=None,
            artist_mbid=None,
            embedding_duration_seconds=None,
        )
        for p in paths
    ]

    load_local_files_table(rows)
    ps = PlaylistGenerator.from_files(
        [Path("test1"), Path("test2")]
    ).list_requested_paths()

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
def test_from_parent_path(paths, expect):
    rows = [
        dict(
            filepath=p,
            embedding_success=False,
            embedding=None,
            artist_mbid=None,
            embedding_duration_seconds=None,
        )
        for p in paths
    ]

    load_local_files_table(data=rows)
    ps = PlaylistGenerator.from_parent_path(Path("test")).list_requested_paths()
    assert set(ps) == set(expect)


def test_get_playlist__no_files_error():
    """Test that get_playlist errors when no files are requested."""
    load_local_files_table(data=[])
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    with pytest.raises(NoFilesRequestedError):
        PlaylistGenerator(
            f"select filepath from {schema}.local_files_flat"
        ).get_playlist()


def test_get_playlist__artist_limit():
    """Test that the get_playlist artist limit works."""
    artist_mbid = uuid.uuid1()
    rows = [
        dict(
            filepath=f"test/{i}",
            embedding_success=True,
            embedding=str([i] * 10),
            artist_mbid=artist_mbid,
            embedding_duration_seconds=90,
        )
        for i in range(10)
    ]
    load_local_files_table(data=rows)
    pg = PlaylistGenerator.from_files([Path("test/5")])

    # should only get 2 songs from the same artist even though 5 songs are requested
    playlist = pg.get_playlist(limit=5, shuffle=False, limit_per_artist=2)
    assert playlist.playlist == [Path("test/4"), Path("test/6")]

    # should only get 4 songs not from the same artist even though 5 songs are requested
    playlist = pg.get_playlist(limit=5, shuffle=False, limit_per_artist=4)
    assert playlist.playlist == [
        Path("test/4"),
        Path("test/6"),
        Path("test/3"),
        Path("test/7"),
    ]


def test_from_files_pass_to_parent():
    """Test that we pass from from_files to from_parent_path if one path is provided."""
    with patch("moomoo.playlist.PlaylistGenerator.from_parent_path") as mock:
        PlaylistGenerator.from_files([Path("test/5")])
        assert mock.call_count == 1
        assert mock.call_args[0][0] == Path("test/5")


def test_get_playlist():
    """Test that get_playlist works."""
    rows = [
        dict(
            filepath=f"test/{i}",
            embedding_success=True,
            embedding=str([i] * 10),
            artist_mbid=uuid.uuid4(),
            embedding_duration_seconds=90,
        )
        for i in range(10)
    ]
    load_local_files_table(data=rows)
    pg = PlaylistGenerator.from_files([Path("test/5")])

    playlist = pg.get_playlist(limit=2, shuffle=False)
    assert playlist.playlist == [Path("test/4"), Path("test/6")]
    assert playlist.source_paths == [Path("test/5")]

    playlist = pg.get_playlist(limit=2, shuffle=False, seed_count=1)
    assert playlist.playlist == [Path("test/5"), Path("test/4"), Path("test/6")]
    assert playlist.source_paths == [Path("test/5")]

    # multiple files requested
    pg = PlaylistGenerator.from_files([Path("test/5"), Path("test/6")])
    assert pg.list_requested_paths() == [Path("test/5"), Path("test/6")]
    playlist = pg.get_playlist(limit=2, shuffle=False)
    assert playlist.playlist == [Path("test/4"), Path("test/7")]
    assert playlist.source_paths == [Path("test/5"), Path("test/6")]
