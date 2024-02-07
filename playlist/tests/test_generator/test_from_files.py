from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from moomoo_playlist import FromFilesPlaylistGenerator, NoFilesRequestedError, Track
from sqlalchemy.orm import Session

from ..conftest import load_local_files_table


@pytest.mark.parametrize(
    "paths, expect",
    [
        ([], []),
        (["test4"], []),
        (["test1"], [Path("test1")]),
        (["test1", "test2"], [Path("test1"), Path("test2")]),
    ],
)
def test_list_source_paths__multi_paths(paths, expect, session: Session):
    """Test that from_files lists the correct requested paths.

    Runs a db query to get the list of requested paths.
    """
    rows = [dict(filepath=p, embedding=None) for p in paths]
    load_local_files_table(rows)
    ps = FromFilesPlaylistGenerator(Path("test1"), Path("test2")).list_source_paths(
        session
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
def test_list_source_paths__parent_path(paths, expect, session: Session):
    rows = [dict(filepath=p, embedding=None) for p in paths]
    load_local_files_table(data=rows)
    ps = FromFilesPlaylistGenerator(Path("test")).list_source_paths(session)
    assert set(ps) == set(expect)


def test_get_playlist__no_files_error(session: Session):
    """Test that get_playlist errors when no files are requested."""
    load_local_files_table(data=[])
    with pytest.raises(NoFilesRequestedError):
        FromFilesPlaylistGenerator(Path("not_real")).get_playlist(session)


@patch("moomoo_playlist.generator.FromFilesPlaylistGenerator.list_source_paths")
@patch("moomoo_playlist.generator.base.stream_similar_tracks")
def test_get_playlist(
    mock_stream_similar_tracks, mock_list_source_paths, session: Session
):
    """Test that get_playlist works."""
    # mock listed sources and stream
    mock_list_source_paths.return_value = [Path("test/0")]

    mock_stream_similar_tracks.return_value = [
        Track(
            filepath=Path(f"test/{i}"),
            artist_mbid=uuid4(),
            album_artist_mbid=uuid4(),
            distance=i,
        )
        for i in range(1, 100)
    ]

    pg = FromFilesPlaylistGenerator(Path("test/0"))
    playlist = pg.get_playlist(limit=2, shuffle=False, session=session)
    assert [i.filepath for i in playlist.tracks] == [Path("test/1"), Path("test/2")]

    # up the limit
    playlist = pg.get_playlist(limit=4, shuffle=False, session=session)
    assert [i.filepath for i in playlist.tracks] == [
        Path("test/1"),
        Path("test/2"),
        Path("test/3"),
        Path("test/4"),
    ]

    # add a seed
    playlist = pg.get_playlist(limit=2, shuffle=False, seed_count=1, session=session)
    assert [i.filepath for i in playlist.tracks] == [Path("test/0"), Path("test/1")]


def test_source_limit_handler(session: Session):
    """Test that the source path limit is enforced."""
    n = FromFilesPlaylistGenerator.limit_source_paths

    # if the user adds more than the limit, it should be randomly sampled on init
    pg = FromFilesPlaylistGenerator(*[Path(f"test/{i}") for i in range(n + 5)])
    assert len(pg.files) == n

    # if a parent path adds more than the limit, it should be randomly sampled on list
    rows = [dict(filepath=f"test/{i}", embedding=str([i] * 10)) for i in range(n * 2)]
    load_local_files_table(data=rows)

    pg = FromFilesPlaylistGenerator(Path("test"))
    assert len(pg.list_source_paths(session=session)) == n
