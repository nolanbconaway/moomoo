import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from moomoo_playlist.generator import (
    Track,
    db_retry,
    get_most_similar_tracks,
    stream_similar_tracks,
)
from psycopg.errors import UndefinedTable
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from ..conftest import load_local_files_table


def base_assert_list_playlist_track(*tracks: Track):
    """Assert that a list of PlaylistTrack objects is valid."""
    assert all(isinstance(i.distance, float) for i in tracks)
    assert all(i.distance >= 0 for i in tracks)
    assert all(isinstance(i.filepath, Path) for i in tracks)
    assert all(isinstance(i.artist_mbid, uuid.UUID) for i in tracks)
    assert all(isinstance(i.album_artist_mbid, uuid.UUID) for i in tracks)


def test_db_retry():
    """Test that db_retry works as expected."""

    class Namespace:
        """Namespace for patching."""

        @staticmethod
        def f():
            return 1

    # not retried bc invalid exc type
    with patch.object(Namespace, "f") as mock_f, pytest.raises(RuntimeError):
        mock_f.side_effect = [RuntimeError]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 1

    # ProgrammingError but not UndefinedTable
    with patch.object(Namespace, "f") as mock_f:
        mock_f.side_effect = [
            ProgrammingError("test", {}, orig=RuntimeError),
            1,
        ]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 2

    # retried once and then succeeded
    with patch.object(Namespace, "f") as mock_f:
        mock_f.side_effect = [
            ProgrammingError("test", {}, orig=UndefinedTable("test")),
            1,
        ]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 2


def test_stream_similar_tracks(session: Session):
    """Test that stream_similar_tracks works as expected."""
    rows = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=uuid.uuid4())
        for i in range(10)
    ]
    load_local_files_table(data=rows)

    target = Path("test/0")

    res = list(stream_similar_tracks([target], session))
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 10)]

    res = list(stream_similar_tracks([target], session, limit=5))
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 6)]


def test_get_most_similar_tracks(session: Session):
    """Test that get_most_similar_tracks works as expected."""
    rows = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=uuid.uuid4())
        for i in range(10)
    ]
    load_local_files_table(data=rows)

    target = Path("test/0")
    res = get_most_similar_tracks([target], session)
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 10)]

    # limit
    res = get_most_similar_tracks([target], session, limit=5)
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 6)]


def test_get_most_similar_tracks__artist_limit(session: Session):
    """Test that the artist limit works."""
    artist_mbid = uuid.uuid1()
    rows = [
        dict(filepath=f"test/{i}", embedding=str([i] * 10), artist_mbid=artist_mbid)
        for i in range(10)
    ]
    load_local_files_table(data=rows)

    # should only get 2 songs, as they are from the same artist
    target = Path("test/0")
    results = get_most_similar_tracks([target], session, limit_per_artist=2, limit=5)
    base_assert_list_playlist_track(*results)
    assert [i.filepath for i in results] == [Path("test/1"), Path("test/2")]

    # should only get 5 songs total even though allow 6 per artist
    results = get_most_similar_tracks([target], session, limit_per_artist=6, limit=5)
    base_assert_list_playlist_track(*results)
    assert [i.filepath for i in results] == [
        Path("test/1"),
        Path("test/2"),
        Path("test/3"),
        Path("test/4"),
        Path("test/5"),
    ]


def test_get_most_similar_tracks__album_artist_limit(session: Session):
    """Test that the album artist limit works."""
    album_artist_mbid = uuid.uuid1()
    rows = [
        dict(
            filepath=f"test/{i}",
            embedding=str([i] * 10),
            artist_mbid=uuid.uuid1(),
            album_artist_mbid=album_artist_mbid,
        )
        for i in range(10)
    ]
    load_local_files_table(data=rows)

    # should only get 2 songs, as they are from the same artist
    target = Path("test/0")
    results = get_most_similar_tracks([target], session, limit_per_artist=2, limit=5)
    base_assert_list_playlist_track(*results)
    assert [i.filepath for i in results] == [Path("test/1"), Path("test/2")]

    # should only get 5 songs total even though allow 6 per artist
    results = get_most_similar_tracks([target], session, limit_per_artist=6, limit=5)
    base_assert_list_playlist_track(*results)
    assert [i.filepath for i in results] == [
        Path("test/1"),
        Path("test/2"),
        Path("test/3"),
        Path("test/4"),
        Path("test/5"),
    ]
