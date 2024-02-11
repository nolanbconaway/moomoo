import os
import uuid
from math import sqrt
from pathlib import Path
from unittest.mock import patch

import pytest
from moomoo_playlist import (
    Track,
    fetch_user_listen_counts,
    get_most_similar_tracks,
    stream_similar_tracks,
)
from moomoo_playlist.db import db_retry
from psycopg.errors import UndefinedTable
from sqlalchemy import text
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


def test_fetch_user_listen_counts(session: Session):
    """Test that fetch_user_listen_counts works as expected."""
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    ddl = f"""
    create table {schema}.file_listen_counts (
        username text, filepath text, lifetime_listen_count int
    )
    """
    session.execute(text(ddl))

    sql = f"""   
    insert into {schema}.file_listen_counts (username, filepath, lifetime_listen_count)
    values (:username, :filepath, :lifetime_listen_count)
    """
    session.execute(
        text(sql),
        [
            dict(username="test", filepath=f"test/{i}", lifetime_listen_count=i)
            for i in range(3)
        ],
    )

    # no matching filepaths
    res = fetch_user_listen_counts(
        filepaths=[Path("a")], session=session, username="test"
    )
    assert res == {Path("a"): 0}

    # nothing to fetch
    res = fetch_user_listen_counts(filepaths=[], session=session, username="test")
    assert res == {}

    # correct values for matched and unmatched filepaths
    res = fetch_user_listen_counts(
        filepaths=[Path("test/0"), Path("test/1"), Path("test/10")],
        session=session,
        username="test",
    )
    assert res == {Path("test/0"): 0, Path("test/1"): 1, Path("test/10"): 0}


def test_stream_similar_tracks(session: Session):
    """Test that stream_similar_tracks works as expected."""
    rows = [dict(filepath=f"test/{i}", embedding=str([i] * 10)) for i in range(10)]
    load_local_files_table(data=rows)

    target = Path("test/0")

    res = list(stream_similar_tracks([target], session))
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 10)]

    res = list(stream_similar_tracks([target], session, limit=5))
    base_assert_list_playlist_track(*res)
    assert [i.filepath for i in res] == [Path(f"test/{i}") for i in range(1, 6)]


def test_stream_similar_tracks__weight_errors(session: Session):
    """Test that the weight validation works."""
    rows = [dict(filepath=f"test/{i}", embedding=str([i] * 10)) for i in range(10)]
    load_local_files_table(data=rows)

    target = Path("test/0")

    # wrong number of weights
    with pytest.raises(ValueError) as e:
        next(stream_similar_tracks([target], session, weights=[0.5, 0.5]))
    assert "weights must be the same length as filepaths" in str(e.value)

    # negative weight
    with pytest.raises(ValueError) as e:
        next(stream_similar_tracks([target], session, weights=[-1]))
    assert "Weights must be non-negative" in str(e.value)

    # zero weight total
    with pytest.raises(ValueError) as e:
        next(stream_similar_tracks([target], session, weights=[0]))
    assert "Weights must sum to a non-zero value" in str(e.value)


def test_stream_similar_tracks__weighted(session: Session):
    """Test the weighted math is correct."""
    rows = [
        dict(filepath="test/0", embedding=str([1, 0.5])),
        dict(filepath="test/1", embedding=str([1, 1])),
        dict(filepath="test/2", embedding=str([2, 2])),
        dict(filepath="test/3", embedding=str([2, 2.5])),
    ]
    load_local_files_table(data=rows)
    targets = [Path("test/1"), Path("test/2")]

    # with uniform weights, should have the same result as without weights
    res = list(stream_similar_tracks(targets, session, weights=[0.5, 0.5]))
    base_assert_list_playlist_track(*res)
    assert res == list(stream_similar_tracks(targets, session))

    # setting weights to 0 should remove the track from the results
    res = list(stream_similar_tracks(targets, session, weights=[0, 1]))
    base_assert_list_playlist_track(*res)
    assert res == list(stream_similar_tracks([targets[1]], session))

    # tracks more similar to more heavily weighted tracks should be first
    res = list(stream_similar_tracks(targets, session, weights=[0.5, 1]))
    base_assert_list_playlist_track(*res)
    assert res[0].filepath == Path("test/3")  # path 2 is weighted; 3 is closer to 2

    res = list(stream_similar_tracks(targets, session, weights=[1, 0.5]))
    base_assert_list_playlist_track(*res)
    assert res[0].filepath == Path("test/0")  # path 1 is weighted; 0 is closer to 1

    # do one with exact math
    res = list(stream_similar_tracks(targets, session, weights=[2, 1]))
    base_assert_list_playlist_track(*res)
    assert res[0].filepath == Path("test/0")
    # distance is l2 (euclidean). note that the weights are normalized
    expect = (sqrt((1 - 1) ** 2 + (0.5 - 1) ** 2) * 2 / 3) + (  # 0 -> 1
        sqrt((1 - 2) ** 2 + (0.5 - 2) ** 2) * 1 / 3  # 0 -> 2
    )
    assert pytest.approx(res[0].distance) == expect


def test_get_most_similar_tracks(session: Session):
    """Test that get_most_similar_tracks works as expected."""
    rows = [dict(filepath=f"test/{i}", embedding=str([i] * 10)) for i in range(10)]
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
