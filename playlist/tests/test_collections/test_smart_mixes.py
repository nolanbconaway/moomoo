import os
from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner
from sqlalchemy import text
from sqlalchemy.orm import Session

from moomoo_playlist.collections.smart_mix import (
    Track,
    compute_track_distance_matrix,
    fetch_cf_similarity_matrix,
    fetch_tracks,
    make_clusters,
)
from moomoo_playlist.collections.smart_mix import main as smart_mix_main
from moomoo_playlist.config import CF_BASELINE
from moomoo_playlist.db import execute_sql_fetchall
from moomoo_playlist.generator import FromFilesPlaylistGenerator, NoFilesRequestedError
from moomoo_playlist.playlist import Playlist

from ..conftest import load_listenbrainz_collaborative_filtering_scores, load_local_files_table


def make_track(fpath: str, **kw) -> Track:
    return Track(
        filepath=fpath,
        track_name=kw.get("track_name", fpath),
        artist_name=kw.get("artist_name", fpath),
        artist_mbid=kw.get("artist_mbid", uuid4()),
        embedding=kw.get("embedding", np.random.uniform(0, 1, size=50)),
    )


def make_distance_matrix(n: int) -> np.ndarray:
    """Make a fake distance matrix for n tracks."""
    # make a random distance matrix
    rng = np.random.RandomState(0)
    mat = rng.uniform(size=(n, n))
    mat = (mat + mat.T) / 2  # make it symmetric
    np.fill_diagonal(mat, 0)  # set diagonal to 0
    mat = mat.astype(np.float16)
    return mat


def make_cf_matrix(tracks: list[Track]) -> pd.DataFrame:
    artist_mbids = list(dict.fromkeys([track.artist_mbid for track in tracks]))
    n = len(artist_mbids)
    mat = 1 - make_distance_matrix(n)
    return pd.DataFrame(mat, index=artist_mbids, columns=artist_mbids)


def test_fetch_tracks(session: Session):
    """Test fetch_tracks.

    This is the main query function for smart_mixes. Later we can mock this out.
    """
    # add tables: file_listen_counts, loved_tracks, local_files
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    session.execute(
        text(
            f"""
            create table {schema}.file_listen_counts (
                filepath text, username text, lifetime_listen_count int
            )
            """
        )
    )

    session.execute(text(f"create table {schema}.loved_tracks (filepath text, username text)"))
    load_local_files_table(
        [
            dict(filepath="a", embedding=[1.0, 2.0], album_artist_name=None),
            dict(filepath="b", embedding=[3.0, 4.0], embedding_success=False),
            dict(filepath="c", embedding=[5.0, 6.0], artist_mbid=None),
            dict(filepath="d", embedding=[7.0, 8.0]),
            dict(filepath="e", embedding=[9.0, 10.0]),
            dict(filepath="f", embedding=[11.0, 12.0]),
        ]
    )
    session.commit()

    # no tracks to fetch
    assert fetch_tracks("a", session) == []

    # add data to file_listen_counts
    session.execute(
        text(
            f"""
            insert into {schema}.file_listen_counts (
                filepath, username, lifetime_listen_count
            )
            values (:filepath, 'a', :lifetime_listen_count)
            """
        ),
        [
            dict(filepath="a", lifetime_listen_count=3),
            dict(filepath="b", lifetime_listen_count=4),
            dict(filepath="c", lifetime_listen_count=5),
            dict(filepath="d", lifetime_listen_count=1),
        ],
    )
    session.commit()

    # should fetch track a. b has no embedding, c no artist_mbid, d not enough listens
    res = fetch_tracks("a", session)
    assert len(res) == 1
    assert res[0].filepath.name == "a"
    assert res[0].embedding == [1, 2]
    assert res[0].artist_name == "artist_name"  # album_artist_name is coalesced out

    # add data to loved_tracks
    session.execute(
        text(
            f"""
            insert into {schema}.loved_tracks (filepath, username)
            values (:filepath, 'a')
            """
        ),
        [dict(filepath="e")],
    )
    session.commit()

    # should fetch track a and e
    res = fetch_tracks("a", session)
    assert len(res) == 2
    assert res[0].filepath.name == "a"
    assert res[1].filepath.name == "e"
    assert res[1].embedding == [9, 10]
    assert res[1].artist_name == "album_artist_name"


def test_fetch_cf_similarity_matrix(session: Session):
    """Test fetch_cf_similarity_matrix."""
    # load the listenbrainz_collaborative_filtering_scores table with columns
    # artist_mbid_a, artist_mbid_b, score_value
    mbid_a = uuid4()
    mbid_b = uuid4()

    data = [
        dict(artist_mbid_a=mbid_a, artist_mbid_b=mbid_a, score_value=1),
        dict(artist_mbid_a=mbid_b, artist_mbid_b=mbid_b, score_value=1),
        dict(artist_mbid_a=mbid_a, artist_mbid_b=mbid_b, score_value=0.5),
        dict(artist_mbid_a=mbid_b, artist_mbid_b=mbid_a, score_value=0.5),
    ]
    load_listenbrainz_collaborative_filtering_scores(data)

    # all tracks match the mbids in the table
    tracks = [
        make_track("a", artist_mbid=mbid_a),
        make_track("b", artist_mbid=mbid_b),
    ]
    res = fetch_cf_similarity_matrix(tracks, session)
    assert np.array_equal(res.values, np.array([[1.0, 0.5], [0.5, 1.0]]))

    # duplicate artist mbids. should be the same result
    tracks = [
        make_track("a", artist_mbid=mbid_a),
        make_track("b", artist_mbid=mbid_a),
        make_track("c", artist_mbid=mbid_b),
        make_track("d", artist_mbid=mbid_b),
    ]
    res = fetch_cf_similarity_matrix(tracks, session)
    assert np.array_equal(res.values, np.array([[1.0, 0.5], [0.5, 1.0]]))

    # no tracks
    assert fetch_cf_similarity_matrix([], session).empty

    # no matches
    tracks = [make_track("a"), make_track("b")]
    res = fetch_cf_similarity_matrix(tracks, session)
    assert np.array_equal(
        res.values, np.array([[1, CF_BASELINE], [CF_BASELINE, 1]], dtype=np.float16)
    )

    # two match, one no match.
    tracks = [
        make_track("a", artist_mbid=mbid_a),
        make_track("b", artist_mbid=mbid_b),
        make_track("c"),
    ]
    res = fetch_cf_similarity_matrix(tracks, session)
    assert np.array_equal(
        res.values,
        np.array(
            [
                [1, 0.5, CF_BASELINE],
                [0.5, 1, CF_BASELINE],
                [CF_BASELINE, CF_BASELINE, 1],
            ],
            dtype=np.float16,
        ),
    )


def test_compute_track_distance_matrix():
    """Test compute_track_distance_matrix."""
    #  no tracks
    assert compute_track_distance_matrix([], pd.DataFrame()) is None

    tracks = [make_track(str(i)) for i in range(5)]
    cf_matrix = make_cf_matrix(tracks)
    res = compute_track_distance_matrix(tracks, cf_matrix)

    # i don't have any other way to test this right now. just check shape
    assert res.shape == (5, 5)


def test_make_clusters__bad_args():
    """Test make_clusters with bad args."""
    #  distance matrix not square
    with pytest.raises(ValueError) as e:
        make_clusters(
            [],
            n_jobs=1,
            max_clusters=10,
            distance_matrix=np.array([[0, 1], [1, 0], [0, 1]]),
        )
    assert "Distance matrix must be square." in str(e.value)

    # distance matrix size does not match number of tracks
    with pytest.raises(ValueError) as e:
        make_clusters([], n_jobs=1, max_clusters=10, distance_matrix=np.array([[0, 1], [1, 0]]))
    assert "Distance matrix size must match number of tracks." in str(e.value)


def test_make_clusters__not_enough_data():
    """Test make_clusters with not enough data."""
    with pytest.raises(RuntimeError) as e:
        n = 49
        tracks = [make_track(str(i)) for i in range(n)]
        distance_matrix = make_distance_matrix(n)
        make_clusters(tracks, n_jobs=1, max_clusters=10, distance_matrix=distance_matrix)
    assert "Not enough tracks to cluster" in str(e.value)


def test_make_clusters__post_cluster_logic():
    """Test the cluster filtering logic in make_clusters.

    Includes:

        1. Select only clusters with > 2 artists. Error if none.
        2. Select top n clusters by track count.

    """
    patch_obj = "moomoo_playlist.collections.smart_mix._run_clusterer"

    # all -1 cluster
    with patch(patch_obj, return_value=np.array([-1] * 10)) as mock:
        tracks = [make_track(str(i)) for i in range(10)]
        distance_matrix = make_distance_matrix(len(tracks))
        with pytest.raises(RuntimeError) as e:
            make_clusters(tracks, n_jobs=1, max_clusters=10, distance_matrix=distance_matrix)
        mock.assert_called_once()
        assert "No clusters with > 2 artists." in str(e.value)

    # all the same artist mbid
    mbid = uuid4()
    with patch(patch_obj, return_value=np.array([0] * 10)) as mock:
        tracks = [make_track(str(i), artist_mbid=mbid) for i in range(10)]
        distance_matrix = make_distance_matrix(len(tracks))
        with pytest.raises(RuntimeError) as e:
            make_clusters(tracks, n_jobs=1, max_clusters=10, distance_matrix=distance_matrix)
        mock.assert_called_once()
        assert "No clusters with > 2 artists." in str(e.value)

    # one cluster with 3 distinct artists, one with 2
    with patch(patch_obj, return_value=np.array([0, 1, 0, 0, 1])) as mock:
        tracks = [make_track(str(i)) for i in range(5)]
        distance_matrix = make_distance_matrix(len(tracks))
        res = make_clusters(tracks, n_jobs=1, max_clusters=10, distance_matrix=distance_matrix)
        mock.assert_called_once()
        assert len(res) == 1
        assert len(res[0]) == 3
        assert set([t.filepath.name for t in res[0]]) == set(["0", "2", "3"])

    # select top 1 cluster
    with patch(patch_obj, return_value=np.array([0, 0, 0, 1, 1, 1, 1, 1, 2, 2])) as mock:
        tracks = [make_track(str(i)) for i in range(10)]
        distance_matrix = make_distance_matrix(len(tracks))
        res = make_clusters(tracks, n_jobs=1, max_clusters=1, distance_matrix=distance_matrix)
        mock.assert_called_once()
        assert len(res) == 1


@pytest.mark.parametrize("n_jobs", [1, 3])
def test_make_clusters__fake_data(n_jobs: int):
    """Test make clusters actually runs on fake data."""
    np.random.seed(0)

    # should run without error
    tracks = [make_track(str(i)) for i in range(1000)]
    distance_matrix = make_distance_matrix(len(tracks))
    make_clusters(tracks, n_jobs=n_jobs, max_clusters=10, distance_matrix=distance_matrix)


@patch("moomoo_playlist.collections.smart_mix.fetch_tracks", return_value=[])
@patch("moomoo_playlist.collections.smart_mix.make_clusters", return_value=[])
def test_main__no_results(patch_cluster, patch_fetch):
    """Test CLI with no results."""

    runner = CliRunner()
    res = runner.invoke(smart_mix_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "Not enough tracks (0) to generate smart mixes." in res.output

    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 0


@patch(
    "moomoo_playlist.collections.smart_mix.fetch_tracks",
    return_value=[make_track(str(i)) for i in range(1000)],
)
@patch(
    "moomoo_playlist.collections.smart_mix.make_clusters",
    return_value=[[make_track("a"), make_track("b")]] * 5,
)
def test_main__playlist_error(patch_cluster, patch_fetch):
    """Test CLI with a playlist error."""
    runner = CliRunner()
    playlist = Playlist(tracks=[])
    with patch.object(
        FromFilesPlaylistGenerator,
        "get_playlist",
        side_effect=[playlist, NoFilesRequestedError, playlist, playlist, playlist],
    ):
        res = runner.invoke(smart_mix_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "No files found for cluster" in res.output
    assert "NoFilesRequestedError" in res.output
    assert "Saved 4 playlist(s) to database." in res.output
    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 1


@patch(
    "moomoo_playlist.collections.smart_mix.make_clusters",
    return_value=[[make_track("a")]] * 3,
)
@patch.object(FromFilesPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[]))
def test_main__downsample(patch_get_playlist, patch_cluster):
    """Test the downsample logic in main."""
    runner = CliRunner()
    track_obj = "moomoo_playlist.collections.smart_mix.fetch_tracks"
    cf_obj = "moomoo_playlist.collections.smart_mix.fetch_cf_similarity_matrix"

    n = 4000
    tracks = [make_track(str(i)) for i in range(n)]
    cf_matrix = make_cf_matrix(tracks)
    with (
        patch(track_obj, return_value=tracks) as patch_fetch,
        patch(cf_obj, return_value=cf_matrix) as patch_cf,
    ):
        runner.invoke(smart_mix_main, ["test", "--count=3"])

    # patch_cluster should have been called with 2/3 of the tracks
    assert patch_fetch.call_count == 1
    assert patch_cf.call_count == 1
    assert patch_cluster.call_count == 1
    assert len(patch_cluster.call_args[1]["tracks"]) == n * 3 // 4

    n = 2001
    tracks = [make_track(str(i)) for i in range(n)]
    cf_matrix = make_cf_matrix(tracks)
    with (
        patch(track_obj, return_value=tracks) as patch_fetch,
        patch(cf_obj, return_value=cf_matrix) as patch_cf,
    ):
        runner.invoke(smart_mix_main, ["test", "--count=3"])

    # patch_cluster should have been called with 1000 tracks
    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 2
    assert len(patch_cluster.call_args[1]["tracks"]) == 2000


@patch(
    "moomoo_playlist.collections.smart_mix.fetch_tracks",
    return_value=[make_track(str(i)) for i in range(1000)],
)
@patch(
    "moomoo_playlist.collections.smart_mix.make_clusters",
    return_value=[[make_track("a"), make_track("b")]] * 5,
)
def test_main__stale_handler(patch_cluster, patch_fetch):
    """The stale handler should skip when the collection is not stale."""
    runner = CliRunner()

    with patch.object(
        FromFilesPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(smart_mix_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 5
    assert res.exit_code == 0
    assert "Saved 5 playlist(s) to database." in res.output

    # test stale handler
    with patch.object(
        FromFilesPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(smart_mix_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 0
    assert res.exit_code == 0
    assert "Collection is not stale; skipping." in res.output

    # test force flag
    with patch.object(
        FromFilesPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(smart_mix_main, ["test", "--count=5", "--force"])

    assert patch_get_playlist.call_count == 5
    assert res.exit_code == 0
    assert "Saved 5 playlist(s) to database." in res.output


@patch(
    "moomoo_playlist.collections.smart_mix.fetch_tracks",
    return_value=[make_track(str(i)) for i in range(1000)],
)
@patch(
    "moomoo_playlist.collections.smart_mix.make_clusters",
    return_value=[[make_track("a"), make_track("b")]] * 3,
)
def test_main__storage(patch_cluster, patch_fetch, session: Session):
    """Test CLI storage is replaced / correct."""
    runner = CliRunner()
    with patch.object(
        FromFilesPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for i in range(3)],
    ):
        res = runner.invoke(smart_mix_main, ["test", "--count=3"])

    assert res.exit_code == 0
    assert "Saved 3 playlist(s) to database." in res.output

    # get titles of playlists
    res = execute_sql_fetchall(
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Smart Mix 1", "Smart Mix 2", "Smart Mix 3"]

    # should replace with new playlists when run again
    with patch.object(
        FromFilesPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for _ in range(3)],
    ):
        res = runner.invoke(smart_mix_main, ["test", "--count=3"])

    assert res.exit_code == 0
    res = execute_sql_fetchall(
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Smart Mix 1", "Smart Mix 2", "Smart Mix 3"]
