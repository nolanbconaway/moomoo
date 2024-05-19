import os
from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pytest
from click.testing import CliRunner
from moomoo_playlist.collections.smart_mix import (
    DIMS,
    Track,
    cluster_avg_distance,
    fetch_tracks,
    make_clusters,
)
from moomoo_playlist.collections.smart_mix import main as smart_mix_main
from moomoo_playlist.db import execute_sql_fetchall
from moomoo_playlist.generator import FromFilesPlaylistGenerator, NoFilesRequestedError
from moomoo_playlist.playlist import Playlist
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..conftest import load_local_files_table


def make_track(fpath: str, **kw) -> Track:
    return Track(
        filepath=fpath,
        track_name=kw.get("track_name", fpath),
        artist_name=kw.get("artist_name", fpath),
        artist_mbid=kw.get("artist_mbid", uuid4()),
        embedding=kw.get("embedding", np.random.uniform(size=DIMS * 2)),
    )


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

    session.execute(
        text(f"create table {schema}.loved_tracks (filepath text, username text)")
    )
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


def test_cluster_avg_distance():
    tracks = [make_track(str(i), embedding=[i, i]) for i in range(2)]
    assert cluster_avg_distance(tracks) == 2**0.5


def test_make_clusters__not_enough_data():
    """Test make_clusters with no data."""
    with pytest.raises(RuntimeError) as e:
        make_clusters([], n_jobs=1, max_clusters=10)
    assert "Not enough tracks to cluster" in str(e.value)

    with pytest.raises(RuntimeError) as e:
        make_clusters([1] * DIMS, n_jobs=1, max_clusters=10)
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
        with pytest.raises(RuntimeError) as e:
            make_clusters(tracks, n_jobs=1, max_clusters=10)
        mock.assert_called_once()
        assert "No clusters with > 2 artists." in str(e.value)

    # all the same artist mbid
    mbid = uuid4()
    with patch(patch_obj, return_value=np.array([0] * 10)) as mock:
        tracks = [make_track(str(i), artist_mbid=mbid) for i in range(10)]
        with pytest.raises(RuntimeError) as e:
            make_clusters(tracks, n_jobs=1, max_clusters=10)
        mock.assert_called_once()
        assert "No clusters with > 2 artists." in str(e.value)

    # one cluster with 3 distinct artists, one with 2
    with patch(patch_obj, return_value=np.array([0, 1, 0, 0, 1])) as mock:
        tracks = [make_track(str(i)) for i in range(5)]
        res = make_clusters(tracks, n_jobs=1, max_clusters=10)
        mock.assert_called_once()
        assert len(res) == 1
        assert len(res[0]) == 3
        assert set([t.filepath.name for t in res[0]]) == set(["0", "2", "3"])

    # select top 1 cluster by track count
    with patch(
        patch_obj, return_value=np.array([0, 0, 0, 1, 1, 1, 1, 1, 2, 2])
    ) as mock:
        tracks = [make_track(str(i)) for i in range(10)]
        res = make_clusters(tracks, n_jobs=1, max_clusters=1)
        mock.assert_called_once()
        assert len(res) == 1
        assert len(res[0]) == 5
        assert [t.filepath.name for t in res[0]] == ["3", "4", "5", "6", "7"]


@pytest.mark.parametrize("n_jobs", [1, 3])
def test_make_clusters__fake_data(n_jobs: int):
    """Test make clusters actually runs on fake data."""
    np.random.seed(0)

    # should run without error
    tracks = [make_track(str(i)) for i in range(1000)]
    make_clusters(tracks, n_jobs=n_jobs, max_clusters=10)


@patch("moomoo_playlist.collections.smart_mix.fetch_tracks", return_value=[])
@patch("moomoo_playlist.collections.smart_mix.make_clusters", return_value=[])
def test_main__no_results(patch_cluster, patch_fetch):
    """Test CLI with no results."""

    runner = CliRunner()
    res = runner.invoke(smart_mix_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "No playlists generated" in res.output

    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 1


@patch("moomoo_playlist.collections.smart_mix.fetch_tracks", return_value=[])
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
@patch.object(
    FromFilesPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
)
def test_main__downsample(patch_get_playlist, patch_cluster):
    """Test the downsample logic in main."""
    runner = CliRunner()

    with patch(
        "moomoo_playlist.collections.smart_mix.fetch_tracks",
        return_value=[make_track(str(i)) for i in range(2000)],
    ) as patch_fetch:
        runner.invoke(smart_mix_main, ["test", "--count=3"])

    # patch_cluster should have been called with 2/3 of the tracks
    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 1
    assert len(patch_cluster.call_args[1]["tracks"]) == 2000 * 2 // 3

    with patch(
        "moomoo_playlist.collections.smart_mix.fetch_tracks",
        return_value=[make_track(str(i)) for i in range(1001)],
    ) as patch_fetch:
        runner.invoke(smart_mix_main, ["test", "--count=3"])

    # patch_cluster should have been called with 1000 tracks
    assert patch_fetch.call_count == 1
    assert patch_cluster.call_count == 2
    assert len(patch_cluster.call_args[1]["tracks"]) == 1000


@patch("moomoo_playlist.collections.smart_mix.fetch_tracks", return_value=[])
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


@patch("moomoo_playlist.collections.smart_mix.fetch_tracks", return_value=[])
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
