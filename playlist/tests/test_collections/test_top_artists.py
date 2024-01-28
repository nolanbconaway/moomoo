import os
from unittest.mock import patch
from uuid import uuid4

import pytest
from click.testing import CliRunner
from moomoo_playlist.collections.top_artists import list_top_artists
from moomoo_playlist.collections.top_artists import main as top_artists_main
from moomoo_playlist.db import execute_sql_fetchall
from moomoo_playlist.generator import (
    FromMbidsPlaylistGenerator,
    NoFilesRequestedError,
)
from moomoo_playlist.playlist import Playlist
from sqlalchemy import text
from sqlalchemy.orm import Session


def populate_artist_listen_counts(session: Session, data: list[dict]):
    """Make the artist_listen_counts table.

    data should be a list of dicts with the following keys:
        artist_mbid: uuid or str (optional)
        artist_name: str
        username: str
        lifetime_listen_count int
        last30_listen_count: int (optional)
        last60_listen_count: int (optional)
        last90_listen_count: int (optional)
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        create table {schema}.artist_listen_counts (
            artist_mbid uuid,
            artist_name text,
            username text,
            lifetime_listen_count int,
            last30_listen_count int,
            last60_listen_count int,
            last90_listen_count int
        )
    """
    session.execute(text(sql))

    for row in data:
        row["artist_mbid"] = row.get("artist_mbid", uuid4())
        n = row["lifetime_listen_count"]
        row["last30_listen_count"] = row.get("last30_listen_count", n)
        row["last60_listen_count"] = row.get("last60_listen_count", n)
        row["last90_listen_count"] = row.get("last90_listen_count", n)
        sql = f"""
            insert into {schema}.artist_listen_counts
            (
                artist_mbid, artist_name, username, lifetime_listen_count,
                last30_listen_count, last60_listen_count, last90_listen_count
            )
            values (
                :artist_mbid, :artist_name, :username, :lifetime_listen_count,
                :last30_listen_count, :last60_listen_count, :last90_listen_count
            )
        """
        session.execute(text(sql), row)

    session.commit()


def test_list_top_artists__count(session: Session):
    """Test handling of the count parameter."""
    populate_artist_listen_counts(
        session,
        [
            dict(
                artist_name=f"test_{i}", username="test", lifetime_listen_count=i + 100
            )
            for i in range(10)
        ],
    )
    res = list_top_artists(
        username="test", history_length="lifetime", count=5, session=session
    )
    assert len(res) == 5
    assert [i.name for i in res] == ["test_9", "test_8", "test_7", "test_6", "test_5"]


def test_list_top_artists__history_length(session: Session):
    """Test handling of the history_length parameter."""
    populate_artist_listen_counts(
        session,
        [
            dict(
                artist_name=f"test_{i}",
                username="test",
                lifetime_listen_count=i + 100,
                last90_listen_count=1000 - i,
            )
            for i in range(10)
        ],
    )
    # should have sorted by last90_listen_count, which is opposite of lifetime
    res = list_top_artists(
        username="test", history_length="90", count=5, session=session
    )
    assert len(res) == 5
    assert [i.name for i in res] == ["test_0", "test_1", "test_2", "test_3", "test_4"]

    # test invalid history length
    with pytest.raises(ValueError):
        list_top_artists(
            username="test", history_length="invalid", count=5, session=session
        )


def test_list_top_artists__no_results(session: Session):
    """Test handling of no results."""
    populate_artist_listen_counts(session, [])
    res = list_top_artists(
        username="test", history_length="lifetime", count=5, session=session
    )
    assert len(res) == 0


def test_main__no_results(session: Session):
    """Test CLI with no results."""
    populate_artist_listen_counts(session, [])
    runner = CliRunner()
    res = runner.invoke(top_artists_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "No playlists generated" in res.output


def test_main__playlist_error(session: Session):
    """Test CLI with a playlist error."""
    populate_artist_listen_counts(
        session,
        [
            dict(
                artist_name=f"test_{i}", username="test", lifetime_listen_count=i + 100
            )
            for i in range(10)
        ],
    )
    runner = CliRunner()
    plist = Playlist(tracks=[])
    with patch.object(FromMbidsPlaylistGenerator, "get_playlist") as mock:
        mock.side_effect = [plist, NoFilesRequestedError, plist, plist, plist]
        res = runner.invoke(top_artists_main, ["test", "--count=5"])
    assert "No files found" in res.output
    assert "NoFilesRequestedError" in res.output
    assert "Saving 4 playlists to database." in res.output


def test_main__storage(session: Session):
    """Test CLI storage is replaced / correct."""
    populate_artist_listen_counts(
        session,
        [
            dict(
                artist_name=f"test_{i}", username="test", lifetime_listen_count=1000 - i
            )
            for i in range(10)
        ],
    )
    runner = CliRunner()
    with patch.object(
        FromMbidsPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for _ in range(10)],
    ):
        res = runner.invoke(top_artists_main, ["test", "--count=5"])

    assert res.exit_code == 0
    assert "Saving 5 playlists to database." in res.output
    assert "Saved playlists to database." in res.output

    # get titles of playlists
    res = execute_sql_fetchall(
        "select title from moomoo_playlist_collections order by collection_order_index",
        session=session,
    )
    assert [i["title"] for i in res] == [
        "Top Artists 1",
        "Top Artists 2",
        "Top Artists 3",
        "Top Artists 4",
        "Top Artists 5",
    ]

    # should replace with new playlists when run again
    with patch.object(
        FromMbidsPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for _ in range(10)],
    ):
        res = runner.invoke(top_artists_main, ["test", "--count=5"])

    assert res.exit_code == 0
    res = execute_sql_fetchall(
        "select title from moomoo_playlist_collections order by collection_order_index",
        session=session,
    )
    assert [i["title"] for i in res] == [
        "Top Artists 1",
        "Top Artists 2",
        "Top Artists 3",
        "Top Artists 4",
        "Top Artists 5",
    ]
