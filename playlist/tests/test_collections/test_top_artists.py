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


@pytest.mark.parametrize("count", [5, 7])
def test_list_top_artists__count(session: Session, count: int):
    """Test handling of the count parameter."""
    populate_artist_listen_counts(
        session,
        [
            dict(artist_name=f"test_{i}", username="test", lifetime_listen_count=100)
            for i in range(10)
        ],
    )
    res = list_top_artists(
        username="test", history_length="lifetime", count=count, session=session
    )
    assert len(res) == count


def test_list_top_artists__history_length(session: Session):
    """Test handling of the history_length parameter."""
    populate_artist_listen_counts(
        session,
        [
            dict(
                artist_name=f"test_{i}",
                username="test",
                lifetime_listen_count=1,
                last90_listen_count=100,
            )
            for i in range(10)
        ],
    )
    # lifetime does not have enough listens, but 90 does.
    res = list_top_artists(
        username="test", history_length="lifetime", count=5, session=session
    )
    assert res == []

    # lifetime does not have enough listens, but 90 does.
    res = list_top_artists(
        username="test", history_length="90", count=5, session=session
    )
    assert len(res) == 5

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
    assert "Saved 4 playlist(s) to database." in res.output


def test_main__stale_handler(session: Session):
    """The stale handler should skip when the collection is not stale."""
    populate_artist_listen_counts(
        session,
        [
            dict(artist_name=f"test_{i}", username="test", lifetime_listen_count=500)
            for i in range(10)
        ],
    )

    runner = CliRunner()

    with patch.object(
        FromMbidsPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(top_artists_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 5
    assert res.exit_code == 0
    assert "Saved 5 playlist(s) to database." in res.output

    # run again, should skip
    with patch.object(
        FromMbidsPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(top_artists_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 0
    assert res.exit_code == 0
    assert "Collection is not stale; skipping." in res.output

    # test force
    with patch.object(
        FromMbidsPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(top_artists_main, ["test", "--count=5", "--force"])

    assert patch_get_playlist.call_count == 5
    assert res.exit_code == 0
    assert "Saved 5 playlist(s) to database." in res.output


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
    assert "Saved 5 playlist(s) to database." in res.output

    # get titles of playlists
    res = execute_sql_fetchall(
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
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
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == [
        "Top Artists 1",
        "Top Artists 2",
        "Top Artists 3",
        "Top Artists 4",
        "Top Artists 5",
    ]
