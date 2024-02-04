import os
from unittest.mock import patch
from uuid import uuid4

from click.testing import CliRunner
from moomoo_playlist.collections.revisit_releases import list_revisit_releases
from moomoo_playlist.collections.revisit_releases import main as revisit_releases_main
from moomoo_playlist.db import execute_sql_fetchall
from moomoo_playlist.generator import NoFilesRequestedError, QueryPlaylistGenerator
from moomoo_playlist.playlist import Playlist
from sqlalchemy import text
from sqlalchemy.orm import Session


def populate_revisit_releases(session: Session, data: list[dict]):
    """Make the revisit_releases table.

    data should be a list of dicts with the following keys:
        release_group_mbid: uuid or str (optional)
        release_group_title : str
        artist_name : str
        username: str
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        create table {schema}.revisit_releases (
            release_group_mbid uuid,
            release_group_title text,
            artist_name text,
            username text
        )
    """
    session.execute(text(sql))

    for row in data:
        row["release_group_mbid"] = row.get("release_group_mbid", uuid4())
        sql = f"""
            insert into {schema}.revisit_releases
            (release_group_mbid, release_group_title, artist_name, username)
            values (:release_group_mbid, :release_group_title, :artist_name, :username)
        """
        session.execute(text(sql), row)

    session.commit()


def test_list_revisit_releases__count_and_seed(session: Session):
    """Test handling of the count parameter, as well as the random seed."""
    populate_revisit_releases(
        session,
        [
            dict(release_group_title="test", artist_name="test", username="test")
            for _ in range(10)
        ],
    )

    res = list_revisit_releases(username="test", count=5, session=session)
    assert len(res) == 5

    # check deterministic
    assert res == list_revisit_releases(username="test", count=5, session=session)

    res = list_revisit_releases(username="test", count=10, session=session)
    assert len(res) == 10

    res = list_revisit_releases(username="test", count=15, session=session)
    assert len(res) == 10


def test_list_revisit_releases__no_results(session: Session):
    """Test handling of no results."""
    populate_revisit_releases(session, [])

    res = list_revisit_releases(username="test", count=5, session=session)
    assert len(res) == 0


def test_main__no_results(session: Session):
    """Test CLI with no results."""
    populate_revisit_releases(session, [])
    runner = CliRunner()
    res = runner.invoke(revisit_releases_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "No playlists generated" in res.output


def test_main__playlist_error(session: Session):
    """Test CLI with a playlist error."""
    populate_revisit_releases(
        session,
        [
            dict(release_group_title="test", artist_name="test", username="test")
            for _ in range(10)
        ],
    )
    runner = CliRunner()
    playlist = Playlist(tracks=[])
    with patch.object(
        QueryPlaylistGenerator,
        "get_playlist",
        side_effect=[playlist, NoFilesRequestedError, playlist, playlist, playlist],
    ):
        res = runner.invoke(revisit_releases_main, ["test", "--count=5"])
    assert res.exit_code == 0
    assert "No files found for release mbid" in res.output
    assert "NoFilesRequestedError" in res.output
    assert "Saved 4 playlist(s) to database." in res.output


def test_main__stale_handler(session: Session):
    """The stale handler should skip when the collection is not stale."""
    populate_revisit_releases(
        session,
        [
            dict(release_group_title="test", artist_name="test", username="test")
            for _ in range(10)
        ],
    )

    runner = CliRunner()

    with patch.object(
        QueryPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(revisit_releases_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 5
    assert res.exit_code == 0
    assert "Saved 5 playlist(s) to database." in res.output

    with patch.object(
        QueryPlaylistGenerator, "get_playlist", return_value=Playlist(tracks=[])
    ) as patch_get_playlist:
        res = runner.invoke(revisit_releases_main, ["test", "--count=5"])

    assert patch_get_playlist.call_count == 0
    assert res.exit_code == 0
    assert "Collection is not stale; skipping." in res.output


def test_main__storage(session: Session):
    """Test CLI storage is replaced / correct."""
    populate_revisit_releases(
        session,
        [
            dict(release_group_title="test", artist_name="test", username="test")
            for _ in range(10)
        ],
    )
    runner = CliRunner()
    with patch.object(
        QueryPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for i in range(10)],
    ):
        res = runner.invoke(revisit_releases_main, ["test", "--count=5"])

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
        "Revisit Release 1",
        "Revisit Release 2",
        "Revisit Release 3",
        "Revisit Release 4",
        "Revisit Release 5",
    ]

    # should replace with new playlists when run again
    with patch.object(
        QueryPlaylistGenerator,
        "get_playlist",
        side_effect=[Playlist(tracks=[]) for _ in range(10)],
    ):
        res = runner.invoke(revisit_releases_main, ["test", "--count=5"])

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
        "Revisit Release 1",
        "Revisit Release 2",
        "Revisit Release 3",
        "Revisit Release 4",
        "Revisit Release 5",
    ]
