import datetime
import os

from click.testing import CliRunner
from moomoo_playlist.collections.loved_tracks import list_loved_tracks
from moomoo_playlist.collections.loved_tracks import main as loved_tracks_main
from moomoo_playlist.db import execute_sql_fetchall
from sqlalchemy import text
from sqlalchemy.orm import Session


def populate_loved_tracks(session: Session, data: list[dict]):
    """Make the loved_tracks table.

    data should be a list of dicts with the following keys:
        filepath: str
        username : str
        love_at : datetime
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        create table {schema}.loved_tracks (
            filepath varchar not null,
            username varchar not null,
            love_at timestamp with time zone not null
        )
    """
    session.execute(text(sql))

    for row in data:
        sql = f"""
            insert into {schema}.loved_tracks (filepath, username, love_at)
            values (:filepath, :username, :love_at)
        """
        session.execute(text(sql), row)

    session.commit()


def test_list_loved_tracks__no_results(session: Session):
    """Test handling of no results."""
    populate_loved_tracks(session, [])
    res = list_loved_tracks(username="test", session=session)
    assert res == []


def test_main__no_results(session: Session):
    """Test CLI with no results."""
    populate_loved_tracks(session, [])
    runner = CliRunner()
    res = runner.invoke(loved_tracks_main, ["test"])
    assert res.exit_code == 0
    assert "No loved tracks found" in res.output


def test_main__storage(session: Session):
    """Test CLI storage is replaced / correct."""
    populate_loved_tracks(
        session,
        [
            dict(
                username="test",
                filepath=f"path/{i}",
                love_at=datetime.datetime(2023, 1, 1, i),
            )
            for i in range(5)
        ],
    )
    runner = CliRunner()
    res = runner.invoke(loved_tracks_main, ["test"])

    assert res.exit_code == 0
    assert "Creating playlist for 5 loved tracks." in res.output
    assert "Saved playlist to database." in res.output

    # get titles of playlists
    res = execute_sql_fetchall(
        """
        select title, playlist
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Loved Tracks"]

    # check ordering is descending on time.
    assert res[0]["playlist"] == [
        {"filepath": "path/4"},
        {"filepath": "path/3"},
        {"filepath": "path/2"},
        {"filepath": "path/1"},
        {"filepath": "path/0"},
    ]

    # should replace with new playlists when run again
    res = runner.invoke(loved_tracks_main, ["test"])
    assert res.exit_code == 0
    res = execute_sql_fetchall(
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Loved Tracks"]
