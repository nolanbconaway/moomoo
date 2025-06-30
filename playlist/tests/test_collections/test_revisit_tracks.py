import os
from pathlib import Path
from uuid import uuid4

from click.testing import CliRunner
from sqlalchemy import text
from sqlalchemy.orm import Session

from moomoo_playlist.collections.revisit_tracks import (
    create_playlist,
    list_revisit_tracks,
)
from moomoo_playlist.collections.revisit_tracks import main as revisit_tracks_main
from moomoo_playlist.db import execute_sql_fetchall
from moomoo_playlist.playlist import Track


def populate_revisit_tracks(session: Session, data: list[dict]):
    """Make the revisit_tracks table.

    data should be a list of dicts with the following keys:
        filepath: str
        username: str
        recording_mbid: uuid or str (optional)
        artist_mbid: uuid or str (optional)
        album_artist_mbid: uuid or str (optional)
        revisit_score: float (optional)
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        create table {schema}.revisit_tracks (
            filepath text,
            recording_mbid uuid,
            artist_mbid uuid,
            album_artist_mbid uuid,
            username text,
            revisit_score float
        )
    """
    session.execute(text(sql))

    for row in data:
        row["recording_mbid"] = row.get("recording_mbid", uuid4())
        row["artist_mbid"] = row.get("artist_mbid", uuid4())
        if not row.get("album_artist_mbid"):
            row["album_artist_mbid"] = row["artist_mbid"]

        row["revisit_score"] = row.get("revisit_score", 2.0)
        sql = f"""
            insert into {schema}.revisit_tracks (
                filepath
                , recording_mbid
                , artist_mbid
                , album_artist_mbid
                , username
                , revisit_score
            )
            values (
                :filepath
                , :recording_mbid
                , :artist_mbid
                , :album_artist_mbid
                , :username
                , :revisit_score
            )
        """
        session.execute(text(sql), row)

    session.commit()


def test_list_revisit_tracks__sorting(session: Session):
    """Test sorting of revisit tracks."""
    populate_revisit_tracks(
        session,
        [dict(filepath=f"path/{i}", username="test", revisit_score=10 + i) for i in range(5)],
    )

    res = list_revisit_tracks(username="test", session=session)
    assert [str(i.filepath) for i in res] == [
        "path/4",
        "path/3",
        "path/2",
        "path/1",
        "path/0",
    ]


def test_create_playlist__dedupe():
    """Test the deduping of revisit tracks."""

    def make_track(filepath, artist_mbid) -> Track:
        return Track(
            filepath=Path(filepath),
            recording_mbid=uuid4(),
            artist_mbid=artist_mbid,
            album_artist_mbid=artist_mbid,
        )

    repeat_artist = uuid4()
    tracks = [
        make_track("path/1", repeat_artist),
        make_track("path/2", repeat_artist),
        make_track("path/3", repeat_artist),  # excluded because >2 prior for the artist
        make_track("path/2", uuid4()),  # excluded because filepath already used
        make_track("path/4", uuid4()),
    ]

    res = create_playlist(tracks, total_tracks=5)
    assert set([str(i.filepath) for i in res.tracks]) == set(["path/1", "path/2", "path/4"])


def test_main__no_results(session: Session):
    """Test CLI with no results."""
    populate_revisit_tracks(session, [])
    runner = CliRunner()
    res = runner.invoke(revisit_tracks_main, ["test"])
    assert res.exit_code == 0
    assert "No revisit tracks found" in res.output


def test_main__storage(session: Session):
    """Test CLI storage is replaced / correct."""
    populate_revisit_tracks(
        session,
        [dict(filepath=f"path/{i}", username="test", revisit_score=i + 10) for i in range(5)],
    )
    runner = CliRunner()
    res = runner.invoke(revisit_tracks_main, ["test"])
    assert res.exit_code == 0
    assert "Saved playlist to database." in res.output

    # check storage
    res = execute_sql_fetchall(
        """
        select title, playlist
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Revisit Tracks"]
    assert set([i["filepath"] for i in res[0]["playlist"]]) == set(
        [
            "path/4",
            "path/3",
            "path/2",
            "path/1",
            "path/0",
        ]
    )

    # should replace with new playlists when run again
    res = runner.invoke(revisit_tracks_main, ["test"])
    assert res.exit_code == 0
    res = execute_sql_fetchall(
        """
        select title
        from moomoo_playlist_collection_items
        order by collection_order_index
        """,
        session=session,
    )
    assert [i["title"] for i in res] == ["Revisit Tracks"]
