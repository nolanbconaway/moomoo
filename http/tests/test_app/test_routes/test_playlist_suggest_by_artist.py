import os
from unittest.mock import patch
from uuid import uuid4

from flask.testing import FlaskClient
from moomoo_http.db import db
from moomoo_http.playlist_generator import Playlist
from sqlalchemy import text

playlist_obj = "moomoo_http.playlist_generator.FromMbidsPlaylistGenerator.get_playlist"


def populate_artist_listen_counts(data: list[dict]):
    """Populate the artist_listen_counts table.

    data should be a list of dicts with keys:
        username, artist_mbid, artist_name, listen_count
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    create table {schema}.artist_listen_counts (
        username text, artist_mbid uuid, artist_name varchar, 
        last30_listen_count int, last60_listen_count int, last90_listen_count int,
        lifetime_listen_count int
    )
    """
    db.session.execute(text(sql))

    sql = f"""
    insert into {schema}.artist_listen_counts (
        username, artist_mbid, artist_name,
        last30_listen_count, last60_listen_count, last90_listen_count,
        lifetime_listen_count
    )
    values (:username, :artist_mbid, :artist_name,
        :listen_count, :listen_count, :listen_count, :listen_count
    )
    """
    if len(data) > 0:
        db.session.execute(text(sql), params=data)

    db.session.commit()


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/suggest/by-artist")
    assert resp.status_code == 404

    resp = http_app.get(
        "/playlist/suggest/by-artist/aaa", query_string=dict(numPlaylists=0)
    )
    assert resp.status_code == 400
    assert resp.json["success"] is False
    assert resp.json["error"] == "numPlaylists must be >= 1."


def test_no_artists_error(http_app: FlaskClient):
    """Test that an error is returned when no artists are found."""
    # make empty table
    populate_artist_listen_counts([])
    resp = http_app.get("/playlist/suggest/by-artist/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert resp.json["error"] == "No artists found for user."


def test_count_playlists(http_app: FlaskClient):
    """Test that the number of playlists returned is correct."""
    artists = [
        dict(username="aaa", artist_mbid=uuid4(), artist_name="1", listen_count=100),
        dict(username="aaa", artist_mbid=uuid4(), artist_name="2", listen_count=99),
        dict(username="aaa", artist_mbid=uuid4(), artist_name="3", listen_count=98),
    ]
    populate_artist_listen_counts(artists)

    # limit correctly applied
    with patch(playlist_obj, return_value=Playlist([])) as mock_playlist:
        resp = http_app.get(
            "/playlist/suggest/by-artist/aaa", query_string=dict(numPlaylists=1)
        )
        assert mock_playlist.call_count == 1
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert len(resp.json["playlists"]) == 1

    # ok if not enough for limit
    with patch(playlist_obj, return_value=Playlist([])) as mock_playlist:
        resp = http_app.get(
            "/playlist/suggest/by-artist/aaa", query_string=dict(numPlaylists=10)
        )
        assert mock_playlist.call_count == 3
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert len(resp.json["playlists"]) == 3


def test_exclude_mbid(http_app: FlaskClient):
    """Test that excludeMbid works."""
    artists = [
        dict(username="aaa", artist_mbid=uuid4(), artist_name="1", listen_count=100),
        dict(username="aaa", artist_mbid=uuid4(), artist_name="2", listen_count=99),
        dict(username="aaa", artist_mbid=uuid4(), artist_name="3", listen_count=98),
    ]
    populate_artist_listen_counts(artists)

    with patch(playlist_obj, return_value=Playlist([])):
        resp = http_app.get(
            "/playlist/suggest/by-artist/aaa",
            query_string=dict(
                numPlaylists=3, excludeMbid=str(artists[0]["artist_mbid"])
            ),
        )
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert len(resp.json["playlists"]) == 2

    # no exclude mbid
    with patch(playlist_obj, return_value=Playlist([])):
        resp = http_app.get(
            "/playlist/suggest/by-artist/aaa", query_string=dict(numPlaylists=3)
        )
        assert resp.status_code == 200
        assert resp.json["success"] is True
        assert len(resp.json["playlists"]) == 3
