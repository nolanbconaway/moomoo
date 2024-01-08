import os

from flask.testing import FlaskClient
from moomoo_http.db import db
from sqlalchemy import text


def populate_loved_tracks(data: list[dict]):
    """Populate the loved_tracks table.

    data should be a list of dicts with keys: username, filepath, love_at
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    create table {schema}.loved_tracks (
        username text, filepath text, love_at timestamp
    )
    """
    db.session.execute(text(sql))

    sql = f"""
    insert into {schema}.loved_tracks (username, filepath, love_at)
    values (:username, :filepath, :love_at)
    """
    if len(data) > 0:
        db.session.execute(text(sql), params=data)

    db.session.commit()


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/loved/")
    assert resp.status_code == 404


def test_no_loves_error(http_app: FlaskClient):
    """Test that an error is returned when no artists are found."""
    # make empty table
    populate_loved_tracks([])
    resp = http_app.get("/playlist/loved/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert "No paths requested" in resp.json["error"]


def test_success(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    data = [
        dict(username="aaa", filepath="aaa", love_at="2021-01-01"),
        dict(username="aaa", filepath="bbb", love_at="2021-01-02"),
        dict(username="aaa", filepath="ccc", love_at="2021-01-03"),
    ]
    populate_loved_tracks(data)
    resp = http_app.get("/playlist/loved/aaa")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 1
    assert len(resp.json["playlists"][0]["playlist"]) == 3
    assert resp.json["playlists"][0]["playlist"] == ["ccc", "bbb", "aaa"]
