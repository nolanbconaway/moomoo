import os

from flask.testing import FlaskClient
from moomoo_http.db import db
from sqlalchemy import text


def populate_revisit_releases(data: list[dict]):
    """Populate the revisit_releases table.

    data should be a list of dicts with keys: release_group_mbid, release_group_title,
    artist_name, username.
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    create table {schema}.revisit_releases (
        release_group_mbid text, release_group_title text,
        artist_name text, username text
    )
    """
    db.session.execute(text(sql))

    sql = f"""
    insert into {schema}.revisit_releases (
        release_group_mbid, release_group_title, artist_name, username
    ) values (
        :release_group_mbid, :release_group_title, :artist_name, :username
    )
    """
    if len(data) > 0:
        db.session.execute(text(sql), params=data)

    db.session.commit()


def populate_map__file_release_group(data: list[dict]):
    """Populate the map__file_release_group table.

    data should be a list of dicts with keys: filepath, release_group_mbid.
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    create table {schema}.map__file_release_group (
        filepath text, release_group_mbid text
    )
    """
    db.session.execute(text(sql))

    sql = f"""
    insert into {schema}.map__file_release_group (filepath, release_group_mbid)
    values (:filepath, :release_group_mbid)
    """
    if len(data) > 0:
        db.session.execute(text(sql), params=data)

    db.session.commit()


def test_arg_errors(http_app: FlaskClient):
    """Test that an error is returned when bad args are sent."""
    # 404 error if no username is provided
    resp = http_app.get("/playlist/revisit-releases/")
    assert resp.status_code == 404


def test_no_revisits_error(http_app: FlaskClient):
    """Test that an error is returned when no artists are found."""
    # make empty table
    populate_revisit_releases([])
    resp = http_app.get("/playlist/revisit-releases/aaa")
    assert resp.status_code == 500
    assert resp.json["success"] is False
    assert resp.json["error"] == "No revisit releases found."


def test_success(http_app: FlaskClient):
    """Test that the correct playlist is returned."""
    revisits = [
        dict(
            release_group_mbid="aaa",
            release_group_title="aaa",
            artist_name="aaa",
            username="aaa",
        ),
        dict(
            release_group_mbid="bbb",
            release_group_title="bbb",
            artist_name="bbb",
            username="aaa",
        ),
    ]
    populate_revisit_releases(revisits)

    files = [
        dict(filepath="aaa", release_group_mbid="aaa"),
        dict(filepath="bbb", release_group_mbid="bbb"),
        dict(filepath="ccc", release_group_mbid="bbb"),
    ]
    populate_map__file_release_group(files)

    resp = http_app.get("/playlist/revisit-releases/aaa")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 2
    assert resp.json["playlists"][0]["playlist"] == ["aaa"]
    assert resp.json["playlists"][1]["playlist"] == ["bbb", "ccc"]

    # test numPlaylists arg
    resp = http_app.get(
        "/playlist/revisit-releases/aaa", query_string={"numPlaylists": 1}
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert len(resp.json["playlists"]) == 1
