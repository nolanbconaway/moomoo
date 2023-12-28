"""Test the base app."""

from flask.testing import FlaskClient


def test_ping(http_app: FlaskClient):
    """Test that the ping endpoint works."""
    resp = http_app.get("/ping")
    assert resp.status_code == 200
    assert resp.json["success"] is True


def test_version(http_app: FlaskClient):
    """Test that the version endpoint works."""
    resp = http_app.get("/version")
    assert resp.status_code == 200

    # assert version is like a semver (x.x.x)
    version = resp.json["version"]
    assert len(version.split(".")) == 3
    assert all(map(lambda x: x.isnumeric(), version.split(".")))
