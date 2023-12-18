"""Test the base app."""

from flask.testing import FlaskClient


def test_ping(http_app: FlaskClient):
    """Test that the ping endpoint works."""
    resp = http_app.get("/ping")
    assert resp.status_code == 200
    assert resp.json["success"] is True
