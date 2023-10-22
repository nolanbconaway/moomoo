"""Test the base app."""

import pytest
from flask.testing import FlaskClient
from moomoo_http.app import create_app


@pytest.fixture
def http_app() -> FlaskClient:
    """Create a test client for the http app."""
    app = create_app()
    return app.test_client()


def test_ping(http_app: FlaskClient):
    """Test that the ping endpoint works."""
    resp = http_app.get("/ping")
    assert resp.status_code == 200
    assert resp.json["success"] is True
