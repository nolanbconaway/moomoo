"""Common fixtures for http tests."""
import pytest
from flask.testing import FlaskClient
from moomoo_http.app import create_app


@pytest.fixture
def http_app() -> FlaskClient:
    """Create a test client for the http app."""
    app = create_app()
    return app.test_client()


@pytest.fixture(autouse=True)
def app_context(http_app):
    """Make sure the app context is created for each test."""
    with http_app.application.app_context():
        yield
