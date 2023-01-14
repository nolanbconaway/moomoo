import time
from pathlib import Path

import psycopg2
import pytest

import moomoo

RESOURCES = Path(__file__).parent / "resources"


class MockConnection:
    def __init__(self, *args, **kwargs):
        pass

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture(autouse=True)
def remove_env_variables(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "dbname=fake user=fake password=fake host=fake")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


@pytest.fixture(autouse=True)
def mock_check_table_exists(monkeypatch):
    monkeypatch.setattr(
        moomoo.utils_, "check_table_exists", lambda *args, **kwargs: True
    )


@pytest.fixture(autouse=True)
def mock_create_table(monkeypatch):
    monkeypatch.setattr(moomoo.utils_, "create_table", lambda *args, **kwargs: ...)


@pytest.fixture(autouse=True)
def mock_db_connect(monkeypatch):
    monkeypatch.setattr(psycopg2, "connect", MockConnection)
