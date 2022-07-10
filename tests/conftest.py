import time

import lastfmrec
import psycopg2
import pytest


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data

    def raise_for_status(self):
        ...

    def json(self):
        return self.json_data


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
    monkeypatch.setenv("LASTFM_API_KEY", "fake")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


@pytest.fixture(autouse=True)
def mock_check_table_exists(monkeypatch):
    monkeypatch.setattr(
        lastfmrec.utils_, "check_table_exists", lambda *args, **kwargs: True
    )


@pytest.fixture(autouse=True)
def mock_create_table(monkeypatch):
    monkeypatch.setattr(lastfmrec.utils_, "create_table", lambda *args, **kwargs: ...)


@pytest.fixture(autouse=True)
def mock_db_connect(monkeypatch):
    monkeypatch.setattr(psycopg2, "connect", MockConnection)
