import datetime
import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from lastfmrec import collect_recent_tracks, utils_

from .conftest import MockResponse

RESOURCES = Path(__file__).parent / "resources"


def load_resource_json(name):
    return json.loads((RESOURCES / name).read_text())


@pytest.fixture(autouse=True)
def mock_check_check_user_in_table(monkeypatch):
    monkeypatch.setattr(
        collect_recent_tracks, "check_user_in_table", lambda *args, **kwargs: True
    )


@pytest.fixture(autouse=True)
def mock_insert(monkeypatch):
    monkeypatch.setattr(collect_recent_tracks, "insert", lambda *args, **kwargs: ...)


def test_get_lastfm_user_registry_dt(monkeypatch):
    expect = utils_.utcfromunixtime(1646784218)
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("userinfo_api_data.json")),
    )
    assert collect_recent_tracks.get_lastfm_user_registry_dt("user") == expect


def test_get_listens_in_period(monkeypatch):
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("recenttracks_api_data.json")),
    )

    res = collect_recent_tracks.get_listens_in_period(
        username="FAKE",
        from_dt=datetime.datetime.utcnow(),
        to_dt=datetime.datetime.utcnow(),
    )

    assert len(res) == 1


def test_cli_main__from_register(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        collect_recent_tracks,
        "get_lastfm_user_registry_dt",
        lambda _: (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            - datetime.timedelta(days=30)
        ),
    )
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("recenttracks_api_data.json")),
    )

    result = runner.invoke(
        collect_recent_tracks.main,
        ["--table=FAKE", "--schema=FAKE", "--since-register", "FAKE_NAME"],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output


def test_cli_main__from_last(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        collect_recent_tracks,
        "get_db_last_listen",
        lambda *a, **kw: (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            - datetime.timedelta(days=30)
        ),
    )
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("recenttracks_api_data.json")),
    )

    result = runner.invoke(
        collect_recent_tracks.main,
        ["FAKE_NAME", "--table=FAKE", "--schema=FAKE", "--since-last"],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output


def test_cli_main__from_last__today(monkeypatch):
    """The usual case will be a since-last call where the last pulay is today."""
    runner = CliRunner()
    monkeypatch.setattr(
        collect_recent_tracks,
        "get_db_last_listen",
        lambda *a, **kw: (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            - datetime.timedelta(minutes=30)
        ),
    )
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("recenttracks_api_data.json")),
    )

    result = runner.invoke(
        collect_recent_tracks.main,
        ["FAKE_NAME", "--table=FAKE", "--schema=FAKE", "--since-last"],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output


def test_cli_main__from_dt(monkeypatch):
    dt = datetime.date.today() - datetime.timedelta(days=30)
    runner = CliRunner()
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("recenttracks_api_data.json")),
    )

    result = runner.invoke(
        collect_recent_tracks.main,
        ["FAKE_NAME", "--table=FAKE", "--schema=FAKE", "--from", dt.isoformat()],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output
