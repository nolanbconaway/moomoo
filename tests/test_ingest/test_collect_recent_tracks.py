import datetime
import json

import pytest
from click.testing import CliRunner

from moomoo.ingest import collect_listen_data

from ..conftest import RESOURCES


def load_resource_json(name):
    return json.loads((RESOURCES / name).read_text())


@pytest.fixture(autouse=True)
def mock_check_check_user_in_table(monkeypatch):
    monkeypatch.setattr(
        collect_listen_data, "check_user_in_table", lambda *args, **kwargs: True
    )


@pytest.fixture(autouse=True)
def mock_insert(monkeypatch):
    monkeypatch.setattr(collect_listen_data, "insert", lambda *args, **kwargs: ...)


def test_get_listens_in_period(monkeypatch):
    monkeypatch.setattr(
        collect_listen_data.ListenBrainz,
        "_get",
        lambda *a, **kw: load_resource_json("sample_listenbrainz_listen.json"),
    )

    res = collect_listen_data.get_listens_in_period(
        username="FAKE",
        from_dt=datetime.datetime.utcnow(),
        to_dt=datetime.datetime.utcnow(),
    )

    assert len(res) == 1


def test_cli_main__from_last(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        collect_listen_data,
        "get_db_last_listen",
        lambda *a, **kw: (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            - datetime.timedelta(days=30)
        ),
    )
    monkeypatch.setattr(
        collect_listen_data.ListenBrainz,
        "_get",
        lambda *a, **kw: load_resource_json("sample_listenbrainz_listen.json"),
    )

    result = runner.invoke(
        collect_listen_data.main,
        ["FAKE_NAME", "--table=FAKE", "--schema=FAKE", "--since-last"],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output


def test_cli_main__from_dt(monkeypatch):
    dt = datetime.date.today() - datetime.timedelta(days=30)
    runner = CliRunner()
    monkeypatch.setattr(
        collect_listen_data.ListenBrainz,
        "_get",
        lambda *a, **kw: load_resource_json("sample_listenbrainz_listen.json"),
    )

    result = runner.invoke(
        collect_listen_data.main,
        ["FAKE_NAME", "--table=FAKE", "--schema=FAKE", "--from", dt.isoformat()],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output
