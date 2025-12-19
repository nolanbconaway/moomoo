import datetime
import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from moomoo_ingest import collect_listen_data
from moomoo_ingest.db import ListenBrainzListen, execute_sql_fetchall
from moomoo_ingest.utils_ import utcnow

from .conftest import RESOURCES


def load_resource_json(name):
    return json.loads((RESOURCES / name).read_text())


@pytest.fixture(autouse=True)
def monkeypatch_lb_get(monkeypatch):
    """Auto mock the ListenBrainz._get method."""
    monkeypatch.setattr(
        'liblistenbrainz.ListenBrainz._get',
        lambda *a, **kw: load_resource_json("sample_listenbrainz_listen.json"),
    )


def test_get_listens_in_period():
    res = collect_listen_data.get_listens_in_period(
        username="FAKE",
        from_dt=datetime.datetime.utcnow(),
        to_dt=datetime.datetime.utcnow(),
    )

    assert len(res) == 1


def test_cli_main__not_table_exists_error():
    runner = CliRunner()
    result = runner.invoke(collect_listen_data.main, ["--from=2021-01-01", "FAKE_NAME"])
    assert result.exit_code != 0
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


def test_cli_main__since_last_from_dt_error():
    runner = CliRunner()
    ListenBrainzListen.create()

    result = runner.invoke(
        collect_listen_data.main, ["--since-last", "--from=2021-01-01", "FAKE_NAME"]
    )
    assert result.exit_code != 0
    assert "--since-last and --from are mutually exclusive." in result.output

    result = runner.invoke(collect_listen_data.main, ["FAKE_NAME"])
    assert result.exit_code != 0
    assert "Must specify either --since-last or --from" in result.output


def test_cli_main__since_last_no_data_error():
    runner = CliRunner()
    ListenBrainzListen.create()

    result = runner.invoke(collect_listen_data.main, ["--since-last", "FAKE_NAME"])
    assert result.exit_code != 0
    assert "No data found for FAKE_NAME" in result.output
    assert "Cannot use --since-last" in result.output


def test_cli_main__since_last__date_buffer():
    runner = CliRunner()
    ListenBrainzListen.create()
    # make fake data for since last
    last_at = utcnow() - datetime.timedelta(days=30)
    ListenBrainzListen(
        listen_md5="a", username="a", json_data={"a": 1}, listen_at_ts_utc=last_at
    ).insert()

    with patch("moomoo_ingest.collect_listen_data.run_ingest") as mocked:
        result = runner.invoke(collect_listen_data.main, ["--since-last", "--buffer-days=1", "a"])
        assert result.exit_code == 0
        assert mocked.call_count == 1
        assert mocked.call_args[1]["from_dt"] == last_at - datetime.timedelta(days=1)


def test_cli_main__from_dt__pass_args():
    runner = CliRunner()
    ListenBrainzListen.create()

    with patch("moomoo_ingest.collect_listen_data.run_ingest") as mocked:
        result = runner.invoke(
            collect_listen_data.main, ["--from=2021-01-01", "--to=2021-01-02", "a"]
        )
        assert result.exit_code == 0
        assert mocked.call_count == 1
        assert mocked.call_args[1]["from_dt"] == datetime.datetime(
            2021, 1, 1, tzinfo=datetime.timezone.utc
        )
        assert mocked.call_args[1]["to_dt"] == datetime.datetime(
            2021, 1, 2, tzinfo=datetime.timezone.utc
        )


def test_cli_main__no_data(monkeypatch):
    # override auto mock
    monkeypatch.setattr(
        'liblistenbrainz.ListenBrainz._get',
        lambda *a, **kw: dict(payload=dict(count=0, listens=[])),
    )

    runner = CliRunner()
    ListenBrainzListen.create()

    result = runner.invoke(collect_listen_data.main, ["FAKE_NAME", "--from=2021-01-01"])

    assert result.exit_code == 0
    assert "No listens found" in result.output


def test_run_ingest__invalid_dates():
    with pytest.raises(ValueError) as e:
        collect_listen_data.run_ingest(
            username="FAKE",
            from_dt=utcnow() + datetime.timedelta(days=1),
            to_dt=utcnow() - datetime.timedelta(days=1),
        )
        assert "from_dt must be before to_dt." in str(e)


def test_run_ingest__insert_upsert():
    ListenBrainzListen.create()
    sql = f"select count(1) as n from {ListenBrainzListen.table_name()}"

    collect_listen_data.run_ingest(
        username="FAKE", from_dt=utcnow() - datetime.timedelta(days=1), to_dt=utcnow()
    )

    # ensure we have 1 row now
    res = execute_sql_fetchall(sql)[0]["n"]
    assert res == 1

    # run again, ensure we still have 1 row
    collect_listen_data.run_ingest(
        username="FAKE", from_dt=utcnow() - datetime.timedelta(days=1), to_dt=utcnow()
    )
    res = execute_sql_fetchall(sql)[0]["n"]
    assert res == 1
