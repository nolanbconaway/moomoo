import datetime
import json

from click.testing import CliRunner

from moomoo.ingest import collect_listen_data
from moomoo.utils_ import create_table, pg_connect

from ..conftest import RESOURCES


def load_resource_json(name):
    return json.loads((RESOURCES / name).read_text())


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
        collect_listen_data.ListenBrainz,
        "_get",
        lambda *a, **kw: load_resource_json("sample_listenbrainz_listen.json"),
    )

    # make fake data for since last
    create_table(schema="test", table="fake", ddl=collect_listen_data.DDL)
    last_at = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ) - datetime.timedelta(days=30)
    with pg_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            insert into test.fake (listen_md5, username, json_data, listen_at_ts_utc)
            values (
                'fakemd5'
                , 'FAKE_NAME'
                , '{"a": 1}'
                , current_timestamp - interval '30 days'
            )
        """
        )
        conn.commit()

    result = runner.invoke(
        collect_listen_data.main,
        ["FAKE_NAME", "--table=fake", "--schema=test", "--since-last"],
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
        [
            "FAKE_NAME",
            "--create",
            "--table=fake",
            "--schema=test",
            "--from",
            dt.isoformat(),
        ],
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output
