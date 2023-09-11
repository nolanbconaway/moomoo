"""Test the annotate_mbids module."""
import uuid
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner, Result
from pylistenbrainz.errors import ListenBrainzAPIException

from moomoo import utils_
from moomoo.enrich import artist_stats


def create_table(schema, table):
    utils_.create_table(schema=schema, table=table, ddl=artist_stats.DDL)


@pytest.fixture
def mbids() -> list[uuid.UUID]:
    return [uuid.uuid1(i) for i in range(10)]


def mock_lb_http(*responses) -> Mock:
    """Make patched objects for the whole module."""
    return patch(
        "moomoo.enrich.artist_stats.ListenBrainz._get",
        Mock(side_effect=responses),
    )


@pytest.mark.parametrize(
    "addtl_args, exit_0",
    [
        ([], True),
        (["--before=2020-01-01"], True),
        (["--before=2020-01-01", "--new"], True),
        (["--limit=0"], False),  # limit < 1
    ],
)
def test_cli_date_args(monkeypatch, addtl_args, exit_0):
    """Test the datetime flags are required together."""
    monkeypatch.setattr(artist_stats, "get_new_mbids", lambda *args, **kwargs: [])
    monkeypatch.setattr(artist_stats, "get_old_mbids", lambda *args, **kwargs: [])

    create_table(schema="test", table="fake")
    args = ["--table=fake", "--schema=test", "--dbt-schema=dbt"]
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(artist_stats.main, args + addtl_args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def cli_run(new_: list[dict], old_: list[dict], args: list[str]) -> Result:
    """Run the cli with the given args and mocked data."""
    create_table(schema="test", table="fake")
    base_args = ["--table=fake", "--schema=test", "--dbt-schema=dbt"]
    runner = CliRunner()
    patch_get_new_mbids = patch.object(artist_stats, "get_new_mbids", return_value=new_)
    patch_get_old_mbids = patch.object(artist_stats, "get_old_mbids", return_value=old_)
    patch_artist_stats = patch.object(
        artist_stats, "_get_artist_stats", return_value=dict(a="ok")
    )
    with patch_get_new_mbids, patch_get_old_mbids, patch_artist_stats:
        return runner.invoke(artist_stats.main, base_args + args)


def test_cli_main__no_args():
    """Test nothing is done if nothing is requested."""
    result = cli_run(new_=[], old_=[], args=[])
    assert "Found 0 mbids to ingest." in result.output
    assert result.exit_code == 0

    # nothing is done if no new mbids are found.
    result = cli_run(new_=[], old_=[], args=["--new"])
    assert "Found 0 mbids to ingest." in result.output
    assert result.exit_code == 0


def test_cli_main__new(mbids: list[dict]):
    """Test working with new mbids."""
    result = cli_run(new_=mbids, old_=[], args=["--new"])
    assert "Found 10 mbids to ingest." in result.output
    assert result.exit_code == 0


def test_cli_main__old(mbids: list[dict]):
    """Test working with old mbids."""
    result = cli_run(new_=[], old_=mbids, args=["--before=2021-01-01"])
    assert "Found 10 mbids to ingest." in result.output
    assert result.exit_code == 0


def test_cli_main__limit(mbids: list[dict]):
    """Test limit handler"""
    limit = len(mbids) // 2
    result = cli_run(new_=mbids, old_=[], args=["--new", f"--limit={limit}"])
    assert "Found 10 mbids to ingest." in result.output
    assert f"Limiting to {limit} mbids randomly." in result.output
    assert result.exit_code == 0

    # limit > mbids
    limit = len(mbids) * 2
    result = cli_run(new_=mbids, old_=[], args=["--new", f"--limit={limit}"])
    assert "Found 10 mbids to ingest." in result.output
    assert f"Limiting to {limit} mbids randomly." not in result.output
    assert result.exit_code == 0


def test_cli_main__exception_handling():
    # fail on nonhandled status code
    err = ListenBrainzAPIException(status_code=500, message="FAKE")
    with mock_lb_http(*[err] * 3) as mock_get:
        res = artist_stats.get_artist_stats("fake_uuid")
    assert not res["success"]
    assert mock_get.call_count == 3  # 3 retries

    # OK with 204
    err = ListenBrainzAPIException(status_code=204, message="FAKE")
    with mock_lb_http(err) as mock_get:
        res = artist_stats.get_artist_stats("fake_uuid")

    assert not res["success"]
    assert mock_get.call_count == 1  # no retries
