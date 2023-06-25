"""Test the annotate_mbids module."""
import uuid

import pytest
from click.testing import CliRunner

from moomoo import utils_
from moomoo.enrich import annotate_mbids


def create_table(schema, table):
    utils_.create_table(schema=schema, table=table, ddl=annotate_mbids.DDL)


@pytest.mark.parametrize(
    "addtl_args, exit_0",
    [
        ([], True),
        (["--re-annotate-lb=2020-01-01", "--re-annotate-ub=2020-02-01"], True),
        (["--re-annotate-lb=2020-01-01"], False),  # missing --re-annotate-ub
        (["--re-annotate-ub=2020-01-01"], False),  # missing --re-annotate-lb
        (
            ["--re-annotate-lb=2020-01-01", "--re-annotate-ub=2020-01-01"],
            False,
        ),  # ub == lb
        (
            ["--re-annotate-lb=2020-02-01", "--re-annotate-ub=2020-01-01"],
            False,
        ),  # ub < lb
    ],
)
def test_cli_date_args(monkeypatch, addtl_args, exit_0):
    """Test the datetime flags are required together."""
    monkeypatch.setattr(
        annotate_mbids, "get_unannotated_mbids", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        annotate_mbids, "get_re_annotate_mbids", lambda *args, **kwargs: []
    )

    create_table(schema="test", table="fake")
    args = ["--table=fake", "--schema=test", "--dbt-schema=dbt"]
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(annotate_mbids.main, args + addtl_args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def test_cli_main(monkeypatch):
    """Test required flags are enforced."""
    mbids = [dict(mbid=uuid.uuid1(), entity="fake") for _ in range(10)]

    create_table(schema="test", table="fake")

    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda **_: [])
    monkeypatch.setattr(annotate_mbids, "get_re_annotate_mbids", lambda **_: [])
    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", lambda *_: dict(a=1))

    runner = CliRunner()
    args = ["--table=fake", "--schema=test", "--dbt-schema=dbt"]

    # no args, no annotation
    result = runner.invoke(annotate_mbids.main, args)
    assert "Found 0 mbids to annotate." in result.output
    assert result.exit_code == 0

    # mbid functions return nothing. no annotation
    result = runner.invoke(annotate_mbids.main, args + ["--find-new"])
    assert "Found 0 mbids to annotate." in result.output
    assert result.exit_code == 0

    # set re annotate mbids. should still have nothing to annotate.
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda **_: [])
    monkeypatch.setattr(annotate_mbids, "get_re_annotate_mbids", lambda **_: mbids)
    result = runner.invoke(annotate_mbids.main, args + ["--find-new"])
    assert "Found 0 mbids to annotate." in result.output
    assert result.exit_code == 0

    # set unannotated mbids. should have something to annotate.
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda **_: mbids)
    monkeypatch.setattr(annotate_mbids, "get_re_annotate_mbids", lambda **_: [])
    result = runner.invoke(annotate_mbids.main, args + ["--find-new"])
    assert "Found 10 mbids to annotate." in result.output
    assert result.exit_code == 0
