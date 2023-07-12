"""Test the annotate_mbids module."""
import uuid
from typing import List
from unittest.mock import patch

import pytest
from click.testing import CliRunner, Result

from moomoo import utils_
from moomoo.enrich import annotate_mbids


def create_table(schema, table):
    utils_.create_table(schema=schema, table=table, ddl=annotate_mbids.DDL)


@pytest.fixture
def mbids() -> List[dict]:
    return [dict(mbid=uuid.uuid1(i), entity="fake") for i in range(10)]


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
        (["--limit=0"], False),  # limit < 1
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


def cli_run(
    unannotated: List[dict], reannotated: List[dict], args: List[str]
) -> Result:
    """Run the cli with the given args and mocked data."""
    create_table(schema="test", table="fake")
    base_args = ["--table=fake", "--schema=test", "--dbt-schema=dbt"]
    runner = CliRunner()
    patch_get_unannotated_mbids = patch.object(
        annotate_mbids, "get_unannotated_mbids", return_value=unannotated
    )
    patch_get_re_annotate_mbids = patch.object(
        annotate_mbids, "get_re_annotate_mbids", return_value=reannotated
    )
    patch_ann_mbid = patch.object(
        annotate_mbids.mbz_utils, "annotate_mbid", return_value=dict(a=uuid.uuid1())
    )
    with patch_get_unannotated_mbids, patch_get_re_annotate_mbids, patch_ann_mbid:
        return runner.invoke(annotate_mbids.main, base_args + args)


def test_cli_main__no_args():
    """Test nothing is done if nothing is requested."""
    result = cli_run(unannotated=[], reannotated=[], args=[])
    assert "Found 0 mbids to annotate." in result.output
    assert result.exit_code == 0

    # nothing is done if no new mbids are found.
    result = cli_run(unannotated=[], reannotated=[], args=["--find-new"])
    assert "Found 0 mbids to annotate." in result.output
    assert result.exit_code == 0


def test_cli_main__unannotated(mbids: List[dict]):
    """Test working with unannotated mbids."""
    result = cli_run(unannotated=mbids, reannotated=[], args=["--find-new"])
    assert "Found 10 mbids to annotate." in result.output
    assert result.exit_code == 0


def test_cli_main__reannotated(mbids: List[dict]):
    """Test working with re-annotated mbids."""
    result = cli_run(
        unannotated=[],
        reannotated=mbids,
        args=["--re-annotate-lb=2021-01-01", "--re-annotate-ub=2021-01-02"],
    )
    assert "Found 10 mbids to annotate." in result.output
    assert result.exit_code == 0


def test_cli_main__limit(mbids: List[dict]):
    """Test limit handler"""
    limit = len(mbids) // 2
    result = cli_run(
        unannotated=mbids, reannotated=[], args=["--find-new", f"--limit={limit}"]
    )
    assert "Found 10 mbids to annotate." in result.output
    assert f"Limiting to {limit} mbids randomly." in result.output
    assert result.exit_code == 0

    # limit > mbids
    limit = len(mbids) * 2
    result = cli_run(
        unannotated=mbids, reannotated=[], args=["--find-new", f"--limit={limit}"]
    )
    assert "Found 10 mbids to annotate." in result.output
    assert f"Limiting to {limit} mbids randomly." not in result.output
    assert result.exit_code == 0
