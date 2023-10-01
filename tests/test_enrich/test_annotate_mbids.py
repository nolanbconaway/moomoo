"""Test the annotate_mbids module."""
import uuid
from unittest.mock import patch

import pytest
from click.testing import CliRunner, Result

from moomoo.db import MusicBrainzAnnotation
from moomoo.enrich import annotate_mbids


@pytest.fixture
def mbids() -> list[dict]:
    return [dict(mbid=uuid.uuid4(), entity="fake") for _ in range(10)]


def test_cli_main__not_table_exists_error():
    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main)
    assert result.exit_code != 0

    name = MusicBrainzAnnotation.table_name()
    assert f"Table {name} does not exist" in result.output


@pytest.mark.parametrize(
    "args, exit_0",
    [
        ([], True),
        (["--before=2020-01-01"], True),
        (["--before=2020-01-01", "--new"], True),
        (["--limit=0"], False),  # limit < 1
    ],
)
def test_cli_date_args(monkeypatch, args, exit_0):
    """Test the datetime flags are required together."""
    MusicBrainzAnnotation.create()
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda *_: [])
    monkeypatch.setattr(annotate_mbids, "get_re_annotate_mbids", lambda *_: [])
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(annotate_mbids.main, args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def cli_run(
    unannotated: list[dict], reannotated: list[dict], args: list[str]
) -> Result:
    """Run the cli with the given args and mocked data."""
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
        return runner.invoke(annotate_mbids.main, args)


def test_cli_main__no_args():
    """Test nothing is done if nothing is requested."""
    MusicBrainzAnnotation.create()

    result = cli_run(unannotated=[], reannotated=[], args=[])
    assert "Found 0 total mbid(s) to annotate." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0

    # nothing is done if no new mbids are found.
    result = cli_run(unannotated=[], reannotated=[], args=["--new"])
    assert "Found 0 total mbid(s) to annotate." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0


def test_cli_main__unannotated(mbids: list[dict]):
    """Test working with unannotated mbids."""
    MusicBrainzAnnotation.create()
    result = cli_run(unannotated=mbids, reannotated=[], args=["--new"])
    assert "Found 10 total mbid(s) to annotate." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert isinstance(res[0]["mbid"], uuid.UUID)  # make sure deserialization works
    assert len(res) == 10


def test_cli_main__reannotated(mbids: list[dict]):
    """Test working with re-annotated mbids."""
    MusicBrainzAnnotation.create()
    result = cli_run(unannotated=[], reannotated=mbids, args=["--before=2021-01-01"])
    assert "Found 10 total mbid(s) to annotate." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == 10


def test_cli_main__limit(mbids: list[dict]):
    """Test limit handler"""
    MusicBrainzAnnotation.create()

    limit = len(mbids) // 2
    result = cli_run(
        unannotated=mbids, reannotated=[], args=["--new", f"--limit={limit}"]
    )
    assert "Found 10 total mbid(s) to annotate." in result.output
    assert f"Limiting to {limit} mbid(s) randomly." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == limit

    MusicBrainzAnnotation.create(drop=True)

    # limit > mbids
    limit = len(mbids) * 2
    result = cli_run(
        unannotated=mbids, reannotated=[], args=["--new", f"--limit={limit}"]
    )
    assert "Found 10 total mbid(s) to annotate." in result.output
    assert f"Limiting to {limit} mbids randomly." not in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == 10


# TODO: test get_unannotated_mbids, get_re_annotate_mbids directly with fake data
