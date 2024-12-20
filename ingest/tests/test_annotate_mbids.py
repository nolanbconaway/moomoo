"""Test the annotate_mbids module."""

import datetime
import uuid

import pytest
from click.testing import CliRunner

from moomoo_ingest import annotate_mbids
from moomoo_ingest.db import MusicBrainzAnnotation
from moomoo_ingest.utils_ import ENTITIES

from .conftest import load_mbids_table


@pytest.fixture
def mbids() -> list[dict]:
    entities = ENTITIES * 3
    return [dict(mbid=uuid.uuid4(), entity=entity) for entity in entities]


@pytest.mark.parametrize(
    "args, exit_0",
    [
        ([], True),
        (["--before=2020-01-01"], True),
        (["--before=2020-01-01", "--new"], True),
        (["--limit=0"], False),  # limit < 1
    ],
)
def test_cli_date_args(args, exit_0):
    """Test the datetime flags are required together."""
    MusicBrainzAnnotation.create()
    load_mbids_table([])  # empty data, should do nothing
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(annotate_mbids.main, args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def test_drop_dangling_annotations():
    """Test the drop dangling annotations function."""
    # add some annotations with no corresponding mbids
    ts = datetime.datetime.now()
    success = MusicBrainzAnnotation(
        mbid=uuid.uuid4(), entity="recording", payload_json=dict(_success=True), ts_utc=ts
    )
    fail = MusicBrainzAnnotation(
        mbid=uuid.uuid4(), entity="recording", payload_json=dict(_success=False), ts_utc=ts
    )
    old_dangle = MusicBrainzAnnotation(
        mbid=uuid.uuid4(),
        entity="recording",
        payload_json=dict(_success=False),
        ts_utc=ts - datetime.timedelta(days=180),
    )
    new_dangle = MusicBrainzAnnotation(
        mbid=uuid.uuid4(), entity="recording", payload_json=dict(_success=False), ts_utc=ts
    )

    # make the tables
    MusicBrainzAnnotation.create()
    load_mbids_table(
        [dict(mbid=success.mbid, entity=success.entity), dict(mbid=fail.mbid, entity=fail.entity)]
    )

    # no annotations upserted yet, so should drop nothing
    assert annotate_mbids.drop_dangling_annotations() == 0

    # add the annotations
    for i in [success, fail, old_dangle, new_dangle]:
        i.upsert()

    # should drop the dangling annotation
    assert annotate_mbids.drop_dangling_annotations() == 1
    mbids = set([i["mbid"] for i in MusicBrainzAnnotation.select_star()])
    assert old_dangle.mbid not in mbids
    assert success.mbid in mbids
    assert fail.mbid in mbids
    assert new_dangle.mbid in mbids


def test_get_unannotated_mbids__no_data():
    """Test the unannotated getter when the table is empty."""
    MusicBrainzAnnotation.create()
    load_mbids_table([])
    res = annotate_mbids.get_unannotated_mbids()
    assert res == []


def test_get_unannotated_mbids__any_data(mbids: list[dict]):
    """Test the unannotated getter when the table is not empty."""
    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)
    res = annotate_mbids.get_unannotated_mbids()
    assert len(res) == len(mbids)


def test_get_unannotated_mbids__invalid_entity(mbids: list[dict]):
    """Test the unannotated getter when the table has an invalid entity."""
    # change one of the entities to an invalid value
    mbids[0]["entity"] = "invalid"

    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)
    res = annotate_mbids.get_unannotated_mbids()

    # should have all but the invalid entity
    assert len(res) == len(mbids) - 1


def test_get_reannotate_mbids__no_data():
    """Test the reannotated getter when the table is empty."""
    MusicBrainzAnnotation.create()
    load_mbids_table([])
    res = annotate_mbids.get_re_annotate_mbids(before=datetime.datetime.now())
    assert res == []


def test_get_reannotate_mbids__any_data(mbids: list[dict]):
    """Test the reannotated getter when the table is not empty."""
    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)

    ts = datetime.datetime(2022, 1, 1)

    # add some annotations
    for i in mbids:
        MusicBrainzAnnotation(
            mbid=i["mbid"], entity=i["entity"], payload_json=dict(a=1), ts_utc=ts
        ).upsert()

    # all annotations are older than the target before
    res = annotate_mbids.get_re_annotate_mbids(before=datetime.datetime.now())
    assert len(res) == len(mbids)

    # skip if annotations are more recent
    res = annotate_mbids.get_re_annotate_mbids(before=ts - datetime.timedelta(days=1))
    assert len(res) == 0


def test_get_reannotate_mbids__invalid_entity(mbids: list[dict]):
    """Test the reannotated getter when the table has an invalid entity."""
    mbids[0]["entity"] = "invalid"

    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)

    ts = datetime.datetime(2022, 1, 1)

    # add some annotations
    for i in mbids:
        MusicBrainzAnnotation(
            mbid=i["mbid"], entity=i["entity"], payload_json=dict(a=1), ts_utc=ts
        ).upsert()

    # should have all but the invalid entity
    res = annotate_mbids.get_re_annotate_mbids(before=datetime.datetime.now())
    assert len(res) == len(mbids) - 1


def test_cli_main__no_mbids():
    """Test nothing is done if nothing is requested."""
    MusicBrainzAnnotation.create()
    load_mbids_table([])  # empty data, should do nothing
    runner = CliRunner()

    # nothing to do
    result = runner.invoke(annotate_mbids.main)
    assert "Found 0 total mbid(s) to annotate." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0

    # nothing is done if no new mbids are found.
    result = runner.invoke(annotate_mbids.main, ["--new"])
    assert "Found 0 total mbid(s) to annotate." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0

    # nothing is done if no re-annotated mbids are found.
    result = runner.invoke(annotate_mbids.main, ["--before=2023-01-01"])
    assert "Found 0 total mbid(s) to annotate." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0


def test_cli_main__not_table_exists_error(mbids: list[dict]):
    """Test handling of the target table not existing."""
    load_mbids_table(mbids)  # table now exists with data
    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main, ["--new"])
    assert result.exit_code != 0
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


def test_cli_main__unannotated(mbids: list[dict]):
    """Test working with unannotated mbids."""
    # add the mbids to the list but without annotations
    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)

    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main, ["--new"])
    assert f"Found {len(mbids)} unannotated mbid(s)." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert isinstance(res[0]["mbid"], uuid.UUID)  # make sure deserialization works
    assert len(res) == len(mbids)


def test_cli_main__reannotated(mbids: list[dict]):
    """Test working with re-annotated mbids."""
    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)

    # add annotations for before 2021-01-01
    for i in mbids:
        MusicBrainzAnnotation(
            mbid=i["mbid"],
            entity=i["entity"],
            payload_json=dict(a=uuid.uuid1()),
            ts_utc="2020-01-01",
        ).upsert()

    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main, ["--before=2021-01-01"])
    assert f"Found {len(mbids)} mbid(s) to re-annotate." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == len(mbids)


def test_cli_main__limit(mbids: list[dict]):
    """Test limit handler"""
    MusicBrainzAnnotation.create()
    load_mbids_table(mbids)

    limit = len(mbids) // 2
    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main, ["--new", f"--limit={limit}"])
    assert f"Found {len(mbids)} total mbid(s) to annotate." in result.output
    assert f"Limiting to {limit} mbid(s) randomly." in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == limit

    # drop the annotations so i can run it again
    MusicBrainzAnnotation.create(drop=True)

    # limit > mbids
    limit = len(mbids) * 2
    result = runner.invoke(annotate_mbids.main, ["--new", f"--limit={limit}"])
    assert f"Found {len(mbids)} total mbid(s) to annotate." in result.output
    assert f"Limiting to {limit} mbids randomly." not in result.output
    assert result.exit_code == 0

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == len(mbids)
