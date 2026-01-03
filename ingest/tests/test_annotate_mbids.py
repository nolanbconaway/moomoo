"""Test the annotate_mbids module."""

import datetime
import uuid

import pytest
from click.testing import CliRunner

from moomoo_ingest import annotate_mbids
from moomoo_ingest.db import MusicBrainzAnnotation, MusicBrainzDataDump, MusicBrainzDataDumpRecord
from moomoo_ingest.utils_ import ENTITIES, MusicBrainzTimeoutError

from .conftest import load_mbids_table


@pytest.fixture
def mbids() -> list[dict]:
    entities = ENTITIES * 3
    return [dict(mbid=uuid.uuid4(), entity=entity) for entity in entities]


@pytest.fixture(autouse=True)
def create_tables():
    """Create and drop the necessary tables for testing."""
    MusicBrainzAnnotation.create()
    MusicBrainzDataDump.create()
    MusicBrainzDataDumpRecord.create()


@pytest.fixture(autouse=True)
def mock_annotate_mbid(monkeypatch):
    """Mock the annotate mbid function to return success."""
    monkeypatch.setattr(
        annotate_mbids.utils_,
        "annotate_mbid",
        lambda *_, **__: dict(_success=True, data={"a": 1}),
    )


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
    load_mbids_table([])  # empty data, should do nothing
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(annotate_mbids.main, args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def test_select_topn_from_multilist_dicts():
    l1 = [{"mbid": 1}, {"mbid": 2}, {"mbid": 3}]
    l2 = [{"mbid": 3}, {"mbid": 4}, {"mbid": 5}]
    l3 = [{"mbid": 5}, {"mbid": 6}, {"mbid": 7}]
    res = annotate_mbids.select_topn_from_multilist_dicts([l1, l2, l3], N=5, identity_key="mbid")
    expected_mbids = [1, 2, 3, 4, 5]
    res_mbids = sorted([i["mbid"] for i in res])
    assert res_mbids == expected_mbids

    # test with inf limit
    res = annotate_mbids.select_topn_from_multilist_dicts(
        [l1, l2, l3], N=float("inf"), identity_key="mbid"
    )
    expected_mbids = [1, 2, 3, 4, 5, 6, 7]
    res_mbids = sorted([i["mbid"] for i in res])
    assert res_mbids == expected_mbids


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
    load_mbids_table([])
    res = annotate_mbids.get_unannotated_mbids()
    assert res == []


def test_get_unannotated_mbids__any_data(mbids: list[dict]):
    """Test the unannotated getter when the table is not empty."""
    load_mbids_table(mbids)
    res = annotate_mbids.get_unannotated_mbids()
    assert len(res) == len(mbids)


def test_get_unannotated_mbids__invalid_entity(mbids: list[dict]):
    """Test the unannotated getter when the table has an invalid entity."""
    # change one of the entities to an invalid value
    mbids[0]["entity"] = "invalid"
    load_mbids_table(mbids)
    res = annotate_mbids.get_unannotated_mbids()
    assert len(res) == len(mbids) - 1  # should have all but the invalid entity


def test_get_very_old_annotations__no_data():
    """Test the very old annotations getter when the table is empty."""
    load_mbids_table([])
    res = annotate_mbids.get_very_old_annotations(before=datetime.datetime.now())
    assert res == []


def test_get_very_old_annotations__any_data(mbids: list[dict]):
    """Test the very old annotations getter when the table is not empty."""
    load_mbids_table(mbids)
    ts = datetime.datetime(2022, 1, 1)

    # add some annotations
    for i in mbids:
        MusicBrainzAnnotation(
            mbid=i["mbid"], entity=i["entity"], payload_json=dict(a=1), ts_utc=ts
        ).upsert()

    # all annotations are older than the target before
    res = annotate_mbids.get_very_old_annotations(before=datetime.datetime.now())
    assert len(res) == len(mbids)

    # skip if annotations are more recent
    res = annotate_mbids.get_very_old_annotations(before=ts - datetime.timedelta(days=1))
    assert len(res) == 0


def test_get_very_old_annotations__invalid_entity(mbids: list[dict]):
    """Test the very old annotations getter when the table has an invalid entity."""
    mbids[0]["entity"] = "invalid"
    load_mbids_table(mbids)
    ts = datetime.datetime(2022, 1, 1)

    # add some annotations
    for i in mbids:
        MusicBrainzAnnotation(
            mbid=i["mbid"], entity=i["entity"], payload_json=dict(a=1), ts_utc=ts
        ).upsert()

    # should have all but the invalid entity
    res = annotate_mbids.get_very_old_annotations(before=datetime.datetime.now())
    assert len(res) == len(mbids) - 1


def test_get_updated_mbids__no_data():
    """Test the updated mbids getter when the table is empty."""
    load_mbids_table([])
    res = annotate_mbids.get_updated_mbids()
    assert res == []


def test_get_updated_mbids__any_data():
    """Test the updated mbids getter when the table is not empty."""
    mbid = uuid.uuid4()
    entity = "recording"

    # add data dump and record for the first mbid
    slug = f"test-slug-{entity}"
    container_mbid = uuid.uuid4()
    MusicBrainzDataDump(
        slug=slug,
        packet_number=1,
        entity=entity,
        dump_timestamp=datetime.datetime.now() - datetime.timedelta(days=1),
    ).insert()
    MusicBrainzDataDumpRecord(
        slug=slug,
        mbid=mbid,
        json_data=dict(containers=[dict(mbid=container_mbid, entity="artist")]),
    ).insert()

    # add an annotation older than the dump
    MusicBrainzAnnotation(
        mbid=mbid,
        entity=entity,
        payload_json=dict(a=1),
        ts_utc=datetime.datetime.now() - datetime.timedelta(days=2),
    ).insert()
    MusicBrainzAnnotation(
        mbid=container_mbid,
        entity="artist",
        payload_json=dict(a=1),
        ts_utc=datetime.datetime.now() - datetime.timedelta(days=2),
    ).insert()

    load_mbids_table([dict(mbid=mbid, entity=entity), dict(mbid=container_mbid, entity="artist")])
    res = annotate_mbids.get_updated_mbids()
    assert len(res) == 2
    assert res[0]["mbid"] == mbid
    assert res[0]["entity"] == entity
    assert res[0]["source"] == "1 update"

    # update the annotation to be more recent than the dump
    MusicBrainzAnnotation(
        mbid=mbid,
        entity=entity,
        payload_json=dict(a=1),
        ts_utc=datetime.datetime.now(),
    ).upsert()
    MusicBrainzAnnotation(
        mbid=container_mbid,
        entity="artist",
        payload_json=dict(a=1),
        ts_utc=datetime.datetime.now(),
    ).upsert()
    res = annotate_mbids.get_updated_mbids()
    assert len(res) == 0


def test_fetch_from_queue(monkeypatch):
    """Test the fetch from queue function."""
    now = datetime.datetime.now(datetime.timezone.utc)
    runfn = annotate_mbids.fetch_from_queue

    # nothing to do
    assert runfn(new_=False, updated=False, reannotate_ts=None, batch_size=10) == []

    # request made but no mbids
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda: [])
    monkeypatch.setattr(annotate_mbids, "get_updated_mbids", lambda: [])
    monkeypatch.setattr(annotate_mbids, "get_very_old_annotations", lambda _: [])
    assert runfn(new_=True, updated=True, reannotate_ts=now, batch_size=10) == []

    # add some mbids to each category
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda: [dict(mbid=1)])
    monkeypatch.setattr(annotate_mbids, "get_updated_mbids", lambda: [dict(mbid=2)])
    monkeypatch.setattr(annotate_mbids, "get_very_old_annotations", lambda _: [dict(mbid=3)])
    res = runfn(new_=True, updated=True, reannotate_ts=now, batch_size=10)
    assert {i["mbid"] for i in res} == {1, 2, 3}

    # only new mbids
    res = runfn(new_=True, updated=False, reannotate_ts=None, batch_size=10)
    assert {i["mbid"] for i in res} == {1}

    # only updated mbids
    res = runfn(new_=False, updated=True, reannotate_ts=None, batch_size=10)
    assert {i["mbid"] for i in res} == {2}

    # only reannotated mbids
    res = runfn(new_=False, updated=False, reannotate_ts=now, batch_size=10)
    assert {i["mbid"] for i in res} == {3}

    # take first from new, then updated, then reannotated for batch size
    res = runfn(new_=True, updated=True, reannotate_ts=now, batch_size=1)
    assert {i["mbid"] for i in res} == {1}
    res = runfn(new_=True, updated=True, reannotate_ts=now, batch_size=2)
    assert {i["mbid"] for i in res} == {1, 2}


def test_ingest_batch(monkeypatch):
    """Test the ingest batch function."""
    # nothing to do
    assert annotate_mbids.ingest_batch(batch=[]) == 0

    batch = [dict(mbid=uuid.uuid4(), entity="recording") for _ in range(500)]
    count = annotate_mbids.ingest_batch(batch=batch)
    assert count == len(batch)

    res = MusicBrainzAnnotation.select_star()
    assert isinstance(res[0]["mbid"], uuid.UUID)  # make sure deserialization works
    assert len(res) == len(batch)

    # timeout handler
    def mock_annotate_mbid(*_, **__) -> dict:
        raise MusicBrainzTimeoutError()

    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", mock_annotate_mbid)
    assert annotate_mbids.ingest_batch(batch=batch) == 0  # all should timeout


def test_cli_main(monkeypatch):
    """Test limit handler"""
    load_mbids_table([])  # start with empty table. needed for --drop handler
    runner = CliRunner()

    # patch the fetcher to return some data
    monkeypatch.setattr(
        annotate_mbids,
        "get_unannotated_mbids",
        lambda: [dict(mbid=uuid.uuid4(), entity="recording") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_updated_mbids",
        lambda: [dict(mbid=uuid.uuid4(), entity="artist") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_very_old_annotations",
        lambda _: [dict(mbid=uuid.uuid4(), entity="release") for _ in range(10)],
    )

    # nothing to do
    result = runner.invoke(annotate_mbids.main, [])
    assert "Nothing to do." in result.output

    # with everything
    result = runner.invoke(annotate_mbids.main, ["--new", "--updated", "--before=2020-01-01"])
    assert "Annotating 30 total mbid(s)." in result.output
    assert len(MusicBrainzAnnotation.select_star()) == 30

    # add a limit
    result = runner.invoke(
        annotate_mbids.main,
        ["--new", "--updated", "--before=2020-01-01", "--limit=5"],
    )
    assert "Annotating 5 total mbid(s)." in result.output

    # limit > mbids
    result = runner.invoke(
        annotate_mbids.main,
        ["--new", "--updated", "--before=2020-01-01", "--limit=100"],
    )
    assert "Annotating 30 total mbid(s)." in result.output

    # timeout
    def mock_annotate_mbid(*_, **__) -> dict:
        raise MusicBrainzTimeoutError()

    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", mock_annotate_mbid)
    result = runner.invoke(
        annotate_mbids.main,
        ["--new", "--updated", "--before=2020-01-01", "--limit=5"],
    )
    assert "Timeout annotating mbid" in result.output
