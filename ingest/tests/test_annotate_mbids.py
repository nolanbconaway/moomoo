"""Test the annotate_mbids module."""

import datetime
import uuid
from collections import deque

import pytest
from click.testing import CliRunner

from moomoo_ingest import annotate_mbids
from moomoo_ingest.annotate_mbids import Mbid
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


def test_Mbid_dataclass():
    """Test the Mbid dataclass."""
    # test from sql rows
    data = [dict(mbid=uuid.uuid4(), entity="recording") for _ in range(5)]
    mbids = Mbid.from_sql_rows(data)
    assert len(mbids) == 5
    assert all(str(i.mbid) == str(j["mbid"]) for i, j in zip(mbids, data))
    assert Mbid.from_sql_rows([]) == []  # no data

    # test to dict
    mbid = Mbid(mbid=uuid.uuid4(), entity="artist")
    assert mbid.to_dict() == dict(mbid=mbid.mbid, entity=mbid.entity)


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
    assert res[0].mbid == mbid
    assert res[0].entity == entity

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


def test_fetch_queue(monkeypatch):
    """Test the fetch from queue function."""
    now = datetime.datetime.now(datetime.timezone.utc)
    runfn = annotate_mbids.fetch_queue

    # nothing to do
    assert runfn(new_=False, updated=False, reannotate_ts=None, limit=10) == deque([])

    # request made but no mbids
    monkeypatch.setattr(annotate_mbids, "get_unannotated_mbids", lambda: [])
    monkeypatch.setattr(annotate_mbids, "get_updated_mbids", lambda: [])
    monkeypatch.setattr(annotate_mbids, "get_very_old_annotations", lambda _: [])
    assert runfn(new_=True, updated=True, reannotate_ts=now, limit=10) == deque([])

    # add some mbids to each category
    monkeypatch.setattr(
        annotate_mbids, "get_unannotated_mbids", lambda: [Mbid(mbid=1, entity="fake")]
    )
    monkeypatch.setattr(annotate_mbids, "get_updated_mbids", lambda: [Mbid(mbid=2, entity="fake")])
    monkeypatch.setattr(
        annotate_mbids, "get_very_old_annotations", lambda _: [Mbid(mbid=3, entity="fake")]
    )
    res = runfn(new_=True, updated=True, reannotate_ts=now, limit=10)
    assert {i.mbid for i in res} == {1, 2, 3}

    # only new mbids
    res = runfn(new_=True, updated=False, reannotate_ts=None, limit=10)
    assert {i.mbid for i in res} == {1}

    # only updated mbids
    res = runfn(new_=False, updated=True, reannotate_ts=None, limit=10)
    assert {i.mbid for i in res} == {2}

    # only reannotated mbids
    res = runfn(new_=False, updated=False, reannotate_ts=now, limit=10)
    assert {i.mbid for i in res} == {3}

    # take first from new, then updated, then reannotated for batch size
    res = runfn(new_=True, updated=True, reannotate_ts=now, limit=1)
    assert {i.mbid for i in res} == {1}
    res = runfn(new_=True, updated=True, reannotate_ts=now, limit=2)
    assert {i.mbid for i in res} == {1, 2}


def test_list_dependents():
    artist_payload = {
        "_args": {"entity": "artist", "mbid": str(uuid.uuid4())},
        "_success": True,
        "data": {"artist": {"release-list": [{"id": str(uuid.uuid4())} for _ in range(3)]}},
    }
    assert annotate_mbids.list_dependents(artist_payload) == [
        Mbid(mbid=uuid.UUID(release["id"]), entity="release")
        for release in artist_payload["data"]["artist"]["release-list"]
    ]

    # no success.
    fail_payload = artist_payload.copy()
    fail_payload["_success"] = False
    assert not annotate_mbids.list_dependents(fail_payload)

    # no data, no release list, or no artist
    nodata_payload = artist_payload.copy()
    del nodata_payload["data"]
    assert not annotate_mbids.list_dependents(nodata_payload)

    noartist_payload = artist_payload.copy()
    noartist_payload["data"] = {}
    assert not annotate_mbids.list_dependents(noartist_payload)

    norelease_payload = artist_payload.copy()
    norelease_payload["data"]["artist"] = {}
    assert not annotate_mbids.list_dependents(norelease_payload)

    # no args.
    noargs_payload = artist_payload.copy()
    del noargs_payload["_args"]
    assert not annotate_mbids.list_dependents(noargs_payload)

    # args but no entity.
    noentity_payload = artist_payload.copy()
    del noentity_payload["_args"]["entity"]
    assert not annotate_mbids.list_dependents(noentity_payload)

    # release entity
    release_payload = {
        "_args": {"entity": "release", "mbid": str(uuid.uuid4())},
        "_success": True,
        "data": {"release": {"release-group": {"id": str(uuid.uuid4())}}},
    }
    assert annotate_mbids.list_dependents(release_payload) == [
        Mbid(
            mbid=uuid.UUID(release_payload["data"]["release"]["release-group"]["id"]),
            entity="release-group",
        )
    ]


def test_filter_dependent_mbids():
    # no dependents, should return empty list
    assert annotate_mbids.filter_dependent_mbids([]) == []

    # make some mbids
    dependents = [
        Mbid(mbid=uuid.uuid4(), entity="recording"),
        Mbid(mbid=uuid.uuid4(), entity="artist"),
    ]

    # sort dependents. the query sorts on mbid, and we need to match that order for comparison.
    dependents.sort(key=lambda x: x.mbid)

    # nothing in the db, should return all
    assert annotate_mbids.filter_dependent_mbids(dependents) == dependents

    # add one to the db and check it is filtered out
    MusicBrainzAnnotation(
        mbid=dependents[0].mbid,
        entity=dependents[0].entity,
        payload_json=dict(a=1),
        ts_utc=datetime.datetime.now(),
    ).upsert()
    res = annotate_mbids.filter_dependent_mbids(dependents)
    assert len(res) == 1
    assert res[0] == dependents[1]


def test_annotate_and_upsert():
    """Test the ingest batch function."""
    # nothing to do
    assert annotate_mbids.annotate_and_upsert(queue=deque([])) == (0, 0)

    n_items = 500
    queue = deque([Mbid(mbid=uuid.uuid4(), entity="recording") for _ in range(n_items)])
    assert annotate_mbids.annotate_and_upsert(queue=queue) == (n_items, 0)

    res = MusicBrainzAnnotation.select_star()
    assert len(res) == n_items
    assert isinstance(res[0]["mbid"], uuid.UUID)  # make sure deserialization works


def test_annotate_and_upsert__timeout(monkeypatch):
    """Test the ingest batch function timeout handling."""

    def mock_annotate_mbid(*_, **__) -> dict:
        raise MusicBrainzTimeoutError()

    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", mock_annotate_mbid)

    n_items = 500
    queue = deque([Mbid(mbid=uuid.uuid4(), entity="recording") for _ in range(n_items)])
    assert annotate_mbids.annotate_and_upsert(queue=queue) == (0, n_items)  # all should timeout

    # should be no annotations in the db
    res = MusicBrainzAnnotation.select_star()
    assert len(res) == 0


def test_annotate_and_upsert__dependents(monkeypatch):
    artist_mbid = uuid.uuid4()
    release_mbid = uuid.uuid4()
    release_group_mbid = uuid.uuid4()
    artist_payload = {
        "_args": {"entity": "artist", "mbid": str(artist_mbid)},
        "_success": True,
        "data": {"artist": {"release-list": [{"id": str(release_mbid)}]}},
    }
    release_payload = {
        "_args": {"entity": "release", "mbid": str(release_mbid)},
        "_success": True,
        "data": {"release": {"release-group": {"id": str(release_group_mbid)}}},
    }
    release_group_payload = {
        "_args": {"entity": "release-group", "mbid": str(release_group_mbid)},
        "_success": True,
        "data": {"release-group": {}},
    }

    # patch the annotate mbid function to return the payloads in order
    payloads = [artist_payload, release_payload, release_group_payload]
    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", lambda *_, **__: payloads.pop(0))

    queue = deque([Mbid(mbid=artist_mbid, entity="artist")])
    annotated, skipped = annotate_mbids.annotate_and_upsert(queue=queue, ingest_dependents=True)
    assert annotated == 3  # artist + release + release-group
    assert skipped == 0

    # run it again. this time there should be only one annotation, as the dependents are already
    # annotated
    payloads = [artist_payload, release_payload, release_group_payload]
    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", lambda *_, **__: payloads.pop(0))
    queue = deque([Mbid(mbid=artist_mbid, entity="artist")])
    annotated, skipped = annotate_mbids.annotate_and_upsert(queue=queue, ingest_dependents=True)
    assert annotated == 1
    assert skipped == 0


def test_cli_main(monkeypatch):
    """Test limit handler"""
    load_mbids_table([])  # start with empty table. needed for --drop handler
    runner = CliRunner()

    # patch the fetcher to return some data
    monkeypatch.setattr(
        annotate_mbids,
        "get_unannotated_mbids",
        lambda: [Mbid(mbid=uuid.uuid4(), entity="recording") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_updated_mbids",
        lambda: [Mbid(mbid=uuid.uuid4(), entity="artist") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_very_old_annotations",
        lambda _: [Mbid(mbid=uuid.uuid4(), entity="release") for _ in range(10)],
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


def test_cli_main__dependents(monkeypatch):
    load_mbids_table([])  # start with empty table. needed for --drop handler
    artist_mbid = uuid.uuid4()
    release_mbid = uuid.uuid4()
    release_group_mbid = uuid.uuid4()
    artist_payload = {
        "_args": {"entity": "artist", "mbid": str(artist_mbid)},
        "_success": True,
        "data": {"artist": {"release-list": [{"id": str(release_mbid)}]}},
    }
    release_payload = {
        "_args": {"entity": "release", "mbid": str(release_mbid)},
        "_success": True,
        "data": {"release": {"release-group": {"id": str(release_group_mbid)}}},
    }
    release_group_payload = {
        "_args": {"entity": "release-group", "mbid": str(release_group_mbid)},
        "_success": True,
        "data": {"release-group": {}},
    }
    payloads = [artist_payload, release_payload, release_group_payload]
    monkeypatch.setattr(annotate_mbids.utils_, "annotate_mbid", lambda *_, **__: payloads.pop(0))

    # patch the queue fetcher to return the artist mbid
    monkeypatch.setattr(
        annotate_mbids, "fetch_queue", lambda **__: deque([Mbid(mbid=artist_mbid, entity="artist")])
    )

    runner = CliRunner()
    result = runner.invoke(annotate_mbids.main, ["--dependents"])

    #  "Annotating 1 total mbid(s)." should appear 3x
    assert result.output.count("Annotating 1 total mbid(s).") == 3

    #  Found 1 dependent mbid(s) to annotate. should appear 2x, for release and release-group
    assert result.output.count("Found 1 dependent mbid(s) to annotate.") == 2

    assert len(MusicBrainzAnnotation.select_star()) == 3
