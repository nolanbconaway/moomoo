import datetime
import io
import json
import re
import tarfile
from typing import Generator
from uuid import uuid1

import pytest
import requests
import requests_mock as requests_mock_lib
from click.testing import CliRunner

from moomoo_ingest import collect_musicbrainz_data_dump as lib
from moomoo_ingest.db import MusicBrainzDataDump, MusicBrainzDataDumpRecord, get_session


@pytest.fixture(autouse=True)
def latest_api_packet_number(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock get_latest_packet_number_from_api to return a fixed number."""
    monkeypatch.setattr(lib, "get_latest_packet_number_from_api", lambda: 1000)


@pytest.fixture(autouse=True)
def create_tables():
    """Create the necessary tables before each test."""
    MusicBrainzDataDump.create()
    MusicBrainzDataDumpRecord.create()


@pytest.fixture
def tarball_bytes() -> Generator[bytes, None, None]:
    # make a fake tarball in memory
    jsonl_data = "\n".join(json.dumps({"id": str(uuid1())}) for _ in range(5))
    tar_bytes_io = io.BytesIO()
    with tarfile.open(fileobj=tar_bytes_io, mode="w:xz") as tar:
        # add TIMESTAMP file
        timestamp_data = b"2025-12-04 23:31:59.823155+00\n"
        timestamp_info = tarfile.TarInfo(name="TIMESTAMP")
        timestamp_info.size = len(timestamp_data)
        tar.addfile(tarinfo=timestamp_info, fileobj=io.BytesIO(timestamp_data))

        # add mbdump/artist file
        jsonl_bytes = jsonl_data.encode("utf-8")
        artist_info = tarfile.TarInfo(name="mbdump/artist")
        artist_info.size = len(jsonl_bytes)
        tar.addfile(tarinfo=artist_info, fileobj=io.BytesIO(jsonl_bytes))

    # ensure tar is closed/flushed before returning the bytes
    tar_bytes = tar_bytes_io.getvalue()
    yield tar_bytes


def test_parse_json_objects() -> None:
    s = '{"a": 1}\n{"b": 2}\n{"c": 3}'
    objs = list(lib.parse_json_objects(s))
    assert objs == [{"a": 1}, {"b": 2}, {"c": 3}]

    s = '{"a": 1}   {"b": 2} \n {"c": 3} '
    objs = list(lib.parse_json_objects(s))
    assert objs == [{"a": 1}, {"b": 2}, {"c": 3}]

    s = '\n\n{"a": 1}\n\n{"b": 2}\n\n{"c": 3}\n\n'
    objs = list(lib.parse_json_objects(s))
    assert objs == [{"a": 1}, {"b": 2}, {"c": 3}]

    # one with a newline in a value
    s = '{"a": "line1\\nline2"}\n{"b": 2}'
    objs = list(lib.parse_json_objects(s))
    assert objs == [{"a": "line1\nline2"}, {"b": 2}]


def test_drop_dumps():
    """Test dropping old dumps."""
    with get_session() as session:
        # works with no data
        lib.drop_dumps(session=session, max_age_days=180)

        dump = MusicBrainzDataDump(
            slug="test-drop-dumps",
            entity="artist",
            packet_number=1,
            dump_timestamp=datetime.datetime.now() - datetime.timedelta(days=365),
        )
        records = [dict(mbid=uuid1(), json_data=dict()) for _ in range(5)]
        session.add(dump)
        session.commit()
        dump.replace_records(session=session, records=records)

    with get_session() as session:
        lib.drop_dumps(session=session, max_age_days=180)
        res = session.get(MusicBrainzDataDump, "test-drop-dumps")
        assert res is None


def test_get_latest_packet_number_from_db():
    """Test getting the latest packet number from the database."""
    with get_session() as session:
        # no data
        latest = lib.get_latest_packet_number_from_db(session=session, entity="artist")
        assert latest is None

        dump1 = MusicBrainzDataDump(
            slug="dump1",
            entity="artist",
            packet_number=5,
            dump_timestamp=datetime.datetime(2023, 1, 1),
        )
        dump2 = MusicBrainzDataDump(
            slug="dump2",
            entity="artist",
            packet_number=10,
            dump_timestamp=datetime.datetime(2023, 6, 1),
        )
        session.add_all([dump1, dump2])
        session.commit()

        latest = lib.get_latest_packet_number_from_db(session=session, entity="artist")
        assert latest == 10


def test_DataDump__download(tarball_bytes: bytes, requests_mock: requests_mock_lib.Mocker) -> None:
    # patch requests to return our tarball bytes in the content
    # use regex to match anything that starts with the API_BASE
    matcher = re.compile(rf"^{re.escape(lib.API_BASE)}.*$")
    requests_mock.get(matcher, content=tarball_bytes)

    dump = lib.DataDump.download(entity="artist", packet_number=1)
    assert dump.entity == "artist"
    assert dump.packet_number == 1
    assert len(dump.records) == 5

    # if 404 error, the server should return text like "Can't find specified JSON dump"
    requests_mock.get(
        matcher,
        status_code=404,
        text="Can't find specified JSON dump",
    )
    dump = lib.DataDump.download(entity="artist", packet_number=9999)
    assert dump is None

    # if the error is something else, then the exception should be raised
    requests_mock.get(
        matcher,
        status_code=500,
        text="Internal Server Error",
    )
    with pytest.raises(requests.exceptions.HTTPError):
        lib.DataDump.download(entity="artist", packet_number=9999)


def test_DataDump__to_db():
    dump = lib.DataDump(
        entity="artist",
        packet_number=1,
        timestamp=datetime.datetime.now(),
        records=[{"id": str(uuid1())} for _ in range(5)],
    )
    with get_session() as session:
        db_dump = dump.to_db(session=session)
        assert db_dump.slug == "1-artist"
        assert db_dump.packet_number == 1
        assert db_dump.entity == "artist"
        assert len(MusicBrainzDataDumpRecord.select_star()) == 5

    # inserting again does not create duplicates
    with get_session() as session:
        db_dump = dump.to_db(session=session)
        assert len(MusicBrainzDataDump.select_star()) == 1
        assert len(MusicBrainzDataDumpRecord.select_star()) == 5


def test_main_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        lib.DataDump,
        "download",
        lambda entity, packet_number: lib.DataDump(
            entity=entity,
            packet_number=packet_number,
            timestamp=datetime.datetime.now(),
            records=[{"id": str(uuid1())} for _ in range(10)],
        ),
    )

    runner = CliRunner()
    result = runner.invoke(lib.main, ["--entity", "artist"])
    assert result.exit_code == 0
    assert "Starting MusicBrainz data dump collection..." in result.output
    assert "Downloading 169 dumps for entity artist." in result.output
    assert "Latest packet number from API: 1000" in result.output
    assert len(MusicBrainzDataDump.select_star()) == 169
    assert len(MusicBrainzDataDumpRecord.select_star()) == 1690  # 10 records per dump

    # check that running again does not add more data (since latest in db is now 1000)
    result = runner.invoke(lib.main, ["--entity", "artist"])
    assert result.exit_code == 0
    assert "No new dumps to download for entity artist" in result.output
