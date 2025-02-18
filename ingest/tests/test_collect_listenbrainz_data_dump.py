import datetime
import io
import json
import tarfile
from typing import Generator
from unittest import mock
from uuid import UUID, uuid1

import pytest
from click.testing import CliRunner

from moomoo_ingest import collect_listenbrainz_data_dump as lib
from moomoo_ingest.db import ListenBrainzDataDump, ListenBrainzDataDumpRecord

from .conftest import RESOURCES

URL = (
    lib.BASE_URL
    + "/listenbrainz-dump-1999-20250213-101618-incremental"
    + "/listenbrainz-listens-dump-1999-20250213-101618-incremental.tar.xz"
)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Auto mock the ListenBrainz._get method."""
    monkeypatch.setattr(lib, "sleep", lambda *_: None)


@pytest.fixture
def tarball() -> Generator[io.BytesIO, None, None]:
    """Access the data in RESOURCES/sample-listenbrainz-listens-dump as an .xz file."""
    src_path = RESOURCES / "sample-listenbrainz-listens-dump"
    # make it into a tarfile
    with io.BytesIO() as f:
        with tarfile.open(fileobj=f, mode="w:xz") as tar:
            # add all files in the directory, with the arcname as the relative path in the src_path
            for file in src_path.glob("**/*"):
                if file.is_file():
                    tar.add(file, arcname=str(file.relative_to(src_path)))
        f.seek(0)
        yield f


def test_Listen__from_line():
    uuid = uuid1()

    line = {"user_id": 1, "track_metadata": {"additional_info": {"lastfm_artist_mbid": uuid.hex}}}
    res = lib.Listen.from_line(json.dumps(line))
    assert len(res) == 1
    assert res[0].user_id == 1
    assert res[0].artist_mbid == uuid

    line = {"user_id": 1, "track_metadata": {"additional_info": {"artist_mbid": uuid.hex}}}
    res = lib.Listen.from_line(json.dumps(line))
    assert len(res) == 1
    assert res[0].user_id == 1
    assert res[0].artist_mbid == uuid

    line = {"user_id": 1, "track_metadata": {"additional_info": {"artist_mbids": [uuid.hex]}}}
    res = lib.Listen.from_line(json.dumps(line))
    assert len(res) == 1
    assert res[0].user_id == 1
    assert res[0].artist_mbid == uuid

    line = {"user_id": 1, "track_metadata": {"additional_info": {}}}
    res = lib.Listen.from_line(json.dumps(line))
    assert not res


def test_DataDump__fetch_list():
    # mock out two requests
    index_html = """
        <html><body>
        <pre><a href="../">../</a>
        <a href="listenbrainz-dump-1999-20250213-101618-incremental/"></a>
        </pre>
        </body></html>
    """
    details_html = """
        <html><body>
        <pre><a href="../">../</a>
        <a href="listenbrainz-listens-dump-1999-20250213-101618-incremental.tar.xz"></a>
        <a href="listenbrainz-listens-dump-1999-20250213-101618-incremental.tar.xz.md5"></a>
        <a href="listenbrainz-listens-dump-1999-20250213-101618-incremental.tar.xz.sha256"></a>
        <a href="listenbrainz-spark-dump-1999-20250213-101618-incremental.tar"></a>
        <a href="listenbrainz-spark-dump-1999-20250213-101618-incremental.tar.md5"></a>
        <a href="listenbrainz-spark-dump-1999-20250213-101618-incremental.tar.sha256"></a>
        </pre>
        </body></html>
    """
    with mock.patch("moomoo_ingest.collect_listenbrainz_data_dump.request_with_retry") as mock_get:
        mock_get.side_effect = [
            mock.Mock(status_code=200, content=index_html.encode("utf-8")),
            mock.Mock(status_code=200, content=details_html.encode("utf-8")),
        ]
        dump = lib.DataDump(URL)
        assert dump.fetch_list() == [lib.DataDump(url=URL)]

    # skipped if date not matched
    with mock.patch("moomoo_ingest.collect_listenbrainz_data_dump.request_with_retry") as mock_get:
        mock_get.side_effect = [
            mock.Mock(status_code=200, content=index_html.encode("utf-8")),
            mock.Mock(status_code=200, content=details_html.encode("utf-8")),
        ]
        assert dump.fetch_list(dates=[datetime.date(2025, 2, 14)]) == []


def test_DataDump__filename():
    dump = lib.DataDump(URL)
    assert dump.filename == "listenbrainz-listens-dump-1999-20250213-101618-incremental.tar.xz"


def test_DataDump__date():
    dump = lib.DataDump(URL)
    assert dump.date == datetime.date(2025, 2, 13)


def test_DataDump__get_start_timestamp(tarball, monkeypatch):
    monkeypatch.setattr(lib.DataDump, "data", tarball)
    dump = lib.DataDump(URL)
    assert dump.get_start_timestamp() == datetime.datetime(
        2025, 2, 13, tzinfo=datetime.timezone.utc
    )


def test_DataDump__get_end_timestamp(tarball, monkeypatch):
    monkeypatch.setattr(lib.DataDump, "data", tarball)
    dump = lib.DataDump(URL)
    assert dump.get_end_timestamp() == datetime.datetime(2025, 2, 14, tzinfo=datetime.timezone.utc)


def test_DataDump__get_listens(tarball, monkeypatch):
    monkeypatch.setattr(lib.DataDump, "data", tarball)
    dump = lib.DataDump(URL)
    listens = dump.get_listens()
    assert listens == [
        lib.Listen(user_id=1, artist_mbid=UUID("00000000-0000-0000-0000-000000000000")),
        lib.Listen(user_id=1, artist_mbid=UUID("00000000-0000-0000-0000-000000000001")),
        lib.Listen(user_id=2, artist_mbid=UUID("00000000-0000-0000-0000-000000000000")),
        lib.Listen(user_id=3, artist_mbid=UUID("00000000-0000-0000-0000-000000000000")),
    ]


def test_main__no_dumps(monkeypatch):
    ListenBrainzDataDump.create()
    monkeypatch.setattr(lib.DataDump, "fetch_list", lambda **_: [])

    runner = CliRunner()
    result = runner.invoke(lib.main)
    assert result.exit_code == 0
    assert "No data dumps to process." in result.output


def test_main(monkeypatch, tarball):
    """Test the pipeline with a ton of mocked data."""
    ListenBrainzDataDump.create()
    ListenBrainzDataDumpRecord.create()

    dump = lib.DataDump(url=URL)
    monkeypatch.setattr(lib.DataDump, "fetch_list", lambda **_: [dump])
    monkeypatch.setattr(lib.DataDump, "data", tarball)

    runner = CliRunner()
    result = runner.invoke(lib.main)
    assert result.exit_code == 0

    # check the db
    res = ListenBrainzDataDump.select_star()
    assert len(res) == 1
    assert res[0]["url"] == URL
    assert len(ListenBrainzDataDumpRecord.select_star()) == 4

    # run it again and should not add more
    result = runner.invoke(lib.main)
    assert result.exit_code == 0
    assert len(ListenBrainzDataDump.select_star()) == 1
    assert len(ListenBrainzDataDumpRecord.select_star()) == 4
