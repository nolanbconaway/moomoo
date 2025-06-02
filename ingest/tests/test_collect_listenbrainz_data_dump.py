import datetime
import io
import json
import tarfile
from pathlib import Path
from typing import Generator
from uuid import UUID, uuid1

import pytest
import zstandard
from click.testing import CliRunner
from pytest_localftpserver.servers import FunctionalityWrapper as PytestFTPServer

from moomoo_ingest import collect_listenbrainz_data_dump as lib
from moomoo_ingest.db import ListenBrainzDataDump, ListenBrainzDataDumpRecord

from .conftest import RESOURCES


@pytest.fixture
def tarball() -> Generator[io.BytesIO, None, None]:
    """Access the data in RESOURCES/sample-listenbrainz-listens-dump as a .tar.zst file."""
    src_path = RESOURCES / "sample-listenbrainz-listens-dump"
    tar_buf = io.BytesIO()
    zst_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tar:
        for file in src_path.glob("**/*"):
            if file.is_file():
                tar.add(file, arcname=str(file.relative_to(src_path)))

        # rewind buffer to the beginning
    tar_buf.seek(0)

    # compress the tarball with zstandard and yield the compressed data
    cctx = zstandard.ZstdCompressor()
    with cctx.stream_writer(zst_buf, closefd=False) as zst_writer:
        zst_writer.write(tar_buf.read())
    zst_buf.seek(0)

    yield zst_buf


@pytest.fixture
def mock_ftp_server(
    ftpserver: PytestFTPServer, monkeypatch: pytest.MonkeyPatch, tarball
) -> PytestFTPServer:
    """Fixture to create a local FTP server for testing.

    Contains the mock tarball of ListenBrainz data dumps.
    """
    # match the FTP_HOST, FTP_PORT and FTP_DIR to be the ftpserver's address and directory
    monkeypatch.setattr(lib, "FTP_HOST", "localhost")
    monkeypatch.setattr(lib, "FTP_PORT", ftpserver.server_port)
    monkeypatch.setattr(lib, "FTP_DIR", "/")
    fpath = f"{ftpserver.anon_root}/listenbrainz-listens-dump-test-incremental.tar.zst"
    with open(fpath, "wb") as f:
        f.write(tarball.getvalue())
    tarball.seek(0)
    return ftpserver


@pytest.fixture
def data_dump(tarball) -> lib.DataDump:
    """Fixture to create a DataDump instance with the tarball data."""
    path = Path(lib.FTP_DIR) / "listenbrainz-listens-dump-20250213-incremental.tar.zst"
    dump = lib.DataDump(
        ftp_path=path, modify_ts=datetime.datetime(2025, 2, 13, tzinfo=datetime.timezone.utc)
    )
    dump._data = tarball
    return dump


def test_Listen__from_line():
    uuid = uuid1()

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


def test_DataDump__fetch_list(mock_ftp_server, tarball):
    # need the mock FTP server to be running, even tho not used here.
    res = lib.DataDump.fetch_list()
    assert len(res) == 1

    # check we can get the data out of the DataDump
    res = res[0]
    assert res.data.getvalue() == tarball.getvalue()


def test_DataDump__get_start_timestamp(data_dump: lib.DataDump):
    assert data_dump.get_start_timestamp() == datetime.datetime(
        2025, 2, 13, tzinfo=datetime.timezone.utc
    )


def test_DataDump__get_end_timestamp(data_dump: lib.DataDump):
    assert data_dump.get_end_timestamp() == datetime.datetime(
        2025, 2, 14, tzinfo=datetime.timezone.utc
    )


def test_DataDump__get_listens(data_dump):
    listens = data_dump.get_listens()
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


def test_main(monkeypatch, data_dump):
    """Test the pipeline with a ton of mocked data."""
    ListenBrainzDataDump.create()
    ListenBrainzDataDumpRecord.create()

    monkeypatch.setattr(lib.DataDump, "fetch_list", lambda **_: [data_dump])

    runner = CliRunner()
    result = runner.invoke(lib.main)
    assert result.exit_code == 0

    # check the db
    res = ListenBrainzDataDump.select_star()
    assert len(res) == 1
    assert res[0]["slug"] == "listenbrainz-listens-dump-20250213-incremental.tar.zst"
    assert len(ListenBrainzDataDumpRecord.select_star()) == 4

    # run it again and should not add more
    result = runner.invoke(lib.main)
    assert result.exit_code == 0
    assert len(ListenBrainzDataDump.select_star()) == 1
    assert len(ListenBrainzDataDumpRecord.select_star()) == 4
