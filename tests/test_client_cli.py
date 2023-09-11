import json
import os
import socket
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner
from xprocess import ProcessStarter

from moomoo import client_cli

from .conftest import load_local_files_table


def get_free_port() -> int:
    """Get a free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def http_server(xprocess, monkeypatch, mock_db):
    """Create a client for the cli to connect to, setting the host envvar correctly."""
    port = get_free_port()
    monkeypatch.setenv("MOOMOO_HOST", f"http://127.0.0.1:{port}")

    class Starter(ProcessStarter):
        pattern = "Starting moomoo http server"
        terminate_on_interrupt = True
        args = ["python", "-m", "moomoo.http.app", f"--port={port}"]
        env = dict(os.environ) | {"POSTGRES_DSN": mock_db, "MOOMOO_DBT_SCHEMA": "test"}

    xprocess.ensure("http_server", Starter)
    yield f"http://127.0.0.1:{port}"
    xprocess.getinfo("http_server").terminate()


@pytest.fixture
def local_files(monkeypatch, tmp_path):
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(tmp_path))
    tmp_path = Path(tmp_path)
    yield tmp_path


@pytest.fixture(autouse=True)
def load_fixed_local_files(local_files):
    """Preload each test with a local files table.

    Has one toplevel file (test.mp3) and 10 files in a subfolder (test/0.mp3,
    test/1.mp3, etc.)
    """
    real_file = dict(
        filepath=f"test.mp3", embedding=str([1, 2] * 5), artist_mbid=uuid.uuid4()
    )
    data = [
        dict(
            filepath=f"test/{i}.mp3", embedding=str([i] * 10), artist_mbid=uuid.uuid4()
        )
        for i in range(10)
    ]
    load_local_files_table(data=[real_file] + data, schema="test")

    # create the files
    (local_files / "test").mkdir()
    for f in [real_file] + data:
        Path(local_files / f["filepath"]).touch(exist_ok=True)


def test_playlist_from_path__media_library_exists_check(monkeypatch, local_files):
    runner = CliRunner()
    result = runner.invoke(client_cli.from_path, [str(local_files / "test.mp3")])
    assert result.exit_code == 0

    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(local_files) + "fakeeee")
    result = runner.invoke(client_cli.from_path, [str(local_files / "test.mp3")])
    assert result.exit_code != 0
    assert str(local_files) + "fakeeee" + " does not exist" in str(result.exception)


def test_playlist_from_path__files_vs_parent_handler(xprocess, local_files):
    """Test that we switch between the two endpoints correctly."""
    logfile = Path(xprocess.getinfo("http_server").logpath)
    runner = CliRunner()

    # should be from parent path because only one path is provided and it is a folder
    result = runner.invoke(client_cli.from_path, [str(local_files / "test")])
    assert result.exit_code == 0
    assert "INFO - from-parent request" in logfile.read_text()
    assert "INFO - from-files request" not in logfile.read_text()

    logfile.write_text("")

    # should be from files because multiple paths are provided
    result = runner.invoke(
        client_cli.from_path,
        [str(local_files / "test.mp3"), str(local_files / "test.mp3")],
    )
    assert result.exit_code == 0
    assert "INFO - from-parent request" not in logfile.read_text()
    assert "INFO - from-files request" in logfile.read_text()

    # should invoke an error because multiple paths are provided but one is a folder
    result = runner.invoke(
        client_cli.from_path, [str(local_files / "test"), str(local_files / "test.mp3")]
    )
    assert result.exit_code != 0
    assert "Multiple paths must be files" in str(result.exception)


def test_playlist_from_path__json_parsable(local_files):
    runner = CliRunner()
    result = runner.invoke(client_cli.from_path, [str(local_files / "test.mp3")])
    assert result.exit_code == 0
    assert len(result.output) > 0
    assert json.loads(result.output)  # good as long as it parses

    result = runner.invoke(
        client_cli.from_path,
        [str(local_files / "test.mp3"), str(local_files / "test.mp3")],
    )
    assert result.exit_code == 0
    assert len(result.output) > 0
    assert json.loads(result.output)  # good as long as it parses


def test_playlist_from_path__error_handling(local_files):
    # make a fake file and pass that. we should have 0 request matches which produces
    # a NoFilesRequestedError.
    runner = CliRunner()

    (local_files / "fake.mp3").touch()

    result = runner.invoke(client_cli.from_path, [str(local_files / "fake.mp3")])
    assert result.exit_code != 0

    # should still be json parsable
    assert len(result.output) > 0
    assert json.loads(result.output)
    assert json.loads(result.output)["success"] is False
    assert "NoFilesRequestedError" in json.loads(result.output)["error"]
